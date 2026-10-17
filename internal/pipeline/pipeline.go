// Package pipeline buffers records in memory and writes them to a Sink in batches.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/marcus-girolneto/beacon/internal/event"
)

// Errors returned by Enqueue.
var (
	ErrBufferFull = errors.New("pipeline buffer full")
	ErrClosed     = errors.New("pipeline closed")
	ErrTooLarge   = errors.New("more records than buffer capacity")
)

// Sink persists batches of records.
// Write must not retain recs after it returns: the slice is reused.
type Sink interface {
	Write(ctx context.Context, recs []event.Record) error
}

// Observer receives pipeline telemetry. It must be safe for concurrent use.
type Observer interface {
	ObserveFlush(trigger Trigger, size int, elapsed time.Duration, err error)
	ObserveLost(n int)
}

// Trigger is why a batch was flushed.
type Trigger string

// Flush triggers.
const (
	TriggerSize     Trigger = "size"
	TriggerInterval Trigger = "interval"
	TriggerShutdown Trigger = "shutdown"
)

// OverflowMode is what Enqueue does when the buffer is full.
type OverflowMode string

// Overflow modes.
const (
	OverflowReject OverflowMode = "reject"
	OverflowBlock  OverflowMode = "block"
)

// ParseOverflowMode parses s.
func ParseOverflowMode(s string) (OverflowMode, error) {
	switch m := OverflowMode(s); m {
	case OverflowReject, OverflowBlock:
		return m, nil
	default:
		return "", fmt.Errorf("unknown overflow mode %q (want reject or block)", s)
	}
}

// Ticker abstracts time.Ticker for tests.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct{ *time.Ticker }

func (t realTicker) C() <-chan time.Time { return t.Ticker.C }

// Config tunes the pipeline. Zero fields get defaults (see withDefaults).
type Config struct {
	BufferSize    int
	BatchSize     int
	FlushInterval time.Duration
	Workers       int
	Overflow      OverflowMode
	BlockTimeout  time.Duration
	WriteTimeout  time.Duration
	MaxRetries    int // negative disables retries
	RetryBackoff  time.Duration
}

func (c Config) withDefaults() Config {
	setDefault(&c.BufferSize, 10_000)
	setDefault(&c.BatchSize, 500)
	setDefault(&c.FlushInterval, time.Second)
	setDefault(&c.Workers, 2)
	setDefault(&c.Overflow, OverflowReject)
	setDefault(&c.BlockTimeout, 100*time.Millisecond)
	setDefault(&c.WriteTimeout, 10*time.Second)
	setDefault(&c.MaxRetries, 2)
	setDefault(&c.RetryBackoff, 100*time.Millisecond)
	c.MaxRetries = max(c.MaxRetries, 0)
	return c
}

func setDefault[T comparable](v *T, def T) {
	var zero T
	if *v == zero {
		*v = def
	}
}

// Option customizes a Pipeline.
type Option func(*Pipeline)

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option {
	return func(p *Pipeline) { p.log = l }
}

// WithObserver sets the telemetry observer.
func WithObserver(o Observer) Option {
	return func(p *Pipeline) { p.obs = o }
}

// WithTicker sets the ticker factory, for tests.
func WithTicker(newTicker func(time.Duration) Ticker) Option {
	return func(p *Pipeline) { p.newTicker = newTicker }
}

// WithSleep sets the retry sleep func, for tests.
func WithSleep(sleep func(context.Context, time.Duration) error) Option {
	return func(p *Pipeline) { p.sleep = sleep }
}

// Pipeline is a bounded, batching queue in front of a Sink.
type Pipeline struct {
	cfg       Config
	sink      Sink
	log       *slog.Logger
	obs       Observer
	newTicker func(time.Duration) Ticker
	sleep     func(context.Context, time.Duration) error

	// acquired before sending, so sends never block and Enqueue is all-or-nothing
	slots *semaphore.Weighted
	queue chan event.Record

	mu      sync.RWMutex
	closed  bool
	closing context.Context
	abort   context.CancelFunc

	startOnce sync.Once
	wg        sync.WaitGroup
}

// New returns a pipeline that writes to sink.
func New(sink Sink, cfg Config, opts ...Option) *Pipeline {
	cfg = cfg.withDefaults()
	closing, abort := context.WithCancel(context.Background())
	p := &Pipeline{
		cfg:  cfg,
		sink: sink,
		log:  slog.New(slog.DiscardHandler),
		obs:  nopObserver{},
		newTicker: func(d time.Duration) Ticker {
			return realTicker{time.NewTicker(d)}
		},
		sleep:   sleepCtx,
		slots:   semaphore.NewWeighted(int64(cfg.BufferSize)),
		queue:   make(chan event.Record, cfg.BufferSize),
		closing: closing,
		abort:   abort,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Config returns the effective config.
func (p *Pipeline) Config() Config { return p.cfg }

// Buffered returns the number of queued records.
func (p *Pipeline) Buffered() int { return len(p.queue) }

// Start launches the batchers. Safe to call more than once.
func (p *Pipeline) Start() {
	p.startOnce.Do(func() {
		for i := range p.cfg.Workers {
			p.wg.Go(func() { p.runBatcher(i) })
		}
	})
}

// Enqueue queues all of recs or none of them.
// It returns ErrBufferFull, ErrClosed, ErrTooLarge or a context error.
func (p *Pipeline) Enqueue(ctx context.Context, recs ...event.Record) error {
	n := int64(len(recs))
	if n == 0 {
		return nil
	}
	if n > int64(p.cfg.BufferSize) {
		return ErrTooLarge
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	if err := p.reserve(ctx, n); err != nil {
		return err
	}
	for _, r := range recs {
		p.queue <- r
	}
	return nil
}

func (p *Pipeline) reserve(ctx context.Context, n int64) error {
	if p.slots.TryAcquire(n) {
		return nil
	}
	if p.cfg.Overflow == OverflowReject {
		return ErrBufferFull
	}

	waitCtx, cancel := context.WithTimeout(ctx, p.cfg.BlockTimeout)
	defer cancel()
	// don't let blocked producers hold up shutdown
	stop := context.AfterFunc(p.closing, cancel)
	defer stop()

	err := p.slots.Acquire(waitCtx, n)
	switch {
	case err == nil:
		return nil
	case p.closing.Err() != nil:
		return ErrClosed
	case ctx.Err() != nil:
		return fmt.Errorf("enqueue: %w", ctx.Err())
	default:
		return ErrBufferFull
	}
}

// Shutdown stops admission and waits for buffered records to be flushed.
// If ctx ends first, batchers keep draining in the background.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.abort()
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()
	p.Start()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("pipeline shutdown: %w", ctx.Err())
	}
}

func (p *Pipeline) runBatcher(id int) {
	ticker := p.newTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]event.Record, 0, p.cfg.BatchSize)
	flush := func(trigger Trigger) {
		if len(batch) == 0 {
			return
		}
		p.deliver(id, trigger, batch)
		clear(batch)
		batch = batch[:0]
	}

	for {
		select {
		case rec, ok := <-p.queue:
			if !ok {
				flush(TriggerShutdown)
				return
			}
			p.slots.Release(1)
			batch = append(batch, rec)
			if len(batch) >= p.cfg.BatchSize {
				flush(TriggerSize)
			}
		case <-ticker.C():
			flush(TriggerInterval)
		}
	}
}

// deliver uses a background context so a draining pipeline can still flush.
func (p *Pipeline) deliver(worker int, trigger Trigger, batch []event.Record) {
	backoff := p.cfg.RetryBackoff
	var err error
	for attempt := 0; attempt <= p.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			_ = p.sleep(context.Background(), backoff)
			backoff *= 2
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), p.cfg.WriteTimeout)
		err = p.sink.Write(ctx, batch)
		cancel()
		p.obs.ObserveFlush(trigger, len(batch), time.Since(start), err)
		if err == nil {
			p.log.Debug("batch flushed", "worker", worker, "trigger", trigger, "size", len(batch), "attempt", attempt+1)
			return
		}
		p.log.Warn("batch write failed", "worker", worker, "trigger", trigger, "size", len(batch), "attempt", attempt+1, "err", err)
	}
	p.obs.ObserveLost(len(batch))
	p.log.Error("batch dropped after retries", "worker", worker, "size", len(batch), "err", err)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type nopObserver struct{}

func (nopObserver) ObserveFlush(Trigger, int, time.Duration, error) {}
func (nopObserver) ObserveLost(int)                                 {}
