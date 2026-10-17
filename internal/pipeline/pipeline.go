// Package pipeline buffers enriched records in memory and delivers them to a
// Sink in batches.
//
// Producers (HTTP handlers) call Enqueue, which admits records into a bounded
// queue. A fixed pool of batcher goroutines drains the queue and flushes a
// batch when it reaches BatchSize or when FlushInterval elapses, whichever
// comes first. When the queue is full the pipeline either rejects new records
// immediately (OverflowReject) or makes producers wait up to BlockTimeout
// (OverflowBlock). Shutdown stops admission, drains the queue and flushes
// every remaining record before returning.
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
	// ErrBufferFull means the queue had no room for the records. Callers
	// should shed load, e.g. respond 503 with Retry-After.
	ErrBufferFull = errors.New("pipeline buffer full")
	// ErrClosed means the pipeline is shutting down and no longer admits
	// records.
	ErrClosed = errors.New("pipeline closed")
	// ErrTooLarge means a single Enqueue call carried more records than the
	// buffer can ever hold.
	ErrTooLarge = errors.New("more records than buffer capacity")
)

// Sink persists batches of records. Write must not retain recs after it
// returns; the pipeline reuses the slice for the next batch.
type Sink interface {
	Write(ctx context.Context, recs []event.Record) error
}

// Observer receives pipeline telemetry. Implementations must be safe for
// concurrent use.
type Observer interface {
	// ObserveFlush is called after every attempt to deliver a batch.
	ObserveFlush(trigger Trigger, size int, elapsed time.Duration, err error)
	// ObserveLost is called with the number of records discarded after a
	// batch exhausted its retries.
	ObserveLost(n int)
}

// Trigger names the reason a batch was flushed.
type Trigger string

// Flush triggers.
const (
	TriggerSize     Trigger = "size"
	TriggerInterval Trigger = "interval"
	TriggerShutdown Trigger = "shutdown"
)

// OverflowMode selects what Enqueue does when the buffer is full.
type OverflowMode string

// Overflow modes.
const (
	// OverflowReject fails fast with ErrBufferFull. It keeps request latency
	// flat and pushes the retry decision to the client.
	OverflowReject OverflowMode = "reject"
	// OverflowBlock waits up to Config.BlockTimeout for room. It absorbs short
	// bursts at the cost of holding connections open.
	OverflowBlock OverflowMode = "block"
)

// ParseOverflowMode converts s into an OverflowMode.
func ParseOverflowMode(s string) (OverflowMode, error) {
	switch m := OverflowMode(s); m {
	case OverflowReject, OverflowBlock:
		return m, nil
	default:
		return "", fmt.Errorf("unknown overflow mode %q (want reject or block)", s)
	}
}

// Ticker abstracts time.Ticker so tests can drive interval flushes.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct{ *time.Ticker }

func (t realTicker) C() <-chan time.Time { return t.Ticker.C }

// Config tunes the pipeline. Zero fields take the defaults listed below.
type Config struct {
	// BufferSize is the maximum number of records queued between producers
	// and batchers. Default 10000.
	BufferSize int
	// BatchSize is the number of records that triggers a flush. Default 500.
	BatchSize int
	// FlushInterval bounds how long a record waits in a partial batch.
	// Default 1s.
	FlushInterval time.Duration
	// Workers is the number of batcher goroutines. Default 2.
	Workers int
	// Overflow selects reject or block behavior on a full buffer. Default
	// OverflowReject.
	Overflow OverflowMode
	// BlockTimeout bounds how long Enqueue waits in OverflowBlock mode.
	// Default 100ms.
	BlockTimeout time.Duration
	// WriteTimeout bounds each Sink.Write call. Default 10s.
	WriteTimeout time.Duration
	// MaxRetries is the number of extra attempts after a failed write.
	// Default 2. Negative disables retries.
	MaxRetries int
	// RetryBackoff is the delay before the first retry; it doubles on each
	// further attempt. Default 100ms.
	RetryBackoff time.Duration
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

// WithLogger sets the logger. The default discards output.
func WithLogger(l *slog.Logger) Option {
	return func(p *Pipeline) { p.log = l }
}

// WithObserver sets the telemetry observer.
func WithObserver(o Observer) Option {
	return func(p *Pipeline) { p.obs = o }
}

// WithTicker replaces the interval ticker factory, for tests.
func WithTicker(newTicker func(time.Duration) Ticker) Option {
	return func(p *Pipeline) { p.newTicker = newTicker }
}

// WithSleep replaces the function used to wait between retries, for tests.
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

	// slots counts free queue capacity. Acquiring before sending guarantees
	// the channel send never blocks, and lets a multi-record Enqueue be
	// admitted all-or-nothing.
	slots *semaphore.Weighted
	queue chan event.Record

	// mu guards closed and the queue against a send after close. Producers
	// hold the read lock for the duration of an Enqueue.
	mu      sync.RWMutex
	closed  bool
	closing context.Context
	abort   context.CancelFunc

	startOnce sync.Once
	wg        sync.WaitGroup
}

// New returns a pipeline that delivers to sink. Call Start to launch the
// batchers and Shutdown to drain them.
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

// Config returns the effective configuration, with defaults applied.
func (p *Pipeline) Config() Config { return p.cfg }

// Buffered returns the number of records waiting in the queue.
func (p *Pipeline) Buffered() int { return len(p.queue) }

// Start launches the batcher goroutines. It is safe to call more than once.
func (p *Pipeline) Start() {
	p.startOnce.Do(func() {
		for i := range p.cfg.Workers {
			p.wg.Go(func() { p.runBatcher(i) })
		}
	})
}

// Enqueue admits recs into the buffer as a unit: either every record is
// queued or none is. It returns ErrBufferFull when there is no room (after
// waiting up to BlockTimeout in OverflowBlock mode), ErrClosed after
// Shutdown has begun, ErrTooLarge if recs can never fit, or the context
// error if ctx ends first.
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
	// Shutdown must not wait for producers parked on a full buffer.
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

// Shutdown stops admitting records, waits for the batchers to flush every
// buffered record and returns. If ctx ends first it returns the context
// error; batchers keep draining in the background but the caller should
// assume buffered records may be lost.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.abort()
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()
	p.Start() // ensure someone drains even if Start was never called

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
		clear(batch) // drop references so flushed records can be collected
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

// deliver writes batch to the sink, retrying with exponential backoff. The
// write context is detached from shutdown so that a draining pipeline can
// still flush; each attempt is bounded by WriteTimeout instead.
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
