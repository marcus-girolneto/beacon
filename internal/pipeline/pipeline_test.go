package pipeline

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcus-girolneto/beacon/internal/event"
)

// fakeSink records every successful batch. If gate is non-nil each Write
// announces itself on entered and then waits for a value on gate. The first
// failures writes return errFake.
type fakeSink struct {
	batches  chan []event.Record
	entered  chan struct{}
	gate     chan struct{}
	failures atomic.Int32
	openOnce sync.Once
}

// open permanently unblocks every current and future Write.
func (s *fakeSink) open() {
	s.openOnce.Do(func() { close(s.gate) })
}

var errFake = errors.New("sink unavailable")

func newFakeSink() *fakeSink {
	return &fakeSink{batches: make(chan []event.Record, 1000)}
}

func (s *fakeSink) Write(ctx context.Context, recs []event.Record) error {
	if s.gate != nil {
		s.entered <- struct{}{}
		select {
		case <-s.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.failures.Add(-1) >= 0 {
		return errFake
	}
	s.batches <- append([]event.Record(nil), recs...)
	return nil
}

func (s *fakeSink) next(t *testing.T) []event.Record {
	t.Helper()
	select {
	case b := <-s.batches:
		return b
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a batch")
		return nil
	}
}

func (s *fakeSink) assertNoBatch(t *testing.T) {
	t.Helper()
	select {
	case b := <-s.batches:
		t.Fatalf("unexpected batch of %d records", len(b))
	default:
	}
}

// manualTicker is a Ticker whose ticks are sent by the test. The channel is
// unbuffered, so a send completes only once a batcher has received the tick.
type manualTicker struct{ ch chan time.Time }

func (m *manualTicker) C() <-chan time.Time { return m.ch }
func (m *manualTicker) Stop()               {}

func (m *manualTicker) tick() { m.ch <- time.Time{} }

type recordingObserver struct {
	mu       sync.Mutex
	triggers []Trigger
	errs     int
	lost     int
}

func (o *recordingObserver) ObserveFlush(tr Trigger, _ int, _ time.Duration, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.triggers = append(o.triggers, tr)
	if err != nil {
		o.errs++
	}
}

func (o *recordingObserver) ObserveLost(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lost += n
}

func records(n int) []event.Record {
	out := make([]event.Record, n)
	for i := range out {
		out[i] = event.Record{MessageID: fmt.Sprintf("m%d", i)}
	}
	return out
}

func noSleep(context.Context, time.Duration) error { return nil }

type harness struct {
	p      *Pipeline
	sink   *fakeSink
	ticker *manualTicker
	obs    *recordingObserver
}

func newHarness(t *testing.T, cfg Config, sink *fakeSink) *harness {
	t.Helper()
	h := &harness{
		sink:   sink,
		ticker: &manualTicker{ch: make(chan time.Time)},
		obs:    &recordingObserver{},
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = time.Hour // ticks only come from the test
	}
	h.p = New(sink, cfg,
		WithTicker(func(time.Duration) Ticker { return h.ticker }),
		WithObserver(h.obs),
		WithSleep(noSleep),
	)
	h.p.Start()
	t.Cleanup(func() {
		if sink.gate != nil {
			sink.open()
		}
		_ = h.p.Shutdown(context.Background())
	})
	return h
}

// waitDrained spins until the batchers have pulled everything off the queue.
func (h *harness) waitDrained(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for h.p.Buffered() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("queue not drained")
		}
		runtime.Gosched()
	}
}

func TestFlushOnBatchSize(t *testing.T) {
	t.Parallel()
	h := newHarness(t, Config{BatchSize: 3, Workers: 1}, newFakeSink())

	if err := h.p.Enqueue(t.Context(), records(7)...); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if got := len(h.sink.next(t)); got != 3 {
			t.Fatalf("batch size = %d, want 3", got)
		}
	}
	h.waitDrained(t)
	h.sink.assertNoBatch(t)

	h.ticker.tick()
	if got := len(h.sink.next(t)); got != 1 {
		t.Fatalf("remainder batch size = %d, want 1", got)
	}
}

func TestFlushOnInterval(t *testing.T) {
	t.Parallel()
	h := newHarness(t, Config{BatchSize: 100, Workers: 1}, newFakeSink())

	h.ticker.tick() // empty batch: nothing to flush
	h.sink.assertNoBatch(t)

	if err := h.p.Enqueue(t.Context(), records(2)...); err != nil {
		t.Fatal(err)
	}
	h.waitDrained(t)
	h.ticker.tick()

	b := h.sink.next(t)
	if len(b) != 2 || b[0].MessageID != "m0" || b[1].MessageID != "m1" {
		t.Fatalf("batch = %+v, want m0,m1 in order", b)
	}
	if err := h.p.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.obs.triggers[0] != TriggerInterval {
		t.Errorf("trigger = %q, want %q", h.obs.triggers[0], TriggerInterval)
	}
}

func TestShutdownDrainsBuffer(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	p := New(sink, Config{BatchSize: 1000, BufferSize: 5000, Workers: 3, FlushInterval: time.Hour}, WithSleep(noSleep))
	p.Start()

	const total = 2500
	for i := range total / 100 {
		recs := records(100)
		for j := range recs {
			recs[j].MessageID = fmt.Sprintf("m%d", i*100+j)
		}
		if err := p.Enqueue(t.Context(), recs...); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	close(sink.batches)

	seen := make(map[string]bool, total)
	for b := range sink.batches {
		for _, r := range b {
			if seen[r.MessageID] {
				t.Fatalf("record %s delivered twice", r.MessageID)
			}
			seen[r.MessageID] = true
		}
	}
	if len(seen) != total {
		t.Fatalf("delivered %d records, want %d", len(seen), total)
	}

	if err := p.Enqueue(t.Context(), records(1)...); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after Shutdown = %v, want ErrClosed", err)
	}
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatalf("second Shutdown = %v, want nil", err)
	}
}

func TestShutdownWithoutStart(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	p := New(sink, Config{BatchSize: 10, Workers: 1})
	if err := p.Enqueue(t.Context(), records(4)...); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := len(sink.next(t)); got != 4 {
		t.Fatalf("drained %d records, want 4", got)
	}
}

func TestShutdownTimeout(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	sink.gate = make(chan struct{})
	sink.entered = make(chan struct{}, 1)
	h := newHarness(t, Config{BatchSize: 1, Workers: 1}, sink)

	if err := h.p.Enqueue(t.Context(), records(1)...); err != nil {
		t.Fatal(err)
	}
	<-sink.entered

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := h.p.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown = %v, want context.Canceled", err)
	}
}

// blockedHarness returns a pipeline whose only batcher is stuck inside
// Sink.Write, so the queue fills deterministically.
func blockedHarness(t *testing.T, mode OverflowMode) *harness {
	t.Helper()
	sink := newFakeSink()
	sink.gate = make(chan struct{})
	sink.entered = make(chan struct{}, 10)
	h := newHarness(t, Config{BufferSize: 4, BatchSize: 1, Workers: 1, Overflow: mode, BlockTimeout: time.Hour}, sink)

	if err := h.p.Enqueue(t.Context(), records(1)...); err != nil {
		t.Fatal(err)
	}
	<-sink.entered // the batcher holds record 1 and is blocked in Write
	if err := h.p.Enqueue(t.Context(), records(4)...); err != nil {
		t.Fatalf("filling buffer: %v", err)
	}
	return h
}

func TestBackpressureReject(t *testing.T) {
	t.Parallel()
	h := blockedHarness(t, OverflowReject)

	if err := h.p.Enqueue(t.Context(), records(1)...); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("Enqueue on full buffer = %v, want ErrBufferFull", err)
	}
	if got := h.p.Buffered(); got != 4 {
		t.Fatalf("Buffered() = %d, want 4", got)
	}

	h.sink.gate <- struct{}{} // finish write of record 1
	<-h.sink.entered          // batcher dequeued record 2 and blocks again
	if err := h.p.Enqueue(t.Context(), records(1)...); err != nil {
		t.Fatalf("Enqueue after room freed = %v, want nil", err)
	}
	if err := h.p.Enqueue(t.Context(), records(1)...); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("Enqueue on refilled buffer = %v, want ErrBufferFull", err)
	}
}

func TestBackpressureRejectIsAllOrNothing(t *testing.T) {
	t.Parallel()
	h := blockedHarness(t, OverflowReject)
	h.sink.gate <- struct{}{}
	<-h.sink.entered // one slot free

	if err := h.p.Enqueue(t.Context(), records(2)...); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("Enqueue(2) with 1 free slot = %v, want ErrBufferFull", err)
	}
	if got := h.p.Buffered(); got != 3 {
		t.Fatalf("Buffered() = %d, want 3 (no partial admission)", got)
	}
}

func TestBackpressureBlockWaitsForRoom(t *testing.T) {
	t.Parallel()
	h := blockedHarness(t, OverflowBlock)

	result := make(chan error, 1)
	go func() { result <- h.p.Enqueue(context.Background(), records(1)...) }()

	select {
	case err := <-result:
		t.Fatalf("Enqueue returned %v before room was available", err)
	default:
	}
	h.sink.gate <- struct{}{}
	if err := <-result; err != nil {
		t.Fatalf("blocked Enqueue = %v, want nil", err)
	}
}

func TestBackpressureBlockHonorsContext(t *testing.T) {
	t.Parallel()
	h := blockedHarness(t, OverflowBlock)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := h.p.Enqueue(ctx, records(1)...)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Enqueue with canceled ctx = %v, want context.Canceled", err)
	}
}

func TestBackpressureBlockTimeout(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	sink.gate = make(chan struct{})
	sink.entered = make(chan struct{}, 10)
	h := newHarness(t, Config{BufferSize: 1, BatchSize: 1, Workers: 1, Overflow: OverflowBlock, BlockTimeout: time.Millisecond}, sink)
	if err := h.p.Enqueue(t.Context(), records(1)...); err != nil {
		t.Fatal(err)
	}
	<-sink.entered
	if err := h.p.Enqueue(t.Context(), records(1)...); err != nil {
		t.Fatal(err)
	}
	if err := h.p.Enqueue(t.Context(), records(1)...); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("Enqueue after BlockTimeout = %v, want ErrBufferFull", err)
	}
}

func TestShutdownReleasesBlockedProducers(t *testing.T) {
	t.Parallel()
	h := blockedHarness(t, OverflowBlock)

	result := make(chan error, 1)
	go func() { result <- h.p.Enqueue(context.Background(), records(1)...) }()

	shutdown := make(chan error, 1)
	go func() { shutdown <- h.p.Shutdown(context.Background()) }()

	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("blocked Enqueue during shutdown = %v, want ErrClosed", err)
	}
	h.sink.open()
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
}

func TestEnqueueEdgeCases(t *testing.T) {
	t.Parallel()
	h := newHarness(t, Config{BufferSize: 2}, newFakeSink())
	if err := h.p.Enqueue(t.Context()); err != nil {
		t.Errorf("Enqueue() with no records = %v, want nil", err)
	}
	if err := h.p.Enqueue(t.Context(), records(3)...); !errors.Is(err, ErrTooLarge) {
		t.Errorf("Enqueue(3) into buffer of 2 = %v, want ErrTooLarge", err)
	}
}

func TestRetryThenSucceed(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	sink.failures.Store(2)
	h := newHarness(t, Config{BatchSize: 2, Workers: 1, MaxRetries: 2}, sink)

	if err := h.p.Enqueue(t.Context(), records(2)...); err != nil {
		t.Fatal(err)
	}
	if got := len(sink.next(t)); got != 2 {
		t.Fatalf("batch size = %d, want 2", got)
	}
	h.obs.mu.Lock()
	defer h.obs.mu.Unlock()
	if h.obs.errs != 2 || h.obs.lost != 0 {
		t.Errorf("errs=%d lost=%d, want errs=2 lost=0", h.obs.errs, h.obs.lost)
	}
}

func TestRetriesExhausted(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	sink.failures.Store(100)
	p := New(sink, Config{BatchSize: 3, Workers: 1, MaxRetries: -1}, WithSleep(noSleep))
	obs := &recordingObserver{}
	p.obs = obs
	p.Start()
	if err := p.Enqueue(t.Context(), records(3)...); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if obs.errs != 1 || obs.lost != 3 {
		t.Errorf("errs=%d lost=%d, want errs=1 lost=3", obs.errs, obs.lost)
	}
}

func TestConcurrentProducers(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	sink.batches = make(chan []event.Record, 10_000)
	p := New(sink, Config{BufferSize: 64, BatchSize: 16, Workers: 4, Overflow: OverflowBlock, BlockTimeout: time.Minute})
	p.Start()

	const producers, perProducer = 8, 500
	var wg sync.WaitGroup
	for range producers {
		wg.Go(func() {
			for range perProducer {
				if err := p.Enqueue(t.Context(), records(1)...); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	close(sink.batches)
	total := 0
	for b := range sink.batches {
		total += len(b)
	}
	if total != producers*perProducer {
		t.Fatalf("delivered %d records, want %d", total, producers*perProducer)
	}
}

func TestParseOverflowMode(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"reject", "block"} {
		if m, err := ParseOverflowMode(s); err != nil || string(m) != s {
			t.Errorf("ParseOverflowMode(%q) = %q, %v", s, m, err)
		}
	}
	if _, err := ParseOverflowMode("drop"); err == nil {
		t.Error("ParseOverflowMode(drop) succeeded, want error")
	}
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()
	cfg := New(newFakeSink(), Config{}).Config()
	if cfg.BufferSize != 10_000 || cfg.BatchSize != 500 || cfg.Workers != 2 ||
		cfg.Overflow != OverflowReject || cfg.FlushInterval != time.Second || cfg.MaxRetries != 2 {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
}

func TestSleepCtx(t *testing.T) {
	t.Parallel()
	if err := sleepCtx(t.Context(), 0); err != nil {
		t.Errorf("sleepCtx(0) = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := sleepCtx(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Errorf("sleepCtx(canceled) = %v", err)
	}
}

type discardSink struct{}

func (discardSink) Write(context.Context, []event.Record) error { return nil }

func BenchmarkEnqueue(b *testing.B) {
	p := New(discardSink{}, Config{BufferSize: 65_536, BatchSize: 1000, Workers: 2, Overflow: OverflowBlock, BlockTimeout: time.Minute})
	p.Start()
	rec := records(1)
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := p.Enqueue(ctx, rec...); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	if err := p.Shutdown(ctx); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkEnqueueBatch100(b *testing.B) {
	p := New(discardSink{}, Config{BufferSize: 65_536, BatchSize: 1000, Workers: 2, Overflow: OverflowBlock, BlockTimeout: time.Minute})
	p.Start()
	recs := records(100)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if err := p.Enqueue(ctx, recs...); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := p.Shutdown(ctx); err != nil {
		b.Fatal(err)
	}
}
