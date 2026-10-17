package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"
)

// Config controls a load test run.
type Config struct {
	// Target is the base URL of the beacon instance.
	Target string
	// Duration is how long to generate traffic.
	Duration time.Duration
	// Concurrency is the number of parallel clients.
	Concurrency int
	// BatchSize is the number of events per request. 1 uses /v1/track;
	// larger values use /v1/batch.
	BatchSize int
	// Rate caps the total request rate per second. Zero means unlimited.
	Rate float64
	// Seed makes the traffic mix reproducible.
	Seed uint64
	// Host is the simulated website.
	Host string
	// Visitors is the size of the simulated visitor pool.
	Visitors int
}

// Report summarizes a run.
type Report struct {
	Elapsed   time.Duration
	Requests  int64
	Events    int64 // events in requests answered 202
	Errors    int64 // transport errors
	Status    map[int]int64
	latencies []time.Duration
}

// EventsPerSecond is the accepted event throughput.
func (r Report) EventsPerSecond() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Events) / r.Elapsed.Seconds()
}

// RequestsPerSecond is the completed request throughput.
func (r Report) RequestsPerSecond() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Requests) / r.Elapsed.Seconds()
}

// Percentile returns the q-th latency percentile (0 < q <= 100).
func (r Report) Percentile(q float64) time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	idx := int(float64(len(r.latencies))*q/100+0.5) - 1
	idx = min(max(idx, 0), len(r.latencies)-1)
	return r.latencies[idx]
}

// WriteTo prints a human-readable summary.
func (r Report) WriteTo(w io.Writer) (int64, error) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "duration:     %s\n", r.Elapsed.Round(time.Millisecond))
	fmt.Fprintf(&b, "requests:     %d (%.0f req/s)\n", r.Requests, r.RequestsPerSecond())
	fmt.Fprintf(&b, "events:       %d accepted (%.0f events/s)\n", r.Events, r.EventsPerSecond())
	fmt.Fprintf(&b, "errors:       %d\n", r.Errors)
	codes := make([]int, 0, len(r.Status))
	for c := range r.Status {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	for _, c := range codes {
		fmt.Fprintf(&b, "status %d:   %d\n", c, r.Status[c])
	}
	fmt.Fprintf(&b, "latency:      p50=%s p90=%s p99=%s max=%s\n",
		r.Percentile(50), r.Percentile(90), r.Percentile(99), r.Percentile(100))
	return b.WriteTo(w)
}

type workerResult struct {
	requests, events, errors int64
	status                   map[int]int64
	latencies                []time.Duration
}

// Run generates traffic until cfg.Duration elapses or ctx is canceled.
func Run(ctx context.Context, client *http.Client, cfg Config) (Report, error) {
	if cfg.Concurrency < 1 || cfg.BatchSize < 1 || cfg.Duration <= 0 {
		return Report{}, errors.New("concurrency, batch size and duration must be positive")
	}
	endpoint := cfg.Target + "/v1/track"
	if cfg.BatchSize > 1 {
		endpoint = cfg.Target + "/v1/batch"
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	var interval time.Duration
	if cfg.Rate > 0 {
		interval = time.Duration(float64(time.Second) * float64(cfg.Concurrency) / cfg.Rate)
	}

	results := make([]workerResult, cfg.Concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range cfg.Concurrency {
		wg.Go(func() {
			gen := NewGenerator(cfg.Seed+uint64(i), cfg.Host, cfg.Visitors) //nolint:gosec // i is a small non-negative index
			results[i] = worker(ctx, client, endpoint, gen, cfg.BatchSize, interval)
		})
	}
	wg.Wait()

	rep := Report{Elapsed: time.Since(start), Status: map[int]int64{}}
	for _, r := range results {
		rep.Requests += r.requests
		rep.Events += r.events
		rep.Errors += r.errors
		for c, n := range r.status {
			rep.Status[c] += n
		}
		rep.latencies = append(rep.latencies, r.latencies...)
	}
	slices.Sort(rep.latencies)
	return rep, nil
}

func worker(ctx context.Context, client *http.Client, endpoint string, gen *Generator, batch int, interval time.Duration) workerResult {
	res := workerResult{status: map[int]int64{}}
	var tick <-chan time.Time
	if interval > 0 {
		t := time.NewTicker(interval)
		defer t.Stop()
		tick = t.C
	}

	var body bytes.Buffer
	events := make([]Event, batch)
	for {
		if tick != nil {
			select {
			case <-tick:
			case <-ctx.Done():
				return res
			}
		}
		if ctx.Err() != nil {
			return res
		}

		now := time.Now()
		var visit Visit
		for i := range events {
			visit = gen.Next(now)
			events[i] = visit.Event
		}
		body.Reset()
		var err error
		if batch > 1 {
			err = json.NewEncoder(&body).Encode(map[string][]Event{"batch": events})
		} else {
			err = json.NewEncoder(&body).Encode(events[0])
		}
		if err != nil {
			res.errors++
			continue
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body.Bytes()))
		if err != nil {
			res.errors++
			return res
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", visit.UserAgent)
		req.Header.Set("X-Forwarded-For", visit.IP)

		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() == nil {
				res.errors++
			}
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		res.latencies = append(res.latencies, time.Since(start))
		res.requests++
		res.status[resp.StatusCode]++
		if resp.StatusCode == http.StatusAccepted {
			res.events += int64(batch)
		}
	}
}
