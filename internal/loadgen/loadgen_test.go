package loadgen

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcus-girolneto/beacon/internal/enrich"
	"github.com/marcus-girolneto/beacon/internal/event"
)

var now = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func TestGeneratorIsDeterministic(t *testing.T) {
	t.Parallel()
	a, b := NewGenerator(42, "example.com", 100), NewGenerator(42, "example.com", 100)
	for range 100 {
		va, vb := a.Next(now), b.Next(now)
		ja, _ := json.Marshal(va)
		jb, _ := json.Marshal(vb)
		if string(ja) != string(jb) {
			t.Fatalf("same seed diverged:\n%s\n%s", ja, jb)
		}
	}
}

// TestGeneratedEventsAreValid round-trips generated events through the real
// validator and enricher, and checks the traffic mix covers every channel
// and device class.
func TestGeneratedEventsAreValid(t *testing.T) {
	t.Parallel()
	gen := NewGenerator(1, "shop.example.com", 500)
	v := event.NewValidator(func() time.Time { return now })
	en := enrich.New(enrich.NewVisitorHasher(nil))

	channels := map[event.Channel]int{}
	devices := map[event.Device]int{}
	for range 5000 {
		visit := gen.Next(now)
		raw, err := json.Marshal(visit.Event)
		if err != nil {
			t.Fatal(err)
		}
		var e event.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatal(err)
		}
		if err := v.Validate(&e); err != nil {
			t.Fatalf("generated invalid event %s: %v", raw, err)
		}
		rec, err := en.Enrich(&e, enrich.RequestMeta{IP: visit.IP, UserAgent: visit.UserAgent, ReceivedAt: now})
		if err != nil {
			t.Fatal(err)
		}
		channels[rec.Channel]++
		devices[rec.Device]++
	}
	for _, c := range []event.Channel{event.ChannelDirect, event.ChannelSearch, event.ChannelSocial, event.ChannelEmail, event.ChannelPaid, event.ChannelReferral} {
		if channels[c] == 0 {
			t.Errorf("no %s traffic generated: %v", c, channels)
		}
	}
	for _, d := range []event.Device{event.DeviceDesktop, event.DeviceMobile, event.DeviceTablet, event.DeviceBot} {
		if devices[d] == 0 {
			t.Errorf("no %s traffic generated: %v", d, devices)
		}
	}
}

func TestRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		batch     int
		wantPath  string
		rate      float64
		status    int
		wantEvent bool
	}{
		{name: "track", batch: 1, wantPath: "/v1/track", status: http.StatusAccepted, wantEvent: true},
		{name: "batch", batch: 10, wantPath: "/v1/batch", status: http.StatusAccepted, wantEvent: true},
		{name: "rate limited", batch: 1, wantPath: "/v1/track", rate: 200, status: http.StatusAccepted, wantEvent: true},
		{name: "backpressure", batch: 1, wantPath: "/v1/track", status: http.StatusServiceUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var hits atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.wantPath || r.Header.Get("X-Forwarded-For") == "" || r.UserAgent() == "" {
					t.Errorf("unexpected request %s %v", r.URL.Path, r.Header)
				}
				hits.Add(1)
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			rep, err := Run(t.Context(), srv.Client(), Config{
				Target: srv.URL, Duration: 150 * time.Millisecond, Concurrency: 4,
				BatchSize: tc.batch, Rate: tc.rate, Seed: 1, Host: "example.com", Visitors: 10,
			})
			if err != nil {
				t.Fatal(err)
			}
			// Requests in flight when the run ends reach the server but are
			// canceled client-side, so the server may see up to one extra
			// request per worker.
			if seen := hits.Load(); rep.Requests == 0 || seen < rep.Requests || seen > rep.Requests+4 {
				t.Fatalf("requests = %d, server saw %d", rep.Requests, seen)
			}
			if tc.wantEvent != (rep.Events == rep.Requests*int64(tc.batch)) {
				t.Errorf("events = %d for %d requests", rep.Events, rep.Requests)
			}
			if tc.rate > 0 && rep.Requests > 60 {
				t.Errorf("rate limit ignored: %d requests in 150ms at 200 req/s", rep.Requests)
			}
			if rep.Percentile(99) <= 0 || rep.Percentile(50) > rep.Percentile(100) {
				t.Errorf("bad percentiles p50=%s p99=%s max=%s", rep.Percentile(50), rep.Percentile(99), rep.Percentile(100))
			}
			var out strings.Builder
			if _, err := rep.WriteTo(&out); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "events/s") {
				t.Errorf("report = %s", out.String())
			}
		})
	}
}

func TestRunErrors(t *testing.T) {
	t.Parallel()
	if _, err := Run(t.Context(), http.DefaultClient, Config{}); err == nil {
		t.Error("Run() with zero config succeeded")
	}

	rep, err := Run(t.Context(), http.DefaultClient, Config{
		Target: "http://127.0.0.1:1", Duration: 50 * time.Millisecond, Concurrency: 1, BatchSize: 1, Visitors: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Errors == 0 {
		t.Error("connection errors not counted")
	}
}

func TestReportEmpty(t *testing.T) {
	t.Parallel()
	var r Report
	if r.EventsPerSecond() != 0 || r.RequestsPerSecond() != 0 || r.Percentile(99) != 0 {
		t.Error("empty report should be all zeros")
	}
}
