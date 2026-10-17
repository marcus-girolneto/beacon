package metrics

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/marcus-girolneto/beacon/internal/pipeline"
)

var _ pipeline.Observer = (*Metrics)(nil)

func TestCounters(t *testing.T) {
	t.Parallel()
	m := New(func() int { return 7 }, 100)

	m.EventsAccepted("batch", 5)
	m.EventsAccepted("track", 1)
	m.EventsRejected("invalid", 2)
	m.EventsDropped("buffer_full", 3)
	m.ObserveFlush(pipeline.TriggerSize, 10, time.Millisecond, nil)
	m.ObserveFlush(pipeline.TriggerInterval, 4, time.Millisecond, errors.New("down"))
	m.ObserveLost(4)
	m.ObserveRequest("POST /v1/track", 202, time.Millisecond)

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"accepted batch", testutil.ToFloat64(m.accepted.WithLabelValues("batch")), 5},
		{"rejected invalid", testutil.ToFloat64(m.rejected.WithLabelValues("invalid")), 2},
		{"dropped", testutil.ToFloat64(m.dropped.WithLabelValues("buffer_full")), 3},
		{"written counts only successful flushes", testutil.ToFloat64(m.written), 10},
		{"lost", testutil.ToFloat64(m.lost), 4},
		{"flush errors", testutil.ToFloat64(m.flushes.WithLabelValues("interval", "error")), 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestHandlerExposesGauges(t *testing.T) {
	t.Parallel()
	m := New(func() int { return 42 }, 1000)
	m.ObserveRequest("GET /healthz", 200, time.Millisecond)

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)

	for _, want := range []string{
		"beacon_buffer_occupancy 42",
		"beacon_buffer_capacity 1000",
		`beacon_http_request_duration_seconds_count{code="200",route="GET /healthz"} 1`,
		"go_goroutines",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
	if m.Registry() == nil {
		t.Error("Registry() is nil")
	}
}
