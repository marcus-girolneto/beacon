package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marcus-girolneto/beacon/internal/enrich"
	"github.com/marcus-girolneto/beacon/internal/event"
	"github.com/marcus-girolneto/beacon/internal/pipeline"
)

type discardSink struct{}

func (discardSink) Write(context.Context, []event.Record) error { return nil }

// benchServer wires the handler to a real pipeline with a no-op sink, so the
// benchmark covers decode, validation, enrichment and buffer admission.
func benchServer(b *testing.B) http.Handler {
	b.Helper()
	p := pipeline.New(discardSink{}, pipeline.Config{
		BufferSize: 1 << 16, BatchSize: 1000, Workers: 2,
		Overflow: pipeline.OverflowBlock, BlockTimeout: time.Minute,
	})
	p.Start()
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return New(Config{}, Deps{Queue: p, Enricher: enrich.New(enrich.NewVisitorHasher(nil))}).Handler()
}

func benchRequest(b *testing.B, h http.Handler, target, body string) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequestWithContext(b.Context(), http.MethodPost, target, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("User-Agent", "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) Mobile/15E148")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusAccepted {
				b.Errorf("status = %d: %s", rec.Code, rec.Body)
				return
			}
		}
	})
}

func BenchmarkTrackHandler(b *testing.B) {
	benchRequest(b, benchServer(b), "/v1/track", validJSON)
}

func BenchmarkBatchHandler50(b *testing.B) {
	events := make([]string, 50)
	for i := range events {
		events[i] = validJSON
	}
	benchRequest(b, benchServer(b), "/v1/batch", batchBody(events...))
}
