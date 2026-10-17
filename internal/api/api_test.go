package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcus-girolneto/beacon/internal/enrich"
	"github.com/marcus-girolneto/beacon/internal/event"
	"github.com/marcus-girolneto/beacon/internal/pipeline"
	"github.com/marcus-girolneto/beacon/internal/stats"
)

var testNow = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

type fakeQueue struct {
	mu   sync.Mutex
	recs []event.Record
	err  error
}

func (q *fakeQueue) Enqueue(_ context.Context, recs ...event.Record) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	q.recs = append(q.recs, recs...)
	return nil
}

func (q *fakeQueue) records() []event.Record {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]event.Record(nil), q.recs...)
}

type fakeRecorder struct {
	mu       sync.Mutex
	accepted map[string]int
	rejected map[string]int
	dropped  map[string]int
	routes   map[string]int
}

func newFakeRecorder() *fakeRecorder {
	return &fakeRecorder{
		accepted: map[string]int{}, rejected: map[string]int{},
		dropped: map[string]int{}, routes: map[string]int{},
	}
}

func (f *fakeRecorder) EventsAccepted(e string, n int) { f.add(f.accepted, e, n) }
func (f *fakeRecorder) EventsRejected(r string, n int) { f.add(f.rejected, r, n) }
func (f *fakeRecorder) EventsDropped(r string, n int)  { f.add(f.dropped, r, n) }

func (f *fakeRecorder) ObserveRequest(route string, code int, _ time.Duration) {
	f.add(f.routes, fmt.Sprintf("%s %d", route, code), 1)
}

func (f *fakeRecorder) add(m map[string]int, k string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m[k] += n
}

func (f *fakeRecorder) get(m map[string]int, k string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return m[k]
}

type fakeStats struct {
	got   stats.Query
	res   stats.Result
	err   error
	block bool
}

func (f *fakeStats) Stats(ctx context.Context, q stats.Query) (stats.Result, error) {
	f.got = q
	if f.block {
		<-ctx.Done()
		return stats.Result{}, fmt.Errorf("query stats: %w", ctx.Err())
	}
	return f.res, f.err
}

type testEnv struct {
	srv     *Server
	handler http.Handler
	queue   *fakeQueue
	rec     *fakeRecorder
	stats   *fakeStats
}

func newEnv(t *testing.T, cfg Config) *testEnv {
	t.Helper()
	cfg.Now = func() time.Time { return testNow }
	env := &testEnv{queue: &fakeQueue{}, rec: newFakeRecorder(), stats: &fakeStats{}}
	env.srv = New(cfg, Deps{
		Queue:    env.queue,
		Enricher: enrich.New(enrich.NewVisitorHasher(nil)),
		Stats:    env.stats,
		Recorder: env.rec,
	})
	env.handler = env.srv.Handler()
	return env
}

func (env *testEnv) do(t *testing.T, method, target, contentType, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6)")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Add(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	return rec
}

const validJSON = `{"type":"pageview","anonymous_id":"anon-1","url":"https://example.com/pricing?utm_source=google&utm_medium=cpc","referrer":"https://www.google.com/"}`

func TestTrack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
		wantInBody  string
		wantReason  string
	}{
		{name: "valid json", contentType: "application/json", body: validJSON, wantStatus: http.StatusAccepted, wantInBody: `"accepted":1`},
		{name: "valid with charset", contentType: "application/json; charset=utf-8", body: validJSON, wantStatus: http.StatusAccepted},
		{name: "text/plain from sendBeacon", contentType: "text/plain;charset=UTF-8", body: validJSON, wantStatus: http.StatusAccepted},
		{name: "missing content type", body: validJSON, wantStatus: http.StatusAccepted},
		{name: "form content type", contentType: "application/x-www-form-urlencoded", body: validJSON, wantStatus: http.StatusUnsupportedMediaType, wantReason: reasonContentType},
		{name: "bad content type", contentType: "%%%", body: validJSON, wantStatus: http.StatusUnsupportedMediaType, wantReason: reasonContentType},
		{name: "empty body", contentType: "application/json", body: "", wantStatus: http.StatusBadRequest, wantInBody: "empty body", wantReason: reasonMalformed},
		{name: "syntax error", contentType: "application/json", body: `{"type":`, wantStatus: http.StatusBadRequest, wantInBody: "unexpected end", wantReason: reasonMalformed},
		{name: "invalid token", contentType: "application/json", body: `{"type":x}`, wantStatus: http.StatusBadRequest, wantInBody: "syntax error", wantReason: reasonMalformed},
		{name: "wrong field type", contentType: "application/json", body: `{"type":1}`, wantStatus: http.StatusBadRequest, wantInBody: `field \"type\"`, wantReason: reasonMalformed},
		{name: "unknown field", contentType: "application/json", body: `{"type":"pageview","userId":"u"}`, wantStatus: http.StatusBadRequest, wantInBody: "unknown field", wantReason: reasonMalformed},
		{name: "trailing data", contentType: "application/json", body: validJSON + `{}`, wantStatus: http.StatusBadRequest, wantInBody: "after JSON value", wantReason: reasonMalformed},
		{name: "trailing whitespace ok", contentType: "application/json", body: validJSON + "\n  ", wantStatus: http.StatusAccepted},
		{name: "validation errors listed", contentType: "application/json", body: `{"type":"pageview"}`, wantStatus: http.StatusBadRequest, wantInBody: `"field":"anonymous_id"`, wantReason: reasonInvalid},
		{
			name: "body too large", contentType: "application/json",
			body:       `{"type":"pageview","anonymous_id":"a","url":"https://x.io","properties":{"k":"` + strings.Repeat("x", 70<<10) + `"}}`,
			wantStatus: http.StatusRequestEntityTooLarge, wantInBody: "must not exceed", wantReason: reasonTooLarge,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnv(t, Config{})
			res := env.do(t, http.MethodPost, "/v1/track", tc.contentType, tc.body)

			if res.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", res.Code, tc.wantStatus, res.Body)
			}
			if !strings.Contains(res.Body.String(), tc.wantInBody) {
				t.Errorf("body %s does not contain %q", res.Body, tc.wantInBody)
			}
			if ct := res.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q", ct)
			}
			if tc.wantReason != "" && env.rec.get(env.rec.rejected, tc.wantReason) != 1 {
				t.Errorf("rejected[%s] = %d, want 1", tc.wantReason, env.rec.get(env.rec.rejected, tc.wantReason))
			}
			wantQueued := 0
			if tc.wantStatus == http.StatusAccepted {
				wantQueued = 1
			}
			if got := len(env.queue.records()); got != wantQueued {
				t.Errorf("queued %d records, want %d", got, wantQueued)
			}
		})
	}
}

func TestTrackEnrichesRecord(t *testing.T) {
	t.Parallel()
	env := newEnv(t, Config{})
	res := env.do(t, http.MethodPost, "/v1/track", "application/json", validJSON)
	if res.Code != http.StatusAccepted {
		t.Fatalf("status = %d: %s", res.Code, res.Body)
	}
	recs := env.queue.records()
	r := recs[0]
	if r.Channel != event.ChannelPaid || r.UTM.Source != "google" || r.Device != event.DeviceDesktop {
		t.Errorf("record not enriched: %+v", r)
	}
	if !r.Timestamp.Equal(testNow) || r.Path != "/pricing" || r.VisitorHash == "" {
		t.Errorf("unexpected record: %+v", r)
	}
	if env.rec.get(env.rec.accepted, endpointTrack) != 1 {
		t.Error("accepted metric not recorded")
	}
	if env.rec.get(env.rec.routes, "POST /v1/track 202") != 1 {
		t.Errorf("request metric not recorded: %v", env.rec.routes)
	}
	if res.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("missing CORS header")
	}
}

func TestBackpressureResponses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantStatus int
		retryAfter string
		dropReason string
		wantInBody string
	}{
		{name: "buffer full", err: pipeline.ErrBufferFull, wantStatus: http.StatusServiceUnavailable, retryAfter: "2", dropReason: "buffer_full", wantInBody: "buffer is full"},
		{name: "shutting down", err: pipeline.ErrClosed, wantStatus: http.StatusServiceUnavailable, retryAfter: "2", dropReason: "shutting_down", wantInBody: "shutting down"},
		{name: "too large", err: pipeline.ErrTooLarge, wantStatus: http.StatusRequestEntityTooLarge, wantInBody: "capacity"},
		{name: "client gone", err: fmt.Errorf("enqueue: %w", context.Canceled), wantStatus: 499, dropReason: "canceled", wantInBody: "canceled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnv(t, Config{RetryAfter: 2 * time.Second})
			env.queue.err = tc.err

			res := env.do(t, http.MethodPost, "/v1/track", "application/json", validJSON)
			if res.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", res.Code, tc.wantStatus)
			}
			if got := res.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
			if !strings.Contains(res.Body.String(), tc.wantInBody) {
				t.Errorf("body %s does not contain %q", res.Body, tc.wantInBody)
			}
			if tc.dropReason != "" && env.rec.get(env.rec.dropped, tc.dropReason) != 1 {
				t.Errorf("dropped[%s] not recorded", tc.dropReason)
			}
		})
	}
}

func batchBody(events ...string) string {
	return `{"batch":[` + strings.Join(events, ",") + `]}`
}

func TestBatch(t *testing.T) {
	t.Parallel()
	invalid := `{"type":"pageview","anonymous_id":"a"}`

	t.Run("all valid", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{})
		res := env.do(t, http.MethodPost, "/v1/batch", "application/json", batchBody(validJSON, validJSON, validJSON))
		if res.Code != http.StatusAccepted {
			t.Fatalf("status = %d: %s", res.Code, res.Body)
		}
		if len(env.queue.records()) != 3 || env.rec.get(env.rec.accepted, endpointBatch) != 3 {
			t.Errorf("queued %d records", len(env.queue.records()))
		}
	})

	t.Run("partial", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{})
		res := env.do(t, http.MethodPost, "/v1/batch", "application/json", batchBody(validJSON, invalid, validJSON))
		if res.Code != http.StatusAccepted {
			t.Fatalf("status = %d: %s", res.Code, res.Body)
		}
		var body acceptedResponse
		if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Accepted != 2 || len(body.Rejected) != 1 || body.Rejected[0].Index != 1 || body.Rejected[0].Fields[0].Field != "url" {
			t.Errorf("response = %+v", body)
		}
		if env.rec.get(env.rec.rejected, reasonInvalid) != 1 {
			t.Error("rejection not recorded")
		}
	})

	t.Run("all invalid", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{})
		res := env.do(t, http.MethodPost, "/v1/batch", "application/json", batchBody(invalid, invalid))
		if res.Code != http.StatusBadRequest {
			t.Fatalf("status = %d: %s", res.Code, res.Body)
		}
		if len(env.queue.records()) != 0 {
			t.Error("records queued")
		}
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{})
		for _, body := range []string{`{"batch":[]}`, `{}`} {
			if res := env.do(t, http.MethodPost, "/v1/batch", "application/json", body); res.Code != http.StatusBadRequest {
				t.Errorf("%s: status = %d", body, res.Code)
			}
		}
	})

	t.Run("too many events", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{MaxBatchEvents: 2})
		res := env.do(t, http.MethodPost, "/v1/batch", "application/json", batchBody(validJSON, validJSON, validJSON))
		if res.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d", res.Code)
		}
	})

	t.Run("body too large", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{MaxBatchBytes: 100})
		res := env.do(t, http.MethodPost, "/v1/batch", "application/json", batchBody(validJSON))
		if res.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d", res.Code)
		}
	})

	t.Run("full buffer rejects whole batch", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{})
		env.queue.err = pipeline.ErrBufferFull
		res := env.do(t, http.MethodPost, "/v1/batch", "application/json", batchBody(validJSON, validJSON))
		if res.Code != http.StatusServiceUnavailable || res.Header().Get("Retry-After") != "1" {
			t.Fatalf("status = %d, Retry-After = %q", res.Code, res.Header().Get("Retry-After"))
		}
		if env.rec.get(env.rec.dropped, "buffer_full") != 2 {
			t.Error("dropped count wrong")
		}
	})
}

func TestPixel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     string
		headers    []string
		queueErr   error
		wantStatus int
		wantQueued bool
	}{
		{
			name:       "explicit url",
			target:     "/v1/pixel.gif?aid=a1&url=https%3A%2F%2Fexample.com%2Fblog%3Futm_campaign%3Dfall&utm_source=newsletter&utm_medium=email",
			wantStatus: http.StatusOK, wantQueued: true,
		},
		{
			name:       "url from referer header",
			target:     "/v1/pixel.gif?aid=a1&t=click",
			headers:    []string{"Referer", "https://example.com/landing"},
			wantStatus: http.StatusOK, wantQueued: true,
		},
		{name: "missing aid", target: "/v1/pixel.gif?url=https%3A%2F%2Fexample.com%2F", wantStatus: http.StatusBadRequest},
		{name: "bad type", target: "/v1/pixel.gif?aid=a&t=nope&url=https%3A%2F%2Fexample.com%2F", wantStatus: http.StatusBadRequest},
		{
			name: "buffer full", target: "/v1/pixel.gif?aid=a&url=https%3A%2F%2Fexample.com%2F",
			queueErr: pipeline.ErrBufferFull, wantStatus: http.StatusServiceUnavailable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnv(t, Config{})
			env.queue.err = tc.queueErr
			res := env.do(t, http.MethodGet, tc.target, "", "", tc.headers...)

			if res.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", res.Code, tc.wantStatus)
			}
			if ct := res.Header().Get("Content-Type"); ct != "image/gif" {
				t.Errorf("Content-Type = %q, want image/gif", ct)
			}
			if !bytes.Equal(res.Body.Bytes(), transparentGIF) {
				t.Error("body is not the transparent GIF")
			}
			if cc := res.Header().Get("Cache-Control"); !strings.Contains(cc, "no-store") {
				t.Errorf("Cache-Control = %q", cc)
			}
			if got := len(env.queue.records()) == 1; got != tc.wantQueued {
				t.Errorf("queued = %v, want %v", got, tc.wantQueued)
			}
		})
	}

	t.Run("enrichment", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{})
		env.do(t, http.MethodGet, tests[0].target, "", "")
		r := env.queue.records()[0]
		if r.Type != event.TypePageview || r.Channel != event.ChannelEmail || r.UTM.Campaign != "fall" || r.UTM.Source != "newsletter" {
			t.Errorf("record = %+v", r)
		}
	})
}

func TestStatsEndpoint(t *testing.T) {
	t.Parallel()

	t.Run("passes parsed query", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{})
		env.stats.res = stats.Result{GroupBy: stats.ByChannel, Rows: []stats.Row{{Key: "search", Events: 3}}}
		res := env.do(t, http.MethodGet, "/v1/stats?from=2026-09-01&to=2026-10-01T00:00:00Z&group_by=channel&host=example.com&limit=5", "", "")
		if res.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", res.Code, res.Body)
		}
		want := stats.Query{
			From:    time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
			To:      time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
			GroupBy: stats.ByChannel, Host: "example.com", Limit: 5,
		}
		if env.stats.got != want {
			t.Errorf("query = %+v, want %+v", env.stats.got, want)
		}
		if !strings.Contains(res.Body.String(), `"key":"search"`) {
			t.Errorf("body = %s", res.Body)
		}
	})

	t.Run("defaults to day", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{})
		env.do(t, http.MethodGet, "/v1/stats?from=2026-09-01&to=2026-10-01", "", "")
		if env.stats.got.GroupBy != stats.ByDay || env.stats.got.Limit != stats.DefaultLimit {
			t.Errorf("query = %+v", env.stats.got)
		}
	})

	badRequests := map[string]string{
		"missing from":   "/v1/stats?to=2026-10-01",
		"missing to":     "/v1/stats?from=2026-10-01",
		"bad from":       "/v1/stats?from=yesterday&to=2026-10-01",
		"bad to":         "/v1/stats?from=2026-09-01&to=10/01/2026",
		"bad group":      "/v1/stats?from=2026-09-01&to=2026-10-01&group_by=country",
		"bad limit":      "/v1/stats?from=2026-09-01&to=2026-10-01&limit=ten",
		"limit too high": "/v1/stats?from=2026-09-01&to=2026-10-01&limit=100000",
		"reversed range": "/v1/stats?from=2026-10-01&to=2026-09-01",
	}
	for name, target := range badRequests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newEnv(t, Config{})
			if res := env.do(t, http.MethodGet, target, "", ""); res.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400: %s", res.Code, res.Body)
			}
		})
	}

	t.Run("backend errors", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{})
		env.stats.err = errors.New("connection refused")
		if res := env.do(t, http.MethodGet, "/v1/stats?from=2026-09-01&to=2026-10-01", "", ""); res.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want 500", res.Code)
		}
		env.stats.err = fmt.Errorf("%w: nope", stats.ErrInvalidQuery)
		if res := env.do(t, http.MethodGet, "/v1/stats?from=2026-09-01&to=2026-10-01", "", ""); res.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", res.Code)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()
		env := newEnv(t, Config{StatsTimeout: time.Millisecond})
		env.stats.block = true
		if res := env.do(t, http.MethodGet, "/v1/stats?from=2026-09-01&to=2026-10-01", "", ""); res.Code != http.StatusGatewayTimeout {
			t.Errorf("status = %d, want 504", res.Code)
		}
	})

	t.Run("no stats backend", func(t *testing.T) {
		t.Parallel()
		srv := New(Config{}, Deps{Queue: &fakeQueue{}, Enricher: enrich.New(enrich.NewVisitorHasher(nil))})
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/stats?from=2026-09-01&to=2026-10-01", nil))
		if rec.Code != http.StatusNotImplemented {
			t.Errorf("status = %d, want 501", rec.Code)
		}
	})
}

func TestProbesAndRouting(t *testing.T) {
	t.Parallel()
	env := newEnv(t, Config{})

	if res := env.do(t, http.MethodGet, "/healthz", "", ""); res.Code != http.StatusOK {
		t.Errorf("healthz = %d", res.Code)
	}
	if res := env.do(t, http.MethodGet, "/readyz", "", ""); res.Code != http.StatusOK {
		t.Errorf("readyz = %d", res.Code)
	}
	env.srv.SetDraining()
	if res := env.do(t, http.MethodGet, "/readyz", "", ""); res.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz while draining = %d", res.Code)
	}
	if res := env.do(t, http.MethodGet, "/v1/track", "", ""); res.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /v1/track = %d, want 405", res.Code)
	}
	res := env.do(t, http.MethodOptions, "/v1/track", "", "")
	if res.Code != http.StatusNoContent || !strings.Contains(res.Header().Get("Access-Control-Allow-Methods"), "POST") {
		t.Errorf("preflight = %d %v", res.Code, res.Header())
	}
}

func TestClientIP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		hops   int
		remote string
		xff    []string
		want   string
	}{
		{name: "peer address", remote: "203.0.113.9:5555", want: "203.0.113.9"},
		{name: "ignores xff without trusted proxy", remote: "203.0.113.9:5555", xff: []string{"192.0.2.66"}, want: "203.0.113.9"},
		{name: "one hop", hops: 1, remote: "10.0.0.2:80", xff: []string{"192.0.2.66, 198.51.100.4"}, want: "198.51.100.4"},
		{name: "two hops", hops: 2, remote: "10.0.0.2:80", xff: []string{"192.0.2.66, 198.51.100.4", "10.0.0.1"}, want: "198.51.100.4"},
		{name: "more hops than entries", hops: 3, remote: "10.0.0.2:80", xff: []string{"198.51.100.4"}, want: "198.51.100.4"},
		{name: "trusted but absent", hops: 1, remote: "10.0.0.2:80", want: "10.0.0.2"},
		{name: "unparseable remote", remote: "pipe", want: "pipe"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := New(Config{ProxyHops: tc.hops}, Deps{})
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			req.RemoteAddr = tc.remote
			for _, v := range tc.xff {
				req.Header.Add("X-Forwarded-For", v)
			}
			if got := srv.clientIP(req); got != tc.want {
				t.Errorf("clientIP() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEndToEndWithPipeline(t *testing.T) {
	t.Parallel()
	sink := &collectSink{}
	p := pipeline.New(sink, pipeline.Config{BatchSize: 10, FlushInterval: time.Hour})
	p.Start()

	srv := New(Config{}, Deps{Queue: p, Enricher: enrich.New(enrich.NewVisitorHasher(nil))})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for range 3 {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/batch", strings.NewReader(batchBody(validJSON, validJSON)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
		if res.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d", res.StatusCode)
		}
	}
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := sink.count(); got != 6 {
		t.Fatalf("sink received %d records, want 6", got)
	}
}

type collectSink struct {
	mu sync.Mutex
	n  int
}

func (s *collectSink) Write(_ context.Context, recs []event.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n += len(recs)
	return nil
}

func (s *collectSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}
