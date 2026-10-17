package app

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcus-girolneto/beacon/internal/config"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testConfig(t *testing.T, args ...string) config.Config {
	t.Helper()
	cfg, err := config.Parse(append([]string{"-flush-interval", "1h"}, args...), func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

type running struct {
	base   string
	cancel context.CancelFunc
	done   chan error
}

func start(t *testing.T, cfg config.Config, stdout io.Writer) *running {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{base: "http://" + ln.Addr().String(), cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- Run(ctx, cfg, ln, stdout, slog.New(slog.DiscardHandler)) }()
	t.Cleanup(cancel)
	return r
}

func (r *running) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Fatalf("Run() = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func request(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b)
}

const event = `{"type":"pageview","anonymous_id":"a1","url":"https://example.com/?utm_campaign=launch"}`

func TestRunStdoutSinkDrainsOnShutdown(t *testing.T) {
	t.Parallel()
	var out syncBuffer
	r := start(t, testConfig(t), &out)

	for range 3 {
		if code, body := request(t, http.MethodPost, r.base+"/v1/track", event); code != http.StatusAccepted {
			t.Fatalf("track = %d: %s", code, body)
		}
	}
	code, body := request(t, http.MethodGet, r.base+"/metrics", "")
	if code != http.StatusOK || !strings.Contains(body, `beacon_events_accepted_total{endpoint="track"} 3`) {
		t.Fatalf("metrics = %d:\n%s", code, body)
	}
	if code, _ := request(t, http.MethodGet, r.base+"/v1/stats?from=2026-01-01&to=2026-02-01", ""); code != http.StatusNotImplemented {
		t.Errorf("stats without postgres = %d, want 501", code)
	}
	if strings.Contains(out.String(), "launch") {
		t.Fatal("events flushed before shutdown despite 1h interval")
	}

	r.stop(t)
	if got := strings.Count(out.String(), `"utm":{"campaign":"launch"}`); got != 3 {
		t.Fatalf("stdout has %d events after drain, want 3:\n%s", got, out.String())
	}
}

func TestRunFileSink(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "events.ndjson")
	r := start(t, testConfig(t, "-sink", "file", "-file-path", path), io.Discard)

	if code, body := request(t, http.MethodPost, r.base+"/v1/batch", `{"batch":[`+event+`,`+event+`]}`); code != http.StatusAccepted {
		t.Fatalf("batch = %d: %s", code, body)
	}
	r.stop(t)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(data, []byte("\n")); got != 2 {
		t.Fatalf("file has %d lines, want 2", got)
	}
}

func TestRunSinkErrors(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t, "-sink", "file", "-file-path", filepath.Join(t.TempDir(), "missing", "x.ndjson"))
	if err := Run(t.Context(), cfg, nil, io.Discard, slog.New(slog.DiscardHandler)); err == nil {
		t.Error("Run() with unopenable file succeeded")
	}

	cfg = testConfig(t)
	cfg.Sink = "kafka"
	if err := Run(t.Context(), cfg, nil, io.Discard, slog.New(slog.DiscardHandler)); err == nil {
		t.Error("Run() with unknown sink succeeded")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	cfg = testConfig(t, "-sink", "postgres", "-postgres-dsn", "host=127.0.0.1 port=1 connect_timeout=1 sslmode=disable")
	if err := Run(ctx, cfg, nil, io.Discard, slog.New(slog.DiscardHandler)); err == nil {
		t.Error("Run() with unreachable postgres succeeded")
	}
}

func TestRunServeError(t *testing.T) {
	t.Parallel()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()
	if err := Run(t.Context(), testConfig(t), ln, io.Discard, slog.New(slog.DiscardHandler)); err == nil {
		t.Error("Run() with closed listener succeeded")
	}
}
