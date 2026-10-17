package config

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/marcus-girolneto/beacon/internal/pipeline"
)

func env(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func TestDefaults(t *testing.T) {
	t.Parallel()
	c, err := Parse(nil, env(nil), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":8080" || c.Sink != SinkStdout || c.Pipeline.BufferSize != 10_000 ||
		c.Pipeline.BatchSize != 500 || c.Pipeline.Overflow != pipeline.OverflowReject ||
		c.LogLevel != slog.LevelInfo || c.ShutdownTimeout != 30*time.Second {
		t.Errorf("unexpected defaults: %+v", c)
	}
}

func TestPrecedence(t *testing.T) {
	t.Parallel()
	c, err := Parse(
		[]string{"-batch-size", "50", "-overflow", "block"},
		env(map[string]string{
			"BEACON_BATCH_SIZE":     "999",
			"BEACON_FLUSH_INTERVAL": "250ms",
			"BEACON_SINK":           "postgres",
			"BEACON_POSTGRES_DSN":   "host=localhost",
			"BEACON_LOG_LEVEL":      "debug",
		}),
		io.Discard,
	)
	if err != nil {
		t.Fatal(err)
	}
	if c.Pipeline.BatchSize != 50 {
		t.Errorf("BatchSize = %d, want flag value 50", c.Pipeline.BatchSize)
	}
	if c.Pipeline.FlushInterval != 250*time.Millisecond {
		t.Errorf("FlushInterval = %v, want env value", c.Pipeline.FlushInterval)
	}
	if c.Sink != SinkPostgres || c.PostgresDSN != "host=localhost" || c.LogLevel != slog.LevelDebug {
		t.Errorf("env not applied: %+v", c)
	}
	if c.Pipeline.Overflow != pipeline.OverflowBlock {
		t.Errorf("Overflow = %q", c.Pipeline.Overflow)
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{name: "unknown sink", args: []string{"-sink", "kafka"}, want: "unknown sink"},
		{name: "postgres without dsn", args: []string{"-sink", "postgres"}, want: "-postgres-dsn is required"},
		{name: "file without path", args: []string{"-sink", "file", "-file-path", ""}, want: "-file-path is required"},
		{name: "bad overflow", args: []string{"-overflow", "drop"}, want: "unknown overflow mode"},
		{name: "bad log level", args: []string{"-log-level", "loud"}, want: "invalid log level"},
		{name: "bad log format", args: []string{"-log-format", "xml"}, want: "unknown log format"},
		{name: "zero batch", args: []string{"-batch-size", "0"}, want: "-batch-size must be positive"},
		{name: "zero interval", args: []string{"-flush-interval", "0s"}, want: "-flush-interval must be positive"},
		{name: "batch exceeds buffer", args: []string{"-buffer-size", "10", "-max-batch-events", "20"}, want: "must not exceed -buffer-size"},
		{name: "negative hops", args: []string{"-proxy-hops", "-1"}, want: "-proxy-hops"},
		{name: "positional args", args: []string{"extra"}, want: "unexpected arguments"},
		{name: "bad flag value", args: []string{"-workers", "many"}, want: "invalid value"},
		{name: "bad env value", env: map[string]string{"BEACON_WORKERS": "many"}, want: "BEACON_WORKERS"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(tc.args, env(tc.env), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Parse() error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestHelp(t *testing.T) {
	t.Parallel()
	var out strings.Builder
	_, err := Parse([]string{"-h"}, env(nil), &out)
	if !errors.Is(err, ErrHelp) {
		t.Fatalf("Parse(-h) = %v, want ErrHelp", err)
	}
	if !strings.Contains(out.String(), "-overflow") {
		t.Error("usage does not list flags")
	}
}

func TestEnvName(t *testing.T) {
	t.Parallel()
	if got := EnvName("flush-interval"); got != "BEACON_FLUSH_INTERVAL" {
		t.Errorf("EnvName() = %q", got)
	}
}
