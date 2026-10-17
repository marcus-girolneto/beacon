// Package config parses beacon's command-line flags, with every flag also
// settable through a BEACON_* environment variable.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/marcus-girolneto/beacon/internal/pipeline"
)

// Sink kinds.
const (
	SinkStdout   = "stdout"
	SinkFile     = "file"
	SinkPostgres = "postgres"
)

// Config is the complete runtime configuration.
type Config struct {
	Addr            string
	Sink            string
	FilePath        string
	PostgresDSN     string
	Pipeline        pipeline.Config
	MaxBatchEvents  int
	MaxEventBytes   int64
	MaxBatchBytes   int64
	RetryAfter      time.Duration
	ProxyHops       int
	ShutdownTimeout time.Duration
	LogLevel        slog.Level
	LogFormat       string
	ShowVersion     bool
}

// ErrHelp is returned when -h or -help was requested.
var ErrHelp = flag.ErrHelp

// Parse builds a Config from args (without the program name) and the
// environment accessor getenv. Flags take precedence over environment
// variables, which take precedence over defaults. Usage and errors are
// written to output.
func Parse(args []string, getenv func(string) string, output io.Writer) (Config, error) {
	var (
		c        Config
		overflow string
		logLevel string
	)
	fs := flag.NewFlagSet("beacon", flag.ContinueOnError)
	fs.SetOutput(output)

	fs.StringVar(&c.Addr, "addr", ":8080", "HTTP listen address")
	fs.StringVar(&c.Sink, "sink", SinkStdout, "event sink: stdout, file or postgres")
	fs.StringVar(&c.FilePath, "file-path", "events.ndjson", "output path for the file sink")
	fs.StringVar(&c.PostgresDSN, "postgres-dsn", "", "PostgreSQL connection string for the postgres sink")
	fs.IntVar(&c.Pipeline.BufferSize, "buffer-size", 10_000, "maximum events queued in memory")
	fs.IntVar(&c.Pipeline.BatchSize, "batch-size", 500, "events per sink write")
	fs.DurationVar(&c.Pipeline.FlushInterval, "flush-interval", time.Second, "maximum time an event waits in a partial batch")
	fs.IntVar(&c.Pipeline.Workers, "workers", 2, "number of batcher goroutines")
	fs.StringVar(&overflow, "overflow", string(pipeline.OverflowReject), "behavior when the buffer is full: reject (503 immediately) or block (wait up to -block-timeout)")
	fs.DurationVar(&c.Pipeline.BlockTimeout, "block-timeout", 100*time.Millisecond, "maximum wait for buffer space in block mode")
	fs.DurationVar(&c.Pipeline.WriteTimeout, "write-timeout", 10*time.Second, "timeout for each sink write")
	fs.IntVar(&c.Pipeline.MaxRetries, "max-retries", 2, "retries for a failed sink write (negative disables)")
	fs.IntVar(&c.MaxBatchEvents, "max-batch-events", 500, "maximum events per /v1/batch request")
	fs.Int64Var(&c.MaxEventBytes, "max-event-bytes", 64<<10, "maximum body size of /v1/track")
	fs.Int64Var(&c.MaxBatchBytes, "max-batch-bytes", 1<<20, "maximum body size of /v1/batch")
	fs.DurationVar(&c.RetryAfter, "retry-after", time.Second, "Retry-After advertised on 503 responses")
	fs.IntVar(&c.ProxyHops, "proxy-hops", 0, "trusted reverse proxies in front of beacon (0 ignores X-Forwarded-For)")
	fs.DurationVar(&c.ShutdownTimeout, "shutdown-timeout", 30*time.Second, "maximum time to drain on shutdown")
	fs.StringVar(&logLevel, "log-level", "info", "log level: debug, info, warn or error")
	fs.StringVar(&c.LogFormat, "log-format", "json", "log format: json or text")
	fs.BoolVar(&c.ShowVersion, "version", false, "print version and exit")

	var envErr error
	fs.VisitAll(func(f *flag.Flag) {
		name := EnvName(f.Name)
		if v := getenv(name); v != "" {
			if err := f.Value.Set(v); err != nil {
				envErr = errors.Join(envErr, fmt.Errorf("%s: %w", name, err))
			}
		}
	})
	if envErr != nil {
		return Config{}, envErr
	}
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}

	var err error
	if c.Pipeline.Overflow, err = pipeline.ParseOverflowMode(overflow); err != nil {
		return Config{}, err
	}
	if err := c.LogLevel.UnmarshalText([]byte(logLevel)); err != nil {
		return Config{}, fmt.Errorf("invalid log level %q", logLevel)
	}
	return c, c.validate()
}

func (c Config) validate() error {
	var errs []error
	switch c.Sink {
	case SinkStdout, SinkFile:
	case SinkPostgres:
		if c.PostgresDSN == "" {
			errs = append(errs, errors.New("-postgres-dsn is required for the postgres sink"))
		}
	default:
		errs = append(errs, fmt.Errorf("unknown sink %q (want stdout, file or postgres)", c.Sink))
	}
	if c.Sink == SinkFile && c.FilePath == "" {
		errs = append(errs, errors.New("-file-path is required for the file sink"))
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		errs = append(errs, fmt.Errorf("unknown log format %q", c.LogFormat))
	}
	for _, p := range [...]struct {
		name string
		val  int64
	}{
		{"buffer-size", int64(c.Pipeline.BufferSize)},
		{"batch-size", int64(c.Pipeline.BatchSize)},
		{"workers", int64(c.Pipeline.Workers)},
		{"max-batch-events", int64(c.MaxBatchEvents)},
		{"max-event-bytes", c.MaxEventBytes},
		{"max-batch-bytes", c.MaxBatchBytes},
		{"flush-interval", int64(c.Pipeline.FlushInterval)},
		{"block-timeout", int64(c.Pipeline.BlockTimeout)},
		{"write-timeout", int64(c.Pipeline.WriteTimeout)},
		{"retry-after", int64(c.RetryAfter)},
		{"shutdown-timeout", int64(c.ShutdownTimeout)},
	} {
		if p.val <= 0 {
			errs = append(errs, fmt.Errorf("-%s must be positive", p.name))
		}
	}
	if c.MaxBatchEvents > c.Pipeline.BufferSize {
		// A batch is admitted all-or-nothing, so it must fit in the buffer.
		errs = append(errs, errors.New("-max-batch-events must not exceed -buffer-size"))
	}
	if c.ProxyHops < 0 {
		errs = append(errs, errors.New("-proxy-hops must not be negative"))
	}
	return errors.Join(errs...)
}

// EnvName returns the environment variable that sets flag name, e.g.
// "flush-interval" becomes "BEACON_FLUSH_INTERVAL".
func EnvName(flagName string) string {
	return "BEACON_" + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}
