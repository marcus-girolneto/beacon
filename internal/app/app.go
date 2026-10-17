// Package app wires beacon's components together and runs the server.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/marcus-girolneto/beacon/internal/api"
	"github.com/marcus-girolneto/beacon/internal/config"
	"github.com/marcus-girolneto/beacon/internal/enrich"
	"github.com/marcus-girolneto/beacon/internal/metrics"
	"github.com/marcus-girolneto/beacon/internal/pipeline"
	"github.com/marcus-girolneto/beacon/internal/postgres"
	"github.com/marcus-girolneto/beacon/internal/sink"
)

// Run serves on ln until ctx is canceled, then drains the pipeline and closes the sink.
func Run(ctx context.Context, cfg config.Config, ln net.Listener, stdout io.Writer, log *slog.Logger) error {
	out, err := openSink(ctx, cfg, stdout)
	if err != nil {
		return err
	}
	defer func() {
		if err := out.close(); err != nil {
			log.Error("close sink", "err", err)
		}
	}()

	var p *pipeline.Pipeline
	m := metrics.New(func() int { return p.Buffered() }, cfg.Pipeline.BufferSize)
	p = pipeline.New(out.sink, cfg.Pipeline,
		pipeline.WithLogger(log.With("component", "pipeline")),
		pipeline.WithObserver(m),
	)

	srv := api.New(api.Config{
		MaxEventBytes:  cfg.MaxEventBytes,
		MaxBatchBytes:  cfg.MaxBatchBytes,
		MaxBatchEvents: cfg.MaxBatchEvents,
		RetryAfter:     cfg.RetryAfter,
		StatsTimeout:   cfg.StatsTimeout,
		ProxyHops:      cfg.ProxyHops,
	}, api.Deps{
		Queue:    p,
		Enricher: enrich.New(enrich.NewVisitorHasher(nil)),
		Stats:    out.stats,
		Recorder: m,
		Logger:   log.With("component", "api"),
	})

	mux := http.NewServeMux()
	mux.Handle("/", srv.Handler())
	mux.Handle("GET /metrics", m.Handler())

	httpSrv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		// leave stats room to answer 504 itself
		WriteTimeout:   max(15*time.Second, cfg.StatsTimeout+5*time.Second),
		IdleTimeout:    90 * time.Second,
		MaxHeaderBytes: 16 << 10,
		ErrorLog:       slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	p.Start()
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(ln) }()

	pc := p.Config()
	log.Info("beacon started",
		"addr", ln.Addr().String(), "sink", cfg.Sink,
		"buffer_size", pc.BufferSize, "batch_size", pc.BatchSize,
		"flush_interval", pc.FlushInterval, "workers", pc.Workers, "overflow", pc.Overflow)

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown requested, draining")
	case err := <-serveErr:
		runErr = fmt.Errorf("http server: %w", err)
	}

	// ctx is already canceled here
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()

	srv.SetDraining()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("http shutdown: %w", err))
	}
	start := time.Now()
	buffered := p.Buffered()
	if err := p.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, err)
	}
	log.Info("pipeline drained", "buffered_at_shutdown", buffered, "elapsed", time.Since(start))
	return runErr
}

type sinkHandle struct {
	sink  pipeline.Sink
	stats api.StatsQuerier
	close func() error
}

func openSink(ctx context.Context, cfg config.Config, stdout io.Writer) (sinkHandle, error) {
	switch cfg.Sink {
	case config.SinkStdout:
		return sinkHandle{sink: sink.NewNDJSON(stdout), close: func() error { return nil }}, nil
	case config.SinkFile:
		f, err := sink.OpenFile(cfg.FilePath)
		if err != nil {
			return sinkHandle{}, err
		}
		return sinkHandle{sink: f, close: f.Close}, nil
	case config.SinkPostgres:
		connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		store, err := postgres.Open(connectCtx, cfg.PostgresDSN)
		if err != nil {
			return sinkHandle{}, err
		}
		if err := store.Migrate(connectCtx); err != nil {
			store.Close()
			return sinkHandle{}, err
		}
		return sinkHandle{sink: store, stats: store, close: func() error { store.Close(); return nil }}, nil
	default:
		return sinkHandle{}, fmt.Errorf("unknown sink %q", cfg.Sink)
	}
}
