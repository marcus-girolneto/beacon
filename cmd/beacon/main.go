// Command beacon is a self-hostable analytics event collector.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/marcus-girolneto/beacon/internal/app"
	"github.com/marcus-girolneto/beacon/internal/config"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	cfg, err := config.Parse(args, getenv, stderr)
	if errors.Is(err, config.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, "beacon:", err)
		return 2
	}
	if cfg.ShowVersion {
		fmt.Fprintln(stdout, "beacon", version)
		return 0
	}

	// Logs go to stderr so they never mix with NDJSON on stdout.
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	var handler slog.Handler = slog.NewJSONHandler(stderr, opts)
	if cfg.LogFormat == "text" {
		handler = slog.NewTextHandler(stderr, opts)
	}
	log := slog.New(handler).With("version", version)

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		log.Error("listen", "addr", cfg.Addr, "err", err)
		return 1
	}
	if err := app.Run(ctx, cfg, ln, stdout, log); err != nil {
		log.Error("beacon exited with error", "err", err)
		return 1
	}
	return 0
}
