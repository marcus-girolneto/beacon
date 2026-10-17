// Command loadgen sends synthetic traffic to beacon and reports throughput and latency.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marcus-girolneto/beacon/internal/loadgen"
)

// set via -ldflags "-X main.version=..."
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg loadgen.Config
	fs.StringVar(&cfg.Target, "target", "http://localhost:8080", "beacon base URL")
	fs.DurationVar(&cfg.Duration, "duration", 30*time.Second, "how long to generate traffic")
	fs.IntVar(&cfg.Concurrency, "concurrency", 64, "parallel clients")
	fs.IntVar(&cfg.BatchSize, "batch", 1, "events per request (1 uses /v1/track, more uses /v1/batch)")
	fs.Float64Var(&cfg.Rate, "rate", 0, "maximum total requests per second (0 = unlimited)")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "random seed for a reproducible traffic mix")
	fs.StringVar(&cfg.Host, "host", "shop.example.com", "simulated website host")
	fs.IntVar(&cfg.Visitors, "visitors", 5000, "size of the simulated visitor pool")
	showVersion := fs.Bool("version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, "loadgen", version)
		return 0
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Concurrency,
			MaxIdleConnsPerHost: cfg.Concurrency,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	fmt.Fprintf(stdout, "loadgen: %s for %s, concurrency=%d batch=%d\n", cfg.Target, cfg.Duration, cfg.Concurrency, cfg.BatchSize)
	rep, err := loadgen.Run(ctx, client, cfg)
	if err != nil {
		fmt.Fprintln(stderr, "loadgen:", err)
		return 1
	}
	if _, err := rep.WriteTo(stdout); err != nil {
		return 1
	}
	return 0
}
