// Package api implements beacon's HTTP interface: event ingestion, the
// tracking pixel, the stats query endpoint and health probes.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/marcus-girolneto/beacon/internal/enrich"
	"github.com/marcus-girolneto/beacon/internal/event"
	"github.com/marcus-girolneto/beacon/internal/stats"
)

// Enqueuer admits enriched records into the ingestion pipeline.
type Enqueuer interface {
	Enqueue(ctx context.Context, recs ...event.Record) error
}

// StatsQuerier answers aggregate queries.
type StatsQuerier interface {
	Stats(ctx context.Context, q stats.Query) (stats.Result, error)
}

// Recorder receives HTTP-level telemetry.
type Recorder interface {
	EventsAccepted(endpoint string, n int)
	EventsRejected(reason string, n int)
	EventsDropped(reason string, n int)
	ObserveRequest(route string, code int, elapsed time.Duration)
}

// Config tunes request handling. Zero fields take the defaults listed below.
type Config struct {
	// MaxEventBytes caps the body of /v1/track. Default 64 KiB.
	MaxEventBytes int64
	// MaxBatchBytes caps the body of /v1/batch. Default 1 MiB.
	MaxBatchBytes int64
	// MaxBatchEvents caps the number of events in one /v1/batch request.
	// Default 500.
	MaxBatchEvents int
	// RetryAfter is advertised to clients when the buffer is full.
	// Default 1s.
	RetryAfter time.Duration
	// ProxyHops is the number of trusted reverse proxies in front of the
	// server. When positive, the client IP is taken from X-Forwarded-For,
	// counting ProxyHops entries from the right; entries further left are
	// client-controlled and ignored. Zero uses the TCP peer address.
	ProxyHops int
	// Now returns the current time. Default time.Now.
	Now func() time.Time
}

func (c Config) withDefaults() Config {
	if c.MaxEventBytes <= 0 {
		c.MaxEventBytes = 64 << 10
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = 1 << 20
	}
	if c.MaxBatchEvents <= 0 {
		c.MaxBatchEvents = 500
	}
	if c.RetryAfter <= 0 {
		c.RetryAfter = time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Server holds the dependencies of the HTTP handlers.
type Server struct {
	cfg       Config
	queue     Enqueuer
	enricher  *enrich.Enricher
	validator event.Validator
	stats     StatsQuerier
	rec       Recorder
	log       *slog.Logger
	draining  atomic.Bool
}

// Deps groups the collaborators a Server needs. Stats may be nil, in which
// case the stats endpoint responds 501.
type Deps struct {
	Queue    Enqueuer
	Enricher *enrich.Enricher
	Stats    StatsQuerier
	Recorder Recorder
	Logger   *slog.Logger
}

// New returns a Server.
func New(cfg Config, deps Deps) *Server {
	cfg = cfg.withDefaults()
	if deps.Recorder == nil {
		deps.Recorder = nopRecorder{}
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	return &Server{
		cfg:       cfg,
		queue:     deps.Queue,
		enricher:  deps.Enricher,
		validator: event.NewValidator(cfg.Now),
		stats:     deps.Stats,
		rec:       deps.Recorder,
		log:       deps.Logger,
	}
}

// SetDraining marks the server as shutting down so readiness probes fail
// and load balancers stop routing new traffic to it.
func (s *Server) SetDraining() { s.draining.Store(true) }

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.route(mux, "POST /v1/track", s.handleTrack)
	s.route(mux, "POST /v1/batch", s.handleBatch)
	s.route(mux, "OPTIONS /v1/{endpoint}", s.handlePreflight)
	s.route(mux, "GET /v1/pixel.gif", s.handlePixel)
	s.route(mux, "GET /v1/stats", s.handleStats)
	s.route(mux, "GET /healthz", s.handleHealth)
	s.route(mux, "GET /readyz", s.handleReady)
	return mux
}

func (s *Server) route(mux *http.ServeMux, pattern string, h http.HandlerFunc) {
	mux.Handle(pattern, s.instrument(pattern, h))
}

// instrument records request latency and sets the CORS header. Ingestion
// endpoints are called from arbitrary origins by design and never use
// credentials, so a wildcard origin is safe.
func (s *Server) instrument(route string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := s.cfg.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		sw.Header().Set("Access-Control-Allow-Origin", "*")
		next(sw, r)
		s.rec.ObserveRequest(route, sw.code, s.cfg.Now().Sub(start))
	})
}

type statusWriter struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.code, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (s *Server) handlePreflight(w http.ResponseWriter, _ *http.Request) {
	h := w.Header()
	h.Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type")
	h.Set("Access-Control-Max-Age", "86400")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if s.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if s.stats == nil {
		writeError(w, http.StatusNotImplemented, "stats require the postgres sink")
		return
	}
	q, err := parseStatsQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := s.stats.Stats(r.Context(), q)
	switch {
	case errors.Is(err, stats.ErrInvalidQuery):
		writeError(w, http.StatusBadRequest, err.Error())
	case err != nil:
		s.log.ErrorContext(r.Context(), "stats query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "stats query failed")
	default:
		writeJSON(w, http.StatusOK, res)
	}
}

func parseStatsQuery(r *http.Request) (stats.Query, error) {
	v := r.URL.Query()
	q := stats.Query{Host: v.Get("host"), Limit: stats.DefaultLimit, GroupBy: stats.ByDay}

	var err error
	if q.From, err = parseTime(v.Get("from")); err != nil {
		return q, errors.New("from: " + err.Error())
	}
	if q.To, err = parseTime(v.Get("to")); err != nil {
		return q, errors.New("to: " + err.Error())
	}
	if g := v.Get("group_by"); g != "" {
		if q.GroupBy, err = stats.ParseDimension(g); err != nil {
			return q, err
		}
	}
	if l := v.Get("limit"); l != "" {
		if q.Limit, err = strconv.Atoi(l); err != nil {
			return q, errors.New("limit must be an integer")
		}
	}
	return q, q.Validate()
}

// parseTime accepts RFC 3339 timestamps and plain dates (midnight UTC).
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, errors.New("is required")
	}
	if t, err := time.Parse(time.DateOnly, s); err == nil {
		return t, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, errors.New("must be a date (2006-01-02) or RFC 3339 timestamp")
	}
	return t, nil
}

// clientIP returns the address used for visitor hashing. See
// Config.ProxyHops for how X-Forwarded-For is trusted.
func (s *Server) clientIP(r *http.Request) string {
	if s.cfg.ProxyHops > 0 {
		if xff := r.Header.Values("X-Forwarded-For"); len(xff) > 0 {
			parts := strings.Split(strings.Join(xff, ","), ",")
			idx := max(len(parts)-s.cfg.ProxyHops, 0)
			if ip := strings.TrimSpace(parts[idx]); ip != "" {
				return ip
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type nopRecorder struct{}

func (nopRecorder) EventsAccepted(string, int)                {}
func (nopRecorder) EventsRejected(string, int)                {}
func (nopRecorder) EventsDropped(string, int)                 {}
func (nopRecorder) ObserveRequest(string, int, time.Duration) {}
