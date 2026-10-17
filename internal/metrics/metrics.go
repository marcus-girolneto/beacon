// Package metrics exposes beacon's Prometheus instrumentation.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/marcus-girolneto/beacon/internal/pipeline"
)

const namespace = "beacon"

// Metrics holds every collector and the registry they are registered with.
// A dedicated registry (rather than the global default) keeps tests isolated
// and lets several instances coexist in one process.
type Metrics struct {
	reg *prometheus.Registry

	accepted        *prometheus.CounterVec
	rejected        *prometheus.CounterVec
	dropped         *prometheus.CounterVec
	written         prometheus.Counter
	lost            prometheus.Counter
	flushes         *prometheus.CounterVec
	batchSize       prometheus.Histogram
	flushDuration   *prometheus.HistogramVec
	requestDuration *prometheus.HistogramVec
}

// New creates and registers the collectors. buffered reports the current
// number of queued records and capacity the queue size.
func New(buffered func() int, capacity int) *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		accepted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "events_accepted_total",
			Help: "Events validated, enriched and admitted into the buffer.",
		}, []string{"endpoint"}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "events_rejected_total",
			Help: "Events refused because the request or event was invalid.",
		}, []string{"reason"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "events_dropped_total",
			Help: "Valid events refused because of backpressure or shutdown.",
		}, []string{"reason"}),
		written: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "events_written_total",
			Help: "Events durably written to the sink.",
		}),
		lost: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "events_lost_total",
			Help: "Accepted events discarded after the sink failed every retry.",
		}),
		flushes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "flushes_total",
			Help: "Sink write attempts by trigger and result.",
		}, []string{"trigger", "result"}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "batch_size",
			Help:    "Number of events per sink write attempt.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 14), // 1 .. 8192
		}),
		flushDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "flush_duration_seconds",
			Help:    "Latency of sink write attempts.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 15), // 0.5ms .. ~8s
		}, []string{"result"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "http_request_duration_seconds",
			Help:    "HTTP request latency by route and status code.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16), // 0.1ms .. ~3s
		}, []string{"route", "code"}),
	}

	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.accepted, m.rejected, m.dropped, m.written, m.lost,
		m.flushes, m.batchSize, m.flushDuration, m.requestDuration,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: "buffer_occupancy",
			Help: "Events currently queued between the HTTP handlers and the batchers.",
		}, func() float64 { return float64(buffered()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: "buffer_capacity",
			Help: "Maximum number of events the buffer can hold.",
		}, func() float64 { return float64(capacity) }),
	)
	return m
}

// Handler serves the metrics in the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// Registry returns the underlying registry.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// EventsAccepted counts n events admitted through endpoint.
func (m *Metrics) EventsAccepted(endpoint string, n int) {
	m.accepted.WithLabelValues(endpoint).Add(float64(n))
}

// EventsRejected counts n events refused for reason.
func (m *Metrics) EventsRejected(reason string, n int) {
	m.rejected.WithLabelValues(reason).Add(float64(n))
}

// EventsDropped counts n valid events shed for reason.
func (m *Metrics) EventsDropped(reason string, n int) {
	m.dropped.WithLabelValues(reason).Add(float64(n))
}

// ObserveRequest records the latency of one HTTP request.
func (m *Metrics) ObserveRequest(route string, code int, elapsed time.Duration) {
	m.requestDuration.WithLabelValues(route, strconv.Itoa(code)).Observe(elapsed.Seconds())
}

// ObserveFlush implements pipeline.Observer.
func (m *Metrics) ObserveFlush(trigger pipeline.Trigger, size int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	} else {
		m.written.Add(float64(size))
	}
	m.flushes.WithLabelValues(string(trigger), result).Inc()
	m.batchSize.Observe(float64(size))
	m.flushDuration.WithLabelValues(result).Observe(elapsed.Seconds())
}

// ObserveLost implements pipeline.Observer.
func (m *Metrics) ObserveLost(n int) {
	m.lost.Add(float64(n))
}
