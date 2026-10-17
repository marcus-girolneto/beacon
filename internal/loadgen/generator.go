// Package loadgen produces realistic synthetic analytics traffic and drives
// it against a beacon instance, reporting throughput and latency.
package loadgen

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/url"
	"time"
)

// weighted picks items with probability proportional to their weight.
type weighted[T any] struct {
	items   []T
	cumul   []int
	totalWt int
}

func newWeighted[T any](pairs ...struct {
	v T
	w int
},
) weighted[T] {
	var w weighted[T]
	for _, p := range pairs {
		w.totalWt += p.w
		w.items = append(w.items, p.v)
		w.cumul = append(w.cumul, w.totalWt)
	}
	return w
}

func (w weighted[T]) pick(r *rand.Rand) T {
	n := r.IntN(w.totalWt)
	for i, c := range w.cumul {
		if n < c {
			return w.items[i]
		}
	}
	return w.items[len(w.items)-1]
}

func p[T any](v T, w int) struct {
	v T
	w int
} {
	return struct {
		v T
		w int
	}{v, w}
}

// source describes how a simulated visitor arrived.
type source struct {
	referrer string
	query    string // appended to the landing URL
}

// Generator creates plausible marketing traffic: a long tail of pages,
// search/social/email/paid/direct acquisition mixes, a device split close
// to typical web traffic and a pool of returning visitors.
type Generator struct {
	rng      *rand.Rand
	host     string
	visitors int
	pages    weighted[string]
	sources  weighted[source]
	agents   weighted[string]
	types    weighted[string]
	clicks   []string
	signups  []string
}

// NewGenerator returns a Generator for site host with a pool of visitors
// distinct visitors. The same seed always yields the same sequence.
func NewGenerator(seed uint64, host string, visitors int) *Generator {
	return &Generator{
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), //nolint:gosec // synthetic traffic, not security
		host:     host,
		visitors: max(visitors, 1),
		pages: newWeighted(
			p("/", 30), p("/pricing", 15), p("/features", 10), p("/blog", 8),
			p("/blog/channel-batching-in-go", 6), p("/blog/postgres-copy-vs-insert", 5),
			p("/docs", 7), p("/docs/getting-started", 6), p("/signup", 5), p("/about", 3),
			p("/careers", 2), p("/contact", 3),
		),
		sources: newWeighted(
			p(source{}, 30),
			p(source{referrer: "https://www.google.com/"}, 25),
			p(source{referrer: "https://duckduckgo.com/"}, 3),
			p(source{referrer: "https://www.bing.com/"}, 3),
			p(source{referrer: "https://t.co/x1y2z3"}, 5),
			p(source{referrer: "https://www.linkedin.com/feed/"}, 4),
			p(source{referrer: "https://news.ycombinator.com/item?id=1"}, 3),
			p(source{referrer: "https://mail.google.com/", query: "utm_source=newsletter&utm_medium=email&utm_campaign=october_digest"}, 6),
			p(source{referrer: "https://www.google.com/", query: "utm_source=google&utm_medium=cpc&utm_campaign=brand_search&utm_term=analytics"}, 8),
			p(source{referrer: "https://www.google.com/", query: "gclid=Cj0KCQjw"}, 3),
			p(source{referrer: "https://www.facebook.com/", query: "utm_source=facebook&utm_medium=paid_social&utm_campaign=fall_launch&utm_content=video_a"}, 5),
			p(source{referrer: "https://blog.example.org/go-tips"}, 5),
		),
		agents: newWeighted(
			p("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36", 30),
			p("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15", 15),
			p("Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1", 25),
			p("Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36", 18),
			p("Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1", 7),
			p("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", 5),
		),
		types:   newWeighted(p("pageview", 80), p("click", 15), p("custom", 5)),
		clicks:  []string{"cta_hero", "nav_pricing", "footer_docs", "plan_pro"},
		signups: []string{"signup_started", "signup_completed", "newsletter_subscribed"},
	}
}

// Event is the JSON payload the generator produces.
type Event struct {
	Type        string          `json:"type"`
	Name        string          `json:"name,omitempty"`
	AnonymousID string          `json:"anonymous_id"`
	URL         string          `json:"url"`
	Referrer    string          `json:"referrer,omitempty"`
	Properties  json.RawMessage `json:"properties,omitempty"`
	Timestamp   time.Time       `json:"timestamp"`
}

// Visit is one synthetic event plus the client attributes to send with it.
type Visit struct {
	Event     Event
	UserAgent string
	IP        string
}

// Next returns a new synthetic visit at time now.
func (g *Generator) Next(now time.Time) Visit {
	visitor := g.rng.IntN(g.visitors)
	src := g.sources.pick(g.rng)
	u := url.URL{Scheme: "https", Host: g.host, Path: g.pages.pick(g.rng), RawQuery: src.query}

	e := Event{
		Type:        g.types.pick(g.rng),
		AnonymousID: fmt.Sprintf("anon-%06d", visitor),
		URL:         u.String(),
		Referrer:    src.referrer,
		Timestamp:   now.UTC().Add(-time.Duration(g.rng.IntN(2000)) * time.Millisecond),
	}
	switch e.Type {
	case "click":
		e.Name = g.clicks[g.rng.IntN(len(g.clicks))]
		e.Properties = json.RawMessage(fmt.Sprintf(`{"x":%d,"y":%d}`, g.rng.IntN(1920), g.rng.IntN(1080)))
	case "custom":
		e.Name = g.signups[g.rng.IntN(len(g.signups))]
		e.Properties = json.RawMessage(fmt.Sprintf(`{"plan":"%s"}`, [...]string{"free", "pro", "team"}[g.rng.IntN(3)]))
	}

	return Visit{
		Event:     e,
		UserAgent: g.agents.pick(g.rng),
		// Addresses from 198.18.0.0/15, reserved for benchmarking (RFC 2544).
		IP: fmt.Sprintf("198.%d.%d.%d", 18+visitor>>16&1, visitor>>8&0xff, visitor&0xff),
	}
}
