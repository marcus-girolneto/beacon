//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/marcus-girolneto/beacon/internal/event"
	"github.com/marcus-girolneto/beacon/internal/postgres"
	"github.com/marcus-girolneto/beacon/internal/stats"
)

var baseDSN string

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("beacon"),
		tcpostgres.WithUsername("beacon"),
		tcpostgres.WithPassword("beacon"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("start postgres container: %v", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			log.Printf("terminate container: %v", err)
		}
	}()
	baseDSN, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return 1
	}
	return m.Run()
}

var dbCounter struct {
	sync.Mutex
	n int
}

func newStore(t *testing.T) *postgres.Store {
	t.Helper()
	ctx := t.Context()

	admin, err := postgres.Open(ctx, baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	dbCounter.Lock()
	dbCounter.n++
	name := fmt.Sprintf("test_%d", dbCounter.n)
	dbCounter.Unlock()
	if err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}

	s, err := postgres.Open(ctx, replaceDatabase(t, baseDSN, name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

var day = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func rec(id string, mutate func(r *event.Record)) event.Record {
	r := event.Record{
		MessageID:   id,
		Type:        event.TypePageview,
		AnonymousID: "anon-" + id,
		VisitorHash: "v-" + id,
		URL:         "https://example.com/",
		Host:        "example.com",
		Path:        "/",
		Channel:     event.ChannelDirect,
		Device:      event.DeviceDesktop,
		Timestamp:   day.Add(time.Hour),
		ReceivedAt:  day.Add(time.Hour),
	}
	if mutate != nil {
		mutate(&r)
	}
	return r
}

func TestWriteAndStats(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := t.Context()

	recs := []event.Record{
		rec("1", func(r *event.Record) {
			r.Channel = event.ChannelPaid
			r.UTM = event.UTM{Source: "google", Medium: "cpc", Campaign: "spring"}
			r.Path = "/pricing"
			r.Properties = json.RawMessage(`{"plan":"pro"}`)
		}),
		rec("2", func(r *event.Record) {
			r.Channel = event.ChannelPaid
			r.UTM = event.UTM{Source: "google", Medium: "cpc", Campaign: "spring"}
			r.VisitorHash = "v-1"
			r.Type = event.TypeClick
		}),
		rec("3", func(r *event.Record) {
			r.Channel = event.ChannelSearch
			r.Path = "/pricing"
			r.Timestamp = day.Add(25 * time.Hour)
		}),
		rec("4", func(r *event.Record) {
			r.Channel = event.ChannelEmail
			r.UTM = event.UTM{Source: "newsletter", Medium: "email", Campaign: "fall"}
			r.Device = event.DeviceMobile
			r.Timestamp = day.Add(26 * time.Hour)
		}),
		rec("other-site", func(r *event.Record) { r.Host = "other.com" }),
		rec("out-of-range", func(r *event.Record) { r.Timestamp = day.Add(-time.Hour) }),
	}
	if err := s.Write(ctx, recs); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, nil); err != nil {
		t.Fatalf("Write(nil) = %v", err)
	}

	query := func(dim stats.Dimension, host string, limit int) stats.Result {
		t.Helper()
		res, err := s.Stats(ctx, stats.Query{From: day, To: day.Add(48 * time.Hour), GroupBy: dim, Host: host, Limit: limit})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	t.Run("channel", func(t *testing.T) {
		res := query(stats.ByChannel, "example.com", 100)
		want := []stats.Row{
			{Key: "paid", Events: 2, Pageviews: 1, Visitors: 1},
			{Key: "email", Events: 1, Pageviews: 1, Visitors: 1},
			{Key: "search", Events: 1, Pageviews: 1, Visitors: 1},
		}
		assertRows(t, res.Rows, want)
		if want := (stats.Row{Events: 4, Pageviews: 3, Visitors: 3}); res.Totals != want {
			t.Errorf("totals = %+v, want %+v", res.Totals, want)
		}
	})

	t.Run("campaign", func(t *testing.T) {
		res := query(stats.ByCampaign, "example.com", 100)
		assertRows(t, res.Rows, []stats.Row{
			{Key: "spring", Events: 2, Pageviews: 1, Visitors: 1},
			{Key: "(none)", Events: 1, Pageviews: 1, Visitors: 1},
			{Key: "fall", Events: 1, Pageviews: 1, Visitors: 1},
		})
	})

	t.Run("day ascending", func(t *testing.T) {
		res := query(stats.ByDay, "example.com", 100)
		assertRows(t, res.Rows, []stats.Row{
			{Key: "2026-10-01", Events: 2, Pageviews: 1, Visitors: 1},
			{Key: "2026-10-02", Events: 2, Pageviews: 2, Visitors: 2},
		})
	})

	t.Run("path", func(t *testing.T) {
		res := query(stats.ByPath, "example.com", 100)
		assertRows(t, res.Rows, []stats.Row{
			{Key: "/", Events: 2, Pageviews: 1, Visitors: 2},
			{Key: "/pricing", Events: 2, Pageviews: 2, Visitors: 2},
		})
	})

	t.Run("all hosts", func(t *testing.T) {
		res := query(stats.ByDevice, "", 100)
		if res.Totals.Events != 5 {
			t.Errorf("totals.events = %d, want 5", res.Totals.Events)
		}
	})

	t.Run("limit keeps totals", func(t *testing.T) {
		res := query(stats.ByChannel, "example.com", 1)
		if len(res.Rows) != 1 || res.Rows[0].Key != "paid" {
			t.Errorf("rows = %+v, want only paid", res.Rows)
		}
		if res.Totals.Events != 4 {
			t.Errorf("totals.events = %d, want 4", res.Totals.Events)
		}
	})

	t.Run("properties stored as jsonb", func(t *testing.T) {
		var plan string
		if err := s.QueryRow(ctx, "SELECT properties->>'plan' FROM events WHERE message_id = '1'", &plan); err != nil {
			t.Fatal(err)
		}
		if plan != "pro" {
			t.Errorf("properties->>'plan' = %q, want pro", plan)
		}
		var isNull bool
		if err := s.QueryRow(ctx, "SELECT properties IS NULL FROM events WHERE message_id = '3'", &isNull); err != nil {
			t.Fatal(err)
		}
		if !isNull {
			t.Error("empty properties not stored as NULL")
		}
	})
}

func TestStatsEmptyRange(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	res, err := s.Stats(t.Context(), stats.Query{From: day, To: day.Add(time.Hour), GroupBy: stats.ByChannel, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 0 || res.Rows == nil {
		t.Errorf("rows = %#v, want empty non-nil slice", res.Rows)
	}
	if res.Totals != (stats.Row{}) {
		t.Errorf("totals = %+v, want zero", res.Totals)
	}
}

func TestStatsInvalidQuery(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	_, err := s.Stats(t.Context(), stats.Query{From: day, To: day.Add(time.Hour), GroupBy: "country", Limit: 10})
	if !errors.Is(err, stats.ErrInvalidQuery) {
		t.Fatalf("Stats() = %v, want ErrInvalidQuery", err)
	}
}

func TestMigrateIsIdempotentAndConcurrent(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := s.Migrate(t.Context()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestWriteLargeBatch(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	recs := make([]event.Record, 5000)
	for i := range recs {
		recs[i] = rec(fmt.Sprint(i), nil)
	}
	if err := s.Write(t.Context(), recs); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.QueryRow(t.Context(), "SELECT count(*) FROM events", &n); err != nil {
		t.Fatal(err)
	}
	if n != len(recs) {
		t.Fatalf("count = %d, want %d", n, len(recs))
	}
}

func TestWriteFailsOnClosedPool(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	s.Close()
	if err := s.Write(context.Background(), []event.Record{rec("x", nil)}); err == nil {
		t.Fatal("Write() on closed pool succeeded")
	}
	if err := s.Ping(context.Background()); err == nil {
		t.Fatal("Ping() on closed pool succeeded")
	}
}

func TestOpenErrors(t *testing.T) {
	t.Parallel()
	if _, err := postgres.Open(t.Context(), "://not a dsn"); err == nil {
		t.Error("Open() with invalid DSN succeeded")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if _, err := postgres.Open(ctx, closedPortDSN()); err == nil {
		t.Error("Open() against closed port succeeded")
	}
}

func closedPortDSN() string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword("beacon", "beacon"), Host: "127.0.0.1:1", Path: "/beacon"}
	return u.String() + "?sslmode=disable&connect_timeout=1"
}

func replaceDatabase(t *testing.T, dsn, name string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

func assertRows(t *testing.T, got, want []stats.Row) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rows = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
