//go:build integration

package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/marcus-girolneto/beacon/internal/stats"
)

func TestEndToEndPostgres(t *testing.T) {
	ctx := t.Context()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("beacon"), tcpostgres.WithUsername("beacon"), tcpostgres.WithPassword("beacon"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig(t, "-sink", "postgres", "-postgres-dsn", dsn, "-flush-interval", "20ms")
	r := start(t, cfg, io.Discard)

	today := time.Now().UTC().Format(time.DateOnly)
	events := []string{
		`{"type":"pageview","anonymous_id":"a","url":"https://example.com/?utm_source=google&utm_medium=cpc&utm_campaign=brand"}`,
		`{"type":"pageview","anonymous_id":"b","url":"https://example.com/pricing","referrer":"https://t.co/abc"}`,
		`{"type":"click","name":"cta","anonymous_id":"b","url":"https://example.com/pricing","referrer":"https://t.co/abc"}`,
	}
	if code, body := request(t, http.MethodPost, r.base+"/v1/batch", `{"batch":[`+strings.Join(events, ",")+`]}`); code != http.StatusAccepted {
		t.Fatalf("batch = %d: %s", code, body)
	}

	tomorrow := time.Now().UTC().Add(24 * time.Hour).Format(time.DateOnly)
	url := r.base + "/v1/stats?group_by=channel&from=" + today + "&to=" + tomorrow
	var res stats.Result
	deadline := time.Now().Add(10 * time.Second)
	for res.Totals.Events < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("events never reached postgres: %+v", res)
		}
		code, body := request(t, http.MethodGet, url, "")
		if code != http.StatusOK {
			t.Fatalf("stats = %d: %s", code, body)
		}
		if err := json.Unmarshal([]byte(body), &res); err != nil {
			t.Fatal(err)
		}
	}

	got := map[string]int64{}
	for _, row := range res.Rows {
		got[row.Key] = row.Events
	}
	if got["social"] != 2 || got["paid"] != 1 || res.Totals.Pageviews != 2 {
		t.Errorf("stats = %+v", res)
	}
	r.stop(t)
}
