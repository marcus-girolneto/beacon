package stats

import (
	"errors"
	"testing"
	"time"
)

func TestQueryValidate(t *testing.T) {
	t.Parallel()
	from := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	valid := Query{From: from, To: from.Add(24 * time.Hour), GroupBy: ByChannel, Limit: DefaultLimit}

	tests := []struct {
		name    string
		mutate  func(q *Query)
		wantErr bool
	}{
		{name: "valid", mutate: func(*Query) {}},
		{name: "max range", mutate: func(q *Query) { q.To = q.From.Add(MaxRange) }},
		{name: "missing from", mutate: func(q *Query) { q.From = time.Time{} }, wantErr: true},
		{name: "missing to", mutate: func(q *Query) { q.To = time.Time{} }, wantErr: true},
		{name: "to before from", mutate: func(q *Query) { q.To = q.From.Add(-time.Hour) }, wantErr: true},
		{name: "empty range", mutate: func(q *Query) { q.To = q.From }, wantErr: true},
		{name: "range too long", mutate: func(q *Query) { q.To = q.From.Add(MaxRange + time.Hour) }, wantErr: true},
		{name: "limit zero", mutate: func(q *Query) { q.Limit = 0 }, wantErr: true},
		{name: "limit too high", mutate: func(q *Query) { q.Limit = MaxLimit + 1 }, wantErr: true},
		{name: "unknown dimension", mutate: func(q *Query) { q.GroupBy = "country" }, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q := valid
			tc.mutate(&q)
			err := q.Validate()
			if tc.wantErr != (err != nil) {
				t.Fatalf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidQuery) {
				t.Errorf("error %v does not match ErrInvalidQuery", err)
			}
		})
	}
}

func TestParseDimension(t *testing.T) {
	t.Parallel()
	for _, d := range Dimensions() {
		got, err := ParseDimension(string(d))
		if err != nil || got != d {
			t.Errorf("ParseDimension(%q) = %q, %v", d, got, err)
		}
	}
	if _, err := ParseDimension("utm_campaign; DROP TABLE events"); !errors.Is(err, ErrInvalidQuery) {
		t.Errorf("ParseDimension(injection) = %v, want ErrInvalidQuery", err)
	}
}
