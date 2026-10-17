// Package postgres stores records in PostgreSQL and serves stats queries.
package postgres

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marcus-girolneto/beacon/internal/event"
	"github.com/marcus-girolneto/beacon/internal/stats"
)

//go:embed schema.sql
var schema string

// advisory lock key so replicas don't race on DDL
const migrationLockID = 0x6265_6163_6f6e // "beacon"

var columns = []string{
	"message_id", "type", "name", "anonymous_id", "visitor_hash", "url", "host", "path",
	"referrer", "referrer_host", "channel", "utm_source", "utm_medium", "utm_campaign",
	"utm_term", "utm_content", "device", "properties", "ts", "received_at",
}

// Store is a PostgreSQL-backed sink and stats backend.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to the database at dsn and verifies the connection.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close closes the pool.
func (s *Store) Close() { s.pool.Close() }

// Ping reports whether the database is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Migrate creates the schema. Safe to run concurrently from several processes.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if _, err := tx.Exec(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}

// Write stores recs with a single, atomic COPY.
func (s *Store) Write(ctx context.Context, recs []event.Record) error {
	if len(recs) == 0 {
		return nil
	}
	n, err := s.pool.CopyFrom(ctx, pgx.Identifier{"events"}, columns, pgx.CopyFromSlice(len(recs), func(i int) ([]any, error) {
		r := &recs[i]
		var props any
		if len(r.Properties) > 0 {
			props = []byte(r.Properties)
		}
		return []any{
			r.MessageID, string(r.Type), r.Name, r.AnonymousID, r.VisitorHash, r.URL, r.Host, r.Path,
			r.Referrer, r.ReferrerHost, string(r.Channel), r.UTM.Source, r.UTM.Medium, r.UTM.Campaign,
			r.UTM.Term, r.UTM.Content, string(r.Device), props, r.Timestamp, r.ReceivedAt,
		}, nil
	}))
	if err != nil {
		return fmt.Errorf("copy events: %w", err)
	}
	if n != int64(len(recs)) {
		return fmt.Errorf("copy events: wrote %d of %d rows", n, len(recs))
	}
	return nil
}

// only these constants are ever interpolated into SQL
func dimensionExpr(d stats.Dimension) (string, error) {
	switch d {
	case stats.ByCampaign:
		return "COALESCE(NULLIF(utm_campaign, ''), '(none)')", nil
	case stats.BySource:
		return "COALESCE(NULLIF(utm_source, ''), '(none)')", nil
	case stats.ByChannel:
		return "channel", nil
	case stats.ByPath:
		return "path", nil
	case stats.ByDevice:
		return "device", nil
	case stats.ByDay:
		return "to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD')", nil
	default:
		return "", fmt.Errorf("%w: unknown group_by %q", stats.ErrInvalidQuery, d)
	}
}

// Stats aggregates events matching q.
func (s *Store) Stats(ctx context.Context, q stats.Query) (stats.Result, error) {
	if err := q.Validate(); err != nil {
		return stats.Result{}, err
	}
	expr, err := dimensionExpr(q.GroupBy)
	if err != nil {
		return stats.Result{}, err
	}
	order := "total_events DESC, key ASC"
	if q.GroupBy == stats.ByDay {
		order = "key ASC"
	}

	const filter = `ts >= $1 AND ts < $2 AND ($3 = '' OR host = $3)`
	sql := fmt.Sprintf(`
(SELECT false AS is_total, key, sum(events)::bigint AS total_events,
        sum(pageviews)::bigint AS total_pageviews, count(*) AS visitors
 FROM (SELECT %[1]s AS key, visitor_hash, count(*) AS events,
              count(*) FILTER (WHERE type = 'pageview') AS pageviews
       FROM events WHERE %[2]s
       GROUP BY 1, 2) per_visitor
 GROUP BY key
 ORDER BY %[3]s
 LIMIT $4)
UNION ALL
SELECT true, '', COALESCE(sum(events), 0)::bigint, COALESCE(sum(pageviews), 0)::bigint, count(*)
FROM (SELECT visitor_hash, count(*) AS events,
             count(*) FILTER (WHERE type = 'pageview') AS pageviews
      FROM events WHERE %[2]s
      GROUP BY 1) totals`, expr, filter, order)

	rows, err := s.pool.Query(ctx, sql, q.From, q.To, q.Host, q.Limit)
	if err != nil {
		return stats.Result{}, fmt.Errorf("query stats: %w", err)
	}
	defer rows.Close()

	res := stats.Result{From: q.From, To: q.To, GroupBy: q.GroupBy, Rows: []stats.Row{}}
	for rows.Next() {
		var (
			isTotal bool
			row     stats.Row
		)
		if err := rows.Scan(&isTotal, &row.Key, &row.Events, &row.Pageviews, &row.Visitors); err != nil {
			return stats.Result{}, fmt.Errorf("scan stats row: %w", err)
		}
		if isTotal {
			row.Key = ""
			res.Totals = row
			continue
		}
		res.Rows = append(res.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return stats.Result{}, fmt.Errorf("read stats rows: %w", err)
	}
	return res, nil
}
