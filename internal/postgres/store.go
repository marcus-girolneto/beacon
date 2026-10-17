// Package postgres stores records in PostgreSQL using the COPY protocol and
// serves aggregate stats queries over them.
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

// migrationLockID is an arbitrary constant used as an advisory lock key so
// that replicas starting together do not race on DDL.
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

// Close releases every pooled connection.
func (s *Store) Close() { s.pool.Close() }

// Ping reports whether the database is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Migrate creates the schema if it does not exist. It is idempotent and safe
// to run from several processes at once.
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

// Write bulk-loads recs with COPY FROM STDIN in a single round trip. COPY
// is atomic: either the whole batch is stored or none of it is.
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

// dimensionExpr maps each dimension to a trusted SQL expression. Only these
// constants are ever interpolated into queries.
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

// Stats aggregates events matching q. Totals and per-group rows come from a
// single scan using GROUPING SETS; the totals row sorts first so LIMIT never
// cuts it off.
func (s *Store) Stats(ctx context.Context, q stats.Query) (stats.Result, error) {
	if err := q.Validate(); err != nil {
		return stats.Result{}, err
	}
	expr, err := dimensionExpr(q.GroupBy)
	if err != nil {
		return stats.Result{}, err
	}
	order := "events DESC, key ASC"
	if q.GroupBy == stats.ByDay {
		order = "key ASC"
	}

	sql := fmt.Sprintf(`
SELECT GROUPING(key) = 1 AS is_total,
       COALESCE(key, '') AS key,
       count(*) AS events,
       count(*) FILTER (WHERE type = 'pageview') AS pageviews,
       count(DISTINCT visitor_hash) AS visitors
FROM (
    SELECT %s AS key, type, visitor_hash
    FROM events
    WHERE ts >= $1 AND ts < $2 AND ($3 = '' OR host = $3)
) e
GROUP BY GROUPING SETS ((key), ())
ORDER BY GROUPING(key) DESC, %s
LIMIT $4`, expr, order)

	rows, err := s.pool.Query(ctx, sql, q.From, q.To, q.Host, q.Limit+1)
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
