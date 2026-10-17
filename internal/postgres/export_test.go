package postgres

import "context"

// Exec runs a statement; it exists for integration tests only.
func (s *Store) Exec(ctx context.Context, sql string) error {
	_, err := s.pool.Exec(ctx, sql)
	return err
}

// QueryRow scans a single-row query into dest; it exists for integration
// tests only.
func (s *Store) QueryRow(ctx context.Context, sql string, dest ...any) error {
	return s.pool.QueryRow(ctx, sql).Scan(dest...)
}
