package postgres

import "context"

func (s *Store) Exec(ctx context.Context, sql string) error {
	_, err := s.pool.Exec(ctx, sql)
	return err
}

func (s *Store) QueryRow(ctx context.Context, sql string, dest ...any) error {
	return s.pool.QueryRow(ctx, sql).Scan(dest...)
}
