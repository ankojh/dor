package store

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/ankojh/dor/migrations"
)

const createMigrationsTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
)`

type migrationFile struct {
	version string
	name    string
	sql     string
}

// Migrate applies every embedded .up.sql migration that has not been applied.
// It is safe to call on every startup.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, createMigrationsTable); err != nil {
		return fmt.Errorf("store: create schema_migrations: %w", err)
	}
	applied, err := s.applied(ctx)
	if err != nil {
		return err
	}
	ups, _, err := loadMigrations()
	if err != nil {
		return err
	}
	for _, m := range ups {
		if applied[m.version] {
			continue
		}
		if err := s.apply(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// MigrateDown rolls back up to steps migrations, newest first. steps <= 0 rolls
// back all applied migrations.
func (s *Store) MigrateDown(ctx context.Context, steps int) error {
	if _, err := s.pool.Exec(ctx, createMigrationsTable); err != nil {
		return fmt.Errorf("store: create schema_migrations: %w", err)
	}
	_, downs, err := loadMigrations()
	if err != nil {
		return err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT version FROM schema_migrations ORDER BY version DESC`)
	if err != nil {
		return fmt.Errorf("store: list migrations: %w", err)
	}
	var versions []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		versions = append(versions, v)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if steps <= 0 || steps > len(versions) {
		steps = len(versions)
	}
	downByVersion := make(map[string]string, len(downs))
	for _, m := range downs {
		downByVersion[m.version] = m.sql
	}
	for i := 0; i < steps; i++ {
		version := versions[i]
		sql, ok := downByVersion[version]
		if !ok {
			return fmt.Errorf("store: no down migration for %s", version)
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("store: begin down %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, sql); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("store: down %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version=$1`, version); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("store: forget %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("store: commit down %s: %w", version, err)
		}
	}
	return nil
}

// Applied returns the applied migration versions in lexical order.
func (s *Store) Applied(ctx context.Context) ([]string, error) {
	if _, err := s.pool.Exec(ctx, createMigrationsTable); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) apply(ctx context.Context, m migrationFile) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin migration %s: %w", m.version, err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return fmt.Errorf("store: migration %s: %w", m.name, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, m.version); err != nil {
		return fmt.Errorf("store: record migration %s: %w", m.version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit migration %s: %w", m.version, err)
	}
	return nil
}

func (s *Store) applied(ctx context.Context) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("store: list applied migrations: %w", err)
	}
	defer rows.Close()
	out := make(map[string]bool)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

func loadMigrations() ([]migrationFile, []migrationFile, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, nil, fmt.Errorf("store: read migrations: %w", err)
	}
	var ups, downs []migrationFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".up.sql"):
			body, err := fs.ReadFile(migrations.FS, name)
			if err != nil {
				return nil, nil, err
			}
			ups = append(ups, migrationFile{version: migrationVersion(name), name: name, sql: string(body)})
		case strings.HasSuffix(name, ".down.sql"):
			body, err := fs.ReadFile(migrations.FS, name)
			if err != nil {
				return nil, nil, err
			}
			downs = append(downs, migrationFile{version: migrationVersion(name), name: name, sql: string(body)})
		}
	}
	sort.Slice(ups, func(i, j int) bool { return ups[i].version < ups[j].version })
	sort.Slice(downs, func(i, j int) bool { return downs[i].version < downs[j].version })
	return ups, downs, nil
}

func migrationVersion(name string) string {
	if i := strings.IndexByte(name, '_'); i > 0 {
		return name[:i]
	}
	return name
}
