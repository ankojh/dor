package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// readDatabaseEnvironment returns the copy's bound environment id, or "" when
// the singleton row is absent (unbound). A missing migration/table is an error:
// audit and apply never create the binding implicitly.
func readDatabaseEnvironment(ctx context.Context, db *pgx.Conn) (string, error) {
	var name string
	if err := db.QueryRow(ctx, "SELECT current_database()").Scan(&name); err != nil {
		return "", err
	}
	if strings.EqualFold(name, "dor") {
		return "", errors.New("maintenance environment binding refuses the live dor database")
	}
	var id string
	err := db.QueryRow(ctx, "SELECT environment_id FROM maintenance_environment WHERE singleton").Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read database environment identity (migration 0011 required): %w", err)
	}
	return strings.TrimSpace(id), nil
}

// bindDatabaseEnvironment performs the explicit insert-once binding for the
// named non-dor copy and returns the actual database name.
func bindDatabaseEnvironment(ctx context.Context, db *pgx.Conn, allowDB, environmentID string) (string, error) {
	var current string
	if err := db.QueryRow(ctx, "SELECT current_database()").Scan(&current); err != nil {
		return "", err
	}
	if strings.EqualFold(current, "dor") {
		return "", errors.New("maintenance environment binding refuses the live dor database")
	}
	if allowDB == "" || current != allowDB {
		return "", errors.New("--bind-environment requires --allow-db matching the non-dor copy")
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO maintenance_environment (environment_id) VALUES ($1)
ON CONFLICT (singleton) DO NOTHING`, environmentID); err != nil {
		return "", fmt.Errorf("bind database environment identity (migration 0011 required): %w", err)
	}
	var bound string
	if err = tx.QueryRow(ctx, "SELECT environment_id FROM maintenance_environment WHERE singleton").Scan(&bound); err != nil {
		return "", err
	}
	bound = strings.TrimSpace(bound)
	if bound != environmentID {
		return "", fmt.Errorf("database is already bound to environment %q, not %q", bound, environmentID)
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return current, nil
}
