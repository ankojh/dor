// Package migrations embeds the Postgres schema migrations applied by
// internal/store. Files are named <version>_<name>.up.sql / .down.sql and are
// applied in lexical order.
package migrations

import "embed"

// FS contains the SQL migration files.
//
//go:embed *.sql
var FS embed.FS
