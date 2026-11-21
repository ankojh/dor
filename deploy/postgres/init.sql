-- Runs once on a fresh compose volume. Migrations land in M1·2; this only makes
-- the extension available so `create extension` in a migration is a no-op.
CREATE EXTENSION IF NOT EXISTS vector;
