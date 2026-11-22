-- Scope saved alerts to the anonymous session that created them. Rows with an
-- empty owner are legacy/unowned and are visible to no client.
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS owner text NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS alerts_owner_idx ON alerts (owner);
