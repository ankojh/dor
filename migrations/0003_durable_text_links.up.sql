-- Durable mirrors for the KV hot caches and source labels used by the
-- story-detail API. Existing rows keep working with empty names/why values.
ALTER TABLE sources ADD COLUMN IF NOT EXISTS name text NOT NULL DEFAULT '';
ALTER TABLE links   ADD COLUMN IF NOT EXISTS unverified boolean NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS stories_updated_idx ON stories (updated_at DESC);
