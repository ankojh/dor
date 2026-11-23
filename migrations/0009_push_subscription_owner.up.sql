-- Bind encrypted Web Push subscriptions to the verified anonymous session that
-- registered them. Existing rows are legacy/unowned (owner = '') and fail
-- closed: they are never delivered or mutated on any owner's behalf.
ALTER TABLE push_subscriptions ADD COLUMN IF NOT EXISTS owner text NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS push_subscriptions_owner_idx ON push_subscriptions (owner);
