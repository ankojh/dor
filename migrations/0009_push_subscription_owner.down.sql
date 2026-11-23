DROP INDEX IF EXISTS push_subscriptions_owner_idx;
ALTER TABLE push_subscriptions DROP COLUMN IF EXISTS owner;
