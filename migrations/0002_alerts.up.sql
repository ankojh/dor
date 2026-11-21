-- M4: alert acknowledgement fields, hit titles and per-story mutes.
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS ntfy_topic text NOT NULL DEFAULT '';
ALTER TABLE alert_hits ADD COLUMN IF NOT EXISTS title text NOT NULL DEFAULT '';
ALTER TABLE alert_hits ADD COLUMN IF NOT EXISTS source_count integer NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS alert_mutes (
    alert_id   text NOT NULL,
    story_id   text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (alert_id, story_id)
);
