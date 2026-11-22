-- Preserve source articles while hiding retained rows with unknowable times.
CREATE TABLE IF NOT EXISTS wire_time_suppression (
    article_id text PRIMARY KEY,
    observed_published_at timestamptz NOT NULL,
    reason text NOT NULL,
    audited_at timestamptz NOT NULL
);
