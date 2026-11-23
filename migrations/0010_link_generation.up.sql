-- Durable per-pair generation fence for the SQL link projection. The links row
-- stays a clean current projection (no fake tombstones); this table remembers
-- the winning generation, including removals, so delayed writer batches from
-- older owners cannot overwrite or resurrect a newer pair state.
CREATE TABLE IF NOT EXISTS link_generations (
    a          text NOT NULL,
    b          text NOT NULL,
    generation bigint NOT NULL DEFAULT 0,
    removed    boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (a, b),
    CHECK (a < b)
);

-- Existing links predate generation fencing. They become generation 0 so the
-- first relation generation (>= 1) is allowed to take ownership.
INSERT INTO link_generations (a, b, generation, removed)
SELECT a, b, 0, false FROM links
ON CONFLICT (a, b) DO NOTHING;
