CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS sources (
    id          text PRIMARY KEY,
    kind        text NOT NULL,
    url         text NOT NULL,
    country     text NOT NULL DEFAULT '',
    region      text NOT NULL DEFAULT '',
    lang        text NOT NULL DEFAULT 'en',
    interval_s  integer NOT NULL DEFAULT 0,
    enabled     boolean NOT NULL DEFAULT false,
    terms_ok    boolean NOT NULL DEFAULT false,
    terms_note  text NOT NULL DEFAULT '',
    hazard      text NOT NULL DEFAULT '',
    notes       text NOT NULL DEFAULT '',
    health_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- articles is partitioned by the ingestion day. created_at comes from the
-- article's published_at (or fetched_at), so a redelivery lands in the same
-- partition and hits the same (id, created_at) conflict target.
CREATE TABLE IF NOT EXISTS articles (
    id           text NOT NULL,
    source_id    text NOT NULL,
    url          text NOT NULL,
    title        text NOT NULL,
    lede         text NOT NULL DEFAULT '',
    published_at timestamptz NOT NULL,
    lang         text NOT NULL DEFAULT 'en',
    dateline     text NOT NULL DEFAULT '',
    category     smallint NOT NULL DEFAULT 0,
    cat_conf     real NOT NULL DEFAULT 0,
    story_id     text,
    created_at   timestamptz NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE OR REPLACE FUNCTION ensure_article_partition(day date) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    part_name text := 'articles_' || to_char(day, 'YYYYMMDD');
    start_ts  timestamptz := (day::timestamp AT TIME ZONE 'UTC');
    end_ts    timestamptz := ((day + 1)::timestamp AT TIME ZONE 'UTC');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF articles FOR VALUES FROM (%L) TO (%L)',
        part_name, start_ts, end_ts
    );
END;
$$;

CREATE INDEX IF NOT EXISTS articles_published_idx ON articles (published_at DESC);
CREATE INDEX IF NOT EXISTS articles_source_idx ON articles (source_id, published_at DESC);
CREATE INDEX IF NOT EXISTS articles_story_idx ON articles (story_id) WHERE story_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS articles_url_idx ON articles (url);

CREATE TABLE IF NOT EXISTS article_locations (
    article_id text NOT NULL,
    place_id   text NOT NULL,
    name       text NOT NULL DEFAULT '',
    lat        double precision NOT NULL DEFAULT 0,
    lon        double precision NOT NULL DEFAULT 0,
    precision  smallint NOT NULL DEFAULT 0,
    role       smallint NOT NULL DEFAULT 0,
    conf       real NOT NULL DEFAULT 0,
    PRIMARY KEY (article_id, place_id, role)
);
CREATE INDEX IF NOT EXISTS article_locations_place_idx ON article_locations (place_id);

CREATE TABLE IF NOT EXISTS stories (
    id           text PRIMARY KEY,
    title        text NOT NULL,
    brief        text NOT NULL DEFAULT '',
    category     smallint NOT NULL DEFAULT 0,
    primary_loc  text NOT NULL DEFAULT '',
    source_count integer NOT NULL DEFAULT 1,
    heat         real NOT NULL DEFAULT 0,
    centroid     vector(384),
    entities     text[] NOT NULL DEFAULT '{}',
    first_seen   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS stories_expires_idx ON stories (expires_at);
CREATE INDEX IF NOT EXISTS stories_centroid_hnsw ON stories
    USING hnsw (centroid vector_cosine_ops) WITH (m = 16, ef_construction = 64);

CREATE TABLE IF NOT EXISTS story_locations (
    story_id  text NOT NULL,
    place_id  text NOT NULL,
    name      text NOT NULL DEFAULT '',
    lat       double precision NOT NULL DEFAULT 0,
    lon       double precision NOT NULL DEFAULT 0,
    precision smallint NOT NULL DEFAULT 0,
    role      smallint NOT NULL DEFAULT 0,
    weight    real NOT NULL DEFAULT 0,
    PRIMARY KEY (story_id, place_id, role)
);
CREATE INDEX IF NOT EXISTS story_locations_place_idx ON story_locations (place_id);

CREATE TABLE IF NOT EXISTS links (
    a          text NOT NULL,
    b          text NOT NULL,
    type       smallint NOT NULL,
    direction  smallint NOT NULL DEFAULT 2,
    confidence real NOT NULL DEFAULT 0,
    strength   real NOT NULL DEFAULT 0,
    why        text,
    updated_at timestamptz NOT NULL,
    decays_at  timestamptz NOT NULL,
    PRIMARY KEY (a, b),
    CHECK (a < b)
);
CREATE INDEX IF NOT EXISTS links_decays_idx ON links (decays_at);

CREATE TABLE IF NOT EXISTS alerts (
    id          text PRIMARY KEY,
    prompt      text NOT NULL,
    min_sources integer NOT NULL DEFAULT 1,
    region      text NOT NULL DEFAULT '',
    strictness  real NOT NULL DEFAULT 0.8,
    channels    jsonb NOT NULL DEFAULT '{}'::jsonb,
    enabled     boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alert_hits (
    alert_id text NOT NULL,
    story_id text NOT NULL,
    level    text NOT NULL DEFAULT 'new',
    fired_at timestamptz NOT NULL,
    PRIMARY KEY (alert_id, story_id, level)
);
CREATE INDEX IF NOT EXISTS alert_hits_story_idx ON alert_hits (story_id, fired_at DESC);

CREATE TABLE IF NOT EXISTS push_subscriptions (
    id         text PRIMARY KEY,
    endpoint   text NOT NULL,
    keys       jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
