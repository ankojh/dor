# Repair F1·1 geocoding

This command uses the current gazetteer evidence rules to audit both location
tables. It repairs invalid coordinates, rejects unsupported automatic place
matches, and caps city/region/country precision. It preserves valid upstream
hazard coordinates, even when no gazetteer ID exists.

The command defaults to a read-only dry run. Set the connection string without
printing its password in logs:

```sh
export DOR_DATABASE_URL='postgres://USER:PASSWORD@HOST:PORT/DATABASE?sslmode=disable'
go run ./scripts/repair-geocoding --geonames-dir /path/to/geonames --dry-run
```

Use `--report /path/to/audit.jsonl` to save each proposed change (old place,
replacement, reason). The summary reports scanned, flagged, replotted,
downgraded and removed row counts for each table.

```sh
go run ./scripts/repair-geocoding --geonames-dir /path/to/geonames --apply
```

An apply runs in one serializable transaction and locks the location tables and
stories against concurrent writes. It deletes rejected votes before inserting
replacements, retains the stronger weight if votes collide, and repairs
`stories.primary_loc` to reference a remaining location, or the empty string.
A second run should flag zero rows. Unrelated story fields are preserved.

For rollout, the coordinator must ensure the old plotter/clusterer and their
hot state cannot overwrite repaired database rows. This command changes only
Postgres; it does not alter NATS KV, snapshots, or running processes. Apply it as
part of the fixed binary's normal rollout and cache rehydration.

The repair uses all stored article titles, ledes and datelines attached to a
story, plus the story title. No model/API keys are needed. Country hints from
the publisher are deliberately not reconstructed from a rejected town. When
article text cannot support a replacement, it removes the dot and leaves the
story/article available without a location. A coordinate outside the gazetteer
is preserved when valid; historical Photon street/point rows are capped to city
because their feature type was not stored.

Verify both tables after applying:

```sql
SELECT 'story_locations' AS table_name, count(*) AS invalid
FROM story_locations
WHERE (lat=0 AND lon=0) OR NOT (lat BETWEEN -90 AND 90)
   OR NOT (lon BETWEEN -180 AND 180)
UNION ALL
SELECT 'article_locations', count(*)
FROM article_locations
WHERE (lat=0 AND lon=0) OR NOT (lat BETWEEN -90 AND 90)
   OR NOT (lon BETWEEN -180 AND 180);

SELECT count(*) AS dangling_primary
FROM stories s
WHERE primary_loc<>'' AND NOT EXISTS (
  SELECT 1 FROM story_locations l
  WHERE l.story_id=s.id AND l.place_id=s.primary_loc
);
```

Offline regression fixtures include thirty headlines copied verbatim from the
live database on 2026-11-21 and a small unmodified GeoNames extract. To repeat
those fixtures against the entire cached GeoNames dataset:

```sh
DOR_TEST_GEONAMES_DIR=/path/to/geonames go test ./internal/geo -run TestLiveHeadlineRegressions
```

## Backup and rollback

Use PostgreSQL client tools matching the server major version (the dev server
is PostgreSQL 16). Keep writers paused for the backup/apply/verification window
so that restoring the snapshot will not discard newer location votes. Capture
both location tables and the primary pointers before applying:

```sh
pg_dump "$DOR_DATABASE_URL" --data-only --no-owner --no-acl \
  --table=story_locations --table=article_locations > /absolute/path/locations-before.sql
psql "$DOR_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -c "\copy (SELECT id,primary_loc FROM stories) TO '/absolute/path/primary-before.csv' CSV HEADER"
```

To undo a completed apply, run this in a single `psql` session with writers
still paused, replacing the two backup paths:

```sql
\set ON_ERROR_STOP on
BEGIN;
LOCK TABLE stories, story_locations, article_locations IN SHARE ROW EXCLUSIVE MODE;
DELETE FROM story_locations;
DELETE FROM article_locations;
\i /absolute/path/locations-before.sql
CREATE TEMP TABLE restore_primary (id text PRIMARY KEY, primary_loc text);
\copy restore_primary FROM '/absolute/path/primary-before.csv' CSV HEADER
UPDATE stories s SET primary_loc=b.primary_loc FROM restore_primary b WHERE s.id=b.id;
COMMIT;
```

This restores the location snapshot and primary pointers without changing
article text, story text, links or alerts. An error before the repair's own
commit rolls its transaction back automatically; no restore is then needed.
