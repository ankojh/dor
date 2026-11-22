# Offline F1·1 geocoding audit and repair

This command uses the production geographic span matcher to audit both
location tables. It repairs invalid coordinates, alias/collision losers and
unsupported precision. Valid upstream hazard points are preserved. Country
fallbacks use a known capital; regions use an ADM1 coordinate, administrative
seat or largest supported populated place within that region. The command
requires all three existing GeoNames files; it never downloads a replacement
dataset. The exact source files are identified by SHA-256 in the audit/bundle.

Automatic matching excludes no-data region aliases that point at country
capitals; those aliases remain available to explicit lookup. A canonical
single-word city needs geographic syntax, or at least 50,000 population and
geographic uniqueness. A capital/administrative seat may also qualify when it
dominates foreign homonyms by at least five to one. A region's own same-named
seat is the same jurisdiction, not a foreign homonym. Person, club, commodity,
common-word and demonym evidence guards still apply. Countries can be actors
and possessives without a locative. Punctuated canonical abbreviations
(St./Ste./Ft./Mt./initials) remain supported. An equal-quality cross-country
multiword homonym resolves only to the exact canonical name of a capital or
administrative seat that dominates the foreign candidate five to one (Los
Angeles); otherwise it abstains and blocks its contained fragments (Santa
Cruz, San Jose, bare Santo Domingo).

The new-point admission threshold is not evidence that an existing canonical
point is false. A valid canonical title witness that passes the explicit
rejection guards is retained when disambiguation remains uncertain. The audit
records `needs_review` counts and complete `reviews` entries instead of an
unsupported deletion. This exception requires a valid coordinate already
within 1 km of that canonical point; invalid or geographically mismatched
coordinates need regular geographic evidence, never an ambiguous rescue. Stronger contradictory country/admin evidence still
replots it; invalid coordinates and unsupported precision are still corrected.
These unresolved retained points are not certified as accurate. They require
separate review and do not appear as destructive changes in the bundle. A
zero-change second run may still have `needs_review` entries; idempotency is not
geographic completion or permission to serve the proposal live.

## Audit and explicit apply

```sh
export DOR_DATABASE_URL='postgres://USER:PASSWORD@HOST:PORT/DATABASE?sslmode=disable'
go run ./scripts/repair-geocoding --geonames-dir /path/to/geonames \
  --dry-run --report /absolute/path/proposed.json
```

Dry-run is read-only and reports `status=proposed, committed=false`. The JSON
file contains all proposed decisions and a deterministic location repair
bundle. Neither its proposals nor a zero-change second run prove that a live
cutover is safe. Review replacement and retained-place frequencies and sample
article title, lede and dateline evidence.

Only an explicit `--apply --allow-db COPY_DATABASE` authorizes writes; `--dry-run=false` alone is an
error. Applying on an isolated copy is the rehearsal path:

```sh
go run ./scripts/repair-geocoding --geonames-dir /path/to/geonames \
  --apply --allow-db dor_f1_1_review --report /absolute/path/committed.json
```

The connected server's `current_database()` must exactly match `--allow-db`.
The live database `dor` is always refused, even when explicitly allowed. This
standalone command has no coordinated cutover gate; a DB-only live apply must
not bypass the future hot-state reconciliation.

Apply uses one serializable transaction with location/story table locks. It
deletes all rejected rows before inserting replacements, coalesces duplicate
votes with the strongest weight, and fixes `stories.primary_loc`. Before
commit it reads the complete after arrays and primary selections back from SQL
and checks them against the plan. An unconfirmed commit emits no committed audit; inspect the database before
retrying because a lost connection can leave the outcome unknown.
After success the audit is atomically replaced and reports `status=committed,
committed=true`. If writing the audit fails after a successful DB commit, the
error explicitly says the repair committed; do not treat that as a rollback.

## Bundle contract

`internal/geo/repair_bundle.go` defines `LocationRepairBundle` version1:

- `dataset_sha256` and deterministic `run_id` bind the source dataset and plan.
- Each affected story carries its DB location before hash, complete after
  locations, primary array index and place ID, and an explicit unlocated flag.
- Each affected article and every stored article attached to an affected story
  carries its DB location before hash, expected story membership, complete
  persisted after votes, and an explicit proposed event place/lat/lon/located.
- The event candidate uses the repaired story's primary, or its unchanged
  primary for an article-only repair. Persisted article votes may be empty
  while this event candidate is located. Every affected article has
  `needs_hot_evidence=true`; there is no final wire patch or serving permission.
- The future fenced planner must capture and bind actual clean Article/RawHints
  and current Wire hashes/revisions in a separate hot-evidence manifest. It
  preserves article-specific location versus story-fallback semantics; missing
  or unreconciled evidence and valid conflicting precise source/hazard hints
  block the plan for review. Empty DB votes never clear or synthesize RawHints.
  Source-country metadata, prompts, language, category, membership and ordering
  must remain intact. Absent/expired hot entities must remain absent.
- `ValidateLocationRepairBundle` checks structural correctness only; it never
  classifies hypothetical hints or resolves a final wire patch. Actual absent,
  invalid/rejected and valid-conflicting hint fixtures belong to the later
  maintenance-adapter rehearsal, not a completed capability of this exporter.
- Before hashes use sorted location values and the DB primary pointer, with
  exact numeric strings (including invalid old NaN/Inf). Run identity uses
  SHA-256 over the deterministic bundle with an empty `run_id` field.
- This exporter supplies DB hashes, **not KV/root revisions**. The future
  read-only maintenance planner must independently bind actual hot-state
  revisions and its own before hashes under the quiesce/fence barrier.
  The async DB mirror can drop queued story writes; the hot-state planner must
  recompute decisions from actual KV stories and article evidence, including
  stories present only in KV. DB bundle coverage is not hot-state coverage.
  `revision_binding=unbound_requires_fenced_hot_state_plan` is intentional.

## Live cutover is blocked

This command changes Postgres only. Cluster restart reads old `live_state`
KV and publisher restart restores its checkpoint/outbox, so ordinary restart
can undo a DB-only repair. Do not apply this repair live until the coordinator
has the approved, rehearsed offline cutover adapter and durable drain barriers.

The accepted ownership boundary is: drain finite queued work without drops;
close gateways and fence all writers; repair DB and reconcile active cluster
KV; forward-publish repair batches through the existing publisher durable
outbox with fresh global sequences; persist a gateway resume/bootstrap floor
and corrected snapshot; then resume. DELTA history is retained. Old clients
must Resync from the new baseline. Unknown outbox acknowledgements are settled
unchanged before repair. No source streams, messages or consumer positions may
be reset. Live cutover explicitly depends on F2·2 durable joins and a rehearsed
quiesce barrier; no temporary join-archive workaround is implemented here.
After forward repair publication, rollback must also publish forward
compensating updates; restoring an old publisher checkpoint would regress
sequence and is prohibited.

## DB-only backup and rollback rehearsal

These commands are for a stopped-writer copy or the DB phase before any
forward publication. Use PostgreSQL client tools matching the server major
version (the development server is PostgreSQL16). Keep writers paused for the
backup/apply/verification window so rollback does not discard newer votes.

```sh
pg_dump "$DOR_DATABASE_URL" --data-only --no-owner --no-acl \
  --table=public.story_locations --table=public.article_locations \
  > /absolute/path/locations-before.sql
psql "$DOR_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -c "\copy (SELECT id,primary_loc FROM public.stories ORDER BY id) TO '/absolute/path/primary-before.csv' CSV HEADER"
```

Run this in one matching-version `psql` session, substituting backup paths:

```sql
\set ON_ERROR_STOP on
BEGIN;
LOCK TABLE public.stories, public.story_locations, public.article_locations IN SHARE ROW EXCLUSIVE MODE;
DELETE FROM public.story_locations;
DELETE FROM public.article_locations;
\i /absolute/path/locations-before.sql
-- pg_dump imports can set search_path to empty. Restore it explicitly and
-- qualify persistent objects independently of that session setting.
SELECT pg_catalog.set_config('search_path','public, pg_temp',false);
CREATE TEMP TABLE restore_primary (id text PRIMARY KEY, primary_loc text);
\copy pg_temp.restore_primary FROM '/absolute/path/primary-before.csv' CSV HEADER
UPDATE public.stories s SET primary_loc=b.primary_loc FROM pg_temp.restore_primary b WHERE s.id=b.id;
COMMIT;
```

This restores location votes and primary pointers without altering story/article
text, links or alerts. A confirmed error before commit rolls back automatically and needs no
snapshot restore. A connection failure during commit can have an unknown
outcome; verify the planned after hashes before retrying or restoring. Once a forward publisher repair
has occurred, coordinate a forward compensating cutover instead.

## Verification

```sql
SELECT 'story_locations' AS table_name, count(*) AS invalid
FROM public.story_locations
WHERE (lat=0 AND lon=0) OR NOT (lat BETWEEN -90 AND 90)
   OR NOT (lon BETWEEN -180 AND 180)
UNION ALL
SELECT 'article_locations', count(*)
FROM public.article_locations
WHERE (lat=0 AND lon=0) OR NOT (lat BETWEEN -90 AND 90)
   OR NOT (lon BETWEEN -180 AND 180);
SELECT count(*) AS dangling_primary
FROM public.stories s WHERE primary_loc<>'' AND NOT EXISTS (
  SELECT 1 FROM public.story_locations l
  WHERE l.story_id=s.id AND l.place_id=s.primary_loc
);
```

Repeat the audit to check idempotency, then compare dominant retained and
replacement places and review evidence samples. This consistency check does
not substitute for geographic accuracy or state-cutover checks.

The unit tests run offline. Additional cached-dataset production plotter
regressions and isolated SQL transaction tests can be run with:

```sh
DOR_TEST_GEONAMES_DIR=/path/to/geonames go test ./internal/geo ./internal/agents
DOR_TEST_REPAIR_DSN='postgres://.../dor_f1_1_sql_test?sslmode=disable' \
  go test ./scripts/repair-geocoding
```
