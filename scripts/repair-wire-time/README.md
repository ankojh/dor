# Retained wire timestamp repair preparation

The command defaults to a read-only audit. It lists Postgres articles whose publication time is still in the future at the audit instant, then reads retained INGEST messages directly without creating a consumer. It matches canonical article identity, including hazard revisions, and reports candidate original fetch times.

```sh
go run ./scripts/repair-wire-time --database "$DOR_DATABASE_URL" --nats nats://127.0.0.1:4222 > wire-time-audit.json
```

Raw retention is limited: a matching event can be a later refetch, so a candidate is not proof. `created_at` is copied from publication and is not an independent clock. A later audit can miss legacy future timestamps that have since elapsed; retain and compare the initial census and available raw evidence. Each unknown or conflicting identity is marked for suppression, never substituted with the repair clock.

A rehearsal can write only to an explicitly named non-`dor` database with migration 0008 applied and requires the operator to verify every proposed candidate is the original fetch:

```sh
go run ./scripts/repair-wire-time --database "$COPY_DATABASE_URL" --nats "$COPY_NATS_URL" \
  --apply --allow-db COPY_DATABASE_NAME --confirm-candidates > wire-time-copy-result.json
```

In a serializable transaction the command moves publication and partition timestamps for verified candidates; it preserves unknown-time source records and writes an explicit suppression audit for each identity. Article details and the histogram exclude suppressed identities. Key conflicts or changed candidate rows abort the transaction. This copy-only database operation does **not** reconcile publisher checkpoints, journal, DELTA, gateway snapshots, cluster state, or live processes. Those require a separately fenced, coordinated reconciliation, including removing suppressed identities from retained wire and preventing stale replay. Cluster story timestamps use processing time in current code, but historical and hot state must be inspected before assuming they are unaffected. No live apply, restart, or cutover is authorized by this command.

## Deterministic maintenance plan export

A read-only plan can be exported without touching the live service:

```sh
go run ./scripts/repair-wire-time --database "$COPY_DATABASE_URL" \
  --nats "$COPY_NATS_URL" --plan-out /absolute/path/wire-time-plan.json
```

The plan binds the audit instant, the actual active publisher root/head/global/raw
revisions, the public snapshot revision, INGEST and DELTA stream tails, and each
affected article's complete before hash. `run_id` is SHA-256 over the canonical
plan with its run id and generation time cleared; appending another audit article
or changing a root revision changes the identity. An override is emitted only for
exactly one independently identified original raw fetch time when
`--confirm-candidates` is supplied; every other identity is an audited
suppression. `created_at == published_at` is never treated as independent
evidence. Export does not authorize or perform the live reconciliation.

## Implemented maintenance protocol

`internal/maintenance` implements the durable cross-store protocol referenced by
the release blocker:

- `maintenance/control` is the fleet fence and forward-only phase journal
  pointer. Once maintenance starts, a missing/corrupt control or floor record
  holds gateways unready; it never falls back to zero.
- `maintenance/floor` is the distinct, monotonic, non-expiring gateway baseline
  `(minimum_resume_seq T, bootstrap_stream_seq D)`. Gateways bootstrap the DELTA
  consumer at raw `D+1` and require a decoded snapshot at or beyond `T`; clients
  below `T` receive Resync plus the corrected snapshot even when retained
  history covers their old range.
- `maintenance/wire_time_policy` is the durable per-article override/suppression
  authority. The publisher applies it to restored state, redelivered input and
  future articles. DELTA has no dedicated removal event, so suppression is a
  forward-only tombstone (`at < 0`) filtered from snapshots, gateways and the
  web client.
- `maintenance/journal` and `maintenance/plan` make the drain, plan, DB repair,
  policy, forward publication, corrected snapshot, floor and admission phases
  crash-resumable. `maintenance.VerifyQuiesce` requires stable producer tails,
  every durable consumer's pending/ack-pending counters, no pending durable
  joins, an empty publisher outbox and an injected DB writer queue snapshot.
  Floor-aware gateways heartbeat into the short-lived `gateway_lease` bucket;
  the coordinator waits until every live heartbeat reports `serving=false`
  before claiming the publisher root.
- The forward repair writes through the existing `publication.Store`
  reserve/publish/commit path with fresh contiguous global and raw sequences.
  An uncertain outbox or PubAck is settled unchanged. Source consumers,
  positions and retained DELTA history are never reset or purged.

`maintenance.Apply` is the orchestration entry point. It accepts the
already-authorized copy-only DB callback and the in-process writer barrier.
Nothing in this repository automatically invokes it; live application still
requires a separate approval, a real quiesce, and an isolated rehearsal.

## Copy-only maintenance adapter

The audit command can run the complete protocol against an explicitly named
non-`dor` copy:

```sh
DOR_RUNTIME_ID=prod-1 DOR_GATEWAY_ID=gw-1 go run ./cmd/dor run ...
go run ./scripts/repair-wire-time --database "$COPY_DATABASE_URL" \
  --nats "$COPY_NATS_URL" --apply --maintenance-apply \
  --allow-db COPY_DATABASE_NAME --confirm-candidates \
  --expected-producer prod-1 --expected-gateway gw-1 \
  --expected-consumer ENRICH/publisher-enrich \
  --expected-consumer STORY/publisher-story \
  --exclusion-authority route-controller \
  --exclusion-evidence route-snapshot-42 \
  --exclusion-generation 42 \
  --drain-timeout 5m
```

Without `--maintenance-apply` (and without `--apply`) the command is read-only
and `--plan-out` exports a deterministic plan. `--apply` without
`--maintenance-apply` is the prior DB-only copy operation. The adapter:

- verifies `current_database()` equals `--allow-db` and refuses `dor`;
- pauses raw/enrichment/story/match/alert publishes and DB writer admissions in
  every runtime that advertises `--expected-producer`, and requires each
  producer/writer queue, in-flight flush and retry set to be empty;
- requires every expected gateway to advertise a fresh
  `serving=false`/wire-tombstone-compatible lease and every expected producer
  runtime to close raw ingress first, then downstream output after the stable
  finite drain; DB writer admission closes only after its queue/in-flight/retry
  set is empty;
- requires every expected durable consumer to be present and its pending and
  ack-pending count to be zero, with no pending durable joins;
- drains until two consecutive samples are stable, then settles an uncertain
  publisher outbox unchanged;
- binds the deterministic plan to the actual publisher root/live-snapshot
  revisions, current wire before hashes and `live_state/article.<id>`
  revisions/hashes;
- records a durable DB outcome (`started`/`committed`) and verifies the
  transaction by readback, so a DB commit and journal interruption resume
  without replaying an ambiguous outcome;
- reconciles hot article timestamps, applies the merged policy at normalize and
  cluster ingress so clean/placed redelivery preserves overrides/suppression,
  persists the policy, publishes forward-only wire corrections/tombstones,
  writes the corrected snapshot and activates the monotonic floor before
  admitting the fleet.

Expected participants that are stopped cannot be excluded by the structured proof
alone. `--excluded-producer`/`--excluded-gateway` require a deployment-specific
machine-verifying `ExclusionAuthority` adapter; none is wired in the embedded
target, so excluded participants fail closed here. Absent/unregistered or
incompatible participants block Apply and require upgrade/ack. The proof remains
audit data. The non-expiring control/floor/policy records survive repeated runs,
and a completed run answers only the same request identity.



## Release blocker: hot-state cutover remains approval-gated

Timestamp repair #4 remains a release/live-repair blocker. A successful audit or
copy-only DB transaction is not a safe hot-state cutover. The accepted
cross-store cutover direction is implemented as the `internal/maintenance`
protocol described above; this command itself does not invoke it and no live
apply is authorized. Functional closure requires these boundaries together:

- A finite drain and fleet fence must bind producer tails, consumer pending and
  acknowledgement floors, durable joins, DB-writer queues, and exclusion of all
  gateways/writers. Ordinary process shutdown or an empty consumer queue alone
  does not prove this barrier.
- A durable, revision-bound plan and resumable phase journal must bind the DB
  audit to the actual active KV/root before state and the identified copy.
  The current report has candidates and a DB `committed` flag, not those bindings.
  Settle uncertain publisher Pending bytes unchanged before forward repair;
  never edit, discard or renumber the outbox.
- Retained publisher state needs durable per-article suppression/time correction
  that also governs redelivered and subsequent clean/placed inputs. DELTA has no
  wire-removal event; deleting `Rows`/`WireOrder` alone does not remove
  `RecentWire`, reconcile clients, or prevent reintroduction. Reconcile wire,
  score/order metadata and affected hot article state without inventing a fetch
  time or deleting source/raw evidence.
- Every gateway needs a non-expiring monotonic resume/bootstrap baseline with
  distinct global sequence `T` and raw DELTA position `D`. Activate it only after
  forward durable repair commits, an empty outbox, and a corrected snapshot
  `U >= T`; require old clients to Resync and bootstrap from raw `D + 1`.
  Quiesce/re-admit the full fleet and invalidate warm state; a KV key or watch
  alone cannot fence old serving replicas. Missing/corrupt active control must
  fail closed, never fall back to zero.

Retain DELTA history and source consumer positions. No source reset/purge,
repair-time timestamp substitution, or old-checkpoint rollback is permitted.
The `internal/maintenance` adapter implements these controls, but this script
does not invoke it. Do not use either the script or the adapter to claim
timestamp repair completion or authorize live application without the separate
approval, rehearsal and fleet barrier.
