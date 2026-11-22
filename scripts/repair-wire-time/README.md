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

## Release blocker: hot-state cutover is not implemented

Timestamp repair #4 remains a release/live-repair blocker. A successful audit or
copy-only DB transaction is not a safe hot-state cutover. The
[accepted cross-store cutover direction](../repair-geocoding/README.md#live-cutover-is-blocked)
describes required future work, not an available adapter. Functional closure
requires these boundaries together:

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
This script implements none of these maintenance controls and must not be used
to claim timestamp repair completion or authorize live application.
