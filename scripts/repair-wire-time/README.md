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
