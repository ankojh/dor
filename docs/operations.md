# Operations: Supervisor, /ops and the replayer

This is the M6 runbook. It covers the in-process Supervisor, the backend `/ops`
page, the wrong-link queue and `tools/replayer`. It does not describe a cloud
deployment pipeline.

## Local all-in-one

```bash
make dev                       # Postgres :5433 + TEI :8090
make build                     # bin/dor, bin/probe, bin/replayer
./bin/dor run --agents=all     # embedded NATS :4222, gateway :8080, admin :8091
```

Open <http://127.0.0.1:8080/ops>. The same page is also served by the admin
listener at <http://127.0.0.1:8091/ops>, which is useful in split mode where a
process does not serve the gateway.

Endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /ops` | Board D1-style operator page (served by the gateway and the admin listener). |
| `GET /api/ops` | The same data as JSON: agents, counters, lag, budgets, sources, actions, pending reports. |
| `POST /api/ops/control` | Narrow runtime controls (`restart`, `concurrency`, `gdelt.backpressure`). |
| `POST /api/report-wrong-link` | Queue one operator report for labelling. |
| `GET /api/report/wrong-link` | Pending reports (newest first, capped at 20). |
| `GET /api/health` | Existing source-health view on the gateway. |
| `GET /healthz`, `GET /metrics` | Existing admin health and Prometheus text. |

Runtime control examples (the ops page buttons use the same endpoint):

```bash
# pause / reduce / resume GDELT (0 normal, 1 cap 50 items/file, 2 hold new files)
curl -s -X POST localhost:8080/api/ops/control \
  -H 'Content-Type: application/json' -d '{"action":"gdelt.backpressure","value":2}'

# scale one unordered consumer (only when it reports scale_supported)
curl -s -X POST localhost:8080/api/ops/control \
  -H 'Content-Type: application/json' -d '{"action":"concurrency","agent":"normalize","value":4}'

# restart one agent with the normal in-process restart path
curl -s -X POST localhost:8080/api/ops/control \
  -H 'Content-Type: application/json' -d '{"action":"restart","agent":"plot"}'
```

A control the process does not expose returns HTTP 409 with the reason; the
action log records it as an advisory, never as a completed action.

The gateway and admin listeners bind to loopback by default. `/api/ops/control`
and `/api/report-wrong-link` are unauthenticated, so do not expose them on a
public interface without an authenticating proxy and rate limits.

The page reads real series; anything absent is shown as `—` and a banner names
the missing surface. It does not synthesise a rate, percentile, budget or
source state.

## Supervisor

The Supervisor is enabled by default in every process that selects at least
one agent (`DOR_SUPERVISOR=true`). It samples every 2 s.

| Action | Trigger | Control |
| --- | --- | --- |
| Alarm | queue lag > 5 s continuously for 1 minute | log entry + active alarm in `/api/ops` |
| Scale | queue lag > 2 s on an unordered consumer; 10 s sustained; 30 s per-agent cooldown | real worker-limit change in the process |
| Restart | agent reports stopped/error/unknown | real in-process restart with 5 s → 5 min backoff |
| GDELT reduce | max pipeline lag > 2 s for 20 s | GDELT caps each file at 50 items |
| GDELT pause | max pipeline lag > 5 s for 20 s | GDELT holds new files until lag < 1 s for 1 minute |
| Budget shed | daily spend reaches 80% / 90% / 95% | the budget manager enforces briefs → “why” → thematic; the Supervisor logs the transition |

Rules the implementation keeps:

- Scaling is only offered by unordered consumer stages. Ordered stages and
  producers report `scale_supported:false` rather than pretending to scale.
- Restart uses the durable consumer and idempotent writes; it does not promise
  recovery of state held only in a handler's memory.
- GDELT control is a real producer gate. RSS, plotting, classification and the
  wire are never paused or shed.
- The shed order is enforced by `budget.Manager.Check`, not by the page. The
  Supervisor only records the real stage transitions.
- If a control is absent, the action log says `advisory: true` and names the
  missing control. No action entry is written for an intervention that did not
  happen.
- The action log is bounded (200 entries by default).

Flags and env:

| Flag | Env | Default |
| --- | --- | --- |
| `--supervisor` | `DOR_SUPERVISOR` | `true` |
| `--supervisor-interval` | `DOR_SUPERVISOR_INTERVAL` | `2s` |
| `--supervisor-scale-lag` | `DOR_SUPERVISOR_SCALE_LAG` | `2s` |
| `--supervisor-alarm-lag` | `DOR_SUPERVISOR_ALARM_LAG` | `5s` |
| `--supervisor-alarm-for` | `DOR_SUPERVISOR_ALARM_FOR` | `1m` |
| `--supervisor-scale-cooldown` | `DOR_SUPERVISOR_SCALE_COOLDOWN` | `30s` |
| `--supervisor-gdelt-backlog` | `DOR_SUPERVISOR_GDELT_BACKLOG` | `20s` |
| `--supervisor-gdelt-recovery` | `DOR_SUPERVISOR_GDELT_RECOVERY` | `1m` |
| `--supervisor-max-concurrency` | `DOR_SUPERVISOR_MAX_CONCURRENCY` | `32` |
| `--supervisor-restart-min` | `DOR_SUPERVISOR_RESTART_MIN` | `5s` |
| `--supervisor-restart-max` | `DOR_SUPERVISOR_RESTART_MAX` | `5m` |

## Recording and replay

Record mode writes only `article.raw` and `article.raw.priority` frames; it
stores the payload the fetcher produced (headline, short summary, link) and
never an article body. The format is in [record-format.md](record-format.md).

```bash
# one fetcher process records while it runs
./bin/dor run --agents=rss --record-dir .dor/record

# list what was recorded
ls .dor/record

# dry run: read the latest day, pace at 10×, publish nothing
./bin/replayer -dir .dor/record -rate 10 -dry-run

# replay one day at 10× into the running local stack
./bin/replayer -dir .dor/record -day 2026-11-20 -rate 10

# unthrottled maximum-pressure replay
./bin/replayer -dir .dor/record -day 2026-11-20 -rate 0
```

The replayer:

- uses `internal/record.Reader`, so hour files and restarted segments are read
  in the recorded order (`HH.pb.gz` before `HH-2.pb.gz`);
- preserves the original raw/priority subject and payload;
- paces by each frame's recorded timestamp at N× and stops on Ctrl-C/SIGTERM;
- never starts an embedded NATS server: `-nats-url` must point at the running
  bus (default `nats://127.0.0.1:4222`);
- publishes with a stage-specific id
  `replay:<run-id>:<raw|raw-priority>:<recorded-msg-id>` and headers
  `Dor-Replay-Id` / `Dor-Replay-Source-Msg-Id`, so a replay is distinguishable
  from live ingest, repeatable idempotently within the dedupe window and
  traceable to the recording;
- reports duplicate acks separately; a repeated run is not a second live
  article set.

Replay into a stack whose `seen` bucket does not already contain those
article URLs. On a long-lived local stack the normalizer's 48 h dedupe can drop
a previously ingested day before it reaches the globe; reset the local NATS
store (or use a fresh stack) when the goal is downstream load. This is the
normalizer doing its job, not a replayer failure. The replayer itself never
fetches an article body.

`make replay` is a convenience wrapper; set `DOR_RECORD_DIR` and optionally
`DAY=... RATE=...`.

## Budget stale-pending GC

`PendingTTL` (default 15 m, `config/budget.yaml`) drops expired pending
reservations. The estimate stays charged, which is conservative. Collection is
now durable and counted only after a committed write:

- a late `Record` for an already-expired reservation still prunes the stored
  entries;
- a cap-denied `Check` prunes expired entries without releasing their charge;
- CAS retries do not overcount committed removals;
- `stale_collected` in `/api/ops` is the committed count for this process.

## Wrong-link queue

`POST /api/report-wrong-link` with:

```json
{"a":"story-id-a","b":"story-id-b","reason":"wrong event"}
```

writes one durable record to the `eval_pending` KV bucket (no TTL). The record
is marked:

```json
{
  "kind": "wrong_link",
  "status": "pending",
  "needs": "agent-label",
  "origin": "report-wrong-link",
  "labeler": ""
}
```

It never claims a human label. The `/ops` page shows the pending count and the
latest report. Labelling/export into `eval/data` remains a separate,
explicitly attributed step.

## Production split processes (one process per agent)

Use an external NATS server. Exactly one process is the provisioning owner and
must run before the others:

```bash
# 1) provisioning owner only
DOR_NATS_URL=nats://127.0.0.1:4222 \
  ./bin/dor run --agents=none --provision --admin-addr 127.0.0.1:8091

# 2) fetchers (three RSS shard processes; one GDELT; one hazard)
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=rss --rss-shard 0 --admin-addr 127.0.0.1:8101
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=rss --rss-shard 1 --admin-addr 127.0.0.1:8102
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=rss --rss-shard 2 --admin-addr 127.0.0.1:8103
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=gdelt --admin-addr 127.0.0.1:8104
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=hazard --admin-addr 127.0.0.1:8105

# 3) enrichment and stories
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=normalize --admin-addr 127.0.0.1:8111
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=plot --admin-addr 127.0.0.1:8112
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=classify --admin-addr 127.0.0.1:8113
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=embed --admin-addr 127.0.0.1:8114
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=cluster --admin-addr 127.0.0.1:8115

# 4) relation judge shards, editor, publisher + gateway
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=relation --relation-shards 3 --relation-shard 0 --admin-addr 127.0.0.1:8121
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=relation --relation-shards 3 --relation-shard 1 --admin-addr 127.0.0.1:8122
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=relation --relation-shards 3 --relation-shard 2 --admin-addr 127.0.0.1:8123
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=editor --admin-addr 127.0.0.1:8124
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=publisher --gateway-addr 127.0.0.1:8080 --admin-addr 127.0.0.1:8125
```

Operational notes for this profile:

- Every process serves `/ops` on its own admin address. The gateway process is
  the primary view at <http://127.0.0.1:8080/ops>.
- The Supervisor is in-process: it can scale and restart only the agents in its
  own process, and only the GDELT process owns the GDELT backpressure gate.
  There is no cross-process replica controller in this build. Use
  `--supervisor=false` on processes you do not want to supervise, and scrape
  every `/metrics` endpoint for a fleet view.
- The `eval_pending` bucket is new in this build. The provisioning owner must be
  the first process upgraded/restarted; older bind-only processes will fail
  validation until that bucket exists.
- Do not start a second server on 4222, 8080, 8091 or 5173 while the local stack
  is running. The replayer always attaches to an existing bus.
- Perf CI, eval CI, soak and chaos automation are not part of this operational
  build. The replayer and `/ops` are the runtime surfaces they will use.
