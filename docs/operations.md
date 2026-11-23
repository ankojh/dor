# Operations: Supervisor, /ops and the replayer

This runbook covers the in-process Supervisor, the backend `/ops` page, the
wrong-link queue, `tools/replayer`, and the required gateway/worker release
gate. This repository does not provide a cloud deployment pipeline.

## Mandatory gateway and web worker release gate

The worker introduced with the sequence no-op fix requires a gateway that
preserves every global sequence slot. A legacy gateway drops filtered wire rows
and other sessions' private alerts; the new worker can then reconnect forever
from the same sequence. The gateway emits a fresh sequence-only delta for each
filtered event, on both live delivery and replay. A legacy worker accepts these
no-ops, so **upgrade every reachable gateway first; publish the new worker
only after all gateways pass verification**. For rollback, restore the legacy
worker first and keep compatible gateways until new workers can no longer reach
any downgraded gateway.

This is a **manual release hold**, not an automatic capability negotiation or
a gate implemented by `make`, CI, the gateway, or the browser. The release
operator must keep the new web artifact inaccessible on every public and
private delivery path until the checks below are signed off. A build is not a
release. If the operator cannot control and verify all paths, hold the release;
do not assume a rolling deployment is safe. Do not route a new worker to a
legacy gateway. If this hold cannot be enforced in a deployment environment,
do not release this worker there until capability negotiation or an equivalent
compatibility mechanism has been implemented and verified.

### Before publishing the new worker

1. Create a release record with operator, time, target environment, candidate
   gateway and web artifact identifiers (immutable commit and binary/image or
   asset digests), previous versions, and the planned rollback owner. Freeze
   gateway autoscaling, failover and automated rollbacks unless their templates
   are pinned to the verified compatible gateway. Reserve the new web artifact
   in an access-controlled staging location; disable automatic publishing,
   prefetch and promotion to any browser-accessible origin or CDN.
2. Inventory **all** gateway processes and routes a worker can reach: local
   all-in-one and split `--agents=gateway`/publisher hosts, replicas and standby
   hosts, load-balancer backends, alternate origins, regions, direct endpoints,
   failover routes, and any `VITE_WS_URL` overrides in deployed web builds.
   Include inactive instances and templates that can later receive traffic.
   Reconcile deployment/orchestrator inventory with load-balancer, DNS, proxy
   and origin configuration. Record each route, instance, immutable artifact
   identity and evidence source. Health checks and `dor version` alone do not
   establish compatibility; the current version string is not a capability
   identifier.
3. Upgrade or remove from **all** possible routing and failover paths every
   legacy instance, including stopped instances and launch templates. Verify
   each instance's running artifact against the approved build containing the
   no-op fix, using the deployment system's immutable digest or a trusted
   on-host binary hash mapped to the release record. Verify routing and
   templates again after changes and before sign-off. An unreachable, unknown,
   stale, newly discovered or unverifiable instance or route is a **hold**:
   keep the new worker unpublished until it is verified or demonstrably
   isolated from every worker path. Repeat inventory on any topology change.
4. On a controlled test route to each gateway instance and each externally
   reachable routing path, use test sessions and test data to check live and
   resumed delivery. With a wire filter enabled, arrange a filtered wire event
   followed by a matching public event. Also arrange an alert owned by a
   different session followed by a public event. Confirm sequence-only no-ops
   occupy the filtered slots, the subsequent public event reaches the worker,
   and the socket stays online without repeated `Hello(lastSeq)` at the same
   value. Reconnect from before the filtered slots and repeat on replay. Check
   that a deliberately omitted sequence causes recovery rather than being
   silently accepted. Use authorized test data and an isolated test setup;
   do not create test alerts or inject events into a live feed without separate
   authorization. A quiet feed, missing replay coverage, inability to target an
   instance, failed assertion, or repeated reconnect is a hold, not a pass.
5. Record the inventory, artifact hashes, route and live/replay smoke results,
   timestamps, exceptions and operator sign-off in the deployment's release
   system or an operator-maintained change record if none exists. A second
   operator verifies that every row and path is accounted for and signs the
   release hold off. Only then publish the new web artifact. Confirm the served
   HTML and worker asset digests through each origin/CDN path and repeat the
   filtered live/replay smoke for the newly served worker. If a verification
   fails or routing changes, stop promotion and keep or restore the compatible
   gateway fleet while investigating.

Review at `6e4c3ce` exercised binary protobuf frames: new gateway/new worker
and new gateway/legacy worker each applied public sequence 7 after a no-op at
6 on one socket; new worker/legacy gateway repeatedly resumed at 5. The latter
combination is prohibited by this gate, not made compatible by this change.
Retain that mixed-version evidence with the release record; rerun the checks
against the actual artifacts and routes rather than treating the review as
deployment verification.

### Rollback and cached or long-lived clients

1. Stop publication of the new worker, restore the legacy web artifact on
   every serving origin, and purge or invalidate CDN, proxy and browser-facing
   HTML and worker assets as applicable. Verify the legacy asset digests at
   every path, including failover. Check service-worker registrations and their
   cache behavior in the actual deployment: the repository's current push
   service worker has no fetch cache, but a hosting layer or other deployed
   service worker may. A cache purge or TTL alone cannot prove every offline or
   open browser has discarded the new worker.
2. Before downgrading **any** reachable gateway, either prove that every new
   worker session has terminated and cannot return from a cache, or enforce an
   isolation boundary that prevents *all* possible new workers from reaching
   any legacy gateway, including reconnects, direct routes and failover. For
   example, deny WebSocket access on all relevant routes and drain existing
   sockets while gateways are still compatible; maintain the denial until a
   verified safe client population and route policy make downgrade safe. A
   normal reload request, closing current connections, or waiting a guessed
   interval is not proof: open or offline clients can reconnect later. Record
   the boundary, its scope and evidence. It may cause an outage.
3. If the delivery and routing system cannot prove retirement or enforce that
   boundary, **do not downgrade the gateways**; keep the compatible gateways
   running while serving the legacy worker, and hold rollback at this point.
   Only after the boundary is verified and signed off may gateways be
   downgraded. Verify every route and template again, then smoke the legacy
   worker with filtered traffic on an isolated, operator-controlled route that
   cannot be reached by new workers. Releasing a WebSocket block requires the same
   proof that no new worker can reach a legacy gateway; otherwise retain the
   block or return to compatible gateways.

### Privacy and capacity

Filtered no-ops contain the sequence only: private alert content, owner and
alert/story/article identity, prompt and score are not included. Observers can
still infer aggregate hidden-event count, order and approximate arrival timing.
This is not a traffic-analysis privacy guarantee. Each hidden event also adds
per-client fanout and acknowledgement work; existing per-client limits do not
constitute a global connection or egress-byte quota.

## Local all-in-one

For any gateway or browser-facing web upgrade or rollback, complete the
[mandatory release gate](#mandatory-gateway-and-web-worker-release-gate) first.

```bash
make dev                       # Postgres :5433 + TEI :8090
make run                       # build + run all agents; embedded NATS :4222, gateway :8080, admin :8091
```

`make run` loads `.env` when present; copy `.env.example` to `.env` for keys.

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
  wire are never paused or shed. The budget shed order is briefs (80%), "why"
  prefetch (90%), then thematic link judging (95%); relation pairs with evidence
  of a causal or reaction link are admitted as default work and survive the
  thematic stage. A hard daily cap still denies every call at 100%.
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
store (or use a fresh stack) when the goal is downstream load. There is no
replayer flag that bypasses normalizer dedupe: the replayer only makes its own
publish ids repeatable/idempotent. For a split stack, start the stack with a
fresh store directory (or clear the `seen` and fetch-id keys) before the replay.
This is the normalizer doing its job, not a replayer failure. The replayer
itself never fetches an article body.

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

Before changing gateway processes or publishing web assets, complete the
[mandatory release gate](#mandatory-gateway-and-web-worker-release-gate).

Exactly one process is the provisioning owner and must run before the others.
On a clean machine let it start the embedded NATS server; when you already run
an external NATS server, point `DOR_NATS_URL` at it instead and keep
`--provision` on the owner.

```bash
# 1) provisioning owner only (embedded NATS on 4222 for a clean-machine run)
./bin/dor run --agents=none --provision --nats-listen 127.0.0.1:4222 \
  --admin-addr 127.0.0.1:8091

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

# 5) prompts/alerts evaluator and notification delivery
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=matcher --admin-addr 127.0.0.1:8131
DOR_NATS_URL=nats://127.0.0.1:4222 ./bin/dor run --agents=notifier --admin-addr 127.0.0.1:8132
```

Give the matcher and notifier processes the same `DOR_DATABASE_URL` as the
provisioning owner. The alert store then reloads saved alerts and records hits
in Postgres; without it the processes still run, but alert definitions live
only in the 48-hour live-state KV, so they can disappear on a cold start. The
delivery queue itself uses its dedicated non-expiring bucket. `notifier` also
needs the same `DOR_NTFY_URL` and VAPID/push-encryption variables as the gateway
if external notifications are configured. Missing channels are logged and the
delivery stays pending rather than being acknowledged as sent.

The gateway process loads the GeoNames gazetteer for `/api/places` itself. A
gateway-only process (`--agents=gateway`) therefore serves Cmd-K place search
even when no plot agent runs there; it still needs the same
`--geonames-dir`/`DOR_GEONAMES_DIR` cache. A process with neither the plotter
nor the gateway skips the gazetteer load.

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

## Relation generation rollout

Migration `0010_link_generation` adds the durable SQL fence for relation links and removals. Apply it before admitting writes from the new binary; normal startup runs migrations before constructing the DB writer. Stop and drain all old relation/DB writers before starting the new writer fleet. Old binaries bypass the generation fence, so a mixed old/new writer rollout does not provide the ordering guarantee.

For rollback, stop and drain the new writer fleet first. Returning to an old binary also removes the generation-ordering guarantee; retain the migration and its removal records until rollback and recovery requirements have been assessed. Code changes and test evidence do not authorize a live migration, service restart or deployment.
