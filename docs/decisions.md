# Decisions

Technical choices the plan left open, or where the environment forced a
specific option. New decisions go at the bottom with a date and the ticket that
made them. The plan remains authoritative for product behavior.

## Repo and toolchain

**Go module `github.com/ankojh/dor`, Go 1.26 toolchain.** `go.mod` says
`go 1.26.0` because the current NATS server/client and OpenTelemetry releases
require it; Go's toolchain auto-download makes this transparent on a machine
with Go 1.23. `protoc-gen-go` is pinned with a Go 1.24 `tool` directive, so
`go tool protoc-gen-go` works without a global install.

**One binary, one command.** `dor run --agents=...` is the only runtime entry
point. Local development uses the same binary with an embedded NATS server;
production runs one process per agent from the same binary against external
NATS. `--agents=none` is allowed and runs only the bus/admin, which is useful
for the restart test and for inspecting a bus.

**Uncommitted work by builders.** M0·1 was built on `v1-build` without commits
or branches; the orchestrator commits after review. Generated code and golden
binaries are checked in so a clean checkout builds and tests without network
protoc/buf installs.

## Bus

**Per-domain streams, not one pipeline stream.** Delivery domains have
different retention, capacity, isolation and dedupe needs. The provisioning
owner creates:

| Stream | Subjects | Storage | MaxAge | MaxBytes | Dedupe |
| --- | --- | --- | --- | --- | --- |
| `INGEST` | `article.raw`, `article.raw.priority` | file | 6 h | 2 GiB | 20 min |
| `ENRICH` | `article.clean`, `article.place`, `article.class`, `article.vec`, `article.placed` | file | 6 h | 2 GiB | 20 min |
| `STORY` | `story.changed.*`, `link.changed` | file | 48 h | 1 GiB | 20 min |
| `ALERT` | `match.alert` | file | 7 d | 1 GiB | 20 min |
| `DELTA` | `delta.out` | file | 10 min | 256 MiB | 10 min |
| `MATCH` | `match.filter.*` | memory | 2 min | 128 MiB | 2 min |
| `DLQ` | `dlq.>` | file | 7 d | 1 GiB | 20 min |
| `ECHO` | `echo.>` | file | 1 h | 128 MiB | 20 min |

All streams use Limits retention and `DiscardOld`. `MaxBytes` binds before
`MaxAge` under load. At the design load of 50 articles/s, a 2 KB article plus
fan-out is roughly 100 KB/s / 360 MB/h; INGEST and ENRICH therefore hold about
5–6 h at saturation, STORY about 1 GiB (a few hours at full rate, much longer in
practice), and the total local default cap is about 6.5 GiB. DELTA is
deliberately short: it exists to cover the 30 s client resume window with
headroom, then older clients resync from a snapshot. MATCH is memory-backed
because it only carries the last 2 min of per-client filter output. Sizes and
ages are per-stream configurable through `bus.Options.StreamPolicies`; the
legacy `StreamMaxAge`/`DuplicateWindow` fields override all streams for tests.

**Provisioning has one owner.** Embedded mode always provisions. An external
NATS deployment runs one explicit owner (`dor run --provision` / `DOR_PROVISION`)
or a future `dor bus provision`; ordinary clients—agents and `tools/probe`—
only bind and validate. A missing stream or KV bucket is a startup error with a
hint to start the owner. A policy mismatch (subjects, storage, MaxAge,
MaxBytes, dedupe, KV TTL) is logged at warn level and the server policy is
kept; ordinary clients never call `CreateOrUpdateStream` or
`CreateOrUpdateKeyValue`.

**Idempotency is by entity key, not by JetStream dedupe.** The bus sets
`Nats-Msg-Id` on every publish; `Nats-Msg-Id` dedupe is a fast path inside the
stream's duplicate window, not the correctness guarantee. Consumers must be
idempotent by article id, story id or link pair. The processing streams default
to a **20 minute** dedupe horizon (NATS accepts this; it is not a 2 minute
server maximum), and each stream can set its own. Output ids are namespaced by
stage (for example `clean:<articleID>`), never a bare entity id reused across
subjects. The echo sample derives `echo:<sha256-prefix>` from the input id.
A crash after the output PubAck and before the input Ack is therefore safe
inside the horizon; beyond it, the entity-key write is what prevents duplicate
state.

**Embedded server on `127.0.0.1:4222`, store `./.dor/nats`.** File storage is
required so killing the process does not lose durable consumers or unacked
messages. Only loopback is bound; there is no auth in local mode. A second
process attaches with `--nats-url nats://127.0.0.1:4222`. If 4222 is taken the
embedded server fails loudly; this is deliberate, so a stale server is never
silently reused.

**Local vs split processes.** `dor run` binds the embedded server and the admin
server to loopback by default (`127.0.0.1:4222`, `127.0.0.1:8091`). The brief
says `:8091`; loopback is the safer local default and `--admin-addr :8091`
exposes it when a container or another host needs to scrape it. The gateway
reserves `:8080` but is not implemented in M0·1.

**Deliveries never expire by attempt count.** Consumers use `MaxDeliver=-1`
(unlimited) and transient handler errors are re-NACKed with exponential backoff
from 1 s to a 2 min cap (`Spec.RetryBase`/`RetryMax`). A dependency outage
longer than ten attempts recovers without operator action. `Terminal(err)` is
the permanent path: the framework writes the original message to
`dlq.<agent>` with original subject/msg id/error headers, waits for the DLQ
PubAck, then `Term`s the input and counts it. A failed DLQ write re-NACKs so the
message is not lost. Long handlers send `InProgress` heartbeats at
`AckWait/8` (bounded), so a slow dependency does not trigger AckWait
redelivery storms.

**Bounded prefetch and concurrency.** The pull callback is unbuffered and the
client pulls one message at a time, so the client never buffers the old 500
default batch. `Spec.Concurrency` runs a bounded worker pool for unordered
stages (default 1). `Spec.Ordered` forces one worker and `MaxAckPending=1`, and
retries in place so a newer same-shard message can never overtake a failed
older one; use it for the shard consumers. `Spec.FilterSubjects` supports
multiple filters within one stream (`article.raw` + `article.raw.priority`,
`article.place` + `article.class` + `article.vec`). `Spec.Producer` is the
scheduled/polling shape for fetchers: no input consumer, a `Produce(ctx, out)`
loop, and the same health/metrics/shutdown contract.

**MaxAckPending default 256.** Configurable with `--max-ack-pending` /
`DOR_MAX_ACK_PENDING`; per-agent `Spec.MaxAckPending` overrides it. Ack policy
is explicit; `AckWait` defaults to 30 s. `NakWithDelay` drives retry timing;
the consumer's backoff list is intentionally not used so the client and
metrics agree on the delay.

**KV TTLs.** `seen` 48 h (dedupe window), `joins` 30 s (partial joins with a
~2 s deadline), `budgets` 1 h (short-lived token buckets only; **daily spend
counters must not live here** and M0·3 must persist them durably), `live_state`
48 h (snapshot rebuilds). TTLs are constants in `internal/bus/bus.go` for now;
a later config ticket can expose them.

**Subject shard function.** `StoryShard` is FNV-1a 32-bit mod 16, exactly as
the plan says. The 16 `story.changed.<shard>` subjects are bound by
`story.changed.*` in STORY.

## Agent framework

**`Base` + `Spec`.** Consumer agents supply `Handle`; scheduled agents supply
`Producer`+`Produce`. Both get lifecycle, durable consumer/loop, ack/nak,
metrics, tracing and health. The `Agent` interface (start/stop/consume/publish)
is satisfied by `Base`; the registry stores agents so `--agents` can select any
subset. `Builtins` is the one place to register new agents.

**Outcomes.** A plain error is transient (retry). `Terminal(err)` writes to the
DLQ, confirms it, then terminates. Panics are recovered and treated as a
transient failure; they never crash the process. In `Ordered` mode a plain
error retries in place forever (unlimited `MaxDeliver`), so a permanent failure
must be returned as `Terminal(err)` or it blocks that shard; the DLQ write and
Term unblock the next message. Every completed attempt is
observed in `dor_agent_handle_seconds{outcome="ok|retry|terminal"}` with buckets
through 300 s, so p95 includes retries and terminal work.

**Shutdown.** SIGINT/SIGTERM stops the admin server, then each agent stops
pulling, waits for the consumer's `Closed()` signal (the real drain completion)
and for worker completion within the shutdown budget, finishes and acks queued
handlers, and only then closes the NATS connection. The budget is the smaller
of the caller's Stop context and `Spec.DrainTimeout`; when it expires, handler
contexts are cancelled and the consumer is stopped. Structural cleanup is not
skipped: callbacks are fenced, `jobs` is closed, workers are joined and
`fullyStopped` closes even if the public Stop deadline has already expired (the
completion path continues in the background so Stop stays bounded). `Close`
waits for the connection's closed callback before shutting the embedded server
down. Unacked work is redelivered on restart. There is no WaitGroup reuse across
runs: an agent is single-use.

**Metrics.** `/metrics` serves one private Prometheus registry plus Go/process
collectors. Low-cardinality `agent` labels; `dor_agent_in_total`,
`dor_agent_out_total`, `dor_agent_errors_total`, `dor_agent_terminal_total`,
`dor_agent_redelivered_total`, `dor_agent_handle_seconds{agent,outcome}`,
`dor_agent_queue_lag_seconds`, `dor_agent_pending`, `dor_agent_ack_pending`,
`dor_agent_inflight`. Queue lag is server-authoritative: on each poll it reads
the consumer, and when work is outstanding it takes the first message after the
consumer's ack floor for each filter subject, using the oldest publish time. It
covers in-flight, NAK-waiting, undelivered and crashed-process ack-pending work
without a local clock, ages while stalled or retrying, and is zero only when
both pending counts are zero. `/healthz` reports terminal counts and treats an
agent that cannot report health as unhealthy.

**Tracing is a noop unless asked.** W3C `traceparent` (and baggage) travel in
NATS headers. Every handled message gets an `agent.handle` consumer span with
`dor.agent`, `dor.article_id`, `messaging.destination.name` and stream
sequence. `out.Publish` injects the active span and copies `Dor-Article-Id`, so
an article's trace follows fetch → globe. `OTEL_EXPORTER_OTLP_ENDPOINT` turns on
the OTLP/HTTP batch exporter; otherwise the global provider stays noop.

**Echo is a real agent, not a special case.** It is built from the same `Base`
and registered as `echo`; it exists to prove the framework end to end. Its
consumer `AckWait` is 5 s (instead of 30 s) so durable-resume tests and the kill
script finish quickly.

## Protobuf

**Buf as a web dev dependency, protoc-gen-go as a Go tool.** No global
install. `@bufbuild/buf` and `@bufbuild/protoc-gen-es` live in
`web/package.json`; `protoc-gen-go` is a `tool` in `go.mod`. `make proto` puts
`web/node_modules/.bin` on PATH and runs `buf generate`.

**Generated paths.** Go: `internal/pb/dorv1` via
`paths=import,module=github.com/ankojh/dor/internal/pb` and
`go_package=...;dorv1`. TS: `web/src/gen/dor/v1/dor_pb.ts` via protobuf-es v2
(`target=ts`). Both are committed and checked by `make proto-check`.

**Zero enum names follow the plan.** `CATEGORY_OTHER`, `PRECISION_COUNTRY`,
`ROLE_VENUE`, `LINK_TYPE_CAUSAL`, `DIRECTION_A_TO_B` are the zero values.
Buf lint's `ENUM_ZERO_VALUE_SUFFIX` is disabled; all other STANDARD lint rules
apply.

**Types the plan referenced but did not sketch.** `LinkKey{a,b}`;
`AlertHit{alert_id, story_id, level, fired_at, title, prompt}`;
`JoinKind{JOIN_KIND_NEW=0, JOIN_KIND_JOINED=1}`; `Snapshot{seq, stories[],
links[], built_at}`; `DeltaBatch{deltas[]}`; `Resync{seq, reason}`;
`ServerMessage` = oneof(snapshot, deltas, resync); client messages
`Hello`, `SetFilters`, `SetPrompts`, `Focus`, `GetText`, `Ack` wrapped in
`ClientMessage`. All plan-sketched field numbers are unchanged.

**Golden files are the cross-language contract.** The Go test writes
deterministic binaries for every message and oneof arm into
`proto/testdata` **only** with `go test ./internal/pb/dorv1 -update` or
`make proto-golden`; a normal test run compares the checked-in bytes and fails
if they are stale. Vitest decodes each and asserts every field. That catches
wire drift in either language, which a same-language round-trip cannot.

**Proto drift checks the full generated set.** `make proto-check` generates
into a fresh temp tree (`buf generate -o "$tmp"`), compares every generated
file byte for byte, and fails on missing or orphaned generated files.
Hand-written tests in generated directories are preserved and ignored by the
comparison.

## Web

**Vite 7, SolidJS 1.9, TypeScript 5.9, Vitest 3, ESLint 9.** These are the
current stable lines that work together; newer majors (Solid 2, Vite 8,
TypeScript 7, Vitest 5) exist but would add migration risk to a scaffold
ticket. TS is strict with `noUncheckedIndexedAccess`. Generated code is
excluded from lint/format and regenerated by `make proto`.

**Worker contract.** `web/src/worker/index.ts` is loaded as a module worker.
It builds a `Story` with generated code, round-trips it through the protobuf
runtime and posts `{type:"ready", protoStoryId, storyBytes, category}`. The
shell shows that line. A Vitest test checks the pure builder; the headless
browser check is part of acceptance.

## Deploy

**Postgres 16 + pgvector on host 5433.** `pgvector/pgvector:pg16` is
multi-arch. Homebrew Postgres already owns 5432, so compose maps 5433 and
credentials are `dor/dor/dor` for local only. Every published compose port is
bound to `127.0.0.1` (Docker otherwise publishes on 0.0.0.0 and IPv6):
`127.0.0.1:5433`, `127.0.0.1:8090`, `127.0.0.1:2322`.

**TEI CPU on host 8090.** `ghcr.io/huggingface/text-embeddings-inference:cpu-1.8`
serves `BAAI/bge-small-en-v1.5` (384-d). The image publishes only amd64, so
compose sets `platform: linux/amd64`; on Apple Silicon it runs under Docker
Desktop's Rosetta emulation. This was chosen over a native Python embedding
sidecar because the plan names TEI and the CPU image is fast enough for 384-d
batches. Measured startup and embedding output are recorded below once
verified.

**Photon behind the `photon` profile, Andorra by default.** The official
`ghcr.io/komoot/photon` image denies anonymous pulls in this environment; the
maintained `rtuszik/photon-docker` image is multi-arch (arm64 native) and wraps
the same Photon jar. A full planet index is ~60 GB compressed, so it is not a
default service. The default region is `andorra` (a small prebuilt index),
`PHOTON_REGION` overrides it (`france-monacco`, `germany`, ...), updates are
disabled locally, and the first `make dev-photon` downloads the extract. If
that download is unwanted, leave the profile off; plotting can use the
in-memory gazetteer until a Photon ticket needs it.

**Healthchecks everywhere.** Postgres `pg_isready`; TEI `GET /health` via
curl; Photon `GET /api?q=andorra` via curl. `make dev` uses
`docker compose up -d --wait`, so it only returns when default services are
healthy.

## Verification notes

- TEI `cpu-1.8` is `linux/amd64` only and ran under Docker Desktop Rosetta on
  the M3 Max. `make dev` reached healthy in about 60 s on the first start
  (model download), and `POST /embed {"inputs":"dor smoke test"}` returned one
  384-dimensional vector. `/health` returns 200.
- Postgres 16 + pgvector initialises `vector` 0.8.2 from
  `deploy/postgres/init.sql` on a fresh volume.
- Photon uses `rtuszik/photon-docker:latest`, which publishes arm64. It is
  behind the profile; keeping `make dev` to Postgres + TEI avoids a
  multi-hundred-MB download on every clean machine.
- `make dev-photon` with the default Andorra region reached healthy in about
  6 s after the image pull, and
  `GET http://127.0.0.1:2322/api?q=andorra&limit=1` returned the country
  feature. The container image is `linux/arm64`.
- Reviewer probes after the fix: `TestReviewerSerialPrefetchRedeliversHealthyHandlers`,
  `TestReviewerLagAdvancesWhileConsumerStalls`,
  `TestReviewerTwoMinuteWindowIsNotServerMaximum` and
  `TestReviewerAttachingClientPreservesServerPolicy` pass. The delay probe is
  intentionally outside the horizon (it sleeps `DefaultDuplicateWindow+1s`);
  `TestPublishBeforeAckCrashInsideHorizonDoesNotDuplicate` covers the
  inside-horizon crash. The outage, old-buffer drain and unordered-reorder
  probes encode the pre-fixup behavior; the equivalents are covered with a
  short configured retry base, `Concurrency: 16`, and `Ordered: true`.
- `make ci`, `make test-echo-restart`, `dor run --agents=all` + `/healthz` and
  `/metrics` were re-run fresh after the topology and framework changes.
- Round-2 probes (`TestReview2*`) all pass: saturated-pool heartbeat, ordered
  backoff, cooperative-handler shutdown cancellation, retry-wait lag, component
  URL redaction, throughput, ordered Terminal unblocking, and same-durable crash
  dedupe.

## Globe renderer (M0·2, 2026-11-20; fixup 1)

**Custom WebGL layer on the MapLibre GL JS v5 globe.** The first decision note
gave a false cause: deck.gl 9.4.1's `ArcLayer`/`LineLayer` **do** render on the
globe. deck's `_GlobeView` defaults to back-face culling, and
`parameters: {cullMode: 'none'}` makes arcs and lines render (reviewer repro;
verified by this tree's `?renderer=deck` page). The decision does not rely on
that failure.

We choose the custom layer because:

- **(a)** focus is exactly the plan's model: per-instance focus texels plus a
  200 ms shader fade, written synchronously by a main-thread CSR mirror in
  O(degree), so keys and clicks land in one frame;
- **(b)** street-zoom precision is verified and ours (on-screen max
  ~2 × 10⁻⁵ px at z 15–17; frozen frames pixel-identical);
- **(c)** all encodings are in one shader set (one arc buffer with strength,
  dashes, pulses, glow; GPU ripple ring buffer; tethers; brackets), with no
  fragile shader injection into another layer;
- **(d)** deck + luma (~220 KB gzip) stay off the app path against the 150 KB
  initial-JS budget; the deck page is a lazily imported dev-only comparison;
- **(e)** no interleaved depth z-fighting with MapLibre's globe.

**Costs:** we own GPU picking, resource lifecycle and about 1.5k lines of
GLSL/TS; there is no WebGPU path; swapping renderers later is a rewrite. deck
is dev-only (`@deck.gl/*` in devDependencies, dynamic import only for
`?renderer=deck`). No deck performance comparison is claimed: the earlier table
measured a full-rebuild integration and was not like-for-like.

**Focus and picking.** The worker ships its CSR adjacency; `FocusMirror` keeps
a lit set on the main thread, resets every old lit index that is not in the new
thread, and writes the new focus bytes synchronously. Stale/dropped worker
patches cannot leave ghost focus. Clicks use a per-frame GPU id pass and a
one-pixel readback (occlusion-correct; no CPU loop over stories). MapLibre
`flyTo` owns flights (interruptible, gesture-cancelled); Esc returns to ambient
without moving the camera. Reusable GPU data uses 2D textures (width ≤ 2048),
pre-allocated capacity, a free-list, sub-range writes, and full disposal.

Full method, numbers, screenshots and caveats:
[docs/globe-spike.md](globe-spike.md).
