# Conventions

Read this before adding an agent, a subject, a proto field or a make target.
The plan is the source of truth for product behavior; this file is the source
of truth for how the repo is wired.

## Layout

```
cmd/dor/                 one binary: dor run --agents=all|a,b,c
internal/agents/         agent interface, Base, registry, runner, metrics, tracing,
                         the dev echo agent and the rss/gdelt/hazard fetchers
internal/bus/            embedded NATS, subjects, stream/KV, durable consumers,
                         idempotent publish
internal/decide/         decide() interface, jev client, question builders
internal/llm/            Claude client, prompts, structured outputs
internal/budget/         model token buckets, daily spend cap, shed order
internal/sources/        loader and validation for deploy/sources/sources.yaml
internal/fetch/          source adapters (RSS/Atom, GDELT, USGS, GDACS, EONET,
                         ReliefWeb), polite HTTP, robots, host budget, health
internal/record/         record mode: raw-stream writer and reader
internal/geo/            GeoNames, Photon, H3                        (later)
internal/store/          pgx, migrations, batch writers              (later)
internal/gateway/        WebSocket gateway, snapshot, resume        (later)
internal/pb/dorv1/       generated Go protobuf (do not edit)
proto/dor/v1/            protobuf source
proto/testdata/          golden binaries written by the Go test
web/                     Vite + SolidJS + TypeScript
  src/worker/            data-worker entry (module worker)
  src/globe/             MapLibre + deck.gl (later)
  src/ui/                wire, panels, alerts (later)
  src/gen/               generated TypeScript protobuf (do not edit)
tools/probe/             dev publisher/counter used by test-echo-restart
tools/feedcheck/         live source verification (writes the seed-list report)
tools/proto-check.sh     proto drift check used by CI
scripts/                 dev/acceptance scripts (test-echo-restart.sh)
eval/                    labelled sets, labelling CLI, eval runner
config/                  thresholds, model pins and budget YAML
deploy/docker-compose.yml  Postgres/pgvector, TEI, Photon profile
deploy/postgres/init.sql   creates the vector extension on a fresh volume
docs/
```

Empty Go packages have a `doc.go` only, so imports and ownership are fixed
before implementation.

## Commands

| Command | What it does |
| --- | --- |
| `make dev` | `docker compose up -d --wait` for Postgres and TEI, then prints status |
| `make dev-photon` | also starts Photon (profile `photon`; first start downloads Andorra) |
| `make dev-down` | stop compose services, keep volumes |
| `make dev-reset` | stop and delete compose volumes |
| `make build` | build `bin/dor` and `bin/probe` |
| `make test` | `go test ./...` |
| `make test-race` | `go test -race ./...` |
| `make proto` | regenerate Go and TypeScript protobuf |
| `make proto-check` | generate into a temp tree and compare the full file set |
| `make proto-golden` | explicitly rewrite `proto/testdata` golden binaries |
| `make web-dev` | Vite dev server on 127.0.0.1:5173 |
| `make web-lint` / `web-format-check` / `web-typecheck` / `web-test` / `web-build` | web gates |
| `make test-echo-restart` | kill -9 the echo agent mid-stream and verify no duplicates |
| `make sources-check` | fetch and verify every source in the seed list (network; JSON to stdout) |
| `make ci` | exactly what `.github/workflows/ci.yml` runs |

`make ci` is the local evidence for a green CI. There is no remote yet.

## Ports

| Port | Owner |
| --- | --- |
| 4222 | embedded NATS in local mode (127.0.0.1 only) |
| 8080 | WebSocket gateway (reserved; later ticket) |
| 8091 | admin HTTP: `GET /healthz`, `GET /metrics` |
| 8090 | TEI (host) → 80 (container) |
| 5433 | Postgres + pgvector (host) → 5432 (container) |
| 2322 | Photon (profile `photon`) |
| 5173 | Vite dev server |

A Homebrew Postgres on 5432 and Redis on 6379 may already be running; do not
move Dor onto those ports.

## Environment and flags

Every flag has a `DOR_*` fallback. An explicit flag wins.

| Flag | Env | Default |
| --- | --- | --- |
| `--agents` | `DOR_AGENTS` | `all` (`none` runs the bus/admin only) |
| `--nats-url` | `DOR_NATS_URL` | empty → embedded server |
| `--nats-listen` | `DOR_NATS_LISTEN` | `127.0.0.1:4222` |
| `--nats-store` | `DOR_NATS_STORE` | `./.dor/nats` |
| `--admin-addr` | `DOR_ADMIN_ADDR` | `127.0.0.1:8091` (empty disables) |
| `--log-level` | `DOR_LOG_LEVEL` | `info` |
| `--max-ack-pending` | `DOR_MAX_ACK_PENDING` | `256` |
| `--provision` | `DOR_PROVISION` | `false` (embedded mode always provisions) |
| `--shutdown-timeout` | `DOR_SHUTDOWN_TIMEOUT` | `45s` |
| `--sources-file` | `DOR_SOURCES_FILE` | `deploy/sources/sources.yaml` |
| `--record-dir` | `DOR_RECORD_DIR` | empty → record mode off |
| `--http-user-agent` | `DOR_HTTP_USER_AGENT` | `DorBot/0.1 (+https://github.com/ankojh/dor)` |
| `--rss-shards` | `DOR_RSS_SHARDS` | `3` |
| `--rss-shard` | `DOR_RSS_SHARD` | `-1` (all shards in this process) |
| `--rss-concurrency` | `DOR_RSS_CONCURRENCY` | `8` |
| `--gdelt-file` | `DOR_GDELT_FILE` | `gkg` (`export` selects CAMEO events) |
| `--gdelt-max-items` | `DOR_GDELT_MAX_ITEMS` | `300` |
| `--gdelt-extra-domains` | `DOR_GDELT_EXTRA_DOMAINS` | empty |
| `--reliefweb-appname` | `DOR_RELIEFWEB_APPNAME` | empty (adapter disabled) |

Tracing: W3C `traceparent` travels in NATS headers. The exporter is a noop
unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set, then OTLP/HTTP is used.

Logging is `log/slog` JSON on stdout. One line per event, stable keys:
`agent`, `subject`, `msg_id`, `err`, `nats_url`, `agents`.

## Local vs split processes

- Local: `./bin/dor run --agents=all` starts an embedded NATS server with
  JetStream **file** storage under `./.dor/nats`; embedded mode is the
  provisioning owner.
- Split: one process owns the bus (`--agents=none`, or an external
  `--provision`), others attach and only bind/validate:
  `./bin/dor run --agents=echo --nats-url nats://127.0.0.1:4222`.
- One process per agent in production, same binary, external NATS. Run exactly
  one provisioning owner; ordinary clients never rewrite streams or KV. They
  log a warning on a policy mismatch and keep the server policy.

`/healthz` returns 200 only when the NATS client is connected and every
requested agent is healthy. An agent that cannot report health counts as
unhealthy. The body includes per-agent terminal counts. `/metrics` is
Prometheus text format. URLs in logs and `/healthz` are redacted: configured
server components are replaced in raw, quoted and URL-normalized form, with an
authority-only fallback for arbitrary URL text. Userinfo is removed; an `@` in
a path or query is not userinfo and is preserved.

## Subjects

Constants live in `internal/bus/subjects.go`. Do not type these strings
anywhere else.

| Subject | Producer → consumer |
| --- | --- |
| `article.raw` | fetchers → normalizer |
| `article.raw.priority` | hazard fetcher → normalizer (priority lane) |
| `article.clean` | normalizer → plotter/classifier/embedder |
| `article.place` | plotter → clusterer |
| `article.class` | classifier → clusterer |
| `article.vec` | embedder → clusterer |
| `story.changed.<0..15>` | clusterer/editor → relation judge/matcher |
| `link.changed` | relation judge → publisher |
| `article.placed` | clusterer → matcher |
| `match.filter.<client>` | matcher → gateway |
| `match.alert` | matcher → notifier |
| `delta.out` | publisher → gateway |
| `echo.in` / `echo.out` | dev sample only |

`StoryShard(id)` is FNV-1a 32-bit of the id mod 16. Always use
`StoryChangedSubject(id)` or `StoryChangedSubjectForShard(n)`.

Streams are per domain; the full table (subjects, storage, MaxAge, MaxBytes,
dedupe) is in [decisions.md](decisions.md#bus). Summary: INGEST, ENRICH, STORY,
ALERT, DELTA, MATCH (memory), DLQ and ECHO. All use Limits retention and
`DiscardOld`; `MaxBytes` binds before `MaxAge` under load. The processing
streams use a 20 min dedupe horizon; DELTA 10 min, MATCH 2 min.

Publishing always sets `Nats-Msg-Id`; an agent that can be retried must pass a
stable stage-namespaced id (for example `clean:<articleID>`). JetStream dedupe
is only a fast path inside the stream window: handlers must be idempotent by
article id, story id or link pair.

KV buckets (created by the provisioning owner):

| Bucket | TTL | Used for |
| --- | --- | --- |
| `seen` | 48 h | dedupe hashes |
| `joins` | 30 s | partial cluster joins with a deadline |
| `budgets` | 1 h | short-lived token buckets only; never daily spend counters |
| `spend` | 48 h | daily model spend, keyed by UTC day (`daily.YYYY-MM-DD`) |
| `live_state` | 48 h | hot stories/links for snapshot rebuilds |
| `source_health` | 7 d | one JSON health record per source (state, last success, counters, validators) |

`budgets` also holds the per-host fetch token buckets (`host.<host>`, host
sanitized to the NATS key charset), shared by every replica. `seen` also holds
the fetch stage's published raw ids (`fetch.<sha256>`, 48 h) so a restart does
not republish. See `docs/sources.md` for the fetch rules and metrics and
`docs/record-format.md` for `--record-dir`.

Feed sharding is `FNV-1a(feed id) mod --rss-shards`; a replica selects one
shard with `--rss-shard`/`DOR_RSS_SHARD` (`-1` runs all shards in one process).
**Stop all replicas to change `--rss-shards`**: the shard map is stable only
for a fixed N.

## Adding an agent

1. Add a file in `internal/agents/` (e.g. `normalize.go`).
2. Build it with `NewBase`:

```go
func NewNormalizer(b *bus.Bus, m *Metrics, log *slog.Logger) (Agent, error) {
    return NewBase(b, m, log, Spec{
        Name:          "normalize",
        Durable:       "normalize",
        FilterSubject: bus.SubjectArticleRaw,
        MaxAckPending: 512,          // 0 → bus default (256)
        AckWait:       30 * time.Second,
        Concurrency:   4,            // unordered worker pool; default 1
        RetryBase:     time.Second,  // NAK backoff 1s → RetryMax (2m)
        Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
            // ... produce article.clean ...
            // Return Terminal(err) for malformed/permanent failures.
            return out.Publish(ctx, bus.SubjectArticleClean, payload,
                bus.WithMsgID("clean:"+msg.ArticleID()))
        },
    })
}
```

- `FilterSubjects: []string{...}` consumes several subjects in one stream
  (`article.raw` + `article.raw.priority`, or place + class + vec).
- `Ordered: true` forces one worker/`MaxAckPending=1` and retries in place.
  Use it for `story.changed.<shard>` consumers so new never overtakes old.
  A permanent failure **must** be returned as `Terminal(err)`: a plain error is
  retried forever and, with `MaxDeliver=-1`, blocks the whole shard. Terminal
  writes the DLQ record and unblocks the next message.
- `Producer: true` + `Produce(ctx, out)` is the scheduled/fetcher shape: no
  input consumer, the loop must return on `ctx.Done()`.
- A plain handler error is retried with exponential backoff, unlimited
  (`MaxDeliver=-1`). `Terminal(err)` writes to `dlq.<agent>` with original
  subject/msg-id/error headers, waits for the PubAck, then terminates the
  input. A heartbeat starts as soon as a message is delivered—before it waits
  for a worker and through the DLQ write—so `AckWait` does not expire behind a
  saturated pool. Handler contexts are cancelled when the shutdown budget
  expires; `DrainTimeout` bounds consumer closure and worker completion.
- Shutdown stops pulling, waits for the consumer closed signal, drains queued
  callbacks and acks, then closes the bus. If the budget expires, callbacks are
  unblocked, `jobs` is still closed, workers are still joined, and the
  completion path continues in the background so `Stop` stays bounded. Do not
  do cleanup outside `Stop`.

3. Register it in `Builtins` (`echo.go` today).
4. `--agents=normalize` now selects it; `all` includes it.
5. Metrics are automatic: `dor_agent_in_total`, `dor_agent_out_total`,
   `dor_agent_errors_total`, `dor_agent_terminal_total`,
   `dor_agent_redelivered_total`,
   `dor_agent_handle_seconds{agent,outcome}` (p95 via
   `histogram_quantile(0.95, ...)`; buckets to 300 s),
   `dor_agent_queue_lag_seconds`, `dor_agent_pending`,
   `dor_agent_ack_pending`, `dor_agent_inflight`.
6. The framework extracts `traceparent` and `Dor-Article-Id` from the input and
   injects both into `out.Publish`, so one trace follows an article.
7. Add tests under `internal/agents/` using the embedded bus
   (`bus.Connect` with `ListenAddr: "127.0.0.1:0"` and `t.TempDir()`).

## Tests and local servers

Agent and bus tests use the embedded server with
`bus.Connect(... ListenAddr: "127.0.0.1:0" ...)` and `t.TempDir()` for the
store. nats-server treats `Port: 0` as "use the default 4222", so `bus` probes
a free port itself before starting the server; do not pass port 0 straight to
`server.Options`. Tests must not rely on Docker or network, and `go test -race
-count=1 ./...` should stay green when packages run in parallel.

## Protobuf workflow

- Source: `proto/dor/v1/dor.proto`, package `dor.v1`.
- Generated: `internal/pb/dorv1` (Go) and `web/src/gen` (TypeScript,
  protobuf-es v2). Both are committed.
- Tools are not installed globally: `@bufbuild/buf` and
  `@bufbuild/protoc-gen-es` are web dev dependencies; `protoc-gen-go` is a Go
  1.24 `tool` directive in `go.mod`.
- `make proto` regenerates in place. `make proto-check` generates into a fresh
  temp tree and compares the **full file set**, including missing and orphaned
  generated files; hand-written tests in generated directories are preserved.
  CI runs it.
- Never renumber or reuse a tag or enum value. Enum zero values keep the plan's
  names (`CATEGORY_OTHER`, `PRECISION_COUNTRY`, ...); buf lint's
  `ENUM_ZERO_VALUE_SUFFIX` rule is disabled for that.
- `internal/pb/dorv1/golden_test.go` writes fully populated binaries to
  `proto/testdata/` **only** with `go test ./internal/pb/dorv1 -update` or
  `make proto-golden`. A normal run compares and fails if the checked-in bytes
  are stale (`TestGoldenBytesAreCurrent`). `web/src/proto/golden.test.ts`
  decodes every one and checks every field. When you add a field, add it to
  both sides and update the goldens explicitly.
- NATS headers are case-sensitive. Use `bus.HeaderArticleID` and
  `bus.HeaderMsgID`; W3C trace headers are lowercase (`traceparent`) and the
  OTel propagator reads and writes them consistently. Do not hand-roll a
  header name.
- Client messages are wrapped in `ClientMessage`; server messages in
  `ServerMessage` (Snapshot / DeltaBatch / Resync).

## Web

Vite 7 + SolidJS 1.9 + TypeScript strict (`noUncheckedIndexedAccess`), pnpm,
ESLint flat config, Prettier, Vitest. The data worker is
`web/src/worker/index.ts`, loaded with
`new Worker(new URL("./worker/index.ts", import.meta.url), { type: "module" })`.
It posts `{type: "ready", ...}` after a protobuf round-trip; the shell renders
that line. Do not put data work on the main thread.

## Docker

`make dev` starts Postgres 16 + pgvector (host 5433) and TEI serving
`BAAI/bge-small-en-v1.5` (host 8090). Photon is behind the `photon` profile
because its index is large; `make dev-photon` starts it with an Andorra extract
by default (`PHOTON_REGION` overrides). All three have healthchecks and every
published port is bound to `127.0.0.1` only.

## Models, budgets and eval (M0·3)

| Package / dir | What it owns |
| --- | --- |
| `config/` | `thresholds.yaml`, `models.yaml`, `budget.yaml` and their loaders/validators |
| `internal/decide/` | the `decide()` interface, jev client, the four question builders and the code-side combinations |
| `internal/budget/` | token buckets, the daily USD cap, the shed order and the `budget`/`spend` KV adapters |
| `internal/llm/` | Claude client, prompts, structured-output schemas and the mock Messages endpoint |
| `eval/` | candidates, labels, labelling CLI, metrics, threshold selection and the report |

`decide` and `llm` both take a budget hook and fail closed: a missing key, a
refusal, an unknown answer type or a budget denial is never a negative answer.
The report is produced by `make eval`; without keys it is the mock report and
must keep its `MOCK BACKENDS — NOT A MEASUREMENT` first line. `make llm-smoke`
verifies prompt caching live and skips without a key.

Commands added:

| Command | What it does |
| --- | --- |
| `make eval` | run classify/plot/relate/match for jev and Claude; writes `eval/report.md` and `eval/report.json` |
| `make eval-collect` | rebuild `eval/data` candidates from public RSS (network; review the diff) |
| `make eval-label` | interactive labelling; requires `LABELER=human:<name>` or `agent:<model>` |
| `make llm-smoke` | two identical Haiku calls; prints the second cache read |

KV bucket added by M0·3: `spend` (48 h TTL) for daily spend, keyed by UTC day.
`budgets` remains short-lived token buckets only.
