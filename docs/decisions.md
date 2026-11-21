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
counters must not live here**), `spend` 48 h (daily model spend, added by
M0·3), `live_state` 48 h (snapshot rebuilds), `source_health` 7 d, and
`eval_pending` with no TTL for operator-reported wrong links awaiting an
explicit agent label. TTLs are constants in `internal/bus/bus.go` for now;
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

## Models and eval (M0·3)

**jev is pinned, fail-closed.** `config/models.yaml` pins `typesafe/jev-1.14`;
`config.ValidateModelPin` rejects `~typesafe/jev-latest` and any id without a
version suffix. A response records the served dated snapshot (`model`). A
refusal, a missing answer, a missing key or an unknown answer type is "not
answered" and never a negative. Thresholds are model-specific and are re-tuned
per pinned release.

**All jev meaning lives in instructions and criteria.** Question keys are never
assumed to reach the model. The four builders in `internal/decide` put the
question, the option definitions and the publisher text in `instructions` /
`criteria`, and append the documented prompt-injection guard to every
instruction that quotes publisher or user text. Combinations happen in code:
type only if related, precision and parties only if the primary place is
accepted, strength = score/3 × P(related) nudged by entity Jaccard, tiers
0.3 / 0.5 / 0.75.

**Budgets have two layers.** Short-lived token buckets per API live in the
`budgets` KV bucket (1 h TTL). Daily spend lives in the new `spend` bucket
(48 h TTL) keyed `daily.YYYY-MM-DD` in UTC, so the cap survives idle hours and
restarts; a process restart does not reset the day. Admission reserves the
estimated dollars and tokens and returns a `budget.Reservation`; `Record`
reconciles it. The shed order is briefs at 80%, why prefetch at 90% and thematic
links at 95% of `daily_usd`, with a hard deny at 100%; each threshold is
configurable. `make eval` uses `budget.NewMemory` with `eval_usd` as the cap, so
a paid run cannot exceed the eval budget; `eval_usd: 0` denies live evals. KV
updates are CAS on the bucket revision, so multiple processes no longer lose
updates.

**Claude goes through the beta Messages endpoint for both models.** The SDK is
`github.com/anthropics/anthropic-sdk-go/v2`; model ids are plain strings. The
beta endpoint is used even for Haiku so structured outputs, effort and the
server-side fallback share one code path. Effort is explicit (`low`) on both
models. Opus jobs send `fallbacks: "default"` with the
`server-side-fallback-2026-07-01` beta header; Haiku job builders never set
fallbacks. No thinking budget, no disabled thinking, no assistant prefill and no
forced tool choice. `stop_reason` is checked before content: `refusal` reads
`stop_details`, `max_tokens` is a typed error, anything unexpected is a typed
error. Retries are ours (`option.WithMaxRetries(0)`): 429/5xx/transport with
jittered backoff, never 400/401/403/404. The budget is checked before every
attempt.

**Prompt caching is designed, not assumed.** Every job has one byte-stable
system block with `cache_control: ephemeral` on it; the volatile item text is
only in the user message, and schemas are marshalled from deterministic maps.
Each production prompt is >3 KB (about 770+ tokens by the conservative byte/4
estimate), above the 512-token model minimum. Usage logs and eval tables record
`cache_read_input_tokens` and `cache_creation_input_tokens`. Live verification
is `make llm-smoke`; without a key it prints `skipped: no ANTHROPIC_API_KEY`.
The mock Messages endpoint simulates cache reads from a prefix hash, which
proves the plumbing, not the real cache.

**Eval labels are honest by construction.** The labelling CLI requires
`--labeler human:<name>` or `agent:<model>`. Agent labels default to
`provisional: true`; a non-provisional label whose labeler is not `human:` is
rejected by the loader. The committed seed is 30 articles and 30 pairs labelled
by `agent:deepseek-v4.1-flash` and marked provisional. The ~300 + ~300 human
labelling is a user follow-up; `eval/README.md` documents the flow. A pair that
is same-event coverage by two outlets is labelled `duplicate: true` and excluded
from the relation metrics, because clustering handles it in production.

**The eval gazetteer is deliberately minimal.** It matches canonical GeoNames
`cities15000` names plus country names, title matches first, with a
capitalisation requirement that removes the worst alternate-name collisions.
It is not a geocoder and has low recall for regions and for places not named
exactly. M2·1 replaces it; the plot numbers are bounded by its candidate recall.
The downloaded data lives in `eval/data/.cache/geonames` (gitignored).

**Match labels are derived.** `match.candidates.jsonl` is generated from the
seeded article categories: one prompt from the article's category (relevant)
and one deliberately distant category (irrelevant), both marked
`derived: true`. It is a smoke set for the match question, not a tuned alert
corpus.

**Threshold selection is code, not a spreadsheet.** `eval.SelectThresholds`
picks the lowest threshold on a 0.50–0.95 grid where accepted place picks reach
90% or relation types reach 85%, with at least 10 accepted cases. The classify
threshold is then chosen as the lowest value that keeps the projected union
fallback rate under 15%. A live run writes `config/thresholds.yaml`. A mock run
never selects and keeps the plan's starting values (classify 0.70, plot primary
0.60, party 0.65, relate 0.60, match 0.80), marked provisional.

**Mock mode exercises the real clients.** `make eval` without
`OPENROUTER_API_KEY` / `TYPESAFE_API_KEY` / `ANTHROPIC_API_KEY` starts an
in-process fake jev endpoint and a fake Claude Messages endpoint, then points
the real HTTP/SDK clients at them. The report's first line is
`MOCK BACKENDS — NOT A MEASUREMENT`, and it says so again in the caveats. This
is plumbing evidence only; it is never used to claim accuracy or to change a
threshold. The Claude price schedule in `config/models.yaml` is a provisional
estimate (the API returns tokens, not money); jev cost is `usage.cost` from the
response.

**Bus change:** `bus.KVBucketSpend` (`spend`, 48 h TTL) is new in M0·3 and is
the only addition to provisioning. M1·1 also touches agents/sources; this is
additive and does not change existing bucket policies.

## M0·3 fixup 1 (review findings)

**Wire schemas are sanitized and validated in code.** Structured outputs reject
JSON-Schema keywords such as `maxItems`, `maxLength`, `minimum` and `maximum`.
`llm.ValidateSchema` is the fail-closed gate: it accepts only the deliberately
narrow contract (six types, closed objects with every property required,
non-empty scalar enums, and the constraints handled locally), and rejects
`format`, `const`, `multipleOf`, `allOf`/`anyOf`/`oneOf`/`not`, `$ref`/`$defs`
and open objects. `llm.SanitizeSchema` strips the handled constraints before the
SDK call and moves them into the property description; `llm.ValidateValue`
checks the original constraints after decoding and returns a typed
`*llm.SchemaError`. Discrete strength is an integer enum `[0,1,2,3]`. The mock
Messages server rejects unsupported keywords and invalid structure with a 400,
like the real API. A wire test covers all seven real job builders.

**Budgets are atomic and exactly-once.** `Check` reserves the estimated USD in
the daily counter and the estimated tokens in the API bucket, and returns a
`budget.Reservation`. The daily document and each token-bucket document carry a
`pending` map keyed by reservation id. `Record` removes the daily entry and the
token entry in separate CAS updates; a missing entry is a no-op, so each store is
independently idempotent and a retry after a partial success completes the
unfinished phase. KV updates are compare-and-swap on the bucket revision with
bounded retry; the memory store holds one mutex across the whole
read-modify-write. Completed reservations are removed rather than accumulated.
Stale pending entries become eligible for garbage collection after
`pending_ttl` (15m default); their estimates stay charged. GC removals are
persisted in a standalone CAS when the admission or reconciliation write that
found them is denied, and `StaleCollected` counts only committed removals, so
CAS retries and cap-denied `Check` calls do not overcount. Pending maps can
still hold in-flight calls until `Record`, but expired entries no longer wait
for a later successful write to be pruned. The daily key is still the UTC day in
the 48 h `spend` bucket. Shed order: briefs at 80%, "why" at 90%, thematic links at
95%, hard deny at 100%; each threshold is configurable in `config/budget.yaml`.

**Links connect distinct events.** `related` is true for a direct or indirect
cause/response, or a specific shared condition or theme. Same-event coverage is
false (the Clusterer merges it) and broad/coincidental topic overlap is false.
The causal type is symmetric in A/B; direction is answered separately. An
indirect causal chain still has a direction; `unclear` is for thematic links or
when no direction is supported. Every
question is self-contained because the model answers them independently. The
Claude eval baseline uses the same semantics. `eval/testdata/relate_contrast.json`
holds same-event, specific-theme, broad-topic, indirect-chain and reversed-causal
contrast fixtures.

**Probability, not concentration.** Gates and metrics use the selected option's
probability: `probabilities[choice]` for a choice, `noul` for a noul, and the
level probability for a score. jev's `confidence` is a normalized concentration;
it is recorded as `mean_concentration` and never used as P. The mock jev returns
distinct probability and confidence values on purpose.

**Relation threshold selection uses raw answers.** It no longer drops
gold-unrelated pairs or reuses predictions gated by the current threshold. At
each candidate threshold it recomputes the accepted link set with the production
gate, includes false positives, and measures accepted type accuracy.

**The fallback projection is complete.** Every labelled article has a category
and place projection record. Errors, refusals, missing answers and missing ids
count as fallback and stay in the denominator. `CombinePlot` returns
`Fallback=true` for a missing, refused, unknown or invalid primary, a `none`
pick, a missing probability and a missing/invalid precision. `eval_usd: 0`
denies live evals; the preflight estimate covers only live backends and is
enforced whenever either backend is live; a backend that returns no usable
answer makes the CLI exit non-zero.

**Calibration has two kinds.** Event-probability calibration is P against
observed truth; decision-confidence calibration uses the confidence for the
answer that was given (never flipped). For jev decision calibration on a binary
question, that is p for a predicted yes and 1−p for a predicted no; Claude's
self-reported confidence already means P(this answer is correct), so it is used
directly, with relatedness AND type as the correct target for relate. Coverage
and strictness curves are recomputed from raw probabilities and labels (and
from the Claude decision observations for the Claude baselines). Relatedness
calibration is separate from type correctness.

**Accounting is complete.** Cost, input/output tokens, cache tokens, served
model and latency are recorded for every call, including paid refusals,
unusable answers, `max_tokens` and other typed stops (the typed stop error also
carries usage). Latency is total elapsed time including retries/backoff;
`last_attempt_latency` is the final attempt. `$/1k` divides total cost by
labelled items (N), not answered items.

**Retries cover the whole 5xx range.** Both clients retry 429 and 500–599,
including 524/529, plus per-attempt transport timeouts while the parent context
is alive. A cancelled or expired parent is never retried. TypeSafe direct is not
implemented: a `TYPESAFE_API_KEY` needs an explicit base URL, path and request
shape (`openrouter-decisions`) and otherwise fails early; the direct
`/v1/systemone` shape is not guessed. Push CI is mock-only and gets no secrets;
only manual `workflow_dispatch` runs live.
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
  O(degree), without waiting on a worker round trip;
- **(b)** control over street-zoom coordinate precision; historical alignment
  measurements were superseded by canonical-coordinate implementation changes
  and are not validation of the current build;
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

## Fetchers (M1·1, 2026-11-20; fixup 1, 2026-11-21)

**The source list is one reviewed YAML file, read at startup.** `deploy/sources/sources.yaml`
holds RSS/Atom feeds and the GDELT/hazard upstreams; `internal/sources` loads
and validates it, and each fetcher selects by `kind`. There is no database
dependency in M1·1; M1·2 seeds the same file into Postgres. `enabled: true`
requires `terms_ok: true` and a non-empty `terms_note` at load time, and source
ids are validated against the NATS KV key charset because they are also KV keys.

**Review by live check, not by trust.** `tools/feedcheck` fetches every source
with the production User-Agent and adapter. On 2026-11-20 it checked 364 RSS
candidates: 63 dead ones were dropped, 52 access-blocked ones stay disabled
with the reason, and 37 entries from 17 publisher hosts were disabled by the
terms decision. The committed `deploy/sources/feedcheck-2026-11-20.json` is the
evidence. This is not legal sign-off: a human review of the 212 enabled feeds
is a follow-up.

**Terms decision.** Publishers whose RSS terms are personal/non-commercial only
or whose content is paywalled are disabled: NYT, Washington Post, WSJ and
MarketWatch (Dow Jones), FT, Bloomberg, The Economist, TIME, The Atlantic,
Foreign Affairs, Foreign Policy, The Telegraph, The Globe and Mail, the New
Statesman, Nikkei Asia, MIT Technology Review and Wired. Every enabled entry
has a per-publisher `terms_note`: a verified terms URL where one exists (135
publisher hosts), otherwise "no published RSS terms found; robots.txt allows
the feed path; headline, snippet and link only, attributed".

**Robots is RFC 9309 with wildcards, and transient failures retry.** `*`
matches any sequence, a trailing `$` anchors the end, and the longest matching
pattern wins (Allow on a tie). A real Disallow blocks the path — ReliefWeb's
`/*/rss.xml` is why `reliefweb-updates` is disabled. A 4xx means "no robots".
A timeout, 429, 5xx or unreachable robots endpoint is a retryable fetch error:
it backs off and retries, never becomes a cached disallow and never quarantines
the host. The body is capped at 512 KiB and one singleflight request per host
serves every caller.

**Three RSS replicas, sharded in-process locally.** `FeedShard` is
FNV-1a(feed id) mod 3. `--rss-shard=-1` runs all three shards in one process
(local mode); a production process sets `DOR_RSS_SHARD=0..2`. Changing
`--rss-shards` moves feeds between replicas, so **stop all replicas first**
(or accept temporary double-publishing until the next restart); the `seen`
KV bucket and JetStream dedupe make the overlap harmless but the shard map is
only stable for a fixed N.

**Adaptive interval, with interval_s as a floor.** After each successful poll,
rate = new items since the previous successful poll / wall time since it,
smoothed with an EWMA (alpha 0.4); interval = 60 s / rate, clamped to
`[interval_s, max(300 s, interval_s)]`. The floor applies to every kind:
hazards keep their configured cadence and a ten-minute EONET feed is not
polled faster than that. Network failures back off 30 s → 15 min. Five
consecutive parse failures quarantine for 30 min; a robots or permanent-403
denial quarantines for the same cool-off without counting as a parse error.
Each feed owns a goroutine and timer, with a semaphore (8) bounding concurrent
fetches per shard, so a hanging feed holds one slot for at most one 20 s
timeout.

**Per-host token buckets in `budgets`, key `host.<host>`.** NATS KV keys allow
only `[-/_=.a-zA-Z0-9]`, so the host is sanitized (`:`, ports and anything
else become `_`). 1 request/s with a burst of 4, updated with KV CAS so three
replicas share the allowance. KV calls time out after 500 ms. If the KV is
unavailable the fetcher uses an in-process limiter and logs a warning once per
host per ten minutes: politeness must not become an ingest outage, and a silent
fallback is not acceptable either.

**No republish: source-scoped ids, a sliding seen set and KV persistence.** The
fetcher id is `raw:<kind>:<sha256(key)>` (`raw:rss:…`, `raw:gdelt:…`,
`raw:usgs:…`, …), so RSS and GDELT can carry the same URL and both survive
until the normalizer merges them. Each feed keeps a bounded seen set sized at
`max(256, 2 × observed window)` with a sliding 48-hour TTL. Published ids are
persisted in the `seen` bucket for 48 hours under `fetch.<sha256(rawID)>`, so a
restart does not republish. On a cold start an RSS item is published only when
`published_at` is within 48 hours; an undated item is accepted once. The
20-minute JetStream dedupe window is a fast path for a crash/restart, not the
correctness guarantee; the KV record and the downstream Normalizer are.

**GDELT consumes GKG by default.** GKG is document-centric and carries both
themes and extracted locations, which is what a raw article needs; the export
family (CAMEO events with action coordinates) is supported with
`--gdelt-file=export` and covered by fixtures; its event roots are mapped onto
the same theme allowlist (14 → protest, 13/15/16/17 → military, 18/19/20 →
armed conflict) so one policy works for both. Both are filtered by a domain
allowlist, a required coordinate and a theme-prefix allowlist, capped at 300
items per file, and marked `burst: true` so the wire can collapse them. GDELT
titles come from the GKG `<PAGE_TITLE>` extras, the location pick is the first
entry at the highest precision (GDELT's salience order), and GDELT is a
separate agent with its own goroutine and backoff: it can never block RSS. A
404 on a file named in `lastupdate.txt` (published before the bytes land) is a
transient backoff, never a quarantine.

**Hazards publish to `article.raw.priority`, keyed by revision.** USGS, GDACS
and EONET each get their own schedule and backoff in one hazard agent. The seen
key and msg id include the event revision (USGS `updated`, GDACS episode id +
alert level, EONET latest geometry date, ReliefWeb `date.changed`) while
`hints.event_id` stays stable, so an escalation or a revised magnitude is a new
message. Items older than 48 hours are not backfilled on a cold start. GDACS
severity is `max(alert-level map, alert score / 3)`. ReliefWeb is implemented
against API v2 but ships disabled: anonymous requests return HTTP 403
("You are not using an approved appname"); enabling it needs
`DOR_RELIEFWEB_APPNAME` and a terms review.

**Record mode is a raw-stream tee with its own writer goroutine.**
`--record-dir` writes `<dir>/YYYY-MM-DD/HH.pb.gz`: a `DORREC\0\1` header and
length-delimited `RecordFrame` protobufs inside one gzip stream. Frames are
enqueued on a bounded channel and dropped (counted separately from disk errors)
rather than blocking a fetcher; the writer flushes every 64 frames **or every
5 seconds**, and a `Record` after `Close` is a no-op. A publish that JetStream
acknowledges as a duplicate is not recorded (`Emitter.PublishAck` surfaces
`PubAck.Duplicate`). A restart opens a new `HH-N.pb.gz` segment instead of
appending, so a crash-truncated tail never corrupts later data; the reader
orders segments by hour then numeric suffix. The reader is
`internal/record.Reader`; the format is `docs/record-format.md`.

**One new KV bucket, `source_health` (7-day TTL).** It is provisioned by the bus
owner with the other buckets. Health records restore `last_success`,
`items_total`, the current-hour counters and the metrics on startup, so a
restart does not reset them. Per-source Prometheus series are accepted because
the ticket needs per-feed health and the cardinality is bounded by the ~300-row
seed list; the metric labels are source ids, never URLs or article ids.

**GDELT's FIPS country code is not mapped to ISO.** `RawHints.country` carries
only ISO 3166 codes; GDELT's location country code is FIPS 10-4 (`BG` is
Bangladesh, `PO` is Portugal), so it stays out of the hint and the full place
name travels in `RawHints.place` instead. Hazards put the upstream place name
in `place` and an ISO3 code in `country` when one exists (GDACS, ReliefWeb).

**Proto additions are append-only at the end of `dor.proto`.** `SourceKind`,
`RawHints`, `RawArticle` and `RecordFrame` are new messages/enum values; no
existing tag moved. `RawHints` gained `place`, `magnitude_unit` and `revision`
in fixup 1 (tags 12, 11, 13). Goldens exist on both sides.



## Normalizer and store (M1·2)

**The durable store is optional at startup.** `cmd/dor` opens
`DOR_DATABASE_URL` (default `postgres://dor:dor@127.0.0.1:5433/dor?sslmode=disable`),
pings with a short budget, runs the embedded migrations, and starts the batch
writer. A refused connection or failed migration logs a warning and the app
continues without durable writes; the normalizer still publishes
`article.clean`. `--db-url=""` (or `off`) disables the attempt.

**Migrations are embedded SQL pairs.** `migrations/0001_init.{up,down}.sql`
create `sources`, day-partitioned `articles` (with
`ensure_article_partition(date)`), `article_locations`, `stories` with
`vector(384)` and an HNSW cosine index, `story_locations`, `links`, `alerts`,
`alert_hits` and `push_subscriptions`. `internal/store.Migrate` tracks applied
versions in `schema_migrations`; `MigrateDown` rolls back newest first.

**Article identity is the SHA-256 of the canonical URL.** `CanonicalURL` strips
fragments, default ports, `www.`, trailing slashes, `/amp` and tracking
parameters, and sorts the remaining query. Exact duplicates are keyed
`norm:exact:<id>` in the 48 h `seen` KV bucket; near duplicates use four 16-bit
SimHash bands keyed `norm:sim:<band>:<value>:<id>` and a Hamming threshold
(default 3). Malformed items, exact duplicates and near duplicates are counted
in `dor_normalizer_dropped_total{reason}` and dropped. Clean articles are
published with `Nats-Msg-Id=clean:<id>`, and the raw `RawHints` and `burst` flag
are appended to the proto `Article` for downstream plotters.

**Writes never block a handler.** `store.Writer` has a bounded queue; `Enqueue`
drops and counts when full. A background goroutine flushes batches of 100 (or
500 ms) and retries transient Postgres errors. Upserts are idempotent by
`(article id, created_at)` and `(article_id, place_id, role)`.


## Enrichment and stories (M2)

**Three lanes run in parallel from `article.clean`.** `plot` writes
`article.place`, `classify` writes `article.class`, and `embed` writes
`article.vec`. Each message carries the clean `Article` snapshot, so the
clusterer can join without a second read. All three use their own durable
consumer and bounded concurrency.

**The join is a KV deadline, not a blocking wait.** Partial results are stored
at `joins join.<articleID>`; the clusterer is the single story writer and
processes an article when all three lanes are present or after 2 s with what
arrived. The 30 s TTL bucket is also swept after a restart. The clusterer
never holds a handler for the deadline.

**Fallbacks are explicitly marked.** Hazard/GDELT hints with coordinates are
the fast path. Without a loaded Gazetteer or Photon, the plotter uses hints,
dateline and country centroids and sets `degraded`/`fallback`; with no model it
never invents a confident city. Without TEI the embedder emits a deterministic
384-d feature hash marked `degraded=true`. Without jev/Claude the classifier
uses keyword rules, also marked degraded.

**Live state uses dots only.** The clusterer writes
`live_state story.<id>` as binary `Story`, `storymeta.<id>` as JSON with
`article_ids`, `source_ids`, `embedding`, `entities`, `first_seen`, `updated_at`,
`article.<id>` as binary `Article` for the editor, and `storymap.<articleID>` as
the story id. Frontend `Story` deltas never carry the 384-d vector or entities.
`story.changed.<shard>` publishes the full current `Story`; `article.placed`
stays an `Article` with append-only `story_id=11` and `join=12`. Stories are
one per event, merge on similarity/time/place guards, keep the primary rule
venue → dateline → first-named party, cap drawn parties at six and expire after
48 h without updates. Heat is refreshed on merge and expires with the state.
