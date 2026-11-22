# Dor

A real-time agentic news globe. Go agents on NATS JetStream ingest, place, classify,
cluster and link news stories; a SolidJS + MapLibre web app shows them on a 3D globe.

## Quickstart

Before upgrading or rolling back a gateway or a browser-facing web build, follow
the mandatory [gateway and web worker release gate](docs/operations.md#mandatory-gateway-and-web-worker-release-gate).
The gate applies to all-in-one and split deployments as well as externally
hosted web assets.

```bash
make dev                 # Postgres/pgvector :5433 + TEI :8090, waits for healthy
make run                 # build and run every agent; loads .env when present
curl -i localhost:8091/healthz
curl -s localhost:8091/metrics | grep dor_agent
open http://127.0.0.1:8080/ops   # Supervisor, agents, budgets, sources and actions
```

`/healthz` returns 200 only when the NATS client is connected and every agent
selected in this process reports running; otherwise it returns 503 and the body
names each agent's state. The embedded-NATS all-in-one above should reach 200
on a healthy machine.

`make run AGENTS=classify,plot` runs a subset of the agents.

### Keys and model backends

Copy `.env.example` to `.env`; `make run`, `make eval` and `make replay` load it
when present (the binaries do not read it themselves). Every key is optional:
without them the affected features fall back to their deterministic paths.

- `TYPESAFE_API_KEY` sends jev to `https://api.typesafe.ai/v1/systemone`
  (`jev-1.14.0`). `OPENROUTER_API_KEY` sends it to OpenRouter instead and wins
  when both are set.
- `DOR_LLM_BACKENDS=claude-cli,codex-cli` runs the Claude features on the
  logged-in Claude / ChatGPT subscriptions, with fallback between them; `api`
  uses `ANTHROPIC_API_KEY` and is the default when that key is set.

Publish a test message to the sample echo agent (`echo.in` → `echo.out`):

```bash
./bin/probe publish --subject echo.in --count 5 --prefix hello
./bin/probe collect --subject echo.out --expected 5
```

Split processes (one owns and provisions the bus, another runs one agent):

```bash
./bin/dor run --agents=none
./bin/dor run --agents=echo --nats-url nats://127.0.0.1:4222 --admin-addr 127.0.0.1:8092
```

For an external NATS server, make exactly one process the provisioning owner
with `--provision` / `DOR_PROVISION=true`; ordinary clients only bind and
validate the streams and KV.

Web shell (the data worker owns the gateway socket and resume loop):

```bash
make web-dev             # http://127.0.0.1:5173
```

Checks:

```bash
make test-race           # Go tests with the race detector
make test-echo-restart   # kill -9 the echo agent mid-stream; no duplicates
make ci                  # exactly what .github/workflows/ci.yml runs
```

Read [docs/conventions.md](docs/conventions.md) before adding an agent, subject,
proto field or make target. Open technical choices are in
[docs/decisions.md](docs/decisions.md). The M6 runbook (Supervisor controls,
`/ops`, split processes, record/replay, budget GC and the wrong-link queue) is
[docs/operations.md](docs/operations.md).

Replay a recorded day at 10× into the running stack:

```bash
DOR_RECORD_DIR=.dor/record make replay
# or explicitly
./bin/replayer -dir .dor/record -day 2026-11-20 -rate 10
```
