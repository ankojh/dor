# Sources

Every v1 source lives in [`deploy/sources/sources.yaml`](../deploy/sources/sources.yaml).
The fetcher agents read that file at startup, and M1·2 seeds the same file into
the Postgres `sources` table (plan §6). There is one registry, not two.

## The list

Reviewed 2026-11-20; terms review 2026-11-21. **284 entries: 158 enabled,
126 disabled.** Of the enabled entries, 154 are RSS/Atom feeds and four are
hazard/GDELT upstreams (GDELT, USGS, GDACS, NASA EONET). The disabled set is
125 RSS feeds plus ReliefWeb. Enabled RSS feeds are spread across regions:

| Region | Enabled RSS |
| --- | ---: |
| Global (international broadcasters, wires, science, climate) | 22 |
| Europe | 17 |
| East and South-East Asia | 19 |
| Africa | 31 |
| North America | 4 |
| Latin America and the Caribbean | 24 |
| South Asia | 17 |
| Middle East | 9 |
| Pacific | 2 |
| Central Asia and the Caucasus | 9 |

Each source has the plan's fields plus two review fields:

| Field | Meaning |
| --- | --- |
| `id` | Stable key, also the metric label and KV key. Never rename a live one. |
| `kind` | `rss`, `gdelt`, `usgs`, `gdacs`, `eonet`, `reliefweb`. |
| `url` | Absolute `https` URL. |
| `country` | Publisher country (ISO 3166-1 alpha-2). Not the event country. |
| `region` | Editorial region, used for balance reporting (RSS only). |
| `lang` | `en`; v1 ingests English only. |
| `interval_s` | Starting poll interval; the fetcher adapts it between 60 and 300 s. |
| `enabled` | Whether the fetcher may poll it. |
| `terms_ok` | Set only after a terms/robots review. `enabled: true` requires it; the loader rejects the combination otherwise. |
| `terms_note` | Why the terms decision was made. Required for an enabled source. |
| `notes` | The live feed-check result and any other review note. |
| `hazard` | Hazard family (`earthquake`, `multi`, ...) for hazard upstreams. |

`make test` validates the file through `internal/sources`, so a bad edit (dupe
id, plain `http`, missing country, enabled without terms) fails CI.

## How the list was reviewed

`tools/feedcheck` fetches every source with the production User-Agent and the
production adapter, checks `robots.txt`, and records the HTTP status, item count
and newest item:

```sh
go run ./tools/feedcheck -sources deploy/sources/sources.yaml \
  -out deploy/sources/feedcheck-2026-11-20.json
```

The committed [report](../deploy/sources/feedcheck-2026-11-20.json) is the
evidence. Dead candidates (404/DNS/timeout/HTML instead of a feed) were
dropped. Feeds that were access-blocked (robots disallow, 401/403/410/429/5xx)
or restricted by the terms review stay in the file as `enabled: false` with the
reason in `notes` or `terms_note`. Disabled entries are deliberately kept: they
document the review and give the next person a starting point.

### Terms decision

The orchestrator's terms review (2026-11-21) disabled **37 RSS entries from 17
publisher hosts** whose RSS terms are personal/non-commercial only or whose
content is paywalled: The New York Times, The Washington Post, The Wall Street
Journal and MarketWatch (Dow Jones), the Financial Times, Bloomberg, The
Economist, TIME, The Atlantic, Foreign Affairs, Foreign Policy, The Telegraph,
The Globe and Mail, the New Statesman, Nikkei Asia, MIT Technology Review and
Wired. They stay in the file as `enabled: false`, `terms_ok: false` with the
reason and terms URL in `terms_note`, so the decision is visible and reversible.

**Every enabled entry carries a per-publisher `terms_note`.** Where the
publisher publishes terms, the note cites the verified URL (136 publisher hosts,
each fetched live by the review tool); otherwise it says "no published RSS terms
found; robots.txt allows the feed path; headline, snippet and link only,
attributed". The note is the same for every feed from one publisher. The terms
review script is a one-off: re-run the equivalent checks when the list changes.

**This is not legal sign-off.** The checks establish that the publisher offers a
public feed, that robots.txt allows the path, and that only headline, snippet
and link are stored. A human legal review of the enabled list is a follow-up
before any public deploy. Sources whose terms or access are unclear stay
disabled.

## Fetching rules

- **Sharding.** Feeds are sharded by `FNV-1a(feed id) mod N` with `N=3`.
  Locally one process runs all three shards (`--rss-shard=-1`, the default);
  in production each process sets `DOR_RSS_SHARD=0..2` and `DOR_RSS_SHARDS=3`.
  Adding or removing a feed only moves that feed's shard.
- **Conditional GET.** ETag and Last-Modified are stored in the source-health
  KV record and replayed on the next request. A 304 counts as a healthy fetch
  and relaxes the interval to 5 minutes.
- **Adaptive interval.** After each successful poll, the new-items rate is
  measured over the wall time since the previous successful poll and smoothed
  with an EWMA (alpha 0.4): `interval = 60 s / rate`, clamped to
  `[interval_s, max(300 s, interval_s)]`. The source's `interval_s` is a
  **floor for every kind**, hazards and GDELT included; a ten-minute EONET
  floor is respected, and a quiet feed relaxes to five minutes rather than
  flipping between two fixed values.
- **Per-host token bucket.** A bucket per host lives in the `budgets` KV bucket
  under `host.<host>` (characters outside the NATS key charset are replaced
  with `_`, so `example.com:8080` works), 1 req/s with a burst of 4, shared by
  all replicas. KV calls time out after 500 ms; if the bucket is unavailable
  the fetcher falls back to an in-process limiter and warns once per host per
  ten minutes rather than silently.
- **Robots.** `robots.txt` is fetched and cached per host for an hour, capped
  at 512 KiB and shared by one singleflight request per host. Matching is RFC
  9309 with `*` wildcards, a trailing `$` anchor and longest-match precedence
  (Allow wins a tie). A real Disallow blocks the path. A 4xx means "no robots".
  A timeout, 429, 5xx or unreachable robots endpoint is a **transient fetch
  failure**, not a disallow and not a host quarantine: the next attempt retries
  after the normal backoff.
- **Timeouts.** 20 s per request, 64 MiB response cap, five redirects.
- **Backoff and quarantine.** Network/HTTP failures back off exponentially from
  30 s to 15 min. Five consecutive parse failures quarantine the feed for
  30 min, then retry; a robots/permanent denial quarantines for the same
  cool-off without counting as a parse error. Each feed has its own goroutine
  and timer, so one broken or hanging feed never delays the others; a semaphore
  bounds concurrent fetches per shard (8 by default).
- **No republication.** Each feed keeps a bounded seen set sized at
  `max(256, 2 × the observed feed window)` with a **sliding** 48-hour TTL (a
  hit refreshes it, so an item that stays in the feed stays remembered).
  Published raw ids are also persisted in the `seen` KV bucket for 48 hours
  under `fetch.<sha256(rawID)>`, so a restart does not republish. On a cold
  start an RSS item is published only when `published_at` is within 48 hours;
  an undated item is accepted once. Message ids are source-scoped —
  `raw:rss:<sha>`, `raw:gdelt:<sha>`, `raw:usgs:<sha>`, … — so the same URL
  from two source families survives and the Normalizer merges them later. The
  canonical article id is still hash(canonical URL) in M1·2.
- **User-Agent.** `DorBot/0.1 (+https://github.com/ankojh/dor)`, override with
  `DOR_HTTP_USER_AGENT`.

## GDELT

The GDELT agent polls `lastupdate.txt` every minute and downloads a new file
when the published name changes. The default family is **GKG** (document-centric
and carrying themes plus locations); `--gdelt-file=export` switches to the
event-centric export files. A file is kept only when all three filters pass:

1. the domain is on the source-quality allowlist
   (`internal/agents/gdelt_domains.go`, extend with `--gdelt-extra-domains`);
2. the row has a usable coordinate;
3. a theme starts with an entry in the relevant-theme allowlist
   (`ARMEDCONFLICT`, `NATURAL_DISASTER`, `PROTEST`, `ECON_`, ...). In export
   mode there are no GKG themes, so the CAMEO event root is mapped onto the
   same list (14 protest; 13/15/16/17 military; 18/19/20 armed conflict) and a
   raw `CAMEO_ROOT_*` theme is kept alongside it.

Each kept row becomes one `RawArticle` with `burst: true`, the GDELT
coordinates, precision, place name, themes and record id as hints. GDELT's
location country code is FIPS 10-4 (`BG` is Bangladesh, not Bulgaria), so it is
deliberately **not** copied into `hints.country`, which is reserved for ISO
codes; the full place name travels in `hints.place`. `--gdelt-max-items`
caps a file (300 by default) so a single burst cannot flood the wire. GDELT is
its own agent with its own goroutine: an outage backs off and retries the next
file without touching RSS.

## Hazards

USGS (all earthquakes, last hour), GDACS (7-day RSS) and NASA EONET (open
events, 7 days) publish to `article.raw.priority` with exact coordinates,
`PRECISION_POINT` (ReliefWeb is country-level), hazard kind, magnitude and
unit, a normalised severity for the heat boost, and the upstream place name.
The seen key and message id include the event **revision**: USGS `updated`,
GDACS episode id + alert level, EONET latest geometry date, ReliefWeb
`date.changed`; `hints.event_id` stays stable and `hints.revision` changes.
An escalation (Green→Orange→Red, a revised magnitude, a later PAGER alert)
therefore reaches the priority lane as a new message. New events older than
48 hours are not backfilled on a cold start. GDACS severity is
`max(alert-level map, alert score / 3)` on one consistent scale. USGS counts as point-precise because
the feed carries epicentre coordinates; the normalizer still decides the final
place.

ReliefWeb is implemented against API v2 but **ships disabled**: on 2026-11-20
anonymous use returns HTTP 403, *"You are not using an approved appname"*.
To use it, request an appname from ReliefWeb, set `DOR_RELIEFWEB_APPNAME`, and
flip `reliefweb-reports` to `enabled: true` after a terms review.

## Health

Every attempt writes one JSON record per source to the `source_health` KV
bucket (7-day TTL, key = source id): state, last success, last error, item and
error counts for the current hour, error rate, ETag/Last-Modified, next attempt,
quarantine end and the current interval. This KV bucket is the live health
source for `/api/health` and `/ops`.

Postgres `sources.health_json` is **not** a live mirror in this build: source
seeding writes `{}`, and no process updates the column afterwards. The durable
`GetSources` fallback therefore reports only `enabled`/`disabled`/`unknown`, not
the current fetch state. Read live health from the KV-backed endpoints.

Prometheus series (per source, ~300 sources):

| Metric | Meaning |
| --- | --- |
| `dor_source_state{source}` | 0 ok, 1 backoff, 2 quarantined |
| `dor_source_last_success_timestamp_seconds{source}` | last successful fetch |
| `dor_source_items_total{source}` | items published since process start |
| `dor_source_items_hour{source}` | items in the current hour |
| `dor_source_interval_seconds{source}` | current adaptive polling interval |
| `dor_source_fetches_total{source}` | attempts |
| `dor_source_errors_total{source}` | failed attempts |
| `dor_source_error_rate{source}` | errors / attempts this hour |

About 300 sources × 7 series is a few thousand samples. That is the point of the
ticket — an operator must be able to see which feed is broken — and the
cardinality is bounded by the reviewed list, not by URLs or article ids.

## Adding or retiring a source

1. Add it to `sources.yaml` with `enabled: false`, `terms_ok: false`.
2. Run `go run ./tools/feedcheck -sources deploy/sources/sources.yaml -kind rss`
   and look at that source.
3. Check the publisher's feed terms and robots policy, write the basis in
   `terms_note`, then set `terms_ok: true` and `enabled: true`.
4. `make test` (loads and validates the file).
5. To retire a source, set `enabled: false` and note why. Do not delete it
   while old articles still reference its id.
