# Alert delivery state

Alert JSON accepts `daily_digest` (default `false`) and `digest_hour_utc`
(default `0`, valid range 0–23). PATCH accepts both fields. Digests collect hits
until the next configured UTC hour; in-app alert deltas still appear immediately.
Duplicate inputs retain the hit’s first routing/day, even after preference changes.
The notifier freezes one summary per alert and UTC delivery day before sending.
Hits arriving after that day's batch froze move to the next day. The summary
lists a bounded selection of headlines and includes the total hit count.

The file-backed NATS `alert_delivery` KV bucket has **no TTL**. It retains
`pending.*` tasks, immutable `route.*` hit routing, `complete.*` hit tombstones,
`digest.*` frozen batches, `receipt.*` per-channel and
per-subscription delivery receipts, `rate.*` reservations, `lease.*` claims,
`hit.*` history and `hitstate.*` escalation state. Do not apply `live_state`'s
48-hour expiry to this bucket. A `match.alert` input is acknowledged only after
its idempotent pending-task write succeeds. A missing provider configuration or
send failure keeps the task pending with its error and exponential retry time;
there is no retry-count cutoff that silently deletes work. Deleting/disabling an
alert or muting its story intentionally cancels its pending sends.

Rate slots use KV revision comparisons across notifier replicas. A per-alert
lease serializes sends and digest freezing; attempts have a shorter deadline
than the lease. Successful channels and push subscriptions have independent
receipts, so failure of another channel/subscription does not resend them.
Receipts, batches and generation state currently have no automatic retention
cleanup; disk usage grows with notification history.

External delivery is at least once: if a destination accepts a notification and
the process crashes before its receipt is committed, that channel may receive a
duplicate on recovery. Neither ntfy publishing nor the Web Push sender here
provides a provider-side idempotency key. Receipt-store errors stop delivery;
they never count as proof that an earlier send did not happen.

An initial hit is generation 0. Every crossed doubling of its original source
count adds a generation (including multiple thresholds crossed in one update).
The generation participates in bus IDs, retained history, the database primary
key, queue IDs, channel receipts, push subscription receipts and push tags.
Without jev, matcher results use conservative embedding or lexical scores and
carry `approx: true`; these scores are similarity estimates, not probabilities.

Backtest reports `scanned`, `eligible`, `evaluated`, `from`, `to` and `partial`.
`would_fire` counts every evaluated match even though the story preview lists at
most 20. Backfill reports the same coverage fields in `matcher.backfill` in
`live_state` and logs them. Both stream prefix-filtered snapshots and apply their
actual time window; a deadline or read/publish failure marks coverage partial.
