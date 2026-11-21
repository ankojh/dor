# Publisher durability validation and rollout

Run this helper against a fresh dedicated NATS store and a copied database.
Defaults are NATS :4323, gateway :8183, admin :8194 and database `dor_f2_1`; no live
listener or live database is changed. See the ticket evidence for the carrier
and DB-copy commands. Rebuild `dor` before running the helper.

The helper seeds a legacy snapshot, starts the real publisher/gateway, injects
real DELTA DiscardNew errors, checks committed global sequence through the
read-only authority API, kills the publisher with a WebSocket connected, and
checks full state plus unchanged pending batch recovery on restart. The 5-second
public cache is an acceleration cache: its sequence need not equal the latest
committed root. `--capacity-only` measures encoded v2 base/root/chunk sizes;
that sizing command is not a sustained throughput or storage-plateau test.

For production upgrade, exclude **all** old publisher binaries and prevent
their supervisors from restarting them before starting the new publisher.
Old publishers do not honor the v2 owner root and are not safe mixed-version
writers. Gateways may run independently with `--agents gateway`.

The provisioning owner must create/update the non-expiring `publisher` bucket
before external clients bind. The v2 policy is file storage, History 1,
DiscardNew and a 1 GiB MaxBytes backstop; other buckets keep their own policies.
Check existing usage and available migration/housekeeping headroom before
applying that cap. If the old bucket has accumulated excess chunks or lacks
headroom, stop and use a reviewed offline recovery/cleanup procedure; never
lower a limit in a way that evicts the only authoritative state. Embedded
startup provisions the bucket automatically. No code merge authorizes changing
the live deployment or running a location repair.

v2 reads legacy full checkpoints and retains an immutable pending outbox during
migration. Missing or corrupt authoritative base/journal content fails closed.
Missing, corrupt, stale or oversized public cache content is recoverable from
the committed base and journal. Source ACK follows the authoritative journal/root
commit, independently of cache writes. Monitoring exposes publication errors,
storage duration/encoded bytes and current bucket bytes; backpressure starts
before the quota to preserve compaction/retirement headroom.

The lease uses wall-clock expiration with bounded I/O and assumes synchronized
hosts (the current takeover wait has 200 ms slack); it is not a server-clock lease.
Safety also requires owner/root CAS and expected raw DELTA sequence. A delayed
owner cannot commit or ACK after takeover even if a duplicate PubAck is returned.
NATS 2.15.3 standalone checks expected-last-sequence before msg-id dedupe, while
the clustered batching path may check dedupe first. Standalone embedded tests
do not establish clustered mixed-binary safety. Tests preserve reachable pending
state and check the owner commit fence independently of duplicate-check ordering.

The later cross-store location cutover, hot-evidence manifest and gateway resume
floor are separate work with F1·1/F2·2 dependencies; this helper does not apply them.

Storage descriptors and decompression are bounded: a base/journal blob may decode
to at most 64 MiB, with immutable chunks of at most 256 KiB. Exceeding a bound
returns an explicit error and retains the pending work without ACK; it is not
a promise of unlimited publication size. Retired generations retain a one-minute
reader grace. Loads retry a changed authoritative generation rather than return
partial state. The quota protects disk use; sustained capacity must still be
measured for the deployment's content and storage latency.
