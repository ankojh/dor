# Production data worker

`index.ts` owns the binary gateway socket, decode, resume and health checks.
`state.ts` owns story/link maps, stable slots, CSR adjacency, filters, prompt
matches, the 1000-row wire ring and GPU array encoding. `messages.ts` defines
the production boundary. Protobuf messages never become main-thread maps.

- `init` is a full renderer snapshot, only for a gateway snapshot/resync.
  The worker transfers copies and retains its authoritative buffers.
- Main grants one `frame` credit per animation frame, with at most one request
  in flight, after the map is ready. The worker coalesces changes by slot until
  that credit. `patch` carries operations, encoded geometry ranges, heat/source
  arrays, changed match bytes, display summaries, wire rows and alert hits.
- Main's `LiveGlobe` queues patches with `PatchHold` during map movement and
  applies at most four groups per second, flushing on moveend. GPU storage grows
  by copying existing buffers; it does not reset focus or ripple data. The
  synchronous `FocusMirror` receives worker-built CSR when topology changes.
- A reused slot has `reset: true` even after multiple coalesced upserts. A normal
  metadata update preserves focus timestamps. Only moved/new arcs carry geometry.
- Shader, pulse, focus and ripple timestamps use seconds relative to the one
  worker session epoch, including across resync. This avoids float32 wall-clock
  precision loss and per-update clock resets.
- Story-supported filters and prompt matches dim instances without removing
  them. Source/language controls remain wire-only because Story has no source or
  language membership. Positive prompt evidence survives display-ring eviction;
  in-ring score corrections retract it. Cache state is pruned on prompt removal,
  story expiry and authoritative resync.
- Worker wire/pending-match rows are capped at 1000. App wire/pending rings are
  each capped at 1000 and evict their seen keys; text caches are LRUs of 128 entries.
  Alert replay identities are bounded at 1000 and preserve bigint generations.

`fake/` remains the development spike fixture and shared renderer layout types.
`perf-feed.ts` seeds 5k stories/2k links from that fixture and feeds 50 articles/s
plus structural bursts into the **production state and patch path**. It is opt-in
with `?perf=1`; normal runs use the gateway. `?mock=1` is also worker-owned.

For runtime diagnostics add `?diagnostics=1`: `window.__dorLive` exposes the map,
PerfMeter, counters, bounded patch timing samples and focus animation state;
`window.__dorApp.bounds` exposes ring/cache sizes. Shift-P shows real rolling FPS,
frame p95, long tasks, rAF JS time, focus-to-render latency and init/patch counters.

The main app waits for contentful paint before importing the runtime/MapLibre
(Paint Timing where available; two frames plus a task otherwise). Production
build inputs contain only index.html, excluding spike and deck.gl entry points.

Reproduce load/heap evidence with a private preview port (never live :5173):

```
pnpm build
pnpm exec vite preview --host 127.0.0.1 --port 5184 --strictPort
F31_URL=http://127.0.0.1:5184 F31_OUT=/tmp/dor-f31-final F31_MINUTES=60 pnpm exec tsx perf/production.perf.ts
```

The harness uses
headed system Chrome, records hardware/power/foreground/workload context, measures
live gateway traffic separately from synthetic input, checks continuous focus
bytes/ripples, and collects post-GC main and worker heap samples through CDP.
Run sustained performance evidence on confirmed AC power and document competing
workloads. Timing from a preliminary or different build is not final acceptance.
