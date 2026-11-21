> Integration note (2026-11-21): implementation changed after the last captured performance/precision evidence. Historical reports below are not validation of the current build. The user waived further test/review/performance gates; those measurements remain unverified follow-ups.

# Globe renderer spike (M0·2, fixup 1)

MapLibre GL JS v5 globe plus our WebGL layers, the shared data worker, focus,
picking and the measurement harness. The spike page is `web/spike.html`; the
main app entry is unchanged.

## Decision

**The custom WebGL layer stays.** The first version of this note claimed deck.gl
could not draw arcs on MapLibre's globe. That was wrong, and the correction
matters:

> deck.gl 9.4.1's `ArcLayer`/`LineLayer` **do** render on the globe. deck's
> `_GlobeView` defaults to back-face culling; passing
> `parameters: {cullMode: 'none'}` makes arcs and lines render. The reviewer
> reproduced it, and this tree's `?renderer=deck` page now shows the 2k arcs on
> the globe (`docs/img/spike/deck-globe-focus.png`).

We choose the custom layer for these reasons, not for a deck failure:

- **(a) Focus is the plan's model exactly.** A focus switch writes per-instance
  focus texels and the shader fades them over 200 ms. A main-thread CSR mirror
  makes the write O(degree) and synchronous in the input handler, so the new
  state lands on the first presented frame from the input event's own
  timestamp.
- **(b) Street-zoom precision is verified.** The anchored double-precision
  vertex path matches MapLibre's own projection to ~2 × 10⁻⁵ px on-screen at
  z 15–17, and frozen frames are pixel-identical.
- **(c) All encodings live in one shader set.** One arc buffer with strength,
  dashes, pulses and glow; a fixed GPU ripple ring buffer; surface tethers;
  brackets; no fragile shader-string injection into someone else's layer.
- **(d) Bundle budget.** deck + luma is about 220 KB gzip. It is off the app
  path: `@deck.gl/*` are devDependencies, `spike.ts` imports the deck renderer
  lazily only for `?renderer=deck`, and the main entry bundle is unchanged.
- **(e) No interleaved depth z-fighting.** Our layer draws screen-space
  primitives with MapLibre's globe clipping instead of sharing deck's depth
  buffer with the basemap.

**Costs we accept:** we own GPU picking, buffer/texture lifecycle and roughly
1.5k lines of GLSL/TypeScript; there is no WebGPU path; a future renderer swap
is a rewrite.

deck.gl is now a **dev-only, lazily loaded comparison page**. There is no deck
performance comparison in this note: the earlier numbers measured a
full-rebuild integration and were not like-for-like. The comparison page exists
to verify the cullMode finding and to keep the option visible.

## How to run

```bash
cd web
pnpm install --frozen-lockfile
pnpm dev --port 5174 --strictPort          # http://127.0.0.1:5174/spike.html
pnpm perf:spike                            # headed Chrome, random private preview port
```

Query parameters: `renderer=custom` (default) / `renderer=deck` (lazy chunk),
`mercator=1`, `still=1`, `debug=1`.

Controls: `↑`/`↓` focus, `Esc` ambient (no camera move), `D` 2nd degree,
`⇧P` perf overlay, drag/scroll/ctrl-drag to pan/zoom/rotate, click a dot to
focus it.

The harness starts its own Vite preview on a random free port and stops it; it
never reuses a foreign server. Headed Chrome with the GPU is required
(`channel: "chrome"`).

## Provider and style

- OpenFreeMap, keyless: TileJSON `https://tiles.openfreemap.org/planet`, glyphs
  `https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf`.
- Our labels-only style: water fill, coastline/waterway hairline, country /
  city / suburb / **street** names (`transportation_name`) and **landmark**
  names (`poi`). No road lines, buildings, landuse or terrain.
- Provider behind `VITE_TILES_URL`, `VITE_GLYPHS_URL` and a full
  `VITE_TILE_STYLE_URL` override. Attribution comes from the same config:
  “© OpenFreeMap · © OpenMapTiles · © OpenStreetMap contributors”.
- Pinned: `maplibre-gl 5.24.1`; `@deck.gl/* 9.4.1` are devDependencies.

## Architecture

```
web/src/worker/fake/    5k stories, 2k links, CSR adjacency, ripples, typed
                        arrays; transferables at most once per frame ack
web/src/globe/
  focus-mirror.ts       main-thread CSR mirror, lit set, O(degree) focus writes
  encoding.ts           dot/arc/focus/ripple encodings (pure)
  ripples.ts            fixed 20-slot buffer, merge, never truncate
  gestures.ts           ≤ 4 data applies/s during user gestures
  camera.ts             MapLibre flyTo/easeTo, no queue
  gpu-layout.ts         2D texture layout + capacity free-list (pure)
  custom/               CustomLayerInterface: shaders, 2D float textures,
                        capacity buffers, id-pass picking, disposal
  deck/renderer.ts      lazy dev-only comparison (cullMode none)
  labels.ts             story names through MapLibre glyphs
  perf.ts               fps / p95 / long tasks / js-per-frame
  spike.ts              page bootstrap, HUD, input, telemetry
web/perf/spike.perf.ts  headed-Chrome measurement harness
```

### Focus: synchronous, ghost-free

The worker sends its CSR adjacency (`adjStart`, `adjList`, `adjLink`) in the
init message (about 36 KB). `FocusMirror` keeps a **lit set** of stories and
links. On input it:

1. walks the focused story's adjacency once (O(degree));
2. builds the new story/link states and HUD;
3. resets every previously lit index that is not in the new thread, then
   writes the new states;
4. returns only the changed bytes; `spike.ts` writes them into the focus
   texture synchronously, before returning from the event handler.

Because the lit set is reset on every input, a dropped, stale or reordered
worker patch cannot leave a ghost `FOCUSED`/`LINKED` byte. Worker focus replies
are used only for the authoritative HUD and surface tethers; they never write
focus bytes or move the camera. HUD/label DOM updates are deferred to the end
of the first focused frame so they cannot delay it.

The worker also follows the focus for its owning state (ripple muting, HUD
tethers, stream patches). Hover-intent prefetch is unnecessary now that the
main thread is synchronous.

### Picking

Clicking uses a GPU **id pass**: the custom layer redraws a 1×-per-frame
offscreen RGBA8 buffer with the story index encoded per instance, using the same
globe projection and clip test as the visible dots. A click reads exactly one
pixel. This is occlusion-correct by construction (far-side dots are clipped by
the globe), never loops over stories on the CPU, and the readback is a single
`readPixels`. The previous `map.project` nearest-dot loop is gone.

### Camera and gestures

`CameraController` delegates to MapLibre `flyTo({duration: 600, essential:
true})`. MapLibre interrupts an active flight when a new one starts, starts from
the current camera, and a user drag cancels it. Gesture-mode holds are driven
only by user events (`e.originalEvent`); camera animations never start a hold.
A drag cancels a flight; a click within 200 ms of a drag end is ignored, so a
drag can never turn into a selection. **Esc returns to ambient in place and
does not move the camera** (it cancels a running flight without a new target).
After ~30 s without input in ambient mode the globe resumes a slow idle
rotation.

### Reusable GPU data

- Story/focus/link data live in **2D RGBA32F textures** with width ≤ 2048 and
  rows for capacity; the 4096 texture-width limit cannot be hit.
- Buffers and textures are pre-allocated to capacity (stories: count + 1024,
  links: count + 512) with spare room. Updates are 1-texel sub-range writes;
  `CapacityPool` gives index insert/remove and a free list for later tickets.
- `onRemove` and re-init free every program, buffer, VAO and texture, plus the
  picking framebuffer.
- Per-frame paths reuse scratch arrays; no `new Float32Array` per frame or per
  patch in the renderer.

## Encodings

| Element | Encoding |
| --- | --- |
| Dot | category colour, size by sources, glow by heat, live ring pulse, focused brackets |
| Arc | type colour, width 0.6→3.4 px, opacity 0.2→0.95, glow, 2 fast pulses ≥ 0.75 / 1 slow ≥ 0.5 / none, thematic dashed; below 0.30 not drawn; rest budget = strongest 55 links above 0.6 |
| Focus | dimmed 25 % / linked / focused / 2nd degree 55 % no pulses, 200 ms shader fade; off-thread arcs faint (0.08) as on board A2 |
| Ripple | ≤ 20 slots, 1.4 s category ring + 0.35 s white flash, rate-limited to ~capacity/lifetime, same-cell merge, dropped rather than truncating a live ripple, muted outside the focused thread |
| Tether | white dotted, flat, no motion, hollow ring |
| Labels | story place names; street and POI names from the labels-only style |

Encoding literals are injected into GLSL from `encoding.ts`/`ripples.ts`
(`DOR_FOCUS_FADE`, `DOR_MIN_STRENGTH`, pulse thresholds, ripple lifetime/flash,
focus alphas), so the shaders and the TypeScript cannot drift.

## Precision and picking evidence

On-screen projection check: the float32 vertex path vs MapLibre's
double-precision `project()`, only stories whose projected point is inside the
viewport (all 5k stories sampled). Method: coarse 1/8192 mercator + residual
per vertex, camera anchor split the same way, double-precision anchor matrix
(MapLibre is already in mercator at street zoom).

| Zoom | On-screen samples | Max error | Mean error |
| --- | --- | --- | --- |
| z 15 | 16 | 0.000025 px | 0.000007 px |
| z 16 | 12 | 0.000018 px | 0.000010 px |
| z 17 | 4 | 0.000019 px | 0.000015 px |

A 0.5 px pan re-check at z 16 gave the same maximum (0.000018 px before and
after). The first fixup note's “1.2 px at z 17” was dominated by off-screen
samples and a stale camera matrix in the checker, not by float32 rounding; the
on-screen path agrees with MapLibre below the pixel grid.

Two frozen frames 800 ms apart at z 16 differed by **0 pixels** of 1,296,000:
there is no frame-to-frame twitch. Picking: a click over the near hemisphere
never returns a far-side story (the harness checks this), because the id pass
uses the globe clip.

## Measurement

- **Machine:** Apple M3 Max, 48 GB — high-end, **not** a mid-range laptop.
- **Browser:** Chrome 157, headed, GPU, 1440×900, DPR 1 in the harness (the app
  caps DPR at 2 in normal use; fill rate at DPR 2 is higher).
- **Profiles:** 1×, 4×, 6× via CDP `Emulation.setCPUThrottlingRate`. CPU
  throttling does **not** throttle the GPU; these are CPU-side proxies, not
  mid-range GPU proof.
- **Latency:** from the input event's own `event.timeStamp` to the first map
  `render` event after the focus bytes were written. The render event is the
  end of CPU render, **not presentation**; `frames` counts presented rAF
  frames to that point. Warm keys, cold keys (two presses inside one frame),
  and real mouse clicks are measured separately.
- **Gestures:** pan, wheel zoom, ctrl-drag rotate, and a wheel **zoom-in** from
  the globe to z 15.
- **Long tasks:** `PerformanceObserver('longtask')`, drained before a window
  closes; window start/end from the app's meter.
- **No queue:** 20 selections 30 ms apart; the harness polls until
  `map.isMoving()`/pending is false and records the settle time. Twenty queued
  600 ms flights would take about 12 s; settling in about one flight is the
  real evidence.
- **Precision:** on-screen samples only; a 0.5 px pan re-check. Jitter: frozen
  frames, pixel diff.
- **perf-results.json** is written by the full run only; `--shots-only` writes
  screenshots/precision only.

| Profile | Refresh | Gestures fps (pan / zoom / rotate / zoom-in) | Frame p95 | Long tasks | JS p95 | Warm key p50 / p95 | Cold key p50 / p95 | Click p50 / p95 | Frames | Settled |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1x | 120 Hz | 120/120/120/120 | 9.3 ms | 0 | 2.4 ms | 4.8 / 9.5 ms | 3.3 / 7.9 ms | 7.1 / 9.3 ms | 1/1/1 | 625 ms |
| 4x | 120 Hz | 120/120/120/120 | 9.2 ms | 0 | 3.6 ms | 10.3 / 12.2 ms | 4.9 / 5.8 ms | 8.1 / 11.8 ms | 1/1/1 | 574 ms |
| 6x | 120 Hz | 120/120/120/120 | 9.2 ms | 0 | 6.1 ms | 10.4 / 15.8 ms | 7.8 / 9.7 ms | 13.3 / 14.9 ms | 1/1/1 | 578 ms |

Reading this table:

- **Gestures**: 120 fps median in every gesture at every profile, p95 9.2–9.3 ms, **zero long tasks**, including the wheel zoom from the globe to z 15. The display is 120 Hz; that says the frame budget was not missed, not that 120 fps is the renderer's ceiling.
- **Focus**: every warm key, cold key and click is under 16 ms at 1×, 4× and 6×, and frames-to-state is 1 in every scenario. Cold/click samples: 10 each; warm: 20. The 6× numbers are the CPU-throttled proxy, not a mid-range laptop.
- **No queue**: 20 selections at 30 ms intervals settled in 574–625 ms with exactly 20 `flyTo` starts. Twenty queued 600 ms flights would take about 12 s, so this is the real no-queue evidence.
- **DPR**: the harness runs at DPR 1. The app caps DPR at 2 in normal use, which doubles fill-rate cost; no DPR-2 numbers are claimed.
- The **M3 Max caveat** above applies: 4×/6× CPU throttling is not a mid-range GPU.
- The machine has a second 60 Hz display. The harness records `refreshHz`; these rows are the 120 Hz display. A 60 Hz run of the same build is vsync-bound (60 fps, p95 ≈16.7 ms) and is not used for the verdict.

## Minors and deferred work

- Ripple spawns are rate-limited and merge in-cell; a full buffer drops a new
  pulse rather than truncating a live one; ripples outside the focused thread
  are muted (worker and shader).
- Street and POI names are symbol layers only; no road lines.
- Attribution is provider config.
- Telemetry arrays are bounded; long-task observers are drained.
- **Deferred:** strength score chips on arcs (M3·3); hex aggregates and auto
  quality (M2·4); URL focus state, 2nd-degree UI, idle autopilot.

## Tests

`web/src/**/*.test.ts` (Vitest, node): worker init/adjacency/determinism,
focus bytes and streams, **FocusMirror** (matches the worker, resets old lit
indices, no ghosts, second degree, link delta), **gpu-layout** (2D rows/texels,
capacity + free-list), ripples (capacity, no truncation, merge, lifetime),
encoding (arc strength, focus opacity, dot size), gestures (≤ 4/s, merging),
mercator, plus the M0·1 proto/worker gates.
