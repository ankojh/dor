# Globe

MapLibre GL JS v5 globe, our WebGL layers, the shared data worker, gestures
and focus switching. Built in the M0·2 spike and reused by the production runtime.

```
config.ts            provider config (VITE_TILES_URL / VITE_GLYPHS_URL /
                     VITE_TILE_STYLE_URL; OpenFreeMap by default)
style/dor-labels.ts  our labels-only OpenMapTiles style (water hairline,
                     country/city/water names only)
mercator.ts          lng/lat <-> mercator, hi/lo split, arc geometry
encoding.ts          dot/arc/focus encodings from the plan
ripples.ts           fixed 20-slot ripple ring buffer + merge
gpu-layout.ts        2D texture layout + capacity free-list (pure)
focus-mirror.ts      main-thread CSR mirror, lit set, O(degree) focus bytes
gestures.ts          PatchHold: <= 4 data applies/s during gestures
camera.ts            interruptible fly-to-fit (one flight, never queued)
perf.ts              fps / frame p95 / long tasks / js-per-frame meter
labels.ts            story names through MapLibre glyphs (focused thread)
custom/              the custom WebGL layer: shaders, GPU buffers, textures
deck/renderer.ts     deck.gl core path (dots + arcs, binary attributes)
spike.ts             the /spike.html bootstrap and HUD
```

The production worker is `../worker/index.ts` with authoritative state in
`../worker/state.ts`. `runtime.ts` consumes full init messages and incremental
held patches; `load.ts` stages its import after shell paint. See the worker
README for the production protocol and performance harness. `fake/`, `spike.ts`
and `deck/` remain development references; the production build excludes the
spike/deck entry points.
