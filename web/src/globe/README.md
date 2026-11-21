# Globe

MapLibre GL JS v5 globe, our WebGL layers, the shared data worker, gestures
and focus switching. Built in the M0·2 spike; M2/M3 reuse these modules.

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

The data worker is `../worker/fake/` (fake stream, state, adjacency, focus
bytes). The spike page is `web/spike.html`; the main app entry is untouched.
