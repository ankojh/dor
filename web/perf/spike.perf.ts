/* eslint-disable @typescript-eslint/no-explicit-any -- page.evaluate crosses the browser boundary */
/**
 * M0·2 globe measurements.
 *
 * Headed Chrome with the GPU (channel: "chrome"). Frame times are rAF deltas,
 * long tasks come from PerformanceObserver, and focus latency is measured from
 * the input event's own `timeStamp` to the first render event after the focus
 * bytes were written. The render event marks the end of CPU render, not
 * presentation; frames-to-state is reported next to the milliseconds.
 *
 * The harness always starts its own Vite preview on a random free port and
 * stops it; it never reuses a foreign server.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { chromium, type Browser, type Page } from "playwright";
import { PNG } from "pngjs";

const WEB_ROOT = process.cwd();
const REPO_ROOT = path.resolve(WEB_ROOT, "..");
const OUT_DIR = path.join(REPO_ROOT, "docs/img/spike");
const VIEWPORT = { width: 1440, height: 900 };

type PerfWindow = {
  label: string;
  durationMs: number;
  frames: number;
  fps: number;
  frameMs: { p50: number; p95: number; p99: number; max: number };
  longTasks: number;
  longestTaskMs: number;
  loafCount: number;
  loafMs: number;
  jsMs: { mean: number; p95: number; max: number };
};

type FocusSample = {
  kind: "key" | "click" | "api";
  requestId: number;
  eventAt: number;
  handlerAt: number;
  appliedAt: number;
  renderAt: number;
  latencyMs: number;
  handlerLatencyMs: number;
  renderCallbacks: number;
  focusIndex: number | null;
  secondDegree: boolean;
  superseded: boolean;
  completed: boolean;
};

type Profile = { label: string; rate: number };

const PROFILES: Profile[] = [
  { label: "1x", rate: 1 },
  { label: "4x", rate: 4 },
  { label: "6x", rate: 6 },
];

function percentile(values: number[], fraction: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))] ?? 0;
}

function stats(values: number[]): { n: number; p50: number; p95: number; max: number } {
  return {
    n: values.length,
    p50: percentile(values, 0.5),
    p95: percentile(values, 0.95),
    max: values.reduce((max, value) => Math.max(max, value), 0),
  };
}

async function httpOk(url: string): Promise<boolean> {
  try {
    const response = await fetch(url);
    return response.ok;
  } catch {
    return false;
  }
}

async function startPreview(): Promise<{ url: string; child: ChildProcess }> {
  for (let attempt = 0; attempt < 20; attempt++) {
    const port = 5200 + Math.floor(Math.random() * 200);
    const url = `http://127.0.0.1:${port}`;
    if (await httpOk(`${url}/spike.html`)) continue;
    const child = spawn(
      path.join(WEB_ROOT, "node_modules/.bin/vite"),
      ["preview", "--host", "127.0.0.1", "--port", String(port), "--strictPort"],
      { cwd: WEB_ROOT, stdio: "ignore" },
    );
    for (let wait = 0; wait < 80; wait++) {
      if (await httpOk(`${url}/spike.html`)) return { url, child };
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    child.kill("SIGKILL");
  }
  throw new Error("could not start a private vite preview server");
}

function stopPreview(child: ChildProcess): void {
  if (child.exitCode !== null) return;
  child.kill("SIGTERM");
  setTimeout(() => {
    if (child.exitCode === null) child.kill("SIGKILL");
  }, 1500).unref?.();
}

async function openPage(
  browser: Browser,
  baseUrl: string,
  query: string,
): Promise<{ page: Page; context: any }> {
  const context = await browser.newContext({ viewport: VIEWPORT, deviceScaleFactor: 1 });
  const page = await context.newPage();
  page.on("pageerror", (error) => console.error(`[pageerror] ${error.message}`));
  await page.goto(`${baseUrl}/spike.html${query}`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => (window as any).__dorSpike?.ready === true, null, {
    timeout: 60000,
  });
  await page.bringToFront();
  await page.waitForTimeout(1000);
  return { page, context };
}

async function beginWindow(page: Page, label: string): Promise<void> {
  await page.evaluate((name) => (window as any).__dorSpike.perf.beginWindow(name), label);
}

async function endWindow(page: Page): Promise<PerfWindow> {
  return (await page.evaluate(() => (window as any).__dorSpike.perf.endWindow())) as PerfWindow;
}

async function pan(page: Page): Promise<void> {
  await page.mouse.move(560, 430);
  await page.mouse.down();
  for (let i = 0; i < 36; i++) {
    await page.mouse.move(560 + Math.sin(i / 5) * 220, 430 + Math.cos(i / 6) * 120);
    await page.waitForTimeout(45);
  }
  await page.mouse.up();
}

async function zoom(page: Page): Promise<void> {
  await page.mouse.move(720, 430);
  for (let i = 0; i < 14; i++) {
    await page.mouse.wheel(0, i % 2 === 0 ? 220 : -180);
    await page.waitForTimeout(160);
  }
}

async function rotate(page: Page): Promise<void> {
  await page.keyboard.down("Control");
  await page.mouse.move(520, 430);
  await page.mouse.down();
  for (let i = 0; i < 30; i++) {
    await page.mouse.move(520 + Math.sin(i / 4) * 180, 430 + Math.cos(i / 5) * 70);
    await page.waitForTimeout(50);
  }
  await page.mouse.up();
  await page.keyboard.up("Control");
}

/**
 * Wheel from the globe to street level; exercises LOD and label loading.
 * Negative wheel deltas zoom in in MapLibre/Chrome. The start and end zoom are
 * recorded and asserted so a reversed sign cannot silently pass as a run.
 */
async function zoomInToStreet(page: Page): Promise<{ startZoom: number; endZoom: number }> {
  const startZoom = (await page.evaluate(() => (window as any).__dorSpike.map.getZoom())) as number;
  await page.mouse.move(720, 430);
  for (let i = 0; i < 24; i++) {
    await page.mouse.wheel(0, -240);
    await page.waitForTimeout(130);
  }
  await page.waitForTimeout(500);
  const endZoom = (await page.evaluate(() => (window as any).__dorSpike.map.getZoom())) as number;
  if (endZoom < 14.5) {
    throw new Error(`street wheel zoom ended at z${endZoom.toFixed(2)}; expected z>=14.5`);
  }
  return { startZoom, endZoom };
}

async function newSamples(page: Page): Promise<FocusSample[]> {
  return (await page.evaluate(
    () => (window as any).__dorSpike.focusSamples as FocusSample[],
  )) as FocusSample[];
}

function summarizeFocus(samples: FocusSample[]): Record<string, unknown> {
  const completed = samples.filter((sample) => sample.completed && !sample.superseded);
  const superseded = samples.filter((sample) => sample.superseded);
  return {
    samples: samples.length,
    completed: completed.length,
    superseded: superseded.length,
    // Stage labels are deliberate. "applied" is state on the GPU; "render"
    // is a MapLibre render callback (CPU), never presentation.
    eventToHandlerMs: stats(completed.map((sample) => sample.handlerAt - sample.eventAt)),
    handlerToAppliedMs: stats(completed.map((sample) => sample.appliedAt - sample.handlerAt)),
    appliedToRenderMs: stats(completed.map((sample) => sample.renderAt - sample.appliedAt)),
    eventToRenderMs: stats(completed.map((sample) => sample.latencyMs)),
    handlerToRenderMs: stats(completed.map((sample) => sample.handlerLatencyMs)),
    renderCallbacks: stats(completed.map((sample) => sample.renderCallbacks)),
    supersededSamples: superseded.map((sample) => ({
      focusIndex: sample.focusIndex,
      eventToHandlerMs: sample.handlerAt - sample.eventAt,
      renderCallbacks: sample.renderCallbacks,
    })),
    note: "render callbacks are CPU render completions, not presented frames",
  };
}

async function measureScenario(
  browser: Browser,
  baseUrl: string,
  profile: Profile,
): Promise<Record<string, unknown>> {
  const { page, context } = await openPage(browser, baseUrl, "?renderer=custom");
  if (profile.rate > 1) {
    const cdp = await context.newCDPSession(page);
    await cdp.send("Emulation.setCPUThrottlingRate", { rate: profile.rate });
  }
  await page.bringToFront();
  await page.waitForTimeout(700);

  const gestures: Record<string, PerfWindow> = {};
  let zoomEvidence: { startZoom: number; endZoom: number } | null = null;
  for (const [kind, action] of [
    ["pan", pan],
    ["zoom", zoom],
    ["rotate", rotate],
  ] as const) {
    await beginWindow(page, kind);
    await action(page);
    gestures[kind] = await endWindow(page);
    await page.waitForTimeout(500);
  }
  await page.evaluate(() => (window as any).__dorSpike.map.jumpTo({ center: [6, 22], zoom: 1.55 }));
  await page.waitForTimeout(600);
  await beginWindow(page, "zoomIn");
  zoomEvidence = await zoomInToStreet(page);
  gestures.zoomIn = await endWindow(page);
  await page.waitForTimeout(500);

  const resetGlobe = async (): Promise<void> => {
    await page.evaluate(() => (window as any).__dorSpike.cameraCancel());
    await page.evaluate(() =>
      (window as any).__dorSpike.map.jumpTo({ center: [6, 22], zoom: 1.55, bearing: 0, pitch: 0 }),
    );
    await page.waitForTimeout(700);
  };
  await resetGlobe();

  const top = (
    (await page.evaluate(() => (window as any).__dorSpike.getState().topStories)) as number[]
  ).slice(0, 60);

  // Warm keys: the main-thread mirror writes synchronously, one render.
  const warmBefore = (await newSamples(page)).length;
  await beginWindow(page, "focusWarm");
  for (let i = 0; i < 20; i++) {
    await page.keyboard.press("ArrowDown");
    await page.waitForTimeout(150);
  }
  const focusWarmWindow = await endWindow(page);
  const warmSamples = (await newSamples(page)).slice(warmBefore);

  // Cold keys: clear, arm a varied cursor, then one real key. A second rapid
  // key in half the rounds leaves an explicitly superseded sample.
  const coldSamples: FocusSample[] = [];
  for (let round = 0; round < 10; round++) {
    await resetGlobe();
    await page.evaluate(() => (window as any).__dorSpike.focusClear());
    await page.evaluate(
      ([cursor]) => (window as any).__dorSpike.setFocusCursor(cursor),
      [((round * 7) % top.length) as number],
    );
    await page.waitForTimeout(120);
    const before = (await newSamples(page)).length;
    await page.keyboard.press("ArrowDown");
    if (round % 2 === 0) await page.keyboard.press("ArrowDown");
    await page.waitForTimeout(240);
    coldSamples.push(...(await newSamples(page)).slice(before));
  }

  // Delayed input: CDP stamps the event 80 ms earlier than the handler, so the
  // sample's eventAt->handlerAt stage is visible instead of being clamped.
  const cdp = await context.newCDPSession(page);
  await resetGlobe();
  await page.evaluate(() => (window as any).__dorSpike.focusClear());
  await page.waitForTimeout(150);
  const delayedBefore = (await newSamples(page)).length;
  const delayedStamp = (Date.now() - 80) / 1000;
  await cdp.send("Input.dispatchKeyEvent", {
    type: "keyDown",
    key: "ArrowDown",
    code: "ArrowDown",
    windowsVirtualKeyCode: 40,
    nativeVirtualKeyCode: 40,
    timestamp: delayedStamp,
  });
  await cdp.send("Input.dispatchKeyEvent", {
    type: "keyUp",
    key: "ArrowDown",
    code: "ArrowDown",
    windowsVirtualKeyCode: 40,
    nativeVirtualKeyCode: 40,
    timestamp: delayedStamp,
  });
  await page.waitForTimeout(240);
  const delayedSamples = (await newSamples(page)).slice(delayedBefore);

  // Diverse real clicks: different destinations, zoom levels and centres.
  const clickSamples: FocusSample[] = [];
  const clickDestinations: Array<Record<string, unknown>> = [];
  const views = [
    { center: [6, 22], zoom: 1.55 },
    { center: [4.4777, 51.9244], zoom: 4 },
    { center: [125, 20], zoom: 1.55 },
    { center: [-90, 15], zoom: 1.55 },
    { center: [4.4777, 51.9244], zoom: 16 },
  ];
  for (let round = 0; round < 12; round++) {
    const view = views[round % views.length]!;
    await page.evaluate((v) => {
      const api = (window as any).__dorSpike;
      api.cameraCancel();
      api.focusClear();
      api.map.jumpTo({ center: v.center, zoom: v.zoom, bearing: 0, pitch: 0 });
    }, view);
    await page.waitForTimeout(350);
    const start = (round * 11 + 3) % top.length;
    let point: { x: number; y: number } | null = null;
    let requested: number | null = null;
    for (let attempt = 0; attempt < top.length; attempt++) {
      const index = top[(start + attempt) % top.length]!;
      const candidate = (await page.evaluate(
        (story) => (window as any).__dorSpike.screenPointOf(story),
        index,
      )) as { x: number; y: number } | null;
      if (
        !candidate ||
        candidate.x < 270 ||
        candidate.x > 1050 ||
        candidate.y < 100 ||
        candidate.y > 770
      ) {
        continue;
      }
      const picked = await page.evaluate(
        ([x, y]) => (window as any).__dorSpike.pickAt(x, y),
        [candidate.x, candidate.y],
      );
      if (picked !== null && picked >= 0) {
        point = candidate;
        requested = index;
        break;
      }
    }
    if (!point) continue;
    const before = (await newSamples(page)).length;
    await page.mouse.click(point.x, point.y);
    await page.waitForTimeout(240);
    const roundSamples = (await newSamples(page)).slice(before);
    clickSamples.push(...roundSamples);
    clickDestinations.push({
      requested,
      x: point.x,
      y: point.y,
      view,
      focusIndex: roundSamples.find((sample) => sample.completed)?.focusIndex ?? null,
    });
  }

  // Real no-queue check: 20 selections 30 ms apart, then settle.
  await resetGlobe();
  const cameraBefore = (await page.evaluate(
    () => (window as any).__dorSpike.getState().camera,
  )) as { started: number };
  const burstStart = Date.now();
  for (let i = 0; i < 20; i++) {
    await page.keyboard.press("ArrowDown");
    await page.waitForTimeout(30);
  }
  const burstEnd = Date.now();
  await page.waitForTimeout(200);
  const during = (await page.evaluate(() => (window as any).__dorSpike.getState().camera)) as any;
  const settleStart = Date.now();
  let settledAt = settleStart;
  for (let i = 0; i < 60; i++) {
    const camera = (await page.evaluate(() => (window as any).__dorSpike.getState().camera)) as any;
    if (!camera.active && !camera.pending) {
      settledAt = Date.now();
      break;
    }
    await page.waitForTimeout(50);
  }
  const after = (await page.evaluate(() => ({
    camera: (window as any).__dorSpike.getState().camera,
    focus: (window as any).__dorSpike.getState().focusIndex,
    pos: (window as any).__dorSpike.map.getCenter(),
  }))) as any;
  const settledMs = settledAt - burstEnd;

  const gesturesSummary = {
    worstFrameP95Ms: Math.max(...Object.values(gestures).map((window) => window.frameMs.p95)),
    longTasks: Object.values(gestures).reduce((sum, window) => sum + window.longTasks, 0),
    worstJsP95Ms: Math.max(...Object.values(gestures).map((window) => window.jsMs.p95)),
  };

  const rendererStats = await page.evaluate(() => (window as any).__dorSpike.rendererStats());
  const dpr = await page.evaluate(() => window.devicePixelRatio);
  await context.close();

  return {
    scenario: "custom-globe",
    profile: profile.label,
    cpuThrottle: profile.rate,
    dpr,
    dprPixelAreaVs1x: dpr * dpr,
    refreshHz: Math.round(1000 / (gestures.pan?.frameMs.p50 || 16.7)),
    viewport: VIEWPORT,
    note: "Custom WebGL layer, MapLibre globe, 50 articles/s fake stream",
    gestures,
    zoomEvidence,
    gestureSummary: gesturesSummary,
    focus: {
      warm: summarizeFocus(warmSamples),
      cold: summarizeFocus(coldSamples),
      click: summarizeFocus(clickSamples),
      delayed: summarizeFocus(delayedSamples),
      clickDestinations,
      samples: [...warmSamples, ...coldSamples, ...clickSamples, ...delayedSamples],
      warmWindow: focusWarmWindow,
    },
    noQueue: {
      cameraBefore,
      during,
      after: after.camera,
      burstMs: burstEnd - burstStart,
      settledMs,
      selections: after.camera.started - cameraBefore.started,
      settledWithinOneFlight: settledMs < 1000,
      settled: after.camera.active === false && after.camera.pending === false,
      finalFocus: after.focus,
      finalCenter: [after.pos.lng, after.pos.lat],
    },
    rendererStats,
  };
}

async function screenshot(page: Page, name: string): Promise<void> {
  await page.screenshot({ path: path.join(OUT_DIR, `${name}.png`) });
}

async function pixelDiff(
  a: string,
  b: string,
): Promise<{ differingPixels: number; totalPixels: number }> {
  const pngA = PNG.sync.read(await readFile(a));
  const pngB = PNG.sync.read(await readFile(b));
  let differing = 0;
  for (let i = 0; i < pngA.data.length; i += 4) {
    if (
      Math.abs(pngA.data[i]! - pngB.data[i]!) > 8 ||
      Math.abs(pngA.data[i + 1]! - pngB.data[i + 1]!) > 8 ||
      Math.abs(pngA.data[i + 2]! - pngB.data[i + 2]!) > 8
    ) {
      differing += 1;
    }
  }
  return { differingPixels: differing, totalPixels: pngA.width * pngA.height };
}

async function captureScreenshots(
  browser: Browser,
  baseUrl: string,
): Promise<Record<string, unknown>> {
  const shots: Record<string, unknown> = {};

  // Custom path: ambient, focused, local focused street, street 16/17.
  const { page, context } = await openPage(browser, baseUrl, "?renderer=custom&still=1");
  await screenshot(page, "custom-globe-ambient");
  await page.evaluate(() => (window as any).__dorSpike.focusByIndex(0));
  await page.waitForTimeout(1200);
  await screenshot(page, "custom-globe-focus");
  await page.evaluate(() => {
    (window as any).__dorSpike.map.jumpTo({
      center: [4.4777, 51.9244],
      zoom: 16,
      bearing: 0,
      pitch: 0,
    });
  });
  await page.waitForTimeout(2400);
  await screenshot(page, "custom-globe-street16-focus");
  await page.evaluate(() => (window as any).__dorSpike.focusClear());
  await page.waitForTimeout(800);
  await screenshot(page, "custom-globe-street16");
  await page.evaluate(() => {
    (window as any).__dorSpike.map.jumpTo({
      center: [4.4777, 51.9244],
      zoom: 17,
      bearing: 0,
      pitch: 0,
    });
  });
  await page.waitForTimeout(2200);
  await screenshot(page, "custom-globe-street17");
  await page.evaluate(() => (window as any).__dorSpike.focusByIndex(0));
  await page.waitForTimeout(900);
  await page.evaluate(() => {
    (window as any).__dorSpike.map.jumpTo({
      center: [4.4777, 51.9244],
      zoom: 15,
      bearing: 0,
      pitch: 0,
    });
  });
  await page.waitForTimeout(2200);
  await screenshot(page, "custom-globe-street15-focus");

  // On-screen precision and sub-pixel pan.
  const precision: Record<string, unknown> = {};
  for (const zoom of [15, 16, 17]) {
    await page.evaluate((z) => {
      (window as any).__dorSpike.map.jumpTo({
        center: [4.4777, 51.9244],
        zoom: z,
        bearing: 0,
        pitch: 0,
      });
    }, zoom);
    await page.waitForTimeout(700);
    precision[`z${zoom}`] = await page.evaluate(() => (window as any).__dorSpike.precisionCheck());
  }
  await page.evaluate(() => {
    (window as any).__dorSpike.map.jumpTo({
      center: [4.4777, 51.9244],
      zoom: 16,
      bearing: 0,
      pitch: 0,
    });
  });
  await page.waitForTimeout(700);
  precision.panHalfPixelBefore = await page.evaluate(() =>
    (window as any).__dorSpike.precisionCheck(),
  );
  await page.evaluate(() => (window as any).__dorSpike.map.panBy([0.5, 0], { duration: 0 }));
  await page.waitForTimeout(700);
  precision.panHalfPixelAfter = await page.evaluate(() =>
    (window as any).__dorSpike.precisionCheck(),
  );
  shots.precision = precision;

  // Frozen-scene jitter: two frames 800 ms apart must be identical.
  await page.evaluate(() => (window as any).__dorSpike.focusClear());
  await page.waitForTimeout(700);
  const first = path.join(OUT_DIR, "custom-globe-still-a.png");
  const second = path.join(OUT_DIR, "custom-globe-still-b.png");
  await page.screenshot({ path: first });
  await page.waitForTimeout(800);
  await page.screenshot({ path: second });
  shots.jitter = await pixelDiff(first, second);

  // Picking: a far-side story must never be returned for an on-screen click over
  // the near hemisphere.
  await page.evaluate(() => (window as any).__dorSpike.cameraCancel());
  await page.evaluate(() => {
    (window as any).__dorSpike.map.jumpTo({ center: [6, 22], zoom: 1.55, bearing: 0, pitch: 0 });
  });
  await page.waitForTimeout(1200);
  const picking = await page.evaluate(() => {
    const api = (window as any).__dorSpike;
    const center = api.map.getCenter();
    const impl = api.map.getLayer("dor-custom-globe").implementation;
    const stories = impl.init.stories;
    let farIndex = -1;
    for (let i = 0; i < stories.lng.length; i++) {
      const dLng = Math.abs(((stories.lng[i] - center.lng + 540) % 360) - 180);
      const point = api.screenPointOf(i);
      if (dLng > 110 && point && point.x > 0 && point.x < 1440 && point.y > 0 && point.y < 900) {
        farIndex = i;
        break;
      }
    }
    const farPoint = farIndex >= 0 ? api.screenPointOf(farIndex) : null;
    const picked = farPoint ? api.pickAt(farPoint.x, farPoint.y) : null;
    // A visible near-side dot must be pickable.
    const nearPoint = api.screenPointOf(0);
    return {
      farIndex,
      pickedFar: picked,
      farIsPicked: picked === farIndex,
      nearPoint,
      pickedNear: nearPoint ? api.pickAt(nearPoint.x, nearPoint.y) : null,
    };
  });
  shots.picking = picking;
  await context.close();

  // Deck comparison page: cullMode none makes arcs render on the globe.
  for (const query of ["?renderer=deck&still=1", "?renderer=deck&mercator=1&still=1"]) {
    const deckPage = await openPage(browser, baseUrl, query);
    await deckPage.page.evaluate(() => (window as any).__dorSpike.focusByIndex(0));
    await deckPage.page.waitForTimeout(1200);
    const id = query.includes("mercator") ? "deck-mercator-focus" : "deck-globe-focus";
    await screenshot(deckPage.page, id);
    await deckPage.context.close();
  }
  return shots;
}

async function main(): Promise<void> {
  await mkdir(OUT_DIR, { recursive: true });
  const shotsOnly = process.argv.includes("--shots-only");
  const { url, child } = await startPreview();
  const browser = await chromium.launch({
    channel: "chrome",
    headless: false,
    args: [
      "--disable-backgrounding-occluded-windows",
      "--disable-renderer-backgrounding",
      "--disable-background-timer-throttling",
      "--window-size=1440,960",
      "--window-position=40,40",
    ],
  });
  const results: Array<Record<string, unknown>> = [];
  try {
    if (!shotsOnly) {
      for (const profile of PROFILES) {
        process.stdout.write(`measuring custom-globe ${profile.label} … `);
        const result = await measureScenario(browser, url, profile);
        results.push(result);
        console.log("done");
      }
      await writeFile(path.join(OUT_DIR, "perf-results.json"), JSON.stringify(results, null, 2));
    }
    const shots = await captureScreenshots(browser, url);
    await writeFile(path.join(OUT_DIR, "perf-screenshots.json"), JSON.stringify(shots, null, 2));

    if (!shotsOnly) {
      console.log(
        "\n| profile | gesture fps pan/zoom/rotate/zoom-in | p95 | long | warm evt->render p50/p95 | cold evt->render p50/p95 | click evt->render p50/p95 | callbacks | zoom | settled |",
      );
      console.log("|---|---|---|---|---|---|---|---|---|---|");
      for (const result of results as any[]) {
        const g = result.gestures;
        const f = result.focus;
        console.log(
          `| ${result.profile} | ${g.pan.fps.toFixed(0)}/${g.zoom.fps.toFixed(0)}/${g.rotate.fps.toFixed(0)}/${g.zoomIn.fps.toFixed(0)} | ` +
            `${result.gestureSummary.worstFrameP95Ms.toFixed(1)} ms | ${result.gestureSummary.longTasks} | ` +
            `${f.warm.eventToRenderMs.p50.toFixed(1)}/${f.warm.eventToRenderMs.p95.toFixed(1)} | ` +
            `${f.cold.eventToRenderMs.p50.toFixed(1)}/${f.cold.eventToRenderMs.p95.toFixed(1)} | ` +
            `${f.click.eventToRenderMs.p50.toFixed(1)}/${f.click.eventToRenderMs.p95.toFixed(1)} | ` +
            `${f.warm.renderCallbacks.max}/${f.cold.renderCallbacks.max}/${f.click.renderCallbacks.max} | ` +
            `${result.zoomEvidence?.startZoom.toFixed(2)}→${result.zoomEvidence?.endZoom.toFixed(2)} | ` +
            `${result.noQueue.settled} in ${result.noQueue.settledMs} ms |`,
        );
      }
    }
    console.log("\nEvidence:", OUT_DIR);
  } finally {
    await browser.close();
    stopPreview(child);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
