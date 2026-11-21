/* eslint-disable @typescript-eslint/no-explicit-any -- diagnostic browser boundary */
/** Run against this worktree's isolated built preview, never the live web process.
 * F31_MINUTES=60 pnpm exec tsx perf/production.perf.ts
 * Uses the production worker/patch/runtime path; ?perf=1 supplies M0-seeded input.
 */
import { chromium, type Page } from "playwright";
import { mkdir, writeFile } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import path from "node:path";

const base = process.env.F31_URL ?? "http://127.0.0.1:5184";
const out = process.env.F31_OUT ?? "/tmp/dor-f31-evidence";
const minutes = Number(process.env.F31_MINUTES ?? 60);
await mkdir(out, { recursive: true });
const initialPower = execFileSync("pmset", ["-g", "batt"], { encoding: "utf8" }).trim();
if (minutes >= 60 && !initialPower.includes("AC Power")) {
  throw new Error("The sustained acceptance run requires confirmed AC power.");
}
const browser = await chromium.launch({
  channel: "chrome",
  headless: false,
  args: [
    "--remote-debugging-port=9235",
    "--disable-background-timer-throttling",
    "--disable-renderer-backgrounding",
    "--disable-backgrounding-occluded-windows",
  ],
});
const report: any = {
  startedAt: new Date().toISOString(),
  power: initialPower,
  commit: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  hardware: execFileSync("sysctl", ["-n", "hw.model", "machdep.cpu.brand_string", "hw.memsize"], {
    encoding: "utf8",
  }).trim(),
  browser: browser.version(),
  viewport: { width: 1440, height: 900 },
  deviceScaleFactor: 1,
  mode: "headed Chrome, GPU, production bundle; no CPU throttling",
  errors: [],
  samples: [],
};
let stopped = false;
process.on("SIGINT", () => {
  stopped = true;
});
process.on("SIGTERM", () => {
  stopped = true;
});
function workload() {
  return execFileSync("ps", ["-Ao", "%cpu,pid,comm"], { encoding: "utf8" })
    .split("\n")
    .slice(1)
    .sort((a, b) => parseFloat(b) - parseFloat(a))
    .slice(0, 10);
}
async function save() {
  await writeFile(path.join(out, "report.json"), JSON.stringify(report, null, 2));
}
async function pageFor(query: string): Promise<Page> {
  const p = await browser.newPage({ viewport: report.viewport, deviceScaleFactor: 1 });
  await p.addInitScript("globalThis.__name = (fn) => fn");
  p.on("pageerror", (e) => {
    report.errors.push(e.message);
    console.error(e.message);
  });
  await p.goto(`${base}/?${query}&diagnostics=1`, { waitUntil: "domcontentloaded" });
  await p.waitForFunction(
    () =>
      (window as any).__dorLive?.state.ready && (window as any).__dorLive?.state.initCount === 1,
    null,
    { timeout: 60000 },
  );
  await p.bringToFront();
  return p;
}
async function summary(p: Page) {
  return p.evaluate(() => {
    const g = (window as any).__dorLive;
    return {
      ...g.diagnostics,
      stories: g.state.storyCount,
      links: g.state.linkCount,
      perf: g.perf.snapshot(),
      bounds: (window as any).__dorApp?.bounds,
      foreground: { visibility: document.visibilityState, focus: document.hasFocus() },
    };
  });
}
async function gestures(p: Page) {
  await p.mouse.move(730, 440);
  await p.mouse.down();
  for (let i = 0; i < 100; i++) {
    await p.mouse.move(730 + Math.sin(i / 12) * 170, 440 + Math.cos(i / 14) * 90);
    await p.waitForTimeout(25);
  }
  await p.mouse.up();
  for (let i = 0; i < 16; i++) {
    await p.mouse.wheel(0, i % 2 ? 160 : -160);
    await p.waitForTimeout(100);
  }
}
// Attach raw CDP to the dedicated test browser so worker heap is measured too.
const version = (await (await fetch("http://127.0.0.1:9235/json/version")).json()) as any;
const socket = new WebSocket(version.webSocketDebuggerUrl);
await new Promise<void>((resolve, reject) => {
  socket.onopen = () => resolve();
  socket.onerror = () => reject(new Error("CDP connection failed"));
});
let id = 0;
const pending = new Map<number, { resolve: (x: any) => void; reject: (e: unknown) => void }>();
socket.onmessage = (event) => {
  const m = JSON.parse(String(event.data));
  const wait = pending.get(m.id);
  if (wait) {
    pending.delete(m.id);
    if (m.error) wait.reject(m.error);
    else wait.resolve(m.result);
  }
};
function cdp(
  method: string,
  params: Record<string, unknown> = {},
  sessionId?: string,
): Promise<any> {
  return new Promise((resolve, reject) => {
    const request = ++id;
    pending.set(request, { resolve, reject });
    socket.send(
      JSON.stringify({ id: request, method, params, ...(sessionId ? { sessionId } : {}) }),
    );
  });
}
let workerSession: string | undefined;
async function workerHeap() {
  if (!workerSession) {
    const { targetInfos } = await cdp("Target.getTargets");
    const worker = targetInfos.find(
      (t: any) => t.type === "worker" && t.url.includes("/assets/index-"),
    );
    if (!worker) return null;
    workerSession = (
      await cdp("Target.attachToTarget", { targetId: worker.targetId, flatten: true })
    ).sessionId;
  }
  await cdp("HeapProfiler.collectGarbage", {}, workerSession);
  return cdp("Runtime.getHeapUsage", {}, workerSession);
}
try {
  // Read-only connection to the gateway through the preview proxy.
  const live = await pageFor("");
  await live.waitForTimeout(5000);
  await live.evaluate(() => (window as any).__dorLive.perf.beginWindow("live gateway pan/zoom"));
  await gestures(live);
  report.live = {
    ...(await summary(live)),
    gesture: await live.evaluate(() => (window as any).__dorLive.perf.endWindow()),
  };
  await live.screenshot({ path: path.join(out, "live-gateway.png") });
  await live.close();
  workerSession = undefined;
  const page = await pageFor("perf=1");
  await page.waitForTimeout(5000);
  report.shell = await page.evaluate(() => ({
    paints: performance
      .getEntriesByType("paint")
      .map((e) => ({ name: e.name, start: e.startTime })),
    runtime: performance
      .getEntriesByType("resource")
      .filter((e) => e.name.includes("runtime-"))
      .map((e) => ({ name: e.name, start: e.startTime })),
    userAgent: navigator.userAgent,
    webgl: (() => {
      const gl = (window as any).__dorLive.map.getCanvas().getContext("webgl2");
      const ext = gl.getExtension("WEBGL_debug_renderer_info");
      return ext ? gl.getParameter(ext.UNMASKED_RENDERER_WEBGL) : gl.getParameter(gl.RENDERER);
    })(),
  }));
  const session = await page.context().newCDPSession(page);
  const started = Date.now();
  await page.evaluate(() => {
    const g = (window as any).__dorLive;
    g.focusStoryId("perf-8");
  });
  await page.waitForTimeout(2000);
  await page.keyboard.press("Shift+P");
  await page.evaluate(() => {
    const w = window as any;
    const g = w.__dorLive;
    const index = g.data.storyById.get("perf-8");
    const initial = g.layer.storyAnimation(index);
    w.__f31Continuity = { initial, frames: 0, resets: 0, wrongState: 0 };
    const check = () => {
      const state = g.layer.storyAnimation(index);
      const c = w.__f31Continuity;
      c.frames++;
      if (state.at !== c.initial.at) c.resets++;
      if (state.state !== 2) c.wrongState++;
      w.__f31Frame = requestAnimationFrame(check);
    };
    w.__f31Frame = requestAnimationFrame(check);
  });
  await page.screenshot({ path: path.join(out, "focus-start.png") });
  await page.evaluate(() =>
    (window as any).__dorLive.perf.beginWindow(
      "50/s production patches + upsert/remove bursts, focused pan/zoom",
    ),
  );
  for (let i = 0; i < 4; i++) await gestures(page);
  report.fakeGesture = await page.evaluate(() => (window as any).__dorLive.perf.endWindow());
  for (let minute = 0; minute <= minutes; minute++) {
    const target = started + minute * 60000;
    while (!stopped && Date.now() < target)
      await page.waitForTimeout(Math.min(1000, target - Date.now()));
    if (stopped) break;
    if (minute > 0 && minute < 3) await gestures(page);
    await session.send("HeapProfiler.collectGarbage");
    const sample = {
      minute,
      power: execFileSync("pmset", ["-g", "batt"], { encoding: "utf8" }).trim(),
      competingWorkload: workload(),
      elapsedMs: Date.now() - started,
      ...(await summary(page)),
      mainHeap: await session.send("Runtime.getHeapUsage"),
      workerHeap: await workerHeap(),
      continuity: await page.evaluate(() => (window as any).__f31Continuity),
    };
    report.samples.push(sample);
    if (minutes >= 60 && !sample.power.includes("AC Power")) {
      stopped = true;
      report.stopReason = "AC power was lost during the sustained run";
    }
    if (minute === 2) report.twoMinutes = sample;
    if (minute === 10) {
      await page.screenshot({ path: path.join(out, "focus-ten-minutes.png") });
      report.tenMinutes = sample;
    }
    await save();
    console.log(
      JSON.stringify({
        minute,
        init: sample.initCount,
        patches: sample.patchCount,
        ripples: sample.rippleCount,
        fps: sample.perf.fps,
        p95: sample.perf.frameMs.p95,
        patchP95: sample.patchMs.p95,
        mainHeap: sample.mainHeap.usedSize,
        workerHeap: sample.workerHeap?.usedSize,
        continuity: sample.continuity,
        bounds: sample.bounds,
      }),
    );
  }
  report.completedAt = new Date().toISOString();
  report.status = stopped ? "stopped before completion" : "complete";
  await save();
} finally {
  socket.close();
  await browser.close();
}
