/**
 * Frame and long-task instrumentation for the spike page.
 *
 * rAF callbacks are wrapped once at boot so every frame's main-thread work
 * (MapLibre render, deck.gl overlay, our loop) is measured, not just our own
 * callback. Long tasks come from PerformanceObserver('longtask'); Chrome's
 * long-animation-frame entries are recorded too when available.
 */

export type Percentiles = { p50: number; p95: number; p99: number; max: number };

export type PerfStats = {
  label: string;
  durationMs: number;
  frames: number;
  fps: number;
  frameMs: Percentiles;
  longTasks: number;
  longestTaskMs: number;
  loafCount: number;
  loafMs: number;
  jsMs: { mean: number; p95: number; max: number };
};

export type PerfSnapshot = PerfStats;

type LongTask = { start: number; duration: number; loaf: boolean };

const rafStats = { totalMs: 0, maxMs: 0, calls: 0 };
let rafWrapped = false;

export function instrumentAnimationFrames(): void {
  if (rafWrapped || typeof window === "undefined") return;
  rafWrapped = true;
  const original = window.requestAnimationFrame.bind(window);
  window.requestAnimationFrame = (callback: FrameRequestCallback): number =>
    original((time) => {
      const start = performance.now();
      try {
        callback(time);
      } finally {
        const duration = performance.now() - start;
        rafStats.totalMs += duration;
        rafStats.maxMs = Math.max(rafStats.maxMs, duration);
        rafStats.calls += 1;
      }
    });
}

function percentile(values: number[], fraction: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.floor(sorted.length * fraction));
  return sorted[index] ?? 0;
}

function summarize(
  label: string,
  durationMs: number,
  frames: number[],
  longTasks: LongTask[],
  js: number[],
): PerfStats {
  const sorted = [...frames].sort((a, b) => a - b);
  const frameMs = {
    p50: percentile(sorted, 0.5),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    max: sorted[sorted.length - 1] ?? 0,
  };
  const meanFrame = frames.reduce((sum, value) => sum + value, 0) / Math.max(1, frames.length);
  return {
    label,
    durationMs,
    frames: frames.length,
    fps: meanFrame > 0 ? 1000 / meanFrame : 0,
    frameMs,
    longTasks: longTasks.filter((task) => !task.loaf).length,
    longestTaskMs: longTasks.reduce(
      (max, task) => (task.loaf ? max : Math.max(max, task.duration)),
      0,
    ),
    loafCount: longTasks.filter((task) => task.loaf).length,
    loafMs: longTasks.filter((task) => task.loaf).reduce((sum, task) => sum + task.duration, 0),
    jsMs: {
      mean: js.reduce((sum, value) => sum + value, 0) / Math.max(1, js.length),
      p95: percentile(js, 0.95),
      max: js.reduce((max, value) => Math.max(max, value), 0),
    },
  };
}

export class PerfMeter {
  private frames: number[] = [];
  private js: number[] = [];
  private longTasks: LongTask[] = [];
  private lastFrameAt = 0;
  private lastRafTotal = 0;
  private observer: PerformanceObserver | null = null;
  private loafObserver: PerformanceObserver | null = null;

  private windowLabel = "";
  private windowStart = 0;
  private windowActive = false;
  private windowFrames: number[] = [];
  private windowJs: number[] = [];

  private overlay: HTMLElement | null = null;
  private overlayTimer = 0;
  private overlayVisible = false;
  private overlayPaused = false;

  /** Pauses overlay DOM updates (used during gestures). */
  setPaused(paused: boolean): void {
    this.overlayPaused = paused;
  }

  constructor() {
    instrumentAnimationFrames();
    try {
      this.observer = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          this.recordLongTask(entry.startTime, entry.duration, false);
        }
      });
      this.observer.observe({ type: "longtask", buffered: true });
    } catch {
      this.observer = null;
    }
    try {
      this.loafObserver = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          const loaf = entry as PerformanceEntry & { duration: number };
          this.recordLongTask(loaf.startTime, loaf.duration, true);
        }
      });
      this.loafObserver.observe({
        type: "long-animation-frame",
        buffered: true,
      } as PerformanceObserverInit);
    } catch {
      this.loafObserver = null;
    }
  }

  /** Pull queued PerformanceObserver entries before closing a window. */
  private drainObservers(): void {
    for (const [observer, loaf] of [
      [this.observer, false],
      [this.loafObserver, true],
    ] as const) {
      if (!observer) continue;
      for (const entry of observer.takeRecords()) {
        this.recordLongTask(entry.startTime, entry.duration, loaf);
      }
    }
  }

  private recordLongTask(start: number, duration: number, loaf: boolean): void {
    this.longTasks.push({ start, duration, loaf });
    if (this.longTasks.length > 500) this.longTasks.splice(0, this.longTasks.length - 500);
  }

  /** Called once per rendered frame from the spike loop. */
  frame(now: number): void {
    if (this.lastFrameAt > 0) {
      const delta = now - this.lastFrameAt;
      if (delta > 0 && delta < 2000) {
        this.frames.push(delta);
        if (this.frames.length > 600) this.frames.splice(0, this.frames.length - 600);
        const rafDelta = rafStats.totalMs - this.lastRafTotal;
        this.lastRafTotal = rafStats.totalMs;
        this.js.push(rafDelta);
        if (this.js.length > 600) this.js.splice(0, this.js.length - 600);
        if (this.windowActive) {
          this.windowFrames.push(delta);
          this.windowJs.push(rafDelta);
          // A very long window must not grow without bound.
          if (this.windowFrames.length > 10000) this.windowFrames.shift();
          if (this.windowJs.length > 10000) this.windowJs.shift();
        }
      }
    }
    this.lastFrameAt = now;
  }

  beginWindow(label: string): void {
    this.windowLabel = label;
    this.windowStart = performance.now();
    this.windowActive = true;
    this.windowFrames = [];
    this.windowJs = [];
  }

  endWindow(): PerfStats {
    this.drainObservers();
    const now = performance.now();
    const longTasks = this.longTasks.filter((task) => task.start >= this.windowStart);
    const summary = summarize(
      this.windowLabel,
      now - this.windowStart,
      this.windowFrames,
      longTasks,
      this.windowJs,
    );
    this.windowActive = false;
    this.windowFrames = [];
    this.windowJs = [];
    return summary;
  }

  /** Test/diagnostic hook: current window sample counts. */
  get windowSampleCounts(): { frames: number; js: number; active: boolean } {
    return {
      frames: this.windowFrames.length,
      js: this.windowJs.length,
      active: this.windowActive,
    };
  }

  /** Rolling five-second snapshot, for the overlay and for tests. */
  snapshot(): PerfSnapshot {
    this.drainObservers();
    const cutoff = performance.now() - 5000;
    const longTasks = this.longTasks.filter((task) => task.start >= cutoff);
    return summarize("rolling", 5000, this.frames, longTasks, this.js);
  }

  /** Long tasks during an explicit window, for the harness. */
  longTasksSince(start: number): LongTask[] {
    return this.longTasks.filter((task) => task.start >= start);
  }

  destroy(): void {
    this.observer?.disconnect();
    this.loafObserver?.disconnect();
    this.stopOverlayUpdates();
    this.overlay?.remove();
    this.overlay = null;
  }

  toggleOverlay(): boolean {
    this.overlayVisible = !this.overlayVisible;
    if (!this.overlay) this.createOverlay();
    if (this.overlay) this.overlay.style.display = this.overlayVisible ? "block" : "none";
    if (this.overlayVisible) this.startOverlayUpdates();
    else this.stopOverlayUpdates();
    return this.overlayVisible;
  }

  get overlayIsVisible(): boolean {
    return this.overlayVisible;
  }

  private startOverlayUpdates(): void {
    if (this.overlayTimer) return;
    this.updateOverlay();
    this.overlayTimer = window.setInterval(() => this.updateOverlay(), 250);
  }

  private stopOverlayUpdates(): void {
    if (this.overlayTimer) window.clearInterval(this.overlayTimer);
    this.overlayTimer = 0;
  }

  private updateOverlay(): void {
    if (!this.overlay || this.overlayPaused) return;
    const stats = this.snapshot();
    const rows = this.overlay.querySelectorAll<HTMLElement>("[data-value]");
    for (const row of rows) {
      const key = row.dataset.value;
      let value = "";
      if (key === "fps") value = stats.fps.toFixed(0);
      else if (key === "p95") value = stats.frameMs.p95.toFixed(1);
      else if (key === "long") value = String(stats.longTasks);
      else if (key === "js") value = stats.jsMs.mean.toFixed(1);
      row.textContent = value;
    }
  }

  private createOverlay(): void {
    const element = document.createElement("div");
    element.style.cssText = [
      "position:fixed",
      "right:16px",
      "top:58px",
      "z-index:40",
      "padding:9px 11px",
      "border:1px solid rgba(120,200,255,0.28)",
      "border-radius:10px",
      "background:rgba(8,14,28,0.82)",
      "backdrop-filter:blur(12px)",
      "font:10.5px/1.55 'JetBrains Mono', monospace",
      "letter-spacing:0.06em",
      "color:#cfe3ff",
      "pointer-events:none",
      "min-width:150px",
    ].join(";");
    element.innerHTML = `
      <div style="color:#7d92b2;letter-spacing:.12em;text-transform:uppercase;margin-bottom:3px">perf · ⇧P</div>
      <div>fps <b data-value="fps" style="float:right">–</b></div>
      <div>p95 frame <b data-value="p95" style="float:right">–</b> ms</div>
      <div>long tasks <b data-value="long" style="float:right">–</b></div>
      <div>js/frame <b data-value="js" style="float:right">–</b> ms</div>
    `;
    element.style.display = "none";
    document.body.appendChild(element);
    this.overlay = element;
  }
}
