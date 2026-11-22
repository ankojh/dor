import { afterEach, describe, expect, it, vi } from "vitest";
import { afterShellPaint } from "./load";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("afterShellPaint", () => {
  it("returns immediately when the signal is already aborted", async () => {
    const controller = new AbortController();
    controller.abort();
    await expect(afterShellPaint(controller.signal)).resolves.toBeUndefined();
  });

  it("resolves immediately when first-contentful-paint was already recorded", async () => {
    vi.stubGlobal(
      "PerformanceObserver",
      class {
        static supportedEntryTypes = ["paint"];
        observe(): void {}
        disconnect(): void {}
      },
    );
    vi.spyOn(performance, "getEntriesByName").mockReturnValue([{} as PerformanceEntry]);
    const controller = new AbortController();
    await expect(afterShellPaint(controller.signal)).resolves.toBeUndefined();
  });

  it("resolves once the PerformanceObserver reports a first-contentful-paint entry", async () => {
    vi.spyOn(performance, "getEntriesByName").mockReturnValue([]);
    let deliver: ((entries: PerformanceEntry[]) => void) | null = null;
    vi.stubGlobal(
      "PerformanceObserver",
      class {
        static supportedEntryTypes = ["paint"];
        constructor(cb: (list: { getEntries(): PerformanceEntry[] }) => void) {
          deliver = (entries) => cb({ getEntries: () => entries });
        }
        observe(): void {}
        disconnect(): void {}
      },
    );
    const controller = new AbortController();
    const promise = afterShellPaint(controller.signal);
    deliver!([{ name: "first-contentful-paint" } as PerformanceEntry]);
    await expect(promise).resolves.toBeUndefined();
  });

  it("falls back to a double-rAF-then-task wait when Paint Timing is unavailable", async () => {
    vi.stubGlobal("PerformanceObserver", undefined);
    const rafQueue: Array<() => void> = [];
    vi.stubGlobal("requestAnimationFrame", (cb: () => void): number => {
      rafQueue.push(cb);
      return 0;
    });
    const controller = new AbortController();
    const promise = afterShellPaint(controller.signal);
    await Promise.resolve();
    expect(rafQueue).toHaveLength(1);
    rafQueue.shift()!();
    await Promise.resolve();
    expect(rafQueue).toHaveLength(1);
    rafQueue.shift()!();
    await expect(promise).resolves.toBeUndefined();
  });
});
