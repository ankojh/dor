import { afterEach, describe, expect, it, vi } from "vitest";
import { afterShellPaint } from "./load";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
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
    vi.useFakeTimers();
    vi.spyOn(performance, "getEntriesByName").mockReturnValue([]);
    let deliver: ((entries: PerformanceEntry[]) => void) | null = null;
    const disconnect = vi.fn();
    vi.stubGlobal(
      "PerformanceObserver",
      class {
        static supportedEntryTypes = ["paint"];
        constructor(cb: (list: { getEntries(): PerformanceEntry[] }) => void) {
          deliver = (entries) => cb({ getEntries: () => entries });
        }
        observe(): void {}
        disconnect = disconnect;
      },
    );
    const controller = new AbortController();
    const promise = afterShellPaint(controller.signal);
    deliver!([{ name: "first-contentful-paint" } as PerformanceEntry]);
    await expect(promise).resolves.toBeUndefined();
    expect(disconnect).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("cleans up the fallback timer and observer when aborted before paint", async () => {
    vi.useFakeTimers();
    vi.spyOn(performance, "getEntriesByName").mockReturnValue([]);
    const disconnect = vi.fn();
    vi.stubGlobal(
      "PerformanceObserver",
      class {
        static supportedEntryTypes = ["paint"];
        observe(): void {}
        disconnect = disconnect;
      },
    );
    const controller = new AbortController();
    const promise = afterShellPaint(controller.signal);

    controller.abort();

    await expect(promise).resolves.toBeUndefined();
    expect(disconnect).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
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

  it("starts when Paint Timing is supported but never delivers an entry", async () => {
    vi.useFakeTimers();
    const disconnect = vi.fn();
    vi.spyOn(performance, "getEntriesByName").mockReturnValue([]);
    vi.stubGlobal(
      "PerformanceObserver",
      class {
        static supportedEntryTypes = ["paint"];
        observe(): void {}
        disconnect = disconnect;
      },
    );
    const promise = afterShellPaint(new AbortController().signal);
    await vi.advanceTimersByTimeAsync(1_000);
    await expect(promise).resolves.toBeUndefined();
    expect(disconnect).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });
});
