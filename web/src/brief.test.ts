/**
 * BriefLoader: completion by story identity, one in-flight request per story,
 * and a retry timer that belongs to the focused story only.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { BriefLoader, type StoryText } from "./brief";

const START = 1_700_000_000_000;
const RETRY_MS = 5_000;

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function success(storyId: string): StoryText {
  return {
    loading: false,
    available: true,
    title: `Title ${storyId}`,
    brief: `Brief ${storyId}`,
    provenance: "generated",
  };
}

function miss(): StoryText {
  return { loading: false, available: false, provenance: "none" };
}

function harness() {
  const calls: Array<{ storyId: string; deferred: Deferred<StoryText> }> = [];
  const fetchBrief = vi.fn((storyId: string) => {
    const call = deferred<StoryText>();
    calls.push({ storyId, deferred: call });
    return call.promise;
  });
  let focused: string | null = null;
  const shown: Array<{ storyId: string; text: StoryText }> = [];
  const loader = new BriefLoader(
    fetchBrief,
    () => focused,
    (storyId, text) => {
      shown.push({ storyId, text });
    },
    RETRY_MS,
  );
  const callsFor = (storyId: string) => calls.filter((call) => call.storyId === storyId);
  return {
    loader,
    fetchBrief,
    calls,
    shown,
    callsFor,
    focus(storyId: string | null) {
      focused = storyId;
    },
  };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(START);
});

afterEach(() => {
  vi.useRealTimers();
});

describe("BriefLoader identity completion", () => {
  it("applies an in-flight request after returning to its story, never stranding it on loading", async () => {
    const h = harness();

    h.focus("A");
    const loadA = h.loader.load("A");
    expect(h.callsFor("A")).toHaveLength(1);

    h.focus("B");
    const loadB = h.loader.load("B");
    expect(h.callsFor("B")).toHaveLength(1);

    // Returning to A while its request is still in flight must not start a
    // second request, and B must not replace A's panel.
    h.focus("A");
    await h.loader.load("A");
    expect(h.callsFor("A")).toHaveLength(1);
    const shownForA = h.shown.filter((entry) => entry.storyId === "A");
    expect(shownForA.at(-1)).toMatchObject({ text: { loading: true } });

    // B succeeds while A is focused: cache it, but do not show it.
    h.callsFor("B")[0]!.deferred.resolve(success("B"));
    await loadB;
    expect(h.loader.size).toBe(1);
    expect(h.shown.some((entry) => entry.storyId === "B" && !entry.text.loading)).toBe(false);

    // A succeeds after the focus round trip: identity, not a global token,
    // decides that this completion may fill the panel.
    h.callsFor("A")[0]!.deferred.resolve(success("A"));
    await loadA;

    expect(h.loader.size).toBe(2);
    expect(h.loader.cached("A")).toMatchObject({ loading: false, available: true });
    expect(h.shown.at(-1)).toEqual({ storyId: "A", text: success("A") });
  });
});

describe("BriefLoader retry ownership", () => {
  it("retries the focused story after the TTL; an inactive miss does not cancel that timer", async () => {
    const h = harness();

    h.focus("A");
    const loadA = h.loader.load("A");
    h.focus("B");
    const loadB = h.loader.load("B");

    // B's miss resolves first while B is focused, so B owns the retry timer.
    h.callsFor("B")[0]!.deferred.resolve(miss());
    await loadB;

    // A's miss resolves later while A is inactive. It must be cached but must
    // not clear or replace B's timer.
    h.callsFor("A")[0]!.deferred.resolve(miss());
    await loadA;

    await vi.advanceTimersByTimeAsync(RETRY_MS - 1);
    expect(h.callsFor("B")).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(1);
    expect(h.callsFor("B")).toHaveLength(2);
    expect(h.callsFor("A")).toHaveLength(1);

    h.loader.dispose();
  });

  it("does not let an inactive story's cached miss replace the focused story's retry", async () => {
    const h = harness();

    h.focus("A");
    const loadA = h.loader.load("A");
    h.callsFor("A")[0]!.deferred.resolve(miss());
    await loadA;

    h.focus("B");
    const loadB = h.loader.load("B");
    h.callsFor("B")[0]!.deferred.resolve(miss());
    await loadB;

    // Loading A's cached miss while B stays focused must leave B's retry
    // untouched: the single timer always belongs to the focused story.
    await h.loader.load("A");
    expect(h.callsFor("A")).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(h.callsFor("B")).toHaveLength(2);
    expect(h.callsFor("A")).toHaveLength(1);

    h.loader.dispose();
  });

  it("cancels the pending retry on dispose", async () => {
    const h = harness();

    h.focus("A");
    const loadA = h.loader.load("A");
    h.callsFor("A")[0]!.deferred.resolve(miss());
    await loadA;
    expect(h.callsFor("A")).toHaveLength(1);

    h.loader.dispose();
    await vi.advanceTimersByTimeAsync(RETRY_MS * 4);

    expect(h.callsFor("A")).toHaveLength(1);
  });

  it("drops a pending request's miss after dispose: no show, no retry scheduled", async () => {
    const h = harness();

    h.focus("A");
    const loadA = h.loader.load("A");
    expect(h.callsFor("A")).toHaveLength(1);

    // Dispose while the request is still in flight (e.g. the owning App
    // instance is torn down), then let it resolve as missing afterward.
    h.loader.dispose();
    h.callsFor("A")[0]!.deferred.resolve(miss());
    await loadA;

    // A disposed loader must not push the late result into the panel.
    expect(h.shown.some((entry) => entry.storyId === "A" && !entry.text.loading)).toBe(false);

    // Nor may it schedule or run a retry for a story it no longer owns.
    await vi.advanceTimersByTimeAsync(RETRY_MS * 4);
    expect(h.callsFor("A")).toHaveLength(1);
  });
});
