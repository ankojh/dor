/**
 * TTL-LRU text cache and the latest-request token used by lazy text lookups.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LatestRequest, TtlLruCache } from "./text-cache";

const START = 1_700_000_000_000;

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(START);
});

afterEach(() => {
  vi.useRealTimers();
});

describe("TtlLruCache expiry", () => {
  it("serves an entry before its TTL and drops it at expiry", () => {
    const cache = new TtlLruCache<string, string>();
    cache.set("brief", "text", 500);

    expect(cache.size).toBe(1);
    expect(cache.get("brief", START + 499)).toBe("text");
    expect(cache.size).toBe(1);

    // Expiry is inclusive: the deadline itself is already stale.
    expect(cache.get("brief", START + 500)).toBeUndefined();
    expect(cache.size).toBe(0);
  });

  it("expires against the wall clock when no now is injected", () => {
    const cache = new TtlLruCache<string, string>();
    cache.set("why", "answer", 1_000);

    vi.setSystemTime(START + 999);
    expect(cache.get("why")).toBe("answer");

    vi.setSystemTime(START + 1_000);
    expect(cache.get("why")).toBeUndefined();
  });

  it("never expires an entry with an infinite TTL", () => {
    const cache = new TtlLruCache<string, string>();
    cache.set("brief", "kept"); // default TTL is Infinity

    vi.setSystemTime(START + 10_000_000_000);
    expect(cache.get("brief")).toBe("kept");
    expect(cache.get("brief", Number.MAX_SAFE_INTEGER)).toBe("kept");
    expect(cache.size).toBe(1);
  });
});

describe("TtlLruCache LRU bound", () => {
  it("evicts the least-recently-used entry when full", () => {
    const cache = new TtlLruCache<string, number>(2);
    cache.set("a", 1);
    cache.set("b", 2);
    cache.set("c", 3);

    expect(cache.size).toBe(2);
    expect(cache.get("a")).toBeUndefined();
    expect(cache.get("b")).toBe(2);
    expect(cache.get("c")).toBe(3);
  });

  it("refreshes recency on get, so a read entry survives eviction", () => {
    const cache = new TtlLruCache<string, number>(2);
    cache.set("a", 1);
    cache.set("b", 2);
    expect(cache.get("a")).toBe(1); // "a" is now the most recently used
    cache.set("c", 3); // must evict "b"

    expect(cache.get("a")).toBe(1);
    expect(cache.get("b")).toBeUndefined();
    expect(cache.get("c")).toBe(3);
  });
});

describe("TtlLruCache bookkeeping", () => {
  it("deletes entries and reports whether anything was removed", () => {
    const cache = new TtlLruCache<string, number>();
    cache.set("a", 1);
    cache.set("b", 2);

    expect(cache.delete("a")).toBe(true);
    expect(cache.delete("a")).toBe(false);
    expect(cache.size).toBe(1);
    expect(cache.get("a")).toBeUndefined();
    expect(cache.get("b")).toBe(2);
  });

  it("clears every entry", () => {
    const cache = new TtlLruCache<string, number>();
    cache.set("a", 1);
    cache.set("b", 2);

    cache.clear();
    expect(cache.size).toBe(0);
    expect(cache.get("a")).toBeUndefined();
    expect(cache.get("b")).toBeUndefined();
  });

  it("returns itself from set for chaining", () => {
    const cache = new TtlLruCache<string, number>();
    expect(cache.set("a", 1)).toBe(cache);
  });
});

describe("LatestRequest", () => {
  it("makes only the newest token current", () => {
    const request = new LatestRequest();
    expect(request.isCurrent(0)).toBe(true);
    expect(request.isCurrent(1)).toBe(false);

    const first = request.next();
    expect(first).toBe(1);
    expect(request.isCurrent(first)).toBe(true);

    const second = request.next();
    expect(second).toBe(2);
    expect(request.isCurrent(first)).toBe(false);
    expect(request.isCurrent(second)).toBe(true);
  });
});
