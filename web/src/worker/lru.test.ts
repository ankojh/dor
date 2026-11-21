import { describe, expect, it } from "vitest";
import { LruCache } from "./lru";

describe("LruCache bounds", () => {
  it("never grows past its limit, evicting the least-recently-used entry", () => {
    const cache = new LruCache<string, number>(3);
    cache.set("a", 1);
    cache.set("b", 2);
    cache.set("c", 3);
    cache.set("d", 4); // evicts "a"
    expect(cache.size).toBe(3);
    expect(cache.has("a")).toBe(false);
    expect([...cache.keys()]).toEqual(["b", "c", "d"]);
  });

  it("refreshes recency on get, so a recently-read entry survives eviction", () => {
    const cache = new LruCache<string, number>(3);
    cache.set("a", 1);
    cache.set("b", 2);
    cache.set("c", 3);
    cache.get("a"); // "a" is now most-recently-used
    cache.set("d", 4); // must evict "b", the true least-recently-used
    expect(cache.has("a")).toBe(true);
    expect(cache.has("b")).toBe(false);
    expect([...cache.keys()]).toEqual(["c", "a", "d"]);
  });

  it("re-setting an existing key moves it to the front without growing the cache", () => {
    const cache = new LruCache<string, number>(3);
    cache.set("a", 1);
    cache.set("b", 2);
    cache.set("c", 3);
    cache.set("a", 10); // refresh + overwrite
    expect(cache.size).toBe(3);
    expect(cache.get("a")).toBe(10);
    expect([...cache.keys()]).toEqual(["b", "c", "a"]);
  });

  it("stays within its bound under many repeated failed-request-style misses", () => {
    const cache = new LruCache<string, number | undefined>(5);
    for (let i = 0; i < 500; i++) {
      cache.get(`missing-${i}`); // a miss must never insert
      cache.set(`key-${i}`, i);
    }
    expect(cache.size).toBe(5);
  });

  it("defaults to a limit of 128", () => {
    const cache = new LruCache<number, number>();
    for (let i = 0; i < 200; i++) cache.set(i, i);
    expect(cache.size).toBe(128);
    // The 200 most recent keys (72..199) must be the survivors.
    expect(cache.has(71)).toBe(false);
    expect(cache.has(72)).toBe(true);
    expect(cache.has(199)).toBe(true);
  });
});
