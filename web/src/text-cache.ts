/**
 * Small LRU with an optional per-entry time-to-live, used for lazy text.
 *
 * Successful briefs and "why" answers live until evicted; misses and failures
 * get a short TTL so a later Editor result is picked up on the next retry
 * instead of being cached forever.
 */
import { LruCache } from "./worker/lru";

type Entry<V> = { value: V; expiresAt: number };

export class TtlLruCache<K, V> {
  private readonly entries: LruCache<K, Entry<V>>;

  constructor(limit = 128) {
    this.entries = new LruCache(limit);
  }

  get(key: K, now = Date.now()): V | undefined {
    const entry = this.entries.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt <= now) {
      this.entries.delete(key);
      return undefined;
    }
    return entry.value;
  }

  set(key: K, value: V, ttlMs = Number.POSITIVE_INFINITY): this {
    const expiresAt = Number.isFinite(ttlMs) ? Date.now() + Math.max(0, ttlMs) : Infinity;
    this.entries.set(key, { value, expiresAt });
    return this;
  }

  delete(key: K): boolean {
    return this.entries.delete(key);
  }

  clear(): void {
    this.entries.clear();
  }

  get size(): number {
    return this.entries.size;
  }
}

/**
 * Monotonic token for "latest request wins" guards. A late response can only
 * apply while no newer request has been issued.
 */
export class LatestRequest {
  private token = 0;

  next(): number {
    this.token += 1;
    return this.token;
  }

  isCurrent(candidate: number): boolean {
    return candidate === this.token;
  }
}
