/** Bounded text cache. Reads refresh recency; failed requests cannot grow it forever. */
export class LruCache<K, V> extends Map<K, V> {
  constructor(readonly limit = 128) {
    super();
  }
  override get(key: K): V | undefined {
    const value = super.get(key);
    if (value !== undefined) {
      super.delete(key);
      super.set(key, value);
    }
    return value;
  }
  override set(key: K, value: V): this {
    super.delete(key);
    super.set(key, value);
    while (this.size > this.limit) super.delete(this.keys().next().value!);
    return this;
  }
}
