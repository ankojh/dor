/** Rolling client receipts, independent of the publish-sorted presentation ring. */
const WINDOW_MS = 60_000;

export class WireArrivals {
  // Wire deltas do not distinguish a new article from a metadata update. Keep
  // identities for the worker session so late updates are not counted again.
  private readonly seen = new Set<string>();
  private times: number[] = [];
  private start = 0;

  accept(key: string, at: number, hydrated: boolean): void {
    this.prune(at);
    if (this.seen.has(key)) return;
    this.seen.add(key);
    if (!hydrated) this.times.push(at);
  }

  recent(now: number): number[] {
    this.prune(now);
    return this.times.slice(this.start);
  }

  private prune(now: number): void {
    while (this.start < this.times.length && this.times[this.start]! < now - WINDOW_MS)
      this.start++;
    if (this.start > 1024 && this.start * 2 >= this.times.length) {
      this.times = this.times.slice(this.start);
      this.start = 0;
    }
  }
}

export function arrivalRate(times: readonly number[], now: number): number {
  return times.filter((at) => at >= now - WINDOW_MS && at <= now).length;
}

export function arrivalBars(times: readonly number[], now: number): number[] {
  const cutoff = now - WINDOW_MS;
  const bins = [0, 0, 0, 0, 0];
  for (const at of times) {
    if (at < cutoff || at > now) continue;
    const index = Math.min(4, Math.floor((at - cutoff) / 12_000));
    bins[index] = bins[index]! + 1;
  }
  const max = Math.max(1, ...bins);
  return bins.map((value) => Math.max(12, Math.round((value / max) * 100)));
}
