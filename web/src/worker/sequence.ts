/**
 * Monotonic sequencing for the gateway delta wire.
 *
 * Every delta carries a gateway sequence number. A resumed connection can
 * deliver duplicates, and a filtered client legitimately sees gaps because
 * the gateway drops wire rows that do not match its filters. So gaps are only
 * treated as transport losses when no wire constraint can explain them.
 */
import type { WireFilters } from "./messages";

export type SequenceDisposition = "apply" | "duplicate" | "gap";

export class DeltaSequence {
  private last = 0;

  get lastSeq(): number {
    return this.last;
  }

  reset(seq = 0): void {
    this.last = Number.isFinite(seq) ? Math.max(0, Math.floor(seq)) : 0;
  }

  /**
   * Decide what to do with a delta before applying it. `allowGaps` is set when
   * the gateway is filtering wire rows for this client, because those skipped
   * rows are indistinguishable from lost frames.
   */
  classify(seq: number, allowGaps = false): SequenceDisposition {
    if (!Number.isFinite(seq)) return "duplicate";
    const value = Math.floor(seq);
    if (value <= this.last) return "duplicate";
    if (!allowGaps && value > this.last + 1) return "gap";
    return "apply";
  }

  commit(seq: number): void {
    if (!Number.isFinite(seq)) return;
    const value = Math.floor(seq);
    if (value > this.last) this.last = value;
  }
}

/** Matches the gateway's `hasWireConstraints`: only these filters drop rows. */
export function hasWireConstraints(filters: WireFilters | null | undefined): boolean {
  if (!filters) return false;
  return (
    filters.categories.length > 0 ||
    filters.regions.length > 0 ||
    filters.minSources > 0 ||
    filters.lang.length > 0 ||
    filters.sources.length > 0 ||
    filters.radiusKm > 0
  );
}
