/**
 * Monotonic sequencing for the gateway delta wire.
 *
 * Every delta carries a gateway sequence number. A resumed connection can
 * deliver duplicates, and a filtered client sees no holes: the gateway keeps
 * the global sequence contiguous by replacing every per-client filtered delta
 * with a seq-only no-op. So a gap always means transport loss.
 */
export type SequenceDisposition = "apply" | "duplicate" | "gap";

export class DeltaSequence {
  private last = 0;

  get lastSeq(): number {
    return this.last;
  }

  reset(seq = 0): void {
    this.last = Number.isFinite(seq) ? Math.max(0, Math.floor(seq)) : 0;
  }

  /** Decide what to do with a delta before applying it. */
  classify(seq: number): SequenceDisposition {
    if (!Number.isFinite(seq)) return "duplicate";
    const value = Math.floor(seq);
    if (value <= this.last) return "duplicate";
    if (value > this.last + 1) return "gap";
    return "apply";
  }

  commit(seq: number): void {
    if (!Number.isFinite(seq)) return;
    const value = Math.floor(seq);
    if (value > this.last) this.last = value;
  }
}
