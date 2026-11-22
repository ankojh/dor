/**
 * Behaviour of the delta-sequence guard.
 *
 * The guard's job is to tell three outcomes apart: a delta to apply, a
 * duplicate/out-of-order frame to drop silently, and a gap that means lost
 * transport frames. The gateway keeps filtered deltas in the sequence as
 * seq-only no-ops, so a hole is never legitimate.
 */
import { describe, expect, it } from "vitest";
import { DeltaSequence } from "./sequence";

describe("DeltaSequence.classify", () => {
  it("classifies the current seq and every older seq as duplicates", () => {
    const sequence = new DeltaSequence();
    sequence.reset(5);

    expect(sequence.classify(5)).toBe("duplicate");
    expect(sequence.classify(4)).toBe("duplicate");
    expect(sequence.classify(1)).toBe("duplicate");
    // Duplicates never move the marker forward.
    expect(sequence.lastSeq).toBe(5);
  });

  it("accepts the first delta after a reset and then every contiguous delta", () => {
    const sequence = new DeltaSequence();
    sequence.reset(0);

    expect(sequence.classify(1)).toBe("apply");
    sequence.commit(1);
    expect(sequence.classify(2)).toBe("apply");
    sequence.commit(2);
    expect(sequence.classify(3)).toBe("apply");
    sequence.commit(3);
    expect(sequence.lastSeq).toBe(3);
  });

  it("accepts the first delta after a non-zero reset", () => {
    const sequence = new DeltaSequence();
    sequence.reset(41);
    expect(sequence.classify(42)).toBe("apply");
  });

  it("reports a gap when the next seq is missing", () => {
    const sequence = new DeltaSequence();
    sequence.reset(3);

    expect(sequence.classify(5)).toBe("gap");
    expect(sequence.classify(10)).toBe("gap");
    // A gap is a classification only; it does not advance the marker.
    expect(sequence.lastSeq).toBe(3);
  });

  it("still treats an older seq as a duplicate after a gap", () => {
    const sequence = new DeltaSequence();
    sequence.reset(3);

    expect(sequence.classify(3)).toBe("duplicate");
    expect(sequence.classify(2)).toBe("duplicate");
  });

  it("floors fractional seqs before comparing", () => {
    const sequence = new DeltaSequence();
    sequence.reset(5);

    expect(sequence.classify(5.9)).toBe("duplicate");
    expect(sequence.classify(6.1)).toBe("apply");
  });

  it("treats every non-finite seq as a duplicate", () => {
    const sequence = new DeltaSequence();
    sequence.reset(7);

    expect(sequence.classify(Number.NaN)).toBe("duplicate");
    expect(sequence.classify(Number.POSITIVE_INFINITY)).toBe("duplicate");
    expect(sequence.classify(Number.NEGATIVE_INFINITY)).toBe("duplicate");
    expect(sequence.lastSeq).toBe(7);
  });
});

describe("DeltaSequence.reset", () => {
  it("defaults to zero", () => {
    const sequence = new DeltaSequence();
    sequence.commit(9);
    sequence.reset();
    expect(sequence.lastSeq).toBe(0);
  });

  it("clamps negative and fractional values", () => {
    const sequence = new DeltaSequence();
    sequence.reset(-4);
    expect(sequence.lastSeq).toBe(0);
    sequence.reset(7.9);
    expect(sequence.lastSeq).toBe(7);
  });

  it("falls back to zero for a non-finite value", () => {
    const sequence = new DeltaSequence();
    sequence.commit(12);
    sequence.reset(Number.NaN);
    expect(sequence.lastSeq).toBe(0);
  });
});

describe("DeltaSequence.commit", () => {
  it("ignores a commit that would move the marker backwards", () => {
    const sequence = new DeltaSequence();
    sequence.commit(9);
    sequence.commit(3);
    expect(sequence.lastSeq).toBe(9);
  });

  it("ignores non-finite commits", () => {
    const sequence = new DeltaSequence();
    sequence.commit(4);
    sequence.commit(Number.NaN);
    sequence.commit(Number.POSITIVE_INFINITY);
    expect(sequence.lastSeq).toBe(4);
  });

  it("floors a fractional commit", () => {
    const sequence = new DeltaSequence();
    sequence.commit(6.9);
    expect(sequence.lastSeq).toBe(6);
    sequence.commit(6.1);
    expect(sequence.lastSeq).toBe(6);
  });
});
