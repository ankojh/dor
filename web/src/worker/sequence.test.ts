/**
 * Behaviour of the delta-sequence guard and the wire-constraint predicate.
 *
 * The guard's job is to tell three outcomes apart: a delta to apply, a
 * duplicate/out-of-order frame to drop silently, and a gap that only means
 * lost transport frames when the gateway is not filtering on this client's
 * behalf.
 */
import { describe, expect, it } from "vitest";
import { DeltaSequence, hasWireConstraints } from "./sequence";
import type { WireFilters } from "./messages";

const unconstrained: WireFilters = {
  categories: [],
  regions: [],
  minSources: 0,
  lang: [],
  sources: [],
  lat: 0,
  lon: 0,
  radiusKm: 0,
};

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

  it("reports a gap when gaps are not allowed", () => {
    const sequence = new DeltaSequence();
    sequence.reset(3);

    expect(sequence.classify(5, false)).toBe("gap");
    expect(sequence.classify(10, false)).toBe("gap");
    // A gap is a classification only; it does not advance the marker.
    expect(sequence.lastSeq).toBe(3);
  });

  it("applies a gap when wire filters can explain the missing rows", () => {
    const sequence = new DeltaSequence();
    sequence.reset(3);

    expect(sequence.classify(5, true)).toBe("apply");
    expect(sequence.classify(99, true)).toBe("apply");
    // Still a duplicate when the seq is not newer, even with gaps allowed.
    expect(sequence.classify(3, true)).toBe("duplicate");
    expect(sequence.classify(2, true)).toBe("duplicate");
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

describe("hasWireConstraints", () => {
  it("is false without filters and for an empty filter set", () => {
    expect(hasWireConstraints(null)).toBe(false);
    expect(hasWireConstraints(undefined)).toBe(false);
    expect(hasWireConstraints(unconstrained)).toBe(false);
  });

  it("is true for each filter that can drop wire rows", () => {
    expect(hasWireConstraints({ ...unconstrained, categories: [1] })).toBe(true);
    expect(hasWireConstraints({ ...unconstrained, regions: ["EU"] })).toBe(true);
    expect(hasWireConstraints({ ...unconstrained, minSources: 2 })).toBe(true);
    expect(hasWireConstraints({ ...unconstrained, lang: ["en"] })).toBe(true);
    expect(hasWireConstraints({ ...unconstrained, sources: ["wire"] })).toBe(true);
    expect(hasWireConstraints({ ...unconstrained, radiusKm: 25 })).toBe(true);
  });

  it("does not treat a bare location as a wire constraint", () => {
    // A centre without a radius filters nothing server-side.
    expect(hasWireConstraints({ ...unconstrained, lat: 51.5, lon: -0.12 })).toBe(false);
  });

  it("ignores prompts, which are not part of the wire filter set", () => {
    const promptsOnly = { ...unconstrained, prompts: ["ceasefire"] } as unknown as WireFilters;
    expect(hasWireConstraints(promptsOnly)).toBe(false);
  });
});
