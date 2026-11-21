import { describe, expect, it } from "vitest";
import { growArray } from "./arrays";

describe("growArray", () => {
  it("returns a larger array of the same typed-array kind, preserving existing values", () => {
    const original = Float32Array.from([1, 2, 3]);
    const grown = growArray(original, 6);
    expect(grown).toBeInstanceOf(Float32Array);
    expect(grown.length).toBe(6);
    expect(Array.from(grown.subarray(0, 3))).toEqual([1, 2, 3]);
    expect(Array.from(grown.subarray(3))).toEqual([0, 0, 0]);
  });

  it("is a no-op (same reference) when already at or above the requested length", () => {
    const original = Uint16Array.from([1, 2, 3]);
    expect(growArray(original, 3)).toBe(original);
    expect(growArray(original, 2)).toBe(original);
  });

  it("preserves values across every typed-array kind the caller can grow", () => {
    expect(Array.from(growArray(Uint8Array.from([9, 8]), 4))).toEqual([9, 8, 0, 0]);
    expect(Array.from(growArray(Uint32Array.from([9, 8]), 4))).toEqual([9, 8, 0, 0]);
    expect(Array.from(growArray(Int32Array.from([-1, 2]), 4))).toEqual([-1, 2, 0, 0]);
    expect(Array.from(growArray(Float64Array.from([1.5, 2.5]), 4))).toEqual([1.5, 2.5, 0, 0]);
  });
});
