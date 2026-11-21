import { describe, expect, it } from "vitest";
import {
  ARC_MIN_STRENGTH,
  FOCUS_AMBIENT,
  FOCUS_FOCUSED,
  FOCUS_LINKED,
  FOCUS_SECOND,
  LINK_CAUSAL,
  LINK_REACTION,
  LINK_THEMATIC,
  dotRadiusPx,
  dotSizeFactor,
  encodeArc,
  focusOpacity,
} from "./encoding";

describe("arc strength encoding", () => {
  it("maps width and opacity linearly from the minimum to the maximum", () => {
    expect(encodeArc(LINK_CAUSAL, 0).widthPx).toBeCloseTo(0.6, 9);
    expect(encodeArc(LINK_CAUSAL, 1).widthPx).toBeCloseTo(3.4, 9);
    expect(encodeArc(LINK_CAUSAL, 0).opacity).toBeCloseTo(0.2, 9);
    expect(encodeArc(LINK_CAUSAL, 1).opacity).toBeCloseTo(0.95, 9);
  });

  it("clamps strength outside 0..1", () => {
    expect(encodeArc(LINK_CAUSAL, -2).widthPx).toBeCloseTo(0.6, 9);
    expect(encodeArc(LINK_CAUSAL, 5).widthPx).toBeCloseTo(3.4, 9);
  });

  it("pulses at the 0.5 and 0.75 thresholds", () => {
    expect(encodeArc(LINK_CAUSAL, 0.49).pulses).toBe(0);
    expect(encodeArc(LINK_CAUSAL, 0.5).pulses).toBe(1);
    expect(encodeArc(LINK_CAUSAL, 0.74).pulses).toBe(1);
    expect(encodeArc(LINK_CAUSAL, 0.75).pulses).toBe(2);
  });

  it("dashes thematic links only", () => {
    expect(encodeArc(LINK_THEMATIC, 0.5).dashed).toBe(true);
    expect(encodeArc(LINK_CAUSAL, 0.5).dashed).toBe(false);
    expect(encodeArc(LINK_REACTION, 0.5).dashed).toBe(false);
  });

  it("respects both the hard minimum and the floor for rest visibility", () => {
    expect(ARC_MIN_STRENGTH).toBe(0.3);
    expect(encodeArc(LINK_CAUSAL, 0.29, 0).visibleAtRest).toBe(false);
    expect(encodeArc(LINK_CAUSAL, 0.3, 0).visibleAtRest).toBe(true);
    expect(encodeArc(LINK_CAUSAL, 0.5).visibleAtRest).toBe(false);
    expect(encodeArc(LINK_CAUSAL, 0.6).visibleAtRest).toBe(true);
    expect(encodeArc(LINK_CAUSAL, 0.7, 0.8).visibleAtRest).toBe(false);
    expect(encodeArc(LINK_CAUSAL, 0.8, 0.8).visibleAtRest).toBe(true);
  });
});

describe("focus opacity", () => {
  it("is full for focused and linked, dimmed for second degree", () => {
    expect(focusOpacity(FOCUS_FOCUSED, true)).toBe(1);
    expect(focusOpacity(FOCUS_LINKED, true)).toBe(1);
    expect(focusOpacity(FOCUS_SECOND, true)).toBe(0.55);
  });

  it("dims ambient only while a focus is active", () => {
    expect(focusOpacity(FOCUS_AMBIENT, true)).toBe(0.25);
    expect(focusOpacity(FOCUS_AMBIENT, false)).toBe(1);
  });
});

describe("dot size", () => {
  it("grows monotonically with sources and heat", () => {
    let last = -1;
    for (const sources of [0, 1, 2, 5, 10, 20, 100]) {
      const radius = dotRadiusPx(sources, 0.5);
      expect(radius).toBeGreaterThanOrEqual(last);
      last = radius;
    }
    last = -1;
    for (const heat of [0, 0.25, 0.5, 0.75, 1]) {
      const radius = dotRadiusPx(5, heat);
      expect(radius).toBeGreaterThan(last);
      last = radius;
    }
  });

  it("keeps the size factor within 0..1", () => {
    for (const sources of [0, 1, 20, 1000, 65535]) {
      for (const heat of [0, 0.5, 1]) {
        const factor = dotSizeFactor(sources, heat);
        expect(factor).toBeGreaterThan(0);
        expect(factor).toBeLessThanOrEqual(1 + 1e-12);
      }
    }
  });
});
