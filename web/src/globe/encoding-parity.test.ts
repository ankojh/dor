import { describe, expect, it } from "vitest";
import { DECK_FOCUS_HELPERS } from "./deck/renderer";
import {
  ARC_MIN_STRENGTH,
  ARC_OPACITY_MAX,
  ARC_OPACITY_MIN,
  ARC_PULSE_1,
  ARC_PULSE_2,
  ARC_WIDTH_MAX,
  ARC_WIDTH_MIN,
  FOCUS_AMBIENT_ALPHA,
  FOCUS_FOCUSED_ALPHA,
  FOCUS_LINKED_ALPHA,
  FOCUS_OFF_THREAD_ALPHA,
  FOCUS_SECOND_ALPHA,
  LINK_CAUSAL,
  encodeArc,
} from "./encoding";
import { GLSL_CONSTANTS } from "./custom/shaders";
import {
  RIPPLE_FLASH_ALPHA,
  RIPPLE_FLASH_SECONDS,
  RIPPLE_LIFETIME_SECONDS,
  RIPPLE_OFF_THREAD_DIM,
} from "./ripples";

function macro(name: string): number {
  const match = GLSL_CONSTANTS.match(new RegExp(`#define ${name} ([0-9.]+)`));
  if (!match) throw new Error(`missing ${name}`);
  return Number(match[1]);
}

function expectMacro(name: string, value: number): void {
  expect(macro(name)).toBeCloseTo(value, 3);
}

describe("shared encodings", () => {
  it("injects the TS focus and arc values into the custom GLSL", () => {
    expectMacro("DOR_FOCUSED_ALPHA", FOCUS_FOCUSED_ALPHA);
    expectMacro("DOR_LINKED_ALPHA", FOCUS_LINKED_ALPHA);
    expectMacro("DOR_SECOND_ALPHA", FOCUS_SECOND_ALPHA);
    expectMacro("DOR_AMBIENT_ALPHA", FOCUS_AMBIENT_ALPHA);
    expectMacro("DOR_OFF_THREAD_ALPHA", FOCUS_OFF_THREAD_ALPHA);
    expectMacro("DOR_ARC_WIDTH_MIN", ARC_WIDTH_MIN);
    expectMacro("DOR_ARC_WIDTH_SPAN", ARC_WIDTH_MAX - ARC_WIDTH_MIN);
    expectMacro("DOR_ARC_OPACITY_MIN", ARC_OPACITY_MIN);
    expectMacro("DOR_ARC_OPACITY_SPAN", ARC_OPACITY_MAX - ARC_OPACITY_MIN);
    expectMacro("DOR_PULSE_1", ARC_PULSE_1);
    expectMacro("DOR_PULSE_2", ARC_PULSE_2);
    expectMacro("DOR_MIN_STRENGTH", ARC_MIN_STRENGTH);
  });

  it("injects the same values into the deck.gl focus helper", () => {
    expect(DECK_FOCUS_HELPERS).toContain(String(FOCUS_FOCUSED_ALPHA));
    expect(DECK_FOCUS_HELPERS).toContain(String(FOCUS_LINKED_ALPHA));
    expect(DECK_FOCUS_HELPERS).toContain(String(FOCUS_SECOND_ALPHA));
    expect(DECK_FOCUS_HELPERS).toContain(String(FOCUS_AMBIENT_ALPHA));
  });

  it("keeps the arc encode and the shader macros in lockstep", () => {
    const low = encodeArc(LINK_CAUSAL, 0);
    const high = encodeArc(LINK_CAUSAL, 1);
    expect(low.widthPx).toBe(ARC_WIDTH_MIN);
    expect(high.widthPx).toBe(ARC_WIDTH_MAX);
    expect(low.opacity).toBe(ARC_OPACITY_MIN);
    expect(high.opacity).toBe(ARC_OPACITY_MAX);
  });

  it("shares ripple flash muting and lifetime with the shader", () => {
    expectMacro("DOR_RIPPLE_LIFE", RIPPLE_LIFETIME_SECONDS);
    expectMacro("DOR_RIPPLE_FLASH", RIPPLE_FLASH_SECONDS);
    expectMacro("DOR_RIPPLE_DIM", RIPPLE_OFF_THREAD_DIM);
    expectMacro("DOR_RIPPLE_FLASH_ALPHA", RIPPLE_FLASH_ALPHA);
  });
});
