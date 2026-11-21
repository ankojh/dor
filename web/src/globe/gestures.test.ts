import { describe, expect, it } from "vitest";
import { GESTURE_APPLY_INTERVAL_MS, PatchHold } from "./gestures";

const merge = (older: number[], newer: number[]): number[] => [...older, ...newer];

describe("PatchHold", () => {
  it("applies immediately when no gesture is active", () => {
    const hold = new PatchHold(merge);
    hold.push([1]);
    expect(hold.take(0)).toEqual([1]);
    hold.push([2]);
    expect(hold.take(1)).toEqual([2]);
    expect(hold.take(2)).toBeNull();
  });

  it("holds and merges patches during a gesture until the interval passes", () => {
    expect(GESTURE_APPLY_INTERVAL_MS).toBe(250);
    const hold = new PatchHold(merge);
    hold.setGestureActive(true);
    hold.push([1]);
    expect(hold.take(1000)).toEqual([1]);

    hold.push([2]);
    hold.push([3]);
    expect(hold.hasPending()).toBe(true);
    expect(hold.take(1100)).toBeNull();
    expect(hold.take(1249)).toBeNull();
    expect(hold.take(1250)).toEqual([2, 3]);
    expect(hold.hasPending()).toBe(false);
  });

  it("flush applies immediately and clears the pending patch", () => {
    const hold = new PatchHold(merge);
    hold.setGestureActive(true);
    hold.push([1]);
    hold.take(1000);
    hold.push([2]);
    hold.push([3]);
    expect(hold.flush()).toEqual([2, 3]);
    expect(hold.hasPending()).toBe(false);
    expect(hold.flush()).toBeNull();
  });

  it("applies right away once the gesture ends", () => {
    const hold = new PatchHold(merge);
    hold.setGestureActive(true);
    hold.push([1]);
    hold.take(1000);
    hold.push([2]);
    expect(hold.take(1050)).toBeNull();
    hold.setGestureActive(false);
    expect(hold.isGestureActive()).toBe(false);
    expect(hold.take(1050)).toEqual([2]);
  });

  it("reports when the next apply is due", () => {
    const hold = new PatchHold(merge);
    expect(hold.dueIn(5)).toBe(0);
    hold.setGestureActive(true);
    hold.push([1]);
    hold.take(1000);
    expect(hold.dueIn(1100)).toBe(150);
    expect(hold.dueIn(2000)).toBe(0);
    hold.setGestureActive(false);
    expect(hold.dueIn(1100)).toBe(0);
  });
});
