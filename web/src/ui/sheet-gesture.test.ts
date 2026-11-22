import { describe, expect, it } from "vitest";
import { sheetSwipeDirection } from "./sheet-gesture";

describe("sheetSwipeDirection", () => {
  it("ignores movement below the default 40px threshold", () => {
    expect(sheetSwipeDirection(100, 139)).toBe(0); // 39px down
    expect(sheetSwipeDirection(100, 61)).toBe(0); // 39px up
    expect(sheetSwipeDirection(100, 100)).toBe(0); // no movement
  });

  it("treats exactly 40px as a swipe because the threshold check is strict", () => {
    expect(sheetSwipeDirection(100, 140)).toBe(-1); // 40px down
    expect(sheetSwipeDirection(100, 60)).toBe(1); // 40px up
  });

  it("returns +1 for a swipe up and -1 for a swipe down past the threshold", () => {
    expect(sheetSwipeDirection(200, 159)).toBe(1); // 41px up
    expect(sheetSwipeDirection(200, 241)).toBe(-1); // 41px down
  });

  it("honours a custom threshold", () => {
    expect(sheetSwipeDirection(0, 99, 100)).toBe(0);
    expect(sheetSwipeDirection(0, 100, 100)).toBe(-1);
    expect(sheetSwipeDirection(0, -150, 100)).toBe(1);
  });

  it("returns 0 when the gesture starts and ends at the same point", () => {
    expect(sheetSwipeDirection(123, 123)).toBe(0);
  });
});
