import { describe, expect, it } from "vitest";
import { nextTabStop } from "./focus";

describe("nextTabStop", () => {
  it("moves forward and wraps to the first stop", () => {
    expect(nextTabStop(3, 0, false)).toBe(1);
    expect(nextTabStop(3, 1, false)).toBe(2);
    expect(nextTabStop(3, 2, false)).toBe(0);
  });

  it("moves backward and wraps to the last stop", () => {
    expect(nextTabStop(3, 2, true)).toBe(1);
    expect(nextTabStop(3, 0, true)).toBe(2);
  });

  it("enters at the first stop on Tab and the last on Shift+Tab from the container", () => {
    expect(nextTabStop(3, -1, false)).toBe(0);
    expect(nextTabStop(3, -1, true)).toBe(2);
  });

  it("treats an out-of-range current index like the container", () => {
    expect(nextTabStop(3, 3, false)).toBe(0);
    expect(nextTabStop(3, 3, true)).toBe(2);
    expect(nextTabStop(3, 99, false)).toBe(0);
  });

  it("handles a single focusable element by wrapping onto itself", () => {
    expect(nextTabStop(1, 0, false)).toBe(0);
    expect(nextTabStop(1, 0, true)).toBe(0);
  });

  it("returns null when the dialog has no focusable elements", () => {
    expect(nextTabStop(0, -1, false)).toBeNull();
    expect(nextTabStop(0, 0, true)).toBeNull();
    expect(nextTabStop(-1, 0, false)).toBeNull();
  });
});
