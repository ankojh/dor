import { describe, expect, it } from "vitest";
import { nextSearchIndex } from "./search";

describe("nextSearchIndex", () => {
  it("moves forward and back within range", () => {
    expect(nextSearchIndex(5, 1, 1)).toBe(2);
    expect(nextSearchIndex(5, 3, -1)).toBe(2);
  });

  it("enters from no selection: down first, up last", () => {
    expect(nextSearchIndex(5, -1, 1)).toBe(0);
    expect(nextSearchIndex(5, -1, -1)).toBe(4);
  });

  it("clamps at both ends instead of wrapping", () => {
    expect(nextSearchIndex(5, 4, 1)).toBe(4);
    expect(nextSearchIndex(5, 0, -1)).toBe(0);
    expect(nextSearchIndex(5, 3, 10)).toBe(4);
    expect(nextSearchIndex(5, 1, -10)).toBe(0);
  });

  it("returns null when there are no options", () => {
    expect(nextSearchIndex(0, -1, 1)).toBeNull();
    expect(nextSearchIndex(0, 0, -1)).toBeNull();
    expect(nextSearchIndex(-2, 0, 1)).toBeNull();
  });

  it("returns null for no movement", () => {
    expect(nextSearchIndex(5, 2, 0)).toBeNull();
    expect(nextSearchIndex(5, -1, 0)).toBeNull();
  });

  it("works with a single option", () => {
    expect(nextSearchIndex(1, -1, 1)).toBe(0);
    expect(nextSearchIndex(1, -1, -1)).toBe(0);
    expect(nextSearchIndex(1, 0, 1)).toBe(0);
  });
});
