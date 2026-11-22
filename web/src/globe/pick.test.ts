/**
 * Id-pass picking: a story dot always beats a link arc under the same pixel,
 * and the 24-bit little-endian id round-trips through the shader bytes.
 */
import { describe, expect, it } from "vitest";
import { choosePick, decodePickId, encodePickId } from "./pick";

describe("choosePick", () => {
  it("prefers a story hit when both passes hit", () => {
    expect(choosePick(3, 7)).toEqual({ kind: "story", index: 3 });
    // Index 0 is a real story, not a miss.
    expect(choosePick(0, 7)).toEqual({ kind: "story", index: 0 });
  });

  it("uses the link when no story was hit", () => {
    expect(choosePick(null, 5)).toEqual({ kind: "link", index: 5 });
    expect(choosePick(-1, 5)).toEqual({ kind: "link", index: 5 });
  });

  it("picks the story when only the story pass hit", () => {
    expect(choosePick(4, null)).toEqual({ kind: "story", index: 4 });
    expect(choosePick(4, -1)).toEqual({ kind: "story", index: 4 });
  });

  it("returns null when no pass hit", () => {
    expect(choosePick(null, null)).toBeNull();
    expect(choosePick(-1, -1)).toBeNull();
    expect(choosePick(null, -2)).toBeNull();
    expect(choosePick(-2, null)).toBeNull();
  });
});

describe("pick ids", () => {
  const ids = [0, 1, 255, 256, 65535, 65536, 16777215];

  it("round-trips every boundary id through decode(encode(id))", () => {
    for (const id of ids) {
      const [red, green, blue] = encodePickId(id);
      expect(decodePickId(red, green, blue)).toBe(id);
    }
  });

  it("encodes as 24-bit little-endian bytes", () => {
    expect(encodePickId(0)).toEqual([0, 0, 0]);
    expect(encodePickId(1)).toEqual([1, 0, 0]);
    expect(encodePickId(255)).toEqual([255, 0, 0]);
    expect(encodePickId(256)).toEqual([0, 1, 0]);
    expect(encodePickId(65535)).toEqual([255, 255, 0]);
    expect(encodePickId(65536)).toEqual([0, 0, 1]);
    expect(encodePickId(16777215)).toEqual([255, 255, 255]);
    expect(encodePickId(0x123456)).toEqual([0x56, 0x34, 0x12]);
  });

  it("decodes bytes with the same little-endian weight", () => {
    expect(decodePickId(0, 0, 0)).toBe(0);
    expect(decodePickId(1, 0, 0)).toBe(1);
    expect(decodePickId(0, 1, 0)).toBe(256);
    expect(decodePickId(0, 0, 1)).toBe(65536);
    expect(decodePickId(255, 255, 255)).toBe(16777215);
    expect(decodePickId(0x56, 0x34, 0x12)).toBe(0x123456);
  });
});
