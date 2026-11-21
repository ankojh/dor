import { describe, expect, it } from "vitest";
import { CapacityPool, TEXTURE_WIDTH, texelOf, textureRows } from "./gpu-layout";

describe("texture layout", () => {
  it("keeps the width at or below 2048 and grows rows", () => {
    expect(TEXTURE_WIDTH).toBe(2048);
    expect(textureRows(0)).toBe(1);
    expect(textureRows(1)).toBe(1);
    expect(textureRows(2048)).toBe(1);
    expect(textureRows(2049)).toBe(2);
    expect(textureRows(5000)).toBe(3);
    expect(textureRows(8192)).toBe(4);
  });

  it("maps an index to a 2D texel", () => {
    expect(texelOf(0)).toEqual({ x: 0, y: 0 });
    expect(texelOf(2047)).toEqual({ x: 2047, y: 0 });
    expect(texelOf(2048)).toEqual({ x: 0, y: 1 });
    expect(texelOf(4096)).toEqual({ x: 0, y: 2 });
    expect(texelOf(5000, 1024)).toEqual({ x: 904, y: 4 });
  });
});

describe("capacity pool", () => {
  it("reserves live entries and hands out the spare capacity", () => {
    const pool = new CapacityPool(10, 4);
    expect(pool.capacity).toBe(10);
    expect(pool.usedCount).toBe(4);
    expect(pool.freeCount).toBe(6);
    expect(pool.spareCapacity).toBe(6);
    expect(pool.allocate()).toBe(4);
    expect(pool.allocate()).toBe(5);
    expect(pool.usedCount).toBe(6);
    expect(pool.freeCount).toBe(4);
  });

  it("reuses freed slots and rejects double frees", () => {
    const pool = new CapacityPool(4, 2);
    const slot = pool.allocate();
    expect(slot).toBe(2);
    expect(pool.isUsed(slot)).toBe(true);
    expect(pool.free(slot)).toBe(true);
    expect(pool.isUsed(slot)).toBe(false);
    expect(pool.free(slot)).toBe(false);
    expect(pool.free(-1)).toBe(false);
    expect(pool.free(4)).toBe(false);
    expect(pool.allocate()).toBe(2);
  });

  it("returns -1 when full and clamps an oversized reserve", () => {
    const pool = new CapacityPool(2, 2);
    expect(pool.allocate()).toBe(-1);
    expect(pool.freeCount).toBe(0);
    const clamped = new CapacityPool(2, 9);
    expect(clamped.usedCount).toBe(2);
    expect(clamped.allocate()).toBe(-1);
  });
});
