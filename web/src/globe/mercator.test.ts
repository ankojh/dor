import { describe, expect, it } from "vitest";
import {
  arcElevation,
  greatCirclePoint,
  lngLatToMercator,
  mercatorToLngLat,
  splitMercator,
  type Vec2,
} from "./mercator";

describe("mercator helpers", () => {
  it("maps (0, 0) to the centre of the unit square", () => {
    const [x, y] = lngLatToMercator(0, 0);
    expect(x).toBeCloseTo(0.5, 12);
    expect(y).toBeCloseTo(0.5, 12);
  });

  it("round-trips lng/lat through mercator", () => {
    const [x, y] = lngLatToMercator(13.4, 52.5);
    const [lng, lat] = mercatorToLngLat(x, y);
    expect(lng).toBeCloseTo(13.4, 9);
    expect(lat).toBeCloseTo(52.5, 9);
  });

  it("splits a value into a coarse part and a small residual", () => {
    for (const value of [0, 0.123456789, 0.5, 0.987654321, 1.3]) {
      const [hi, lo] = splitMercator(value);
      expect(hi + lo).toBeCloseTo(value, 12);
      expect(Math.abs(lo)).toBeLessThanOrEqual(1 / 16384 + 1e-15);
    }
  });

  it("interpolates a great circle between its endpoints", () => {
    const a: Vec2 = [-74, 40.7];
    const b: Vec2 = [139.7, 35.7];
    const start = greatCirclePoint(a, b, 0);
    const end = greatCirclePoint(a, b, 1);
    expect(start[0]).toBeCloseTo(a[0], 6);
    expect(start[1]).toBeCloseTo(a[1], 6);
    expect(end[0]).toBeCloseTo(b[0], 6);
    expect(end[1]).toBeCloseTo(b[1], 6);
  });

  it("lifts arcs only between the endpoints", () => {
    const a: Vec2 = [0, 0];
    const b: Vec2 = [40, 10];
    expect(arcElevation(a, b, 0)).toBeCloseTo(0, 6);
    expect(arcElevation(a, b, 1)).toBeCloseTo(0, 6);
    expect(arcElevation(a, b, 0.5)).toBeGreaterThan(0);
  });
});
