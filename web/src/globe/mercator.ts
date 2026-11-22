/**
 * Web-mercator helpers shared by the worker (geometry, hi/lo splits) and the
 * custom layer (anchor matrix).
 *
 * Precision plan for the custom WebGL layer at street zoom: vertices are
 * uploaded as a coarse mercator position (multiples of 1/8192, exactly
 * representable in float32) plus a small residual. The shader subtracts a
 * camera anchor split the same way before multiplying by a double-precision
 * anchor matrix, so nothing large is ever subtracted in float32.
 */

export const MERCATOR_GRID = 8192;
export const EARTH_RADIUS_M = 6_371_008.8;

export type Vec2 = [number, number];

export function lngLatToMercator(lng: number, lat: number): Vec2 {
  const clamped = Math.max(-85.05112878, Math.min(85.05112878, lat));
  const x = (lng + 180) / 360;
  const sin = Math.sin((clamped * Math.PI) / 180);
  const y = 0.5 - Math.log((1 + sin) / (1 - sin)) / (4 * Math.PI);
  return [x, y];
}

export function mercatorToLngLat(x: number, y: number): Vec2 {
  const lng = x * 360 - 180;
  const n = Math.PI - 2 * Math.PI * y;
  const lat = (180 / Math.PI) * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)));
  return [lng, lat];
}

/** Coarse + residual split used by the vertex shaders. */
export function splitMercator(value: number): [number, number] {
  const hi = Math.round(value * MERCATOR_GRID) / MERCATOR_GRID;
  return [hi, value - hi];
}

/** Column-major mat4 (or mat4f64) multiplied by a translation, as float32. */
export function matrixTranslate(
  m: ArrayLike<number>,
  tx: number,
  ty: number,
  tz: number,
  out: Float32Array = new Float32Array(16),
): Float32Array {
  for (let i = 0; i < 12; i++) out[i] = m[i] ?? 0;
  out[12] = (m[0] ?? 0) * tx + (m[4] ?? 0) * ty + (m[8] ?? 0) * tz + (m[12] ?? 0);
  out[13] = (m[1] ?? 0) * tx + (m[5] ?? 0) * ty + (m[9] ?? 0) * tz + (m[13] ?? 0);
  out[14] = (m[2] ?? 0) * tx + (m[6] ?? 0) * ty + (m[10] ?? 0) * tz + (m[14] ?? 0);
  out[15] = (m[3] ?? 0) * tx + (m[7] ?? 0) * ty + (m[11] ?? 0) * tz + (m[15] ?? 0);
  return out;
}

/** Unit vector on the sphere for a lng/lat in degrees. */
function toUnit(lng: number, lat: number): [number, number, number] {
  const la = (lat * Math.PI) / 180;
  const lo = (lng * Math.PI) / 180;
  return [Math.cos(la) * Math.sin(lo), Math.sin(la), Math.cos(la) * Math.cos(lo)];
}

function fromUnit(v: [number, number, number]): Vec2 {
  const lng = (Math.atan2(v[0], v[2]) * 180) / Math.PI;
  const lat = (Math.asin(Math.max(-1, Math.min(1, v[1]))) * 180) / Math.PI;
  return [lng, lat];
}

export function angularDistance(a: Vec2, b: Vec2): number {
  const ua = toUnit(a[0], a[1]);
  const ub = toUnit(b[0], b[1]);
  const dot = Math.max(-1, Math.min(1, ua[0] * ub[0] + ua[1] * ub[1] + ua[2] * ub[2]));
  return Math.acos(dot);
}

/** Spherical linear interpolation; t in 0..1. */
export function greatCirclePoint(a: Vec2, b: Vec2, t: number): Vec2 {
  const ua = toUnit(a[0], a[1]);
  const ub = toUnit(b[0], b[1]);
  const dot = Math.max(-1, Math.min(1, ua[0] * ub[0] + ua[1] * ub[1] + ua[2] * ub[2]));
  const omega = Math.acos(dot);
  if (omega < 1e-6) return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
  const sinOmega = Math.sin(omega);
  const wa = Math.sin((1 - t) * omega) / sinOmega;
  const wb = Math.sin(t * omega) / sinOmega;
  return fromUnit([ua[0] * wa + ub[0] * wb, ua[1] * wa + ub[1] * wb, ua[2] * wa + ub[2] * wb]);
}

/** Arc lift in metres above the sphere (sin curve, 10 % of the span). */
export function arcElevation(a: Vec2, b: Vec2, t: number): number {
  const distance = angularDistance(a, b) * EARTH_RADIUS_M;
  return Math.sin(Math.PI * t) * Math.min(distance * 0.035, 420_000);
}

/** Spherical centroid, weighted; used for camera fits. */
export function sphericalCentroid(points: Vec2[], weightIndex = 0): Vec2 {
  let x = 0;
  let y = 0;
  let z = 0;
  points.forEach((point, index) => {
    const v = toUnit(point[0], point[1]);
    const w = index === weightIndex ? 3 : 1;
    x += v[0] * w;
    y += v[1] * w;
    z += v[2] * w;
  });
  const length = Math.hypot(x, y, z) || 1;
  return fromUnit([x / length, y / length, z / length]);
}
