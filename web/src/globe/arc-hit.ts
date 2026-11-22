import {
  ARC_MIN_STRENGTH,
  ARC_OPACITY_MIN,
  ARC_OPACITY_MAX,
  ARC_SECOND_ALPHA_SCALE,
  FOCUS_FADE_SECONDS,
  FOCUS_AMBIENT_ALPHA,
  FOCUS_FOCUSED_ALPHA,
  FOCUS_LINKED_ALPHA,
  FOCUS_SECOND_ALPHA,
  FOCUS_OFF_THREAD_ALPHA,
} from "./encoding";
import { EARTH_RADIUS_M } from "./mercator";

// These thresholds also generate the visible/click GLSL below.
const ZOOM_LIMITS = [14000, 250, 25, 2] as const;
const STREET_LOW = 0.52;
const STREET_HIGH = 0.68;
export const ARC_ALPHA_CUTOFF = 0.004;
const clamp = (n: number) => Math.max(0, Math.min(1, n));
function smoothstep(a: number, b: number, x: number): number {
  const t = clamp((x - a) / (b - a));
  return t * t * (3 - 2 * t);
}

export type ArcVisibility = {
  zoom: number;
  length: number;
  strength: number;
  type: number;
  state: number;
  prev: number;
  at: number;
  time: number;
  focusAt: number;
  hasFocus: boolean;
  floor: number;
  dimmed: boolean;
  dropThematic: boolean;
};

export function arcVisibility(v: ArcVisibility): number {
  if (
    v.strength < ARC_MIN_STRENGTH ||
    (!v.hasFocus && v.strength < v.floor) ||
    (v.dropThematic && v.type > 1.5)
  )
    return 0;
  const focusOpacity = (state: number): number => {
    if (state > 1.5 && state < 2.5) return FOCUS_FOCUSED_ALPHA;
    if (state > 0.5 && state < 1.5) return FOCUS_LINKED_ALPHA;
    if (state > 2.5) return FOCUS_SECOND_ALPHA;
    const target = Number(v.hasFocus);
    const level = 1 - target + (2 * target - 1) * clamp((v.time - v.focusAt) / FOCUS_FADE_SECONDS);
    return 1 + (FOCUS_AMBIENT_ALPHA - 1) * level;
  };
  const fade = clamp((v.time - v.at) / FOCUS_FADE_SECONDS);
  const alpha = focusOpacity(v.prev) * (1 - fade) + focusOpacity(v.state) * fade;
  const maxKm = ZOOM_LIMITS[v.zoom > 13 ? 3 : v.zoom > 10 ? 2 : v.zoom > 8 ? 1 : 0];
  return (
    alpha *
    (v.dimmed ? 0.12 : 1) *
    (ARC_OPACITY_MIN + (ARC_OPACITY_MAX - ARC_OPACITY_MIN) * v.strength) *
    (v.state > 2.5 ? ARC_SECOND_ALPHA_SCALE : 1) *
    (v.state < 0.5 && v.hasFocus ? FOCUS_OFF_THREAD_ALPHA : 1) *
    (1 - smoothstep(maxKm * 0.8, maxKm, v.length * 0.001)) *
    (v.zoom > 13 ? smoothstep(STREET_LOW, STREET_HIGH, v.strength) : 1)
  );
}

/** Shared by visible and click passes; the CPU equivalent above uses these same constants. */
export const ARC_VISIBILITY_GLSL = `
float dorArcZoomVisibility(float arcLength) {
  float maxArcKm = uZoom > 13.0 ? ${ZOOM_LIMITS[3].toFixed(1)} : uZoom > 10.0 ? ${ZOOM_LIMITS[2].toFixed(1)} : uZoom > 8.0 ? ${ZOOM_LIMITS[1].toFixed(1)} : ${ZOOM_LIMITS[0].toFixed(1)};
  return 1.0 - smoothstep(maxArcKm * 0.8, maxArcKm, arcLength * 0.001);
}
float dorArcStreetVisibility(float strength) {
  return uZoom > 13.0 ? smoothstep(${STREET_LOW}, ${STREET_HIGH}, strength) : 1.0;
}
float dorArcVisibility(float aArcLength, float type, vec4 focus) {
  float strength = abs(focus.w);
  if (strength < DOR_MIN_STRENGTH || (uHasFocus < 0.5 && strength < uFloor) ||
      (uDropThematic > 0.5 && type > 1.5)) return 0.0;
  float fade = clamp((uTime - focus.z) / DOR_FOCUS_FADE, 0.0, 1.0);
  float alpha = mix(dorFocusOpacity(focus.y), dorFocusOpacity(focus.x), fade);
  alpha *= focus.w < 0.0 ? 0.12 : 1.0;
  alpha *= (DOR_ARC_OPACITY_MIN + DOR_ARC_OPACITY_SPAN * strength);
  alpha *= focus.x > 2.5 ? DOR_ARC_SECOND_ALPHA : 1.0;
  alpha *= focus.x < 0.5 && uHasFocus > 0.5 ? DOR_OFF_THREAD_ALPHA : 1.0;
  return alpha * dorArcZoomVisibility(aArcLength) * dorArcStreetVisibility(strength);
}
`;

export type ArcProjection = {
  main: ArrayLike<number>;
  fallback: ArrayLike<number>;
  anchor: ArrayLike<number>;
  tile: ArrayLike<number>;
  clip: ArrayLike<number>;
  transition: number;
  globe: boolean;
  width: number;
  height: number;
  anchorHiX: number;
  anchorHiY: number;
  anchorLoX: number;
  anchorLoY: number;
};

function transform(m: ArrayLike<number>, x: number, y: number, z: number): number[] {
  return [0, 1, 2, 3].map((i) => m[i]! * x + m[i + 4]! * y + m[i + 8]! * z + m[i + 12]!);
}

/** CPU counterpart of dorProject + dorClipped, including MapLibre's elevated globe clipping Z. */
export function projectArcVertex(
  s: ArcProjection,
  hiX: number,
  hiY: number,
  loX: number,
  loY: number,
  elevation: number,
): { x: number; y: number } | null {
  let c: number[];
  if (!s.globe) {
    c = transform(
      s.anchor,
      hiX - s.anchorHiX + (loX - s.anchorLoX),
      hiY - s.anchorHiY + (loY - s.anchorLoY),
      0,
    );
  } else {
    // hi+lo is a float addition in the vertex shader.
    const x = Math.fround(hiX + loX),
      y = Math.fround(hiY + loY);
    const lng = (s.tile[0]! + s.tile[2]! * x) * Math.PI * 2 + Math.PI;
    const lat =
      2 * Math.atan(Math.exp(Math.PI - (s.tile[1]! + s.tile[3]! * y) * Math.PI * 2)) - Math.PI / 2;
    const r = 1 + elevation / EARTH_RADIUS_M;
    const sx = Math.sin(lng) * Math.cos(lat) * r,
      sy = Math.sin(lat) * r,
      sz = Math.cos(lng) * Math.cos(lat) * r;
    c = transform(s.main, sx, sy, sz);
    c[2] = (1 - (sx * s.clip[0]! + sy * s.clip[1]! + sz * s.clip[2]! + s.clip[3]!)) * c[3]!;
    if (s.transition <= 0.999) {
      const flat = transform(s.fallback, x, y, elevation);
      c[2] *= clamp((s.transition - 0.2) / 0.8);
      for (const i of [0, 1, 3]) c[i] = flat[i]! * (1 - s.transition) + c[i]! * s.transition;
    }
    if (c[3] === 0 || c[2]! / c[3]! < -1 || c[2]! / c[3]! > 1) return null;
  }
  if (c[3]! <= 0 || !c.every(Number.isFinite)) return null;
  return {
    x: ((c[0]! / c[3]!) * 0.5 + 0.5) * s.width,
    y: (0.5 - (c[1]! / c[3]!) * 0.5) * s.height,
  };
}

export type HitArcArrays = {
  startHi: ArrayLike<number>;
  startLo: ArrayLike<number>;
  endHi: ArrayLike<number>;
  endLo: ArrayLike<number>;
  startElev: ArrayLike<number>;
  endElev: ArrayLike<number>;
};

/** Only the focused adjacency's uploaded segments are visited; never a GPU call or scene scan. */
export function hitArcSegments(
  arcs: HitArcArrays,
  offset: number,
  count: number,
  projection: ArcProjection,
  x: number,
  y: number,
  radius: number,
): boolean {
  for (let i = offset; i < offset + count; i++) {
    const a = projectArcVertex(
      projection,
      arcs.startHi[i * 2]!,
      arcs.startHi[i * 2 + 1]!,
      arcs.startLo[i * 2]!,
      arcs.startLo[i * 2 + 1]!,
      arcs.startElev[i]!,
    );
    const b = projectArcVertex(
      projection,
      arcs.endHi[i * 2]!,
      arcs.endHi[i * 2 + 1]!,
      arcs.endLo[i * 2]!,
      arcs.endLo[i * 2 + 1]!,
      arcs.endElev[i]!,
    );
    // The renderer rejects a whole segment when either endpoint is clipped.
    if (!a || !b) continue;
    const dx = b.x - a.x,
      dy = b.y - a.y;
    const t = clamp(((x - a.x) * dx + (y - a.y) * dy) / (dx * dx + dy * dy || 1));
    if (Math.hypot(x - a.x - t * dx, y - a.y - t * dy) <= radius) return true;
  }
  return false;
}
