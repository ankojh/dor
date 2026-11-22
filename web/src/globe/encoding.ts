/**
 * Pure encodings for the globe layers, shared by the custom WebGL layer, the
 * deck.gl layer set and the worker. Values come from the plan (§4 visual
 * language), the relationship-strength page and the A1/A2/A3 boards.
 */

/** Per-instance focus state. Ambient is "not in the focused thread". */
export const FOCUS_AMBIENT = 0;
export const FOCUS_LINKED = 1;
export const FOCUS_FOCUSED = 2;
export const FOCUS_SECOND = 3;

export type FocusState =
  typeof FOCUS_AMBIENT | typeof FOCUS_LINKED | typeof FOCUS_FOCUSED | typeof FOCUS_SECOND;

export const LINK_CAUSAL = 0;
export const LINK_REACTION = 1;
export const LINK_THEMATIC = 2;
export type LinkType = typeof LINK_CAUSAL | typeof LINK_REACTION | typeof LINK_THEMATIC;

export const CATEGORY_OTHER = 0;
export const CATEGORY_CONFLICT = 1;
export const CATEGORY_POLITICS = 2;
export const CATEGORY_ECONOMY = 3;
export const CATEGORY_TECH = 4;
export const CATEGORY_CLIMATE = 5;
export const CATEGORY_DISASTER = 6;
export const CATEGORY_HEALTH = 7;
export const CATEGORY_SCIENCE = 8;
export const CATEGORY_SPORTS = 9;
export const CATEGORY_CULTURE = 10;

/** sRGB hex values from ui-designs/assets/dor.css, as 0..255 triples. */
export const CATEGORY_COLORS: Readonly<Record<number, readonly [number, number, number]>> = {
  [CATEGORY_OTHER]: [125, 146, 178], // --text-3
  [CATEGORY_CONFLICT]: [255, 79, 110], // #ff4f6e
  [CATEGORY_POLITICS]: [185, 160, 255], // #b9a0ff
  [CATEGORY_ECONOMY]: [255, 194, 92], // #ffc25c
  [CATEGORY_TECH]: [94, 242, 255], // #5ef2ff (accent)
  [CATEGORY_CLIMATE]: [69, 240, 166], // #45f0a6
  [CATEGORY_DISASTER]: [255, 122, 69], // #ff7a45
  [CATEGORY_HEALTH]: [255, 138, 208],
  [CATEGORY_SCIENCE]: [155, 184, 255],
  [CATEGORY_SPORTS]: [159, 232, 112], // #9fe870
  [CATEGORY_CULTURE]: [224, 163, 255], // #e0a3ff
};

export const LINK_COLORS: Readonly<Record<number, readonly [number, number, number]>> = {
  [LINK_CAUSAL]: [255, 194, 92], // #ffc25c
  [LINK_REACTION]: [255, 111, 216], // #ff6fd8
  [LINK_THEMATIC]: [94, 242, 255], // #5ef2ff
};

export const CATEGORY_LABELS: Readonly<Record<number, string>> = {
  [CATEGORY_OTHER]: "Other",
  [CATEGORY_CONFLICT]: "Conflict",
  [CATEGORY_POLITICS]: "Politics",
  [CATEGORY_ECONOMY]: "Economy",
  [CATEGORY_TECH]: "Tech",
  [CATEGORY_CLIMATE]: "Climate",
  [CATEGORY_DISASTER]: "Disaster",
  [CATEGORY_HEALTH]: "Health",
  [CATEGORY_SCIENCE]: "Science",
  [CATEGORY_SPORTS]: "Sports",
  [CATEGORY_CULTURE]: "Culture",
};

export const LINK_LABELS: Readonly<Record<number, string>> = {
  [LINK_CAUSAL]: "Causal",
  [LINK_REACTION]: "Reaction",
  [LINK_THEMATIC]: "Thematic",
};

/** Colour for a category id, with a dark-safe fallback. */
export function categoryColor(category: number): readonly [number, number, number] {
  return CATEGORY_COLORS[category] ?? CATEGORY_COLORS[CATEGORY_OTHER]!;
}

export function linkColor(type: number): readonly [number, number, number] {
  return LINK_COLORS[type] ?? LINK_COLORS[LINK_CAUSAL]!;
}

export function categoryLabel(category: number): string {
  return CATEGORY_LABELS[category] ?? "Other";
}

export function linkLabel(type: number): string {
  return LINK_LABELS[type] ?? "Causal";
}

/**
 * Encoding literals shared by TS, both GLSL paths and the docs. Keeping them
 * here is what makes the custom and deck shaders provably use the same plan.
 */
export const FOCUS_FOCUSED_ALPHA = 1;
export const FOCUS_LINKED_ALPHA = 1;
export const FOCUS_SECOND_ALPHA = 0.55;
export const FOCUS_AMBIENT_ALPHA = 0.25;
/** Ambient context outside the focused thread, only while a focus is active. */
export const FOCUS_OFF_THREAD_ALPHA = 0.08;
/** 200 ms focus fade, shared by both renderers. */
export const FOCUS_FADE_SECONDS = 0.2;

/** Opacity multiplier for a per-instance focus state. */
export function focusOpacity(state: number, focusActive: boolean): number {
  switch (state) {
    case FOCUS_FOCUSED:
      return FOCUS_FOCUSED_ALPHA;
    case FOCUS_LINKED:
      return FOCUS_LINKED_ALPHA;
    case FOCUS_SECOND:
      return FOCUS_SECOND_ALPHA;
    default:
      return focusActive ? FOCUS_AMBIENT_ALPHA : 1;
  }
}

/** Strength floor at rest; below 0.3 a link is never drawn. */
export const ARC_MIN_STRENGTH = 0.3;
export const DEFAULT_STRENGTH_FLOOR = 0.6;
/** At rest, spend the arc budget on the strongest links first. */
export const ARC_BUDGET = 900;
export const ARC_WIDTH_MIN = 0.6;
export const ARC_WIDTH_MAX = 3.4;
export const ARC_OPACITY_MIN = 0.2;
export const ARC_OPACITY_MAX = 0.95;
export const ARC_PULSE_1 = 0.5;
export const ARC_PULSE_2 = 0.75;
/** Second-degree arcs are narrower and dimmer than linked arcs. */
export const ARC_SECOND_WIDTH_SCALE = 0.65;
export const ARC_SECOND_ALPHA_SCALE = 0.5;
/** Link half-width glow multiplier in the custom shader. */
export const ARC_GLOW_WIDTH_SCALE = 1.35;

export type ArcEncoding = {
  /** Stroke width in CSS pixels, 0.6 → 3.4. */
  widthPx: number;
  /** Base line opacity, 0.2 → 0.95. */
  opacity: number;
  /** 0 = none, 1 = slow single pulse, 2 = two fast pulses. */
  pulses: 0 | 1 | 2;
  dashed: boolean;
  /** Draw these links when nothing is focused (strength >= floor and in budget). */
  visibleAtRest: boolean;
  /** Halo strength for the glow pass. */
  glow: number;
};

/**
 * Arc strength encoding from docs and the relation-strength page:
 * width 0.6→3.4 px, opacity 0.2→0.95, pulses at 0.3 / 0.5 / 0.75,
 * dashed for thematic. Below 0.30 no line exists.
 */
export function encodeArc(
  type: number,
  strength: number,
  floor: number = DEFAULT_STRENGTH_FLOOR,
): ArcEncoding {
  const s = Math.min(1, Math.max(0, strength));
  return {
    widthPx: ARC_WIDTH_MIN + (ARC_WIDTH_MAX - ARC_WIDTH_MIN) * s,
    opacity: ARC_OPACITY_MIN + (ARC_OPACITY_MAX - ARC_OPACITY_MIN) * s,
    pulses: s >= ARC_PULSE_2 ? 2 : s >= ARC_PULSE_1 ? 1 : 0,
    dashed: type === LINK_THEMATIC,
    visibleAtRest: s >= ARC_MIN_STRENGTH && s >= floor,
    glow: 3 + 16 * s,
  };
}

export const DOT_RADIUS_BASE = 2;
export const DOT_RADIUS_SOURCE = 3.6;
export const DOT_RADIUS_HEAT = 1.4;
export const DOT_SIZE_NORMALIZER = 7;
/** GPU dot radius before the zoom scale: base + normalised story size. */
export const DOT_GPU_BASE = 1.7;
export const DOT_GPU_SCALE = 2.9;

/** Dot radius in CSS pixels: 2 px base, size by source count, glow by heat. */
export function dotRadiusPx(sources: number, heat: number): number {
  const sourceTerm = Math.min(1, Math.sqrt(Math.max(0, sources)) / Math.sqrt(20));
  return (
    DOT_RADIUS_BASE +
    DOT_RADIUS_SOURCE * sourceTerm +
    DOT_RADIUS_HEAT * Math.min(1, Math.max(0, heat))
  );
}

/** True when a story counts as live (ring pulse) given its age in seconds. */
export function isLive(ageSeconds: number): boolean {
  return ageSeconds >= 0 && ageSeconds < 10;
}

/**
 * Dot size for the GPU texture: normalised so the shader can multiply.
 * Keeps the same shape as dotRadiusPx but without the base radius.
 */
export function dotSizeFactor(sources: number, heat: number): number {
  return dotRadiusPx(sources, heat) / DOT_SIZE_NORMALIZER;
}
