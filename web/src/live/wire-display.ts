/**
 * Display rules for wire rows. Pure functions so ordering, the rolling rate,
 * place labels and provenance can be tested against a deterministic fixture
 * without a browser or the live stack.
 */
import type { WireRow } from "../gen/dor/v1/dor_pb";
import { displaySourceName } from "./source-names";

/** Wire rows keep their publish time and the moment they entered this client. */
export type WireItemLike = {
  row: WireRow;
  key: string;
  receivedAt: number;
  /**
   * True for cold-start snapshot rows. They are history, not arrivals, so the
   * rolling rate must ignore them; otherwise every load or reconnect spikes.
   */
  hydrated?: boolean;
};

/** Provenance fields F1·2 adds to WireRow. Zero values mean unknown. */
export type WireProvenanceFields = {
  degraded?: boolean;
  fallback?: boolean;
  categoryBackend?: string;
  categoryConfidence?: number;
  placeBackend?: string;
  placeDegraded?: boolean;
  placeFallback?: boolean;
};

export type WireStoryContext = {
  title: string;
  sourceCount: number;
  /** Primary place name from the live story, when it has one. */
  place?: string;
};

export const RATE_WINDOW_MS = 60_000;
const RATE_BUCKETS = 5;

/**
 * Effective publish time. The server clamps a future source clock at ingest;
 * this is only a backstop for rows stored before that fix (seen as 22:52 at
 * 14:58 UTC). A normal server value is already in the past and is untouched.
 */
export function wirePublishMs(row: WireRow, receivedAt: number): number {
  const seconds = Number(row.at);
  const published = Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : receivedAt;
  return Math.min(published, receivedAt);
}

/** Newest-first by publish time; ties keep the newest arrival, then the key. */
export function sortWireItems<T extends WireItemLike>(items: readonly T[]): T[] {
  return [...items].sort((a, b) => {
    const delta = wirePublishMs(b.row, b.receivedAt) - wirePublishMs(a.row, a.receivedAt);
    if (delta !== 0) return delta;
    if (a.receivedAt !== b.receivedAt) return b.receivedAt - a.receivedAt;
    return a.key.localeCompare(b.key);
  });
}

/**
 * Articles that arrived live in the last rolling minute. Publish time is not
 * arrival time: a real feed reaches the wire 1–10 minutes after publication
 * (poll interval, normalize, embed, cluster), so counting publish times shows
 * "— / min" on a healthy stream. Cold-start snapshot rows are excluded.
 */
export function rollingMinuteRate(items: readonly WireItemLike[], nowMs: number): number {
  const cutoff = nowMs - RATE_WINDOW_MS;
  let count = 0;
  for (const item of items) {
    if (item.hydrated) continue;
    if (item.receivedAt >= cutoff && item.receivedAt <= nowMs) count++;
  }
  return count;
}

/** Five 12 s arrival buckets across the same rolling minute (12–100). */
export function rollingMinuteBars(items: readonly WireItemLike[], nowMs: number): number[] {
  const cutoff = nowMs - RATE_WINDOW_MS;
  const buckets = new Array<number>(RATE_BUCKETS).fill(0);
  for (const item of items) {
    if (item.hydrated) continue;
    const at = item.receivedAt;
    if (at < cutoff || at > nowMs) continue;
    const bucket = Math.min(
      RATE_BUCKETS - 1,
      Math.floor((at - cutoff) / (RATE_WINDOW_MS / RATE_BUCKETS)),
    );
    buckets[bucket] = (buckets[bucket] ?? 0) + 1;
  }
  const max = Math.max(1, ...buckets);
  return buckets.map((value) => Math.max(12, Math.round((value / max) * 100)));
}

/** UTC clock, HH:MM:SS, from the effective (never future) publish time. */
export function wireTime(row: WireRow, receivedAt: number): string {
  return new Date(wirePublishMs(row, receivedAt)).toISOString().slice(11, 19);
}

const REGION_NAMES =
  typeof Intl !== "undefined" && typeof Intl.DisplayNames === "function"
    ? new Intl.DisplayNames(["en"], { type: "region" })
    : null;

function regionName(code: string): string {
  const region = code.trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(region)) return "";
  try {
    const name = REGION_NAMES?.of(region) ?? "";
    return name && name !== region ? name : region;
  } catch {
    return region;
  }
}

export type WirePlace = {
  text: string;
  global: boolean;
  title: string;
};

/**
 * Located rows show a place; unlocated rows get the "global" tag. A located
 * row whose own place is not filled yet falls back to the live story's
 * primary place, then the country, then "Located" — never a dash.
 */
export function wirePlace(row: WireRow, storyPlace = ""): WirePlace {
  const place = row.place.trim();
  if (!row.located) {
    return {
      text: "Global",
      global: true,
      title: place ? `No mapped location · source dateline ${place}` : "No mapped location",
    };
  }
  if (place) return { text: place, global: false, title: place };
  const fromStory = storyPlace.trim();
  if (fromStory) {
    return { text: fromStory, global: false, title: `Story place · ${fromStory}` };
  }
  const country = regionName(row.country);
  if (country) {
    return { text: country, global: false, title: `Country only · ${country}` };
  }
  return { text: "Located", global: false, title: "Mapped location; place name pending" };
}

export type WireMetadataProvenance = {
  approx: boolean;
  category: boolean;
  place: boolean;
  title: string;
};

/**
 * Reads F1·2's provenance fields through one seam. Category is approximate
 * only when the rule backend classified it (matching the story panel);
 * `degraded` is a combined category-or-place flag and must not be read as
 * category-degraded. Place is approximate when `place_degraded` is set.
 */
export function wireMetadataProvenance(row: WireRow): WireMetadataProvenance {
  const meta = row as WireRow & WireProvenanceFields;
  const categoryBackend = (meta.categoryBackend ?? "").trim();
  const confidence = Number(meta.categoryConfidence ?? 0);
  const placeBackend = (meta.placeBackend ?? "").trim();
  const category = categoryBackend.toLowerCase() === "rule";
  const place = meta.placeDegraded === true;
  const parts: string[] = [];
  if (category) {
    let text = "Category approximated by the rule classifier";
    if (confidence > 0) text += ` (${Math.round(confidence * 100)}% confidence)`;
    parts.push(text);
  }
  if (place) {
    parts.push(placeBackend ? `Place approximated from ${placeBackend}` : "Place approximated");
  }
  if (parts.length > 0 && (meta.fallback === true || meta.placeFallback === true)) {
    parts.push("Fallback path used");
  }
  return {
    approx: category || place,
    category,
    place,
    title: parts.length > 0 ? parts.join(" · ") : "Metadata approximate",
  };
}

/** Source publication name, never the raw id slug. */
export function wireSourceName(row: WireRow): string {
  return displaySourceName(row.source);
}

/**
 * Join context as a story title and source count, never a raw story id.
 * `story` is the live summary; when it has not arrived yet the line stays
 * honest and short, and updates in place when the summary lands.
 */
export function wireJoin(row: WireRow, story?: WireStoryContext): string {
  if (row.join === 1) {
    const title = story?.title.trim() ?? "";
    if (!title) return "↳ joins a story";
    const sources = Math.max(0, Math.trunc(story?.sourceCount ?? 0));
    return sources > 0
      ? `↳ joins: ${title} · ${sources} source${sources === 1 ? "" : "s"}`
      : `↳ joins: ${title}`;
  }
  if (!row.storyId) return "";
  const sources = Math.max(0, Math.trunc(story?.sourceCount ?? 0));
  return sources > 1 ? `↳ new story · ${sources} sources` : "↳ new story";
}
