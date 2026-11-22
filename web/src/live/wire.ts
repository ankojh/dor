/**
 * Wire rows are upserted by article_id. article.clean arrives first; later
 * placement/prompt updates carry only a subset of fields (or protobuf zero
 * values), so never overwrite known metadata with an empty update.
 */
import type { WireRow } from "../gen/dor/v1/dor_pb";

type WireExtra = {
  country?: string;
  lang?: string;
  lat?: number;
  lon?: number;
  located?: boolean;
  approxPrompts?: string[];
  // F1·2 provenance: append-only and zero means unknown, so a partial update
  // must never erase a known degraded marker.
  degraded?: boolean;
  fallback?: boolean;
  categoryBackend?: string;
  categoryConfidence?: number;
  placeBackend?: string;
  placeDegraded?: boolean;
  placeFallback?: boolean;
};

export function mergeWireRow(existing: WireRow, incoming: WireRow): WireRow {
  const merged = {
    ...existing,
    ...incoming,
    storyId: incoming.storyId || existing.storyId,
    title: incoming.title || existing.title,
    source: incoming.source || existing.source,
    place: incoming.place || existing.place,
    category:
      incoming.category !== 0 || existing.category === 0 ? incoming.category : existing.category,
    at: Number(incoming.at) !== 0 ? incoming.at : existing.at,
    join: incoming.join === 1 || existing.join !== 1 ? incoming.join : existing.join,
    promptScores:
      Object.keys(incoming.promptScores).length > 0 ? incoming.promptScores : existing.promptScores,
  } as WireRow & WireExtra;
  const oldExtra = existing as WireRow & WireExtra;
  const newExtra = incoming as WireRow & WireExtra;
  const extra = merged as WireRow & WireExtra;
  extra.country = newExtra.country || oldExtra.country;
  extra.lang = newExtra.lang || oldExtra.lang;
  extra.lat = newExtra.lat !== 0 && newExtra.lat !== undefined ? newExtra.lat : oldExtra.lat;
  extra.lon = newExtra.lon !== 0 && newExtra.lon !== undefined ? newExtra.lon : oldExtra.lon;
  extra.located = newExtra.located || oldExtra.located;
  extra.approxPrompts =
    newExtra.approxPrompts && newExtra.approxPrompts.length > 0
      ? newExtra.approxPrompts
      : oldExtra.approxPrompts;
  extra.degraded = newExtra.degraded || oldExtra.degraded;
  extra.fallback = newExtra.fallback || oldExtra.fallback;
  extra.categoryBackend = newExtra.categoryBackend || oldExtra.categoryBackend;
  extra.categoryConfidence =
    newExtra.categoryConfidence !== undefined && newExtra.categoryConfidence !== 0
      ? newExtra.categoryConfidence
      : oldExtra.categoryConfidence;
  extra.placeBackend = newExtra.placeBackend || oldExtra.placeBackend;
  extra.placeDegraded = newExtra.placeDegraded || oldExtra.placeDegraded;
  extra.placeFallback = newExtra.placeFallback || oldExtra.placeFallback;
  return merged;
}
