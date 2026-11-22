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
  return merged;
}
