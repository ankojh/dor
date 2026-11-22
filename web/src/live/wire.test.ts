import { describe, expect, it } from "vitest";
import type { WireRow } from "../gen/dor/v1/dor_pb";
import type { WireProvenanceFields } from "./wire-display";
import { mergeWireRow } from "./wire";

type Row = WireRow & WireProvenanceFields;

function makeRow(overrides: Partial<Row> = {}): WireRow {
  return {
    articleId: "article-1",
    storyId: "story-1",
    title: "Story title",
    source: "economictimes",
    place: "Brussels",
    category: 2,
    at: 1_700_000_000n,
    join: 1,
    promptScores: { energy: 0.9 },
    country: "BE",
    lang: "en",
    lat: 50.85,
    lon: 4.35,
    located: true,
    approxPrompts: [],
    ...overrides,
  } as unknown as WireRow;
}

/** A later article.clean/placement update that carries only zero/absent fields. */
function zeroPartialRow(): WireRow {
  return makeRow({
    storyId: "",
    title: "",
    source: "",
    place: "",
    category: 0,
    at: 0n,
    join: 0,
    promptScores: {},
    country: "",
    lang: "",
    lat: 0,
    lon: 0,
    located: false,
    approxPrompts: [],
  });
}

describe("mergeWireRow provenance", () => {
  it("keeps every known degraded marker when a partial later row omits them", () => {
    const earlier = makeRow({
      degraded: true,
      placeDegraded: true,
      categoryBackend: "rule",
      placeBackend: "hints",
      categoryConfidence: 0.62,
      placeFallback: true,
      fallback: true,
    });

    const merged = mergeWireRow(earlier, zeroPartialRow()) as Row;
    expect(merged.degraded).toBe(true);
    expect(merged.placeDegraded).toBe(true);
    expect(merged.categoryBackend).toBe("rule");
    expect(merged.placeBackend).toBe("hints");
    expect(merged.categoryConfidence).toBe(0.62);
    expect(merged.placeFallback).toBe(true);
    expect(merged.fallback).toBe(true);
  });

  it("does not erase known provenance when the later row carries explicit zero values", () => {
    const earlier = makeRow({
      degraded: true,
      placeDegraded: true,
      categoryBackend: "rule",
      placeBackend: "hints",
      categoryConfidence: 0.62,
    });
    const later = makeRow({
      degraded: false,
      placeDegraded: false,
      categoryBackend: "",
      placeBackend: "",
      categoryConfidence: 0,
    });

    const merged = mergeWireRow(earlier, later) as Row;
    expect(merged.degraded).toBe(true);
    expect(merged.placeDegraded).toBe(true);
    expect(merged.categoryBackend).toBe("rule");
    expect(merged.placeBackend).toBe("hints");
    expect(merged.categoryConfidence).toBe(0.62);
  });

  it("preserves storyId, place, category, at, join and promptScores exactly as before", () => {
    const earlier = makeRow({ degraded: true, categoryConfidence: 0.62 });
    const merged = mergeWireRow(earlier, zeroPartialRow());

    expect(merged.storyId).toBe("story-1");
    expect(merged.place).toBe("Brussels");
    expect(merged.category).toBe(2);
    expect(merged.at).toBe(1_700_000_000n);
    expect(merged.join).toBe(1);
    expect(merged.promptScores).toEqual({ energy: 0.9 });
  });
});
