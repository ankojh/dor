import { describe, expect, it } from "vitest";
import type { WireRow } from "../gen/dor/v1/dor_pb";
import { displaySourceName, matchesSourceFilter, sourceFilterIds } from "./source-names";
import {
  RATE_WINDOW_MS,
  rollingMinuteBars,
  rollingMinuteRate,
  sortWireItems,
  wireJoin,
  wireMetadataProvenance,
  wirePlace,
  wirePublishMs,
  wireTime,
  type WireItemLike,
  type WireProvenanceFields,
  type WireStoryContext,
} from "./wire-display";

const NOW = Date.UTC(2026, 10, 22, 12, 0, 0);
const NOW_SEC = NOW / 1000;

type RowOverrides = {
  articleId?: string;
  storyId?: string;
  title?: string;
  source?: string;
  place?: string;
  category?: number;
  at?: bigint | number;
  join?: number;
  promptScores?: Record<string, number>;
  country?: string;
  lang?: string;
  lat?: number;
  lon?: number;
  located?: boolean;
  approxPrompts?: string[];
};

let seq = 0;
function makeRow(overrides: RowOverrides = {}): WireRow {
  seq += 1;
  return {
    articleId: overrides.articleId ?? `article-${seq}`,
    storyId: overrides.storyId ?? "",
    title: overrides.title ?? "Wire title",
    source: overrides.source ?? "economictimes",
    place: overrides.place ?? "",
    category: overrides.category ?? 0,
    at: (overrides.at ?? BigInt(NOW_SEC)) as bigint,
    join: overrides.join ?? 0,
    promptScores: overrides.promptScores ?? {},
    country: overrides.country ?? "",
    lang: overrides.lang ?? "en",
    lat: overrides.lat ?? 0,
    lon: overrides.lon ?? 0,
    located: overrides.located ?? false,
    approxPrompts: overrides.approxPrompts ?? [],
  } as unknown as WireRow;
}

function makeItem(row: WireRow, receivedAt = NOW, key?: string, hydrated = false): WireItemLike {
  return { row, key: key ?? row.articleId, receivedAt, hydrated };
}

/** Seeded Fisher-Yates so fixture arrival order is reproducible. */
function shuffled<T>(values: readonly T[], seed: number): T[] {
  const out = [...values];
  let state = seed >>> 0;
  for (let i = out.length - 1; i > 0; i--) {
    state = (state * 1664525 + 1013904223) >>> 0;
    const j = state % (i + 1);
    const a = out[i]!;
    const b = out[j]!;
    out[i] = b;
    out[j] = a;
  }
  return out;
}

/**
 * 120 historical rows published every 12 s plus four rows whose source
 * reported local time as UTC (8 h ahead), all arriving at the same moment.
 */
function deterministicFixture(seed: number): WireItemLike[] {
  const items: WireItemLike[] = [];
  for (let i = 0; i < 124; i++) {
    const isFuture = i >= 120;
    const at = isFuture
      ? BigInt(NOW_SEC + 8 * 3600 - (i - 120) * 30)
      : BigInt(NOW_SEC - 10 - i * 12);
    const articleId = `fixture-${i}`;
    items.push(
      makeItem(
        makeRow({ articleId, at, place: "Brussels", country: "BE", located: true }),
        NOW,
        articleId,
      ),
    );
  }
  return shuffled(items, seed);
}

describe("sortWireItems with the deterministic 124-row fixture", () => {
  it("sorts strictly newest-first by effective publish time", () => {
    const sorted = sortWireItems(deterministicFixture(1));
    expect(sorted).toHaveLength(124);
    for (let i = 1; i < sorted.length; i++) {
      const previous = sorted[i - 1]!;
      const current = sorted[i]!;
      expect(wirePublishMs(previous.row, previous.receivedAt)).toBeGreaterThanOrEqual(
        wirePublishMs(current.row, current.receivedAt),
      );
    }
  });

  it("is deterministic across shuffles of the same set", () => {
    const first = sortWireItems(deterministicFixture(11)).map((item) => item.key);
    const second = sortWireItems(deterministicFixture(20261122)).map((item) => item.key);
    expect(first).toEqual(second);
  });

  it("never reports an effective publish time later than arrival", () => {
    for (const item of deterministicFixture(7)) {
      expect(wirePublishMs(item.row, item.receivedAt)).toBeLessThanOrEqual(item.receivedAt);
    }
  });

  it("clamps future timestamps so wireTime never runs ahead of arrival", () => {
    const items = deterministicFixture(99);
    const receivedClock = new Date(NOW).toISOString().slice(11, 19);
    const future = items.filter((item) => Number(item.row.at) > NOW_SEC);
    expect(future).toHaveLength(4);

    for (const item of items) {
      expect(wirePublishMs(item.row, item.receivedAt)).toBeLessThanOrEqual(item.receivedAt);
      expect(wireTime(item.row, item.receivedAt) <= receivedClock).toBe(true);
    }
    for (const item of future) {
      expect(wirePublishMs(item.row, item.receivedAt)).toBe(NOW);
      expect(wireTime(item.row, item.receivedAt)).toBe(receivedClock);
    }
  });
});

describe("rollingMinuteRate under a steady stream", () => {
  it("holds near 15/min for every sample second after the first minute", () => {
    const start = Date.UTC(2026, 10, 22, 0, 0, 0);
    const items: WireItemLike[] = [];
    for (let k = 1; k <= 75; k++) {
      const receivedAt = start + k * 4_000; // one article every 4 s
      const articleId = `steady-${k}`;
      items.push(makeItem(makeRow({ articleId }), receivedAt, articleId));
    }

    for (let sample = 61; sample <= 300; sample++) {
      const rate = rollingMinuteRate(items, start + sample * 1000);
      expect(Math.abs(rate - 15) / 15).toBeLessThanOrEqual(0.3);
    }
  });

  it("renders five buckets across the minute whose tallest bar is 100", () => {
    const end = Date.UTC(2026, 10, 22, 0, 5, 0);
    const items: WireItemLike[] = Array.from({ length: 30 }, (_, k) => {
      const articleId = `bars-${k}`;
      return makeItem(makeRow({ articleId }), end - k * 2_000, articleId);
    });

    const bars = rollingMinuteBars(items, end);
    expect(bars).toHaveLength(5);
    expect(Math.max(...bars)).toBe(100);
  });
});

describe("rollingMinuteRate tracks arrival, not publish time", () => {
  it("holds ~30/min when a 30/min feed arrives with a 60–600 s publish lag", () => {
    const start = Date.UTC(2026, 10, 22, 0, 0, 0);
    let state = 123456789;
    const nextLagSeconds = () => {
      state = (state * 1664525 + 1013904223) >>> 0;
      return 60 + (state % 541); // 60..600 s
    };

    const items: WireItemLike[] = [];
    for (let k = 0; k < 600; k++) {
      const receivedAt = start + k * 2_000; // 30 arrivals/min for 20 min
      const articleId = `lagged-${k}`;
      const at = receivedAt / 1000 - nextLagSeconds();
      items.push(makeItem(makeRow({ articleId, at }), receivedAt, articleId));
    }

    for (let sample = 120; sample <= 1200; sample += 15) {
      const rate = rollingMinuteRate(items, start + sample * 1000);
      expect(rate).toBeGreaterThanOrEqual(21);
      expect(rate).toBeLessThanOrEqual(39);
    }

    // At the first sample every publish time is already >=60 s old, so a
    // publish-based count is near zero while arrivals show a full minute.
    const nowMs = start + 120_000;
    const arrived = items.filter((item) => item.receivedAt <= nowMs);
    const publishCount = arrived.filter((item) => {
      const at = wirePublishMs(item.row, item.receivedAt);
      return at >= nowMs - RATE_WINDOW_MS && at <= nowMs;
    }).length;
    expect(rollingMinuteRate(items, nowMs)).toBeGreaterThanOrEqual(21);
    expect(publishCount).toBeLessThan(5);
  });
});

describe("rollingMinuteRate cold start", () => {
  it("ignores the 500-row snapshot and counts only live arrivals", () => {
    const fourHoursMs = 4 * 60 * 60 * 1000;
    const stepMs = fourHoursMs / 500; // one article every 28.8 s
    const backlog: WireItemLike[] = Array.from({ length: 500 }, (_, i) => {
      const publishedMs = NOW - i * stepMs;
      const articleId = `backlog-${i}`;
      return makeItem(makeRow({ articleId, at: publishedMs / 1000 }), NOW, articleId, true);
    });

    // Snapshot history is hydrated: it is not an arrival, so it never spikes
    // the rate on load or reconnect.
    expect(rollingMinuteRate(backlog, NOW)).toBe(0);

    const live: WireItemLike[] = Array.from({ length: 30 }, (_, k) => {
      const articleId = `live-${k}`;
      return makeItem(makeRow({ articleId }), NOW - k * 2_000, articleId);
    });
    expect(rollingMinuteRate([...backlog, ...live], NOW)).toBe(30);
    expect(rollingMinuteRate([...backlog, ...live], NOW)).toBeLessThan(500);
  });
});

describe("future timestamps", () => {
  it("ignores hydrated snapshot rows while still clamping their clock", () => {
    const future = [0, 1, 2, 3].map((i) => {
      const articleId = `future-${i}`;
      return makeItem(
        makeRow({ articleId, at: BigInt(NOW_SEC + 8 * 3600 - i * 30) }),
        NOW,
        articleId,
        true,
      );
    });

    expect(rollingMinuteRate(future, NOW)).toBe(0);

    const receivedClock = new Date(NOW).toISOString().slice(11, 19);
    for (const item of future) {
      expect(wirePublishMs(item.row, item.receivedAt)).toBe(NOW);
      expect(wireTime(item.row, item.receivedAt)).toBe(receivedClock);
      expect(wireTime(item.row, item.receivedAt) <= receivedClock).toBe(true);
    }
  });
});

describe("wirePublishMs server clamp fallback", () => {
  it("leaves an already-clamped publish time untouched", () => {
    const receivedAt = Date.UTC(2026, 10, 22, 14, 5, 0);
    const at = Math.floor(Date.UTC(2026, 10, 22, 14, 0, 0) / 1000);
    expect(wirePublishMs(makeRow({ at }), receivedAt)).toBe(Date.UTC(2026, 10, 22, 14, 0, 0));
    expect(wireTime(makeRow({ at }), receivedAt)).toBe("14:00:00");
  });

  it("clamps a future source clock to the arrival time as a backstop", () => {
    const receivedAt = Date.UTC(2026, 10, 22, 14, 5, 0);
    const at = Math.floor(Date.UTC(2026, 10, 22, 14, 50, 0) / 1000);
    expect(wirePublishMs(makeRow({ at }), receivedAt)).toBe(receivedAt);
    expect(wireTime(makeRow({ at }), receivedAt)).toBe("14:05:00");
  });
});

describe("wirePlace", () => {
  it("shows the place for a located row", () => {
    const place = wirePlace(makeRow({ located: true, place: "Brussels", country: "BE" }));
    expect(place).toEqual({ text: "Brussels", global: false, title: "Brussels" });
  });

  it("marks an unlocated row Global and keeps its dateline as context", () => {
    const place = wirePlace(makeRow({ located: false, place: "Kyiv", country: "UA" }));
    expect(place.text).toBe("Global");
    expect(place.global).toBe(true);
    expect(place.title).toContain("Kyiv");
    expect(place.title).toContain("dateline");
  });

  it("falls back to the country name when a located row has no place", () => {
    const place = wirePlace(makeRow({ located: true, place: "", country: "US" }));
    expect(place.text).toBe("United States");
    expect(place.global).toBe(false);
  });

  it("uses 'Located' when neither place nor country is known", () => {
    const place = wirePlace(makeRow({ located: true, place: "", country: "" }));
    expect(place.text).toBe("Located");
    expect(place.global).toBe(false);
  });

  it("prefers the story place over the country when the row has no place", () => {
    const place = wirePlace(makeRow({ located: true, place: "", country: "US" }), "Nairobi");
    expect(place.text).toBe("Nairobi");
    expect(place.global).toBe(false);
    expect(place.title).toContain("Story place");
  });

  it("uses the story place when country is unknown, but never for an unlocated row", () => {
    const located = wirePlace(makeRow({ located: true, place: "", country: "" }), "Nairobi");
    expect(located.text).toBe("Nairobi");
    expect(located.global).toBe(false);

    const unlocated = wirePlace(makeRow({ located: false, place: "Kyiv" }), "Nairobi");
    expect(unlocated.text).toBe("Global");
    expect(unlocated.global).toBe(true);
  });

  it("never renders a dash or an empty label", () => {
    const rows = [
      makeRow({ located: true, place: "Brussels", country: "BE" }),
      makeRow({ located: false, place: "Kyiv", country: "UA" }),
      makeRow({ located: true, place: "", country: "US" }),
      makeRow({ located: true, place: "", country: "" }),
      makeRow({ located: false, place: "", country: "" }),
    ];
    for (const row of rows) {
      const { text } = wirePlace(row);
      expect(text.length).toBeGreaterThan(0);
      expect(text).not.toBe("—");
      expect(text).not.toBe("-");
    }
  });
});

const STORY: WireStoryContext = { title: "Geneva talks", sourceCount: 4 };

describe("wireJoin", () => {
  it("shows the story title and source count for a joined row, never the raw story id", () => {
    const row = makeRow({ join: 1, storyId: "story-secret-id" });
    expect(wireJoin(row, STORY)).toBe("↳ joins: Geneva talks · 4 sources");
    expect(wireJoin(row, STORY)).not.toContain("story-secret-id");
  });

  it("stays honest when the joined story summary has not arrived", () => {
    expect(wireJoin(makeRow({ join: 1, storyId: "story-1" }))).toBe("↳ joins a story");
  });

  it("labels a new story without a raw story id", () => {
    const row = makeRow({ join: 0, storyId: "story-1" });
    expect(wireJoin(row)).toBe("↳ new story");
    expect(wireJoin(row)).not.toContain("story-1");
    expect(wireJoin(row, { title: "Ignored", sourceCount: 3 })).toBe("↳ new story · 3 sources");
  });

  it("returns nothing when the row belongs to no story", () => {
    expect(wireJoin(makeRow({ join: 0, storyId: "" }))).toBe("");
  });
});

function withProvenance(overrides: WireProvenanceFields): WireRow & WireProvenanceFields {
  return Object.assign(makeRow(), overrides) as WireRow & WireProvenanceFields;
}

describe("wireMetadataProvenance", () => {
  it("leaves rows from an older backend unmarked", () => {
    const meta = wireMetadataProvenance(makeRow());
    expect(meta.approx).toBe(false);
    expect(meta.category).toBe(false);
    expect(meta.place).toBe(false);
  });

  it("flags a rule-classified category and mentions it in the title", () => {
    const meta = wireMetadataProvenance(withProvenance({ categoryBackend: "rule" }));
    expect(meta.approx).toBe(true);
    expect(meta.category).toBe(true);
    expect(meta.title.toLowerCase()).toContain("category");
  });

  it("includes the rule backend and rounded confidence for a rule category", () => {
    const meta = wireMetadataProvenance(
      withProvenance({ categoryBackend: "rule", categoryConfidence: 0.62 }),
    );
    expect(meta.approx).toBe(true);
    expect(meta.title).toContain("62% confidence");
  });

  it("flags a degraded place and mentions it in the title", () => {
    const meta = wireMetadataProvenance(withProvenance({ placeDegraded: true }));
    expect(meta.approx).toBe(true);
    expect(meta.place).toBe(true);
    expect(meta.title.toLowerCase()).toContain("place");
  });

  it("does not read the combined degraded flag as a category marker (C-7)", () => {
    const meta = wireMetadataProvenance(
      withProvenance({
        degraded: true,
        categoryBackend: "jev",
        categoryConfidence: 0.85,
        placeDegraded: true,
      }),
    );
    expect(meta.category).toBe(false);
    expect(meta.place).toBe(true);
    expect(meta.approx).toBe(true);
    expect(meta.title.toLowerCase()).toContain("place");
    expect(meta.title.toLowerCase()).not.toContain("category");
  });

  it("treats degraded alone as unknown, not as an approximate category", () => {
    const meta = wireMetadataProvenance(withProvenance({ degraded: true }));
    expect(meta.approx).toBe(false);
    expect(meta.category).toBe(false);
  });

  it("only mentions fallback alongside a real approx marker", () => {
    const alone = wireMetadataProvenance(withProvenance({ fallback: true }));
    expect(alone.approx).toBe(false);
    expect(alone.title.toLowerCase()).not.toContain("fallback");

    const withPlace = wireMetadataProvenance(
      withProvenance({ placeDegraded: true, fallback: true }),
    );
    expect(withPlace.approx).toBe(true);
    expect(withPlace.title.toLowerCase()).toContain("fallback");
  });
});

describe("displaySourceName", () => {
  it("maps known source ids to publication names", () => {
    expect(displaySourceName("economictimes")).toBe("Economic Times");
    expect(displaySourceName("dw-world")).toBe("DW");
    expect(displaySourceName("thehindu-world")).toBe("The Hindu");
  });

  it("title-cases an unknown source id instead of showing a slug", () => {
    expect(displaySourceName("new-source")).toBe("New Source");
  });

  it("uses 'Wire' for an empty source", () => {
    expect(displaySourceName("")).toBe("Wire");
  });

  it("never leaks a slug separator", () => {
    const ids = ["economictimes", "dw-world", "thehindu-world", "new-source", "", "unknown_source"];
    for (const id of ids) {
      const name = displaySourceName(id);
      expect(name).not.toContain("-");
      expect(name).not.toContain("_");
    }
  });
});

describe("source filter resolution", () => {
  it("resolves a publication name to its source id slug", () => {
    expect(sourceFilterIds("Economic Times")).toEqual(["economictimes"]);
  });

  it("expands a shared display name to every matching id", () => {
    const ids = sourceFilterIds("DW");
    expect(ids).toHaveLength(5);
    expect(ids).toEqual(
      expect.arrayContaining(["dw-all", "dw-world", "dw-business", "dw-europe", "dw-germany"]),
    );
  });

  it("keeps an exact slug as a single id", () => {
    expect(sourceFilterIds("dw-world")).toEqual(["dw-world"]);
  });

  it("passes an unknown term through as typed", () => {
    expect(sourceFilterIds("reuters")).toEqual(["reuters"]);
  });

  it("matches by slug or publication name", () => {
    expect(matchesSourceFilter("economictimes", ["Economic Times"])).toBe(true);
    expect(matchesSourceFilter("economictimes", ["economictimes"])).toBe(true);
    expect(matchesSourceFilter("economictimes", ["BBC News"])).toBe(false);
  });
});
