import { describe, expect, it } from "vitest";
import type { AlertHit, Delta, Link, Story, WireRow } from "../gen/dor/v1/dor_pb";
import { LiveState, MAX_SLOTS, WIRE_LIMIT } from "./state";
import type { WireFilters } from "./messages";

const EPOCH = 1_700_000_000_000;

/**
 * These fixtures build plain objects shaped like the real protobuf messages
 * (dropping `$typeName` and other generated-message machinery), the same
 * convention `src/live/mock.ts` uses for its dev fixtures. The cast to the
 * real type at the end is intentional: LiveState only reads plain fields off
 * these objects, never protobuf methods.
 */
type FixtureLocation = { name: string; lat: number; lon: number; role: number; precision: number };
type StoryOverrides = {
  id: string;
  title?: string;
  category?: number;
  primary?: number;
  sourceCount?: number;
  heat?: number;
  firstSeen?: bigint;
  updatedAt?: bigint;
  locations?: FixtureLocation[];
};
type LinkOverrides = {
  type?: number;
  strength?: number;
  dir?: number;
  confidence?: number;
  unverified?: boolean;
  updatedAt?: bigint;
};
type WireRowOverrides = {
  storyId: string;
  articleId?: string;
  at?: bigint;
  source?: string;
  title?: string;
  category?: number;
  place?: string;
  country?: string;
  lang?: string;
  located?: boolean;
  lat?: number;
  lon?: number;
  promptScores?: Record<string, number>;
  approxPrompts?: string[];
  join?: number;
};

let storySeq = 0;
function story(overrides: StoryOverrides): Story {
  storySeq++;
  return {
    id: overrides.id,
    title: overrides.title ?? `Story ${overrides.id}`,
    category: overrides.category ?? 0,
    primary: overrides.primary ?? 0,
    sourceCount: overrides.sourceCount ?? 1,
    heat: overrides.heat ?? 0.5,
    firstSeen: overrides.firstSeen ?? BigInt(Math.floor(EPOCH / 1000) - 60),
    updatedAt: overrides.updatedAt ?? BigInt(Math.floor(EPOCH / 1000)),
    locations: overrides.locations ?? [
      { name: `Place ${storySeq}`, lat: 10 + storySeq, lon: 20 + storySeq, role: 0, precision: 2 },
    ],
  } as unknown as Story;
}

function unlocatedStory(id: string): Story {
  return { ...story({ id }), locations: [] } as unknown as Story;
}

function link(a: string, b: string, overrides: LinkOverrides = {}): Link {
  return {
    a,
    b,
    type: overrides.type ?? 0,
    strength: overrides.strength ?? 0.6,
    dir: overrides.dir ?? 0,
    confidence: overrides.confidence ?? 0.75,
    unverified: overrides.unverified ?? false,
    updatedAt: overrides.updatedAt ?? BigInt(Math.floor(EPOCH / 1000)),
  } as unknown as Link;
}

function wireRow(overrides: WireRowOverrides): WireRow {
  return {
    articleId: overrides.articleId ?? `article-${overrides.storyId}-${Math.random()}`,
    storyId: overrides.storyId,
    at: overrides.at ?? BigInt(Math.floor(EPOCH / 1000)),
    source: overrides.source ?? "RSS",
    title: overrides.title ?? "Wire title",
    category: overrides.category ?? 0,
    place: overrides.place ?? "Place",
    country: overrides.country ?? "US",
    lang: overrides.lang ?? "en",
    located: overrides.located ?? true,
    lat: overrides.lat ?? 10,
    lon: overrides.lon ?? 20,
    promptScores: overrides.promptScores ?? {},
    approxPrompts: overrides.approxPrompts ?? [],
    join: overrides.join ?? 0,
  } as unknown as WireRow;
}

function delta(seq: number, body: Delta["body"]): Delta {
  return { seq: BigInt(seq), body } as unknown as Delta;
}

function storyUpsert(seq: number, value: Story): Delta {
  return delta(seq, { case: "storyUpsert", value });
}
function storyExpire(seq: number, id: string): Delta {
  return delta(seq, { case: "storyExpire", value: id });
}
function linkUpsert(seq: number, value: Link): Delta {
  return delta(seq, { case: "linkUpsert", value });
}
function linkRemove(seq: number, a: string, b: string): Delta {
  return delta(seq, { case: "linkRemove", value: { a, b } as never });
}
type AlertHitOverrides = {
  alertId?: string;
  storyId: string;
  level?: string;
  firedAt?: bigint;
  title?: string;
  prompt?: string;
  generation?: bigint;
  approx?: boolean;
  sourceCount?: number;
};
function alertHit(overrides: AlertHitOverrides): AlertHit {
  return {
    alertId: overrides.alertId ?? `alert-${overrides.storyId}`,
    storyId: overrides.storyId,
    level: overrides.level ?? "new",
    firedAt: overrides.firedAt ?? BigInt(Math.floor(EPOCH / 1000)),
    title: overrides.title ?? "Title",
    prompt: overrides.prompt ?? "energy",
    generation: overrides.generation ?? 0n,
    approx: overrides.approx ?? false,
    sourceCount: overrides.sourceCount ?? 1,
  } as unknown as AlertHit;
}
function alertDelta(seq: number, value: AlertHit): Delta {
  return delta(seq, { case: "alert", value });
}
function wireDelta(seq: number, row: WireRow): Delta {
  return delta(seq, { case: "wire", value: row });
}

describe("LiveState snapshot and incremental patches", () => {
  it("builds a full init on snapshot and never again until the next snapshot", () => {
    const state = new LiveState(EPOCH);
    const a = story({ id: "a" });
    const init = state.snapshot([a], [], [], 1);
    expect(init.type).toBe("init");
    expect(init.init.storyCount).toBe(1);

    // Deltas only ever produce incremental patches, not another init-shaped payload.
    state.apply(storyUpsert(2, story({ id: "b" })));
    const patch = state.flush(EPOCH + 1000)!;
    expect(patch.type).toBe("patch");
    expect(patch.storyCount).toBe(2);
    expect(patch.stories).toHaveLength(1);
    expect(patch.stories[0]!.op).toBe("add");
  });

  it("drops deltas safely before any snapshot has arrived", () => {
    const state = new LiveState(EPOCH);
    state.apply(storyUpsert(5, story({ id: "a" })));
    expect(state.seq).toBe(5); // seq still advances even with no init yet
    expect(state.flush(EPOCH)).toBeNull();
    expect(state.stories.size).toBe(0);
  });

  it("returns null from flush when nothing changed since the last flush", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    expect(state.flush(EPOCH)).toBeNull();
  });
});

describe("LiveState epoch", () => {
  it("keeps one stable epoch across snapshots and deltas", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([], [], [], 1);
    expect(state.epochMs).toBe(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 2);
    expect(state.epochMs).toBe(EPOCH); // readonly: the session clock never resets
  });

  it("encodes updatedAt relative to the fixed epoch, not wall clock at apply time", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([], [], [], 1);
    const atEpoch = story({ id: "a", updatedAt: BigInt(EPOCH / 1000) });
    state.apply(storyUpsert(2, atEpoch));
    const patch = state.flush(EPOCH + 50_000)!;
    expect(patch.stories[0]!.updatedAt).toBeCloseTo(0, 6);
  });
});

describe("LiveState slot removal and reuse", () => {
  it("frees a story's slot on expire and reuses it for the next new story", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([], [], [], 1);
    state.apply(storyUpsert(2, story({ id: "a" })));
    const addPatch = state.flush(EPOCH)!;
    const slotA = addPatch.stories[0]!.index;

    state.apply(storyExpire(3, "a"));
    const removePatch = state.flush(EPOCH)!;
    expect(removePatch.stories[0]).toMatchObject({ op: "remove", index: slotA });

    state.apply(storyUpsert(4, story({ id: "c" })));
    const reusePatch = state.flush(EPOCH)!;
    expect(reusePatch.stories[0]).toMatchObject({ op: "add", index: slotA });
  });

  it("removes incident links when their story expires", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" }), story({ id: "b" })], [link("a", "b")], [], 1);
    state.apply(storyExpire(2, "a"));
    const patch = state.flush(EPOCH)!;
    expect(patch.links).toHaveLength(1);
    expect(patch.links[0]!.op).toBe("remove");
  });

  it("keeps a story with no location in the map but assigns it no slot or patch", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([], [], [], 1);
    state.apply(storyUpsert(2, unlocatedStory("ghost")));
    // The story record is kept (it may gain a location later), but it never
    // reaches the GPU slot table, so there is nothing to patch.
    expect(state.stories.has("ghost")).toBe(true);
    expect(state.flush(EPOCH)).toBeNull();
  });

  it("grows story capacity once the free list is exhausted", () => {
    const state = new LiveState(EPOCH);
    const init = state.snapshot([], [], [], 1);
    const initialCapacity = init.init.storyCapacity;
    let seq = 2;
    let lastPatch = null as ReturnType<LiveState["flush"]>;
    for (let i = 0; i < initialCapacity + 1; i++) {
      state.apply(storyUpsert(seq++, story({ id: `s${i}` })));
      lastPatch = state.flush(EPOCH);
      if (lastPatch) state.flush(EPOCH); // drain so storyOps doesn't pile up across iterations
    }
    // Re-flush once more to read the latest capacity after the growth-triggering add.
    state.apply(storyUpsert(seq++, story({ id: "overflow" })));
    const grown = state.flush(EPOCH)!;
    expect(grown.storyCapacity).toBeGreaterThan(initialCapacity);
    void lastPatch;
  });

  it("rejects slot capacity beyond the uint16 wire limit", () => {
    expect(MAX_SLOTS).toBe(65536);
  });
});

describe("LiveState filters and prompts", () => {
  function filters(overrides: Partial<WireFilters> = {}): WireFilters {
    return {
      categories: [],
      regions: [],
      minSources: 0,
      lang: [],
      sources: [],
      lat: 0,
      lon: 0,
      radiusKm: 0,
      ...overrides,
    };
  }

  it("dims every story (without removing it) when prompts match nothing", () => {
    const state = new LiveState(EPOCH);
    state.snapshot(
      [story({ id: "a" }), story({ id: "b" })],
      [],
      [wireRow({ storyId: "a", promptScores: { energy: 0.1 } })],
      1,
    );
    state.setPrompts(["energy"]);
    const patch = state.flush(EPOCH)!;
    expect(patch.storyStates.indices.length).toBe(2);
    expect(Array.from(patch.storyStates.dimmed)).toEqual([1, 1]);
    // The story is still tracked; only its match/dim state changed.
    expect(state.stories.has("a")).toBe(true);
    expect(state.stories.has("b")).toBe(true);
    expect(patch.stories).toHaveLength(0);
  });

  it("un-dims once a wire row crosses the prompt threshold", () => {
    const state = new LiveState(EPOCH);
    state.snapshot(
      [story({ id: "a" })],
      [],
      [wireRow({ storyId: "a", articleId: "art-1", promptScores: { energy: 0.1 } })],
      1,
    );
    state.setPrompts(["energy"]);
    const dimmed = state.flush(EPOCH)!;
    expect(Array.from(dimmed.storyStates.dimmed)).toEqual([1]);

    state.apply(
      wireDelta(2, wireRow({ storyId: "a", articleId: "art-1", promptScores: { energy: 0.9 } })),
    );
    const patch = state.flush(EPOCH)!;
    expect(Array.from(patch.storyStates.dimmed)).toEqual([0]);
  });

  it("dims links whose endpoint story is filtered out", () => {
    const state = new LiveState(EPOCH);
    state.snapshot(
      [story({ id: "a", category: 1 }), story({ id: "b", category: 2 })],
      [link("a", "b")],
      [],
      1,
    );
    state.setFilters(filters({ categories: [1] }));
    const patch = state.flush(EPOCH)!;
    expect(Array.from(patch.storyStates.dimmed)).toContain(1);
    expect(Array.from(patch.linkStates.dimmed)).toEqual([1]);
  });

  it("skips unchanged filter values instead of re-matching", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    const f = filters({ minSources: 2 });
    expect(state.setFilters(f)).toBe(true);
    expect(state.setFilters({ ...f })).toBe(false);
    expect(state.setPrompts(["x"])).toBe(true);
    expect(state.setPrompts(["x"])).toBe(false);
  });
});

describe("LiveState adjacency and link metadata", () => {
  it("builds CSR adjacency touching both endpoints after a link add", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" }), story({ id: "b" }), story({ id: "c" })], [], [], 1);
    state.apply(linkUpsert(2, link("a", "b")));
    const patch = state.flush(EPOCH)!;
    expect(patch.adjacency).toBeDefined();
    const { adjStart, adjList } = patch.adjacency!;
    const linkOp = patch.links[0]!;
    const [a, b] = [linkOp.a, linkOp.b];
    expect(Array.from(adjList.subarray(adjStart[a]!, adjStart[a + 1]!))).toContain(b);
    expect(Array.from(adjList.subarray(adjStart[b]!, adjStart[b + 1]!))).toContain(a);
  });

  it("carries direction, confidence, distance and unverified onto the link op", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" }), story({ id: "b" })], [], [], 1);
    state.apply(
      linkUpsert(2, link("a", "b", { dir: 2, confidence: 0.42, unverified: true, strength: 0.8 })),
    );
    const patch = state.flush(EPOCH)!;
    const op = patch.links[0]!;
    expect(op.direction).toBe(2);
    expect(op.confidence).toBeCloseTo(0.42, 6);
    expect(op.unverified).toBe(true);
    expect(op.distanceKm).toBeGreaterThan(0);
    expect(op.arcs).toBeDefined();
  });

  it("does not emit a link for an unresolved endpoint, and omits adjacency when nothing structural changed", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    state.apply(linkUpsert(2, link("a", "missing")));
    expect(state.flush(EPOCH)).toBeNull();

    state.apply(wireDelta(3, wireRow({ storyId: "a" })));
    const patch = state.flush(EPOCH)!;
    expect(patch.adjacency).toBeUndefined();
  });

  it("removes a link by key and marks it dim-free in the next patch", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" }), story({ id: "b" })], [link("a", "b")], [], 1);
    state.apply(linkRemove(2, "a", "b"));
    const patch = state.flush(EPOCH)!;
    expect(patch.links[0]).toMatchObject({ op: "remove" });
  });
});

describe("LiveState ripples over a long-running session", () => {
  it("keeps spawning ripples for wire activity 10+ minutes after the session epoch", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    let sawRippleAfterTenMinutes = false;
    const tenMinutes = 10 * 60 * 1000;
    for (let i = 0; i < 20; i++) {
      const now = EPOCH + tenMinutes + i * 2000; // spaced beyond the 1.4s ripple lifetime
      state.apply(
        wireDelta(100 + i, wireRow({ storyId: "a", at: BigInt(Math.floor(now / 1000)) })),
      );
      const patch = state.flush(now);
      if (patch && patch.stream.ripples.length > 0) sawRippleAfterTenMinutes = true;
    }
    expect(sawRippleAfterTenMinutes).toBe(true);
  });

  it("never spawns a ripple for a story that is currently dimmed out by filters", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a", category: 5 })], [], [], 1);
    state.setFilters({
      categories: [1],
      regions: [],
      minSources: 0,
      lang: [],
      sources: [],
      lat: 0,
      lon: 0,
      radiusKm: 0,
    });
    state.apply(wireDelta(2, wireRow({ storyId: "a" })));
    const patch = state.flush(EPOCH)!;
    expect(patch.stream.ripples).toHaveLength(0);
  });
});

describe("LiveState patch coalescing: storyOps vs. stream-only updates", () => {
  it("routes a heat-only update through stream + summaries, not storyOps", () => {
    const state = new LiveState(EPOCH);
    const fixedLocation = [{ name: "Fixed", lat: 12, lon: 34, role: 0, precision: 2 }];
    state.snapshot(
      [story({ id: "a", heat: 0.3, sourceCount: 2, locations: fixedLocation })],
      [],
      [],
      1,
    );
    state.flush(EPOCH); // drain the initial structural add

    state.apply(
      storyUpsert(2, story({ id: "a", heat: 0.9, sourceCount: 7, locations: fixedLocation })),
    );
    const patch = state.flush(EPOCH)!;
    expect(patch.stories).toHaveLength(0);
    expect(Array.from(patch.stream.storyIndex)).toHaveLength(1);
    expect(patch.stream.heat[0]).toBeCloseTo(0.9, 6);
    expect(patch.stream.sources[0]).toBe(7);
    expect(patch.summaries).toHaveLength(1);
    expect(patch.summaries[0]!.story.heat).toBeCloseTo(0.9, 6);
  });

  it("still emits a storyOp when the title changes with the position held fixed, alongside the stream update", () => {
    const state = new LiveState(EPOCH);
    const fixedLocation = [{ name: "Fixed", lat: 12, lon: 34, role: 0, precision: 2 }];
    state.snapshot([story({ id: "a", locations: fixedLocation })], [], [], 1);
    state.flush(EPOCH);

    state.apply(
      storyUpsert(2, story({ id: "a", title: "A different headline", locations: fixedLocation })),
    );
    const patch = state.flush(EPOCH)!;
    expect(patch.stories).toHaveLength(1);
    expect(patch.stories[0]!.title).toBe("A different headline");
    expect(Array.from(patch.stream.storyIndex)).toHaveLength(1);
  });

  it("coalesces two structural upserts to the same story within one flush into a single op", () => {
    const state = new LiveState(EPOCH);
    const fixedLocation = [{ name: "Fixed", lat: 12, lon: 34, role: 0, precision: 2 }];
    state.snapshot([], [], [], 1);
    state.apply(storyUpsert(2, story({ id: "a", title: "First", locations: fixedLocation })));
    state.apply(storyUpsert(3, story({ id: "a", title: "Second", locations: fixedLocation })));
    const patch = state.flush(EPOCH)!;
    expect(patch.stories).toHaveLength(1);
    expect(patch.stories[0]!.title).toBe("Second");
  });
});

describe("LiveState reset semantics on new and reused slots", () => {
  it("marks a brand-new story slot reset, and a plain geometry update on the same slot not reset", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([], [], [], 1);
    state.apply(storyUpsert(2, story({ id: "a" })));
    const added = state.flush(EPOCH)!;
    expect(added.stories[0]!.reset).toBe(true);

    state.apply(storyUpsert(3, story({ id: "a", title: "Renamed" })));
    const updated = state.flush(EPOCH)!;
    expect(updated.stories[0]!.reset).toBe(false);
  });

  it("marks a reused story slot reset again, so stale focus/GPU state is not inherited", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([], [], [], 1);
    state.apply(storyUpsert(2, story({ id: "a" })));
    const slotA = state.flush(EPOCH)!.stories[0]!.index;
    state.apply(storyExpire(3, "a"));
    state.flush(EPOCH);

    state.apply(storyUpsert(4, story({ id: "b" })));
    const reused = state.flush(EPOCH)!;
    expect(reused.stories[0]!.index).toBe(slotA);
    expect(reused.stories[0]!.reset).toBe(true);
  });

  it("marks a brand-new link reset, and a later metadata-only update on it not reset", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" }), story({ id: "b" })], [], [], 1);
    state.apply(linkUpsert(2, link("a", "b")));
    const added = state.flush(EPOCH)!;
    expect(added.links[0]!.reset).toBe(true);

    state.apply(linkUpsert(3, link("a", "b", { strength: 0.9 })));
    const updated = state.flush(EPOCH)!;
    // Unlike StoryPatch.reset (always a concrete boolean via `!old`), LinkPatch.reset
    // can come back as `undefined` here (`isNew || previous?.reset` with no `previous`
    // across flushes) rather than `false`. Both are falsy to a consumer checking
    // `if (op.reset)`, and `reset` is typed optional, so this asserts the falsy
    // contract rather than the exact value.
    expect(updated.links[0]!.reset).toBeFalsy();
  });
});

describe("LiveState link geometry vs. metadata-only updates", () => {
  it("builds arcs for a brand-new link", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" }), story({ id: "b" })], [], [], 1);
    state.apply(linkUpsert(2, link("a", "b")));
    const patch = state.flush(EPOCH)!;
    expect(patch.links[0]!.arcs).toBeDefined();
  });

  it("omits arcs on a later strength/metadata-only update to the same link", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" }), story({ id: "b" })], [], [], 1);
    state.apply(linkUpsert(2, link("a", "b")));
    state.flush(EPOCH); // land the geometry so the next update is metadata-only

    state.apply(linkUpsert(3, link("a", "b", { strength: 0.95, confidence: 0.1 })));
    const patch = state.flush(EPOCH)!;
    expect(patch.links[0]!.arcs).toBeUndefined();
    expect(patch.links[0]!.strength).toBeCloseTo(0.95, 6);
    expect(patch.links[0]!.confidence).toBeCloseTo(0.1, 6);
  });

  it("rebuilds arcs for an existing link when a story endpoint moves", () => {
    const state = new LiveState(EPOCH);
    state.snapshot(
      [
        story({ id: "a" }),
        story({ id: "b", locations: [{ name: "B", lat: 5, lon: 5, role: 0, precision: 2 }] }),
      ],
      [],
      [],
      1,
    );
    state.apply(linkUpsert(2, link("a", "b")));
    state.flush(EPOCH);

    state.apply(
      storyUpsert(
        3,
        story({
          id: "b",
          locations: [{ name: "B moved", lat: 40, lon: 40, role: 0, precision: 2 }],
        }),
      ),
    );
    const patch = state.flush(EPOCH)!;
    const linkOp = patch.links.find((op) => op.op === "add");
    expect(linkOp?.arcs).toBeDefined();
  });
});

describe("LiveState typed array padding to capacity", () => {
  it("pads the GPU story arrays out to storyCapacity right after a snapshot, not just storyCount", () => {
    const state = new LiveState(EPOCH);
    const init = state.snapshot([story({ id: "a" })], [], [], 1);
    // Inserting a second story must land within already-allocated spare
    // capacity, never throw a RangeError from a too-short typed array.
    expect(init.init.storyCapacity).toBeGreaterThan(init.init.storyCount);
    expect(() => state.apply(storyUpsert(2, story({ id: "b" })))).not.toThrow();
    const patch = state.flush(EPOCH)!;
    expect(patch.stories.map((op) => op.index)).toContain(1);
  });
});

describe("LiveState bounded wire map", () => {
  it("never grows the wire ring past WIRE_LIMIT rows", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    for (let i = 0; i < WIRE_LIMIT + 50; i++) {
      state.apply(wireDelta(10 + i, wireRow({ storyId: "a", articleId: `wire-${i}` })));
    }
    expect(state.wire.size).toBe(WIRE_LIMIT);
  });

  it("merges a later update into the existing row instead of growing the map", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    state.apply(
      wireDelta(2, wireRow({ storyId: "a", articleId: "art-1", title: "First headline" })),
    );
    expect(state.wire.size).toBe(1);
    state.apply(wireDelta(3, wireRow({ storyId: "a", articleId: "art-1", title: "" })));
    expect(state.wire.size).toBe(1);
    expect(state.wire.get("art-1")!.title).toBe("First headline");
  });
});

describe("LiveState prompt match cache (promptMatches)", () => {
  function dimOfFreshUpsert(preexistingRow: boolean): number {
    const state = new LiveState(EPOCH);
    state.snapshot([], [], [], 1);
    state.setPrompts(["energy"]);
    if (preexistingRow) {
      state.apply(
        wireDelta(2, wireRow({ storyId: "a", articleId: "art-1", promptScores: { energy: 0.9 } })),
      );
    }
    state.apply(storyUpsert(3, story({ id: "a" })));
    const patch = state.flush(EPOCH);
    const op = patch?.stories.find((o) => o.op === "add");
    if (!patch || !op) return 0;
    const at = Array.from(patch.storyStates.indices).indexOf(op.index);
    return at === -1 ? 0 : patch.storyStates.dimmed[at]!;
  }

  it("keeps a positive prompt match alive after its wire row is evicted from the WIRE_LIMIT ring", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    state.setPrompts(["energy"]);
    state.apply(
      wireDelta(2, wireRow({ storyId: "a", articleId: "first", promptScores: { energy: 0.9 } })),
    );
    const matched = state.flush(EPOCH)!;
    expect(Array.from(matched.storyStates.dimmed)).toEqual([0]);

    // Push enough unrelated rows to evict "first" from the 1000-row ring.
    for (let i = 0; i < WIRE_LIMIT; i++) {
      state.apply(wireDelta(10 + i, wireRow({ storyId: "a", articleId: `filler-${i}` })));
    }
    expect(state.wire.has("first")).toBe(false);

    const patch = state.flush(EPOCH)!;
    // No dim transition for story 0: the cached match survives the row's eviction.
    expect(Array.from(patch.storyStates.indices)).not.toContain(0);
  });

  it("clears the cached match on story expiry; a re-added id with no surviving positive row stays dimmed", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    state.setPrompts(["energy"]);
    state.apply(
      wireDelta(2, wireRow({ storyId: "a", articleId: "art-1", promptScores: { energy: 0.9 } })),
    );
    state.flush(EPOCH);

    state.apply(storyExpire(3, "a"));
    state.flush(EPOCH);

    // Evict "art-1" from the wire ring too, so there is no surviving positive row to recompute from.
    for (let i = 0; i < WIRE_LIMIT; i++) {
      state.apply(wireDelta(10 + i, wireRow({ storyId: "filler", articleId: `filler-${i}` })));
    }
    expect(state.wire.has("art-1")).toBe(false);

    state.apply(storyUpsert(2000, story({ id: "a" })));
    const patch = state.flush(EPOCH)!;
    expect(Array.from(patch.storyStates.dimmed)).toContain(1);
  });

  it("prunes a story's cached match when its matching prompt is deactivated", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    state.setPrompts(["energy"]);
    state.apply(
      wireDelta(2, wireRow({ storyId: "a", articleId: "art-1", promptScores: { energy: 0.9 } })),
    );
    const matched = state.flush(EPOCH)!;
    expect(Array.from(matched.storyStates.dimmed)).toEqual([0]);

    // Swap to a prompt set that drops "energy"; no row matches "weather" either.
    state.setPrompts(["weather"]);
    const patch = state.flush(EPOCH)!;
    expect(Array.from(patch.storyStates.dimmed)).toEqual([1]);
  });

  it("seeds matches for a newly active prompt directly from the existing wire ring, with no fresh delta", () => {
    const state = new LiveState(EPOCH);
    state.snapshot(
      [story({ id: "a" })],
      [],
      [wireRow({ storyId: "a", articleId: "art-1", promptScores: { weather: 0.9 } })],
      1,
    );
    state.setPrompts(["other"]); // active prompt doesn't match the ring row -> dimmed
    const dimmed = state.flush(EPOCH)!;
    expect(Array.from(dimmed.storyStates.dimmed)).toEqual([1]);

    state.setPrompts(["weather"]); // switch to a prompt the existing ring row already satisfies
    const patch = state.flush(EPOCH)!;
    expect(Array.from(patch.storyStates.dimmed)).toEqual([0]); // un-dimmed without any new wire delta
  });

  it("considers a wire row that arrived before its story existed, once the story is first upserted", () => {
    expect(dimOfFreshUpsert(false)).toBe(1); // no matching row at all: dimmed
    expect(dimOfFreshUpsert(true)).toBe(0); // row arrived early, applied once "a" exists: not dimmed
  });

  it("rebuilds the match cache purely from the new recentWire on a resync-style snapshot", () => {
    const state = new LiveState(EPOCH);
    state.setPrompts(["energy"]);
    state.snapshot(
      [story({ id: "a" })],
      [],
      [wireRow({ storyId: "a", articleId: "art-1", promptScores: { energy: 0.9 } })],
      1,
    );
    // Already matched at init time (baseline = not dimmed); no transition to report yet.
    expect(state.flush(EPOCH)).toBeNull();

    // Resync: a fresh snapshot whose recentWire no longer carries a positive row for "a".
    state.snapshot([story({ id: "a" })], [], [], 2);
    const patch = state.flush(EPOCH)!;
    // The prior session's match did not leak across the resync.
    expect(Array.from(patch.storyStates.dimmed)).toEqual([1]);
  });

  it("does not let filters.sources/lang influence per-story dim state (membership stays wire-only, never inferred)", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    state.setFilters({
      categories: [],
      regions: [],
      minSources: 0,
      lang: ["fr"],
      sources: ["BBC"],
      lat: 0,
      lon: 0,
      radiusKm: 0,
    });
    // Neither field is checked per story; nothing should dim as a result.
    expect(state.flush(EPOCH)).toBeNull();
  });
});

describe("LiveState coalesced slot reuse before a flush", () => {
  it("keeps reset=true through an expire→reuse→rename, all coalesced before the next flush", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([], [], [], 1);
    state.apply(storyUpsert(2, story({ id: "b" })));
    state.flush(EPOCH); // land B so the slot has a real "old" entry

    state.apply(storyExpire(3, "b")); // frees the slot
    state.apply(storyUpsert(4, story({ id: "c" }))); // reuses the freed slot: reset=true
    state.apply(storyUpsert(5, story({ id: "c", title: "Renamed while still unflushed" }))); // coalesced rename on the same unflushed add
    const patch = state.flush(EPOCH)!;
    const op = patch.stories.find((o) => o.op === "add")!;
    expect(op.reset).toBe(true);
    expect(op.title).toBe("Renamed while still unflushed");
  });
});

describe("LiveState link-only updates recompute endpoint degree/strongestLink", () => {
  it("updates both endpoints' summary degree/strongestLink on link add, strength change and remove, with no storyOp", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" }), story({ id: "b" }), story({ id: "c" })], [], [], 1);
    state.flush(EPOCH); // land the three story adds

    state.apply(linkUpsert(2, link("a", "b", { strength: 0.4 })));
    let patch = state.flush(EPOCH)!;
    expect(patch.stories).toHaveLength(0); // link-only: no storyOp emitted
    let a = patch.summaries.find((s) => s.story.id === "a")!;
    let b = patch.summaries.find((s) => s.story.id === "b")!;
    expect(a.degree).toBe(1);
    expect(a.strongestLink).toBeCloseTo(0.4, 6);
    expect(b.degree).toBe(1);
    expect(b.strongestLink).toBeCloseTo(0.4, 6);

    // Strength-only update to the existing link.
    state.apply(linkUpsert(3, link("a", "b", { strength: 0.9 })));
    patch = state.flush(EPOCH)!;
    a = patch.summaries.find((s) => s.story.id === "a")!;
    expect(a.degree).toBe(1); // unchanged
    expect(a.strongestLink).toBeCloseTo(0.9, 6);

    // Remove the link: both endpoints' degree drops back to 0.
    state.apply(linkRemove(4, "a", "b"));
    patch = state.flush(EPOCH)!;
    a = patch.summaries.find((s) => s.story.id === "a")!;
    b = patch.summaries.find((s) => s.story.id === "b")!;
    expect(a.degree).toBe(0);
    expect(a.strongestLink).toBe(0);
    expect(b.degree).toBe(0);
  });
});

describe("LiveState prompt match: in-ring correction vs. ring eviction", () => {
  it("dims again when an in-ring row's score is corrected downward", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    state.setPrompts(["energy"]);
    state.apply(
      wireDelta(2, wireRow({ storyId: "a", articleId: "art-1", promptScores: { energy: 0.9 } })),
    );
    let patch = state.flush(EPOCH)!;
    expect(Array.from(patch.storyStates.dimmed)).toEqual([0]); // matched

    // Correct the SAME row (same articleId) downward while it is still in the ring.
    state.apply(
      wireDelta(3, wireRow({ storyId: "a", articleId: "art-1", promptScores: { energy: 0.1 } })),
    );
    patch = state.flush(EPOCH)!;
    expect(Array.from(patch.storyStates.dimmed)).toEqual([1]); // corrected away while still in-ring
  });

  it("still retains the match via the aggregate bit when the positive row is instead evicted, not corrected", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    state.setPrompts(["energy"]);
    state.apply(
      wireDelta(2, wireRow({ storyId: "a", articleId: "art-1", promptScores: { energy: 0.9 } })),
    );
    state.flush(EPOCH);

    for (let i = 0; i < WIRE_LIMIT; i++) {
      state.apply(wireDelta(10 + i, wireRow({ storyId: "a", articleId: `filler-${i}` })));
    }
    expect(state.wire.has("art-1")).toBe(false);

    const patch = state.flush(EPOCH)!;
    // No dim transition for story 0: the retained aggregate bit keeps it matched.
    expect(Array.from(patch.storyStates.indices)).not.toContain(0);
  });
});

describe("LiveState alert deltas with a uint64 generation", () => {
  it("carries a generation beyond Number.MAX_SAFE_INTEGER through apply/flush unchanged", () => {
    const state = new LiveState(EPOCH);
    state.snapshot([story({ id: "a" })], [], [], 1);
    const huge = 18446744073709551615n; // 2^64 - 1
    state.apply(alertDelta(2, alertHit({ storyId: "a", generation: huge })));
    const patch = state.flush(EPOCH)!;
    expect(patch.alerts).toHaveLength(1);
    expect(patch.alerts[0]!.generation).toBe(huge);
  });
});

describe("LiveState pending positive match survives ring eviction before the story exists", () => {
  it("promotes an unknown-story positive row into retainedMatches once its story is upserted, even after it has been evicted from the wire ring", () => {
    const state = new LiveState(EPOCH);
    // "filler" is known from the start, so the filler churn below updates its
    // match state directly and never competes with "a"'s row for a slot in
    // the separate (also WIRE_LIMIT-bounded) pendingMatches map.
    state.snapshot([story({ id: "filler" })], [], [], 1); // "a" is not known yet
    state.setPrompts(["energy"]);

    // The positive row for "a" arrives while "a" is still completely unknown.
    state.apply(
      wireDelta(2, wireRow({ storyId: "a", articleId: "art-1", promptScores: { energy: 0.9 } })),
    );

    // Evict "art-1" from the 1000-row ring with unrelated filler rows, all
    // while "a" is still unknown (so the normal eviction→retain path, which
    // only fires for known stories, cannot be what saves this match).
    for (let i = 0; i < WIRE_LIMIT; i++) {
      state.apply(wireDelta(10 + i, wireRow({ storyId: "filler", articleId: `filler-${i}` })));
    }
    expect(state.wire.has("art-1")).toBe(false);

    // "a" becomes known only now, long after its one positive row is gone from the ring.
    state.apply(storyUpsert(2000, story({ id: "a" })));
    const patch = state.flush(EPOCH)!;
    const op = patch.stories.find((o) => o.op === "add" && o.title === "Story a")!;
    // Matched, not dimmed: the pending row's positive evidence was retained.
    expect(Array.from(patch.storyStates.indices)).not.toContain(op.index);
  });
});
