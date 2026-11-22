import { describe, expect, it } from "vitest";
import type { Link, Story } from "../gen/dor/v1/dor_pb";
import { buildLiveGlobe, threadRankScore, type LiveStory } from "./live-data";

type StoryOverrides = {
  id: string;
  title?: string;
  sourceCount?: number;
  heat?: number;
  hazard?: string;
  magnitude?: number;
  locations?: Array<{ name: string; lat: number; lon: number; role: number; precision: number }>;
};

let f12Seq = 0;
function story(overrides: StoryOverrides): Story {
  f12Seq++;
  return {
    id: overrides.id,
    title: overrides.title ?? `Story ${overrides.id}`,
    category: 0,
    primary: 0,
    sourceCount: overrides.sourceCount ?? 1,
    heat: overrides.heat ?? 0.5,
    firstSeen: 1_700_000_000n,
    updatedAt: 1_700_000_100n,
    hazard: overrides.hazard ?? "",
    magnitude: overrides.magnitude ?? 0,
    locations: overrides.locations ?? [
      { name: `Place ${f12Seq}`, lat: 10 + f12Seq, lon: 20 + f12Seq, role: 0, precision: 2 },
    ],
  } as unknown as Story;
}

function link(a: string, b: string, strength: number, unverified: boolean): Link {
  return {
    a,
    b,
    type: 2,
    strength,
    dir: 0,
    confidence: unverified ? 0.2 : 0.8,
    unverified,
    updatedAt: 1_700_000_100n,
  } as unknown as Link;
}

function entry(
  overrides: Partial<LiveStory["story"]> & { strongestVerified?: number; degree?: number },
): LiveStory {
  const { strongestVerified, degree, ...storyOverrides } = overrides;
  return {
    story: {
      id: "s",
      title: "t",
      category: 0,
      primary: 0,
      sourceCount: 1,
      heat: 0.5,
      firstSeen: 1n,
      updatedAt: 1n,
      hazard: "",
      magnitude: 0,
      locations: [],
      ...storyOverrides,
    },
    index: 0,
    lng: 0,
    lat: 0,
    place: "",
    category: 0,
    color: [0, 0, 0, 0],
    parties: [],
    partyTotal: 0,
    degree: degree ?? 0,
    strongestLink: strongestVerified ?? 0,
    strongestVerified: strongestVerified ?? 0,
  };
}

describe("F1·2 live-thread ranking", () => {
  it("ranks a multi-source linked thread above a single-source micro-quake with many unverified links", () => {
    const thread = story({ id: "thread", sourceCount: 6, heat: 0.8 });
    const partner = story({ id: "partner", sourceCount: 2, heat: 0.3 });
    const quake = story({
      id: "quake",
      title: "M 1.4 - 7 km SSW of Pahala, Hawaii",
      sourceCount: 1,
      heat: 0.6,
      hazard: "earthquake",
      magnitude: 1.4,
    });
    const fillerA = story({ id: "filler-a", heat: 0.2 });
    const fillerB = story({ id: "filler-b", heat: 0.2 });

    const data = buildLiveGlobe(
      [thread, partner, quake, fillerA, fillerB],
      [
        link("thread", "partner", 0.8, false),
        link("quake", "partner", 0.9, true),
        link("quake", "filler-a", 0.85, true),
        link("quake", "filler-b", 0.8, true),
      ],
    );

    expect(data.topStoryIds[0]).toBe("thread");
    expect(data.topStoryIds.indexOf("thread")).toBeLessThan(data.topStoryIds.indexOf("quake"));

    const quakeEntry = data.stories.find((s) => s.story.id === "quake")!;
    expect(quakeEntry.story.hazard).toBe("earthquake");
    expect(quakeEntry.story.magnitude).toBe(1.4);
    expect(quakeEntry.degree).toBe(3);
    expect(quakeEntry.strongestVerified).toBe(0);

    const threadEntry = data.stories.find((s) => s.story.id === "thread")!;
    expect(threadEntry.story.sourceCount).toBe(6);
    expect(threadEntry.strongestVerified).toBeCloseTo(0.8, 6);
  });

  it("counts only verified links in strongestVerified but keeps strongestLink overall", () => {
    const a = story({ id: "a" });
    const b = story({ id: "b" });
    const c = story({ id: "c" });

    const data = buildLiveGlobe([a, b, c], [link("a", "b", 0.9, true), link("a", "c", 0.4, false)]);

    const aEntry = data.stories.find((s) => s.story.id === "a")!;
    expect(aEntry.strongestLink).toBeCloseTo(0.9, 6);
    expect(aEntry.strongestVerified).toBeCloseTo(0.4, 6);

    const bEntry = data.stories.find((s) => s.story.id === "b")!;
    expect(bEntry.strongestLink).toBeCloseTo(0.9, 6);
    expect(bEntry.strongestVerified).toBe(0);
  });

  it("scores verified evidence above unverified similarity and demotes micro-quakes", () => {
    const base = entry({ heat: 0.7, sourceCount: 4 });
    const verified = threadRankScore(entry({ heat: 0.7, sourceCount: 4, strongestVerified: 0.9 }));
    const unverified = threadRankScore(base);
    expect(verified).toBeGreaterThan(unverified);

    const quake = threadRankScore(
      entry({ heat: 0.7, sourceCount: 1, hazard: "earthquake", magnitude: 1.4 }),
    );
    const ordinary = threadRankScore(entry({ heat: 0.7, sourceCount: 1 }));
    expect(quake).toBeLessThan(ordinary);

    const largeQuake = threadRankScore(
      entry({ heat: 0.7, sourceCount: 1, hazard: "earthquake", magnitude: 5.2 }),
    );
    expect(largeQuake).toBeGreaterThan(quake);

    const linked = threadRankScore(
      entry({ heat: 0.7, sourceCount: 4, degree: 2, strongestVerified: 0.9 }),
    );
    expect(linked).toBeGreaterThan(verified);

    const multiSource = threadRankScore(
      entry({ heat: 0.7, sourceCount: 6, strongestVerified: 0.9 }),
    );
    expect(multiSource).toBeGreaterThan(verified);
  });

  it("clamps heat and source count instead of letting one field dominate", () => {
    const hot = threadRankScore(entry({ heat: 1, sourceCount: 1 }));
    const clampedHot = threadRankScore(entry({ heat: 40, sourceCount: 1 }));
    expect(clampedHot).toBeCloseTo(hot, 6);

    const many = threadRankScore(entry({ heat: 0.5, sourceCount: 30 }));
    const beyond = threadRankScore(entry({ heat: 0.5, sourceCount: 5000 }));
    expect(beyond).toBeCloseTo(many, 6);
  });
});
