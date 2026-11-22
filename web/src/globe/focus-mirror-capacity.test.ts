import { describe, expect, it } from "vitest";
import { FOCUS_AMBIENT, FOCUS_FOCUSED, FOCUS_LINKED } from "./encoding";
import { FocusMirror } from "./focus-mirror";
import { FakeWorld } from "../worker/fake/state";

const NOW = 1_700_000_000_000;
const options = { storyCount: 50, linkCount: 20, seed: 3, now: NOW };

describe("FocusMirror.ensureCapacity preserves focus and GPU-side data", () => {
  it("keeps the lit thread and per-instance bytes after growing past the old capacity", () => {
    const world = new FakeWorld(options);
    const init = world.initMessage(NOW);
    const mirror = new FocusMirror(init);

    const index = 0;
    mirror.focus(index, false, 1000);
    expect(mirror.isLit(index)).toBe(true);
    const litBefore = new Set(mirror.litStoryIndices);
    const degreeBefore = mirror.degree(index);

    const grownStories = init.storyCount * 4;
    const grownLinks = init.linkCount * 4;
    mirror.ensureCapacity(grownStories, grownLinks);

    // The old thread is unchanged: nothing is reset by a pure capacity grow.
    expect(mirror.isLit(index)).toBe(true);
    expect(new Set(mirror.litStoryIndices)).toEqual(litBefore);
    expect(mirror.storyStateOf(index)).toBe(FOCUS_FOCUSED);
    expect(mirror.degree(index)).toBe(degreeBefore);
    for (const story of litBefore) {
      if (story !== index) expect(mirror.storyStateOf(story)).toBe(FOCUS_LINKED);
    }

    // The newly grown slots exist but start out inactive/ambient.
    expect(mirror.degree(init.storyCapacity! + 1)).toBe(0);
  });

  // Regression guard for a bug found while writing this test: ensureCapacity()
  // used to skip every field whose name starts with "adj" (adjStart/adjList/
  // adjLink), so those CSR arrays never grew past their construction-time
  // size and rebuildAdjacency() silently dropped one side of any edge
  // touching a newly grown index. Now fixed upstream.
  it("lets a slot beyond the old capacity become a real, focusable story after growth", () => {
    const world = new FakeWorld(options);
    const init = world.initMessage(NOW);
    const mirror = new FocusMirror(init);
    const newIndex = init.storyCapacity! + 5;

    const linkIndex = init.linkCapacity!;
    mirror.ensureCapacity(newIndex + 1, linkIndex + 1);
    mirror.applyStoryDelta(
      newIndex,
      {
        lng: 12,
        lat: 34,
        category: 1,
        title: "Grown story",
        place: "Nowhere",
        sources: 1,
        heat: 0.4,
      },
      true,
    );
    mirror.applyLinkDelta(linkIndex, 0, newIndex, true, 1, 0.5);

    expect(mirror.hasNeighbour(0, newIndex)).toBe(true);
    const result = mirror.focus(newIndex, false, 2000);
    expect(result.hud?.story.index).toBe(newIndex);
    expect(mirror.storyStateOf(newIndex)).toBe(FOCUS_FOCUSED);
    expect(mirror.storyStateOf(0)).toBe(FOCUS_LINKED);
  });

  it("does not shrink or clobber capacity on a repeated call with a smaller requirement", () => {
    const world = new FakeWorld(options);
    const init = world.initMessage(NOW);
    const mirror = new FocusMirror(init);
    mirror.ensureCapacity(init.storyCount * 2, init.linkCount * 2);
    const before = mirror.degree(0);
    mirror.ensureCapacity(1, 1); // smaller than current: must be a no-op, not a shrink
    expect(mirror.degree(0)).toBe(before);
  });
});

describe("FocusMirror metadata patches do not disturb focus state", () => {
  it("applyStoryMetadata updates party geometry without changing lit/ambient state", () => {
    const world = new FakeWorld(options);
    const init = world.initMessage(NOW);
    const mirror = new FocusMirror(init);
    mirror.focus(0, false, 1000);
    const stateBefore = mirror.storyStateOf(0);

    mirror.applyStoryMetadata({
      op: "add",
      index: 0,
      lng: 1,
      lat: 1,
      category: 0,
      sources: 1,
      heat: 0.5,
      updatedAt: 0,
      title: "t",
      place: "p",
      color: [0, 0, 0, 255],
      summary: {
        index: 0,
        lng: 1,
        lat: 1,
        place: "p",
        category: 0,
        color: [0, 0, 0, 255],
        story: {
          id: "a",
          title: "t",
          category: 0,
          primary: 0,
          sourceCount: 1,
          heat: 0.5,
          firstSeen: 0n,
          updatedAt: 0n,
          hazard: "",
          magnitude: 0,
          locations: [],
        },
        parties: [{ lng: 2, lat: 2, label: "Party" }],
        partyTotal: 1,
        degree: 0,
        strongestLink: 0,
        strongestVerified: 0,
      },
    });

    expect(mirror.storyStateOf(0)).toBe(stateBefore);
  });

  it("applyLinkMetadata updates direction/confidence/distance without changing focus bytes", () => {
    const world = new FakeWorld(options);
    const init = world.initMessage(NOW);
    const mirror = new FocusMirror(init);
    mirror.focus(0, false, 1000);
    const linkStateBefore = mirror.linkStateOf(0);

    mirror.applyLinkMetadata({
      op: "add",
      index: 0,
      a: 0,
      b: 1,
      type: 0,
      strength: 0.9,
      direction: 2,
      confidence: 0.3,
      distanceKm: 123,
      unverified: true,
    });

    expect(mirror.linkStateOf(0)).toBe(linkStateBefore);
  });
});

void FOCUS_AMBIENT;
