import { describe, expect, it } from "vitest";
import { FOCUS_AMBIENT, FOCUS_FOCUSED, FOCUS_LINKED, FOCUS_SECOND } from "./encoding";
import { FocusMirror } from "./focus-mirror";
import { FakeWorld } from "../worker/fake/state";

const NOW = 1_700_000_000_000;
const options = { storyCount: 200, linkCount: 80, seed: 7, now: NOW };

function makeMirror(): {
  world: FakeWorld;
  mirror: FocusMirror;
  lit: Uint8Array;
  litLinks: Uint8Array;
  apply: (result: ReturnType<FocusMirror["focus"]>) => void;
  neighbours: Map<number, Set<number>>;
} {
  const world = new FakeWorld(options);
  const init = world.initMessage(NOW);
  const mirror = new FocusMirror(init);
  const lit = new Uint8Array(init.storyCount);
  const litLinks = new Uint8Array(init.linkCount);
  const neighbours = new Map<number, Set<number>>();
  for (let link = 0; link < init.linkCount; link++) {
    const a = init.links.a[link]!;
    const b = init.links.b[link]!;
    if (!neighbours.has(a)) neighbours.set(a, new Set());
    if (!neighbours.has(b)) neighbours.set(b, new Set());
    neighbours.get(a)!.add(b);
    neighbours.get(b)!.add(a);
  }
  const apply = (result: ReturnType<FocusMirror["focus"]>): void => {
    for (const change of result.storyChanges) lit[change.index] = change.state;
    for (const change of result.linkChanges) litLinks[change.index] = change.state;
  };
  return { world, mirror, lit, litLinks, apply, neighbours };
}

function busiest(neighbours: Map<number, Set<number>>, skip = new Set<number>()): number {
  return [...neighbours.entries()]
    .filter(([story]) => !skip.has(story))
    .sort((left, right) => right[1].size - left[1].size || left[0] - right[0])[0]![0];
}

describe("FocusMirror", () => {
  it("matches the worker's focus bytes for the same thread", () => {
    const { world, mirror, neighbours } = makeMirror();
    const index = busiest(neighbours);
    const result = mirror.focus(index, false, 1000);
    const workerPatch = world.setFocus(index, false, NOW + 1000, 1);
    const init = world.initMessage(NOW);
    for (let story = 0; story < world.storyCount; story++) {
      expect(mirror.storyStateOf(story)).toBe(init.stories.focus[story]);
    }
    for (let link = 0; link < world.linkCount; link++) {
      expect(mirror.linkStateOf(link)).toBe(init.links.focus[link]);
    }
    expect(result.hud?.story.index).toBe(index);
    expect(result.hud?.links.length).toBe(workerPatch.hud?.links.length);
    expect(result.points[0]).toEqual([init.stories.lng[index], init.stories.lat[index]]);
  });

  it("resets every old lit index when two inputs arrive before any frame", () => {
    const { mirror, lit, litLinks, neighbours, apply } = makeMirror();
    const [a, b] = [busiest(neighbours), busiest(neighbours, new Set([busiest(neighbours)]))];
    apply(mirror.focus(a!, false, 1000));
    expect(mirror.storyStateOf(a!)).toBe(FOCUS_FOCUSED);
    // Two focus inputs in one task, as if a stale worker reply never arrived.
    apply(mirror.focus(b!, false, 1001));
    apply(mirror.focus(null, false, 1002));
    for (let story = 0; story < lit.length; story++) expect(lit[story]).toBe(FOCUS_AMBIENT);
    for (let link = 0; link < litLinks.length; link++) {
      expect(litLinks[link]).toBe(FOCUS_AMBIENT);
    }
    expect(mirror.litStoryCount).toBe(0);
    expect(mirror.litLinkCount).toBe(0);
  });

  it("cannot leave ghosts when the new thread overlaps the old one", () => {
    const { mirror, lit, neighbours, apply } = makeMirror();
    const a = busiest(neighbours);
    const b = [...neighbours.get(a)!][0]!;
    apply(mirror.focus(a, false, 1000));
    const firstLit = new Set(mirror.litStoryIndices);
    apply(mirror.focus(b, false, 1001));
    // Every story lit for A but not linked to B must be back to ambient.
    const bThread = new Set([b, ...(neighbours.get(b) ?? [])]);
    for (const story of firstLit) {
      if (!bThread.has(story)) expect(lit[story]).toBe(FOCUS_AMBIENT);
    }
    expect(mirror.storyStateOf(b)).toBe(FOCUS_FOCUSED);
  });

  it("marks 2nd-degree stories and links, with no pulses in the encoding", () => {
    const { mirror, neighbours } = makeMirror();
    const index = busiest(neighbours);
    const result = mirror.focus(index, true, 1000);
    expect(result.hud?.secondDegree).toBe(true);
    const states = new Set<number>();
    for (let story = 0; story < neighbours.size; story++) {
      const state = mirror.storyStateOf(story);
      if (state !== FOCUS_AMBIENT) states.add(state);
    }
    expect(
      states.has(FOCUS_SECOND) || result.storyChanges.some((c) => c.state === FOCUS_SECOND),
    ).toBe(true);
    expect(mirror.storyStateOf(index)).toBe(FOCUS_FOCUSED);
    for (const other of neighbours.get(index) ?? []) {
      expect(mirror.storyStateOf(other)).toBe(FOCUS_LINKED);
    }
  });

  it("rebuilds adjacency when a link is added or removed", () => {
    const { mirror, world } = makeMirror();
    const a = 120;
    const b = 121;
    const before = mirror.degree(a);
    mirror.applyLinkDelta(79, a, b, true);
    expect(mirror.degree(a)).toBe(before + 1);
    expect(mirror.hasNeighbour(a, b)).toBe(true);
    mirror.applyLinkDelta(79, a, b, false);
    expect(mirror.degree(a)).toBe(before);
    void world;
  });
});
