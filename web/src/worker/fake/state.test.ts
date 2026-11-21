import { describe, expect, it } from "vitest";
import { FOCUS_AMBIENT, FOCUS_FOCUSED, FOCUS_LINKED, FOCUS_SECOND } from "../../globe/encoding";
import { lngLatToMercator } from "../../globe/mercator";
import { FakeWorld } from "./state";

const NOW = 1_700_000_000_000;
const options = { storyCount: 200, linkCount: 80, seed: 7, now: NOW };

function makeWorld(): FakeWorld {
  return new FakeWorld(options);
}

type Graph = { neighbours: Map<number, Set<number>>; linksOf: Map<number, number[]> };

function graphOf(world: FakeWorld): Graph {
  const { links } = world.initMessage(NOW);
  const neighbours = new Map<number, Set<number>>();
  const linksOf = new Map<number, number[]>();
  for (let l = 0; l < world.linkCount; l++) {
    const a = links.a[l]!;
    const b = links.b[l]!;
    for (const [x, y] of [
      [a, b],
      [b, a],
    ] as const) {
      if (!neighbours.has(x)) neighbours.set(x, new Set());
      neighbours.get(x)!.add(y);
      if (!linksOf.has(x)) linksOf.set(x, []);
      linksOf.get(x)!.push(l);
    }
  }
  return { neighbours, linksOf };
}

function threadOf(graph: Graph, index: number): Set<number> {
  return new Set([index, ...(graph.neighbours.get(index) ?? [])]);
}

function linkThread(graph: Graph, index: number): Set<number> {
  return new Set(graph.linksOf.get(index) ?? []);
}

/** Focus candidates ordered by degree, so tests get non-trivial threads. */
function candidates(graph: Graph): number[] {
  return [...graph.neighbours.keys()].sort(
    (l, r) => graph.neighbours.get(r)!.size - graph.neighbours.get(l)!.size || l - r,
  );
}

function secondDegree(graph: Graph, index: number): { stories: Set<number>; links: Set<number> } {
  const ring1 = graph.neighbours.get(index) ?? new Set<number>();
  const stories = new Set<number>();
  const links = new Set<number>();
  for (const story of ring1) {
    for (const link of graph.linksOf.get(story)!) {
      const touchesFocus = graph.linksOf.get(index)!.includes(link);
      if (touchesFocus) continue;
      links.add(link);
    }
    for (const other of graph.neighbours.get(story)!) {
      if (other !== index && !ring1.has(other)) stories.add(other);
    }
  }
  return { stories, links };
}

describe("FakeWorld init message", () => {
  it("reports consistent counts and array lengths", () => {
    const world = makeWorld();
    const init = world.initMessage(NOW);
    expect(init.storyCount).toBe(200);
    expect(init.linkCount).toBe(80);
    expect(init.stories.lng).toHaveLength(200);
    expect(init.stories.lat).toHaveLength(200);
    expect(init.stories.mercHi).toHaveLength(400);
    expect(init.stories.mercLo).toHaveLength(400);
    expect(init.stories.category).toHaveLength(200);
    expect(init.stories.sources).toHaveLength(200);
    expect(init.stories.heat).toHaveLength(200);
    expect(init.stories.color).toHaveLength(800);
    expect(init.stories.title).toHaveLength(200);
    expect(init.links.a).toHaveLength(80);
    expect(init.links.b).toHaveLength(80);
    expect(init.links.strength).toHaveLength(80);
    expect(init.links.color).toHaveLength(320);
    expect(init.ripples.spawnAt).toHaveLength(init.rippleCapacity);
  });

  it("builds 20 arc segments per link covering t = 0..1", () => {
    const init = makeWorld().initMessage(NOW);
    expect(init.arcSegments).toBe(20);
    const total = init.linkCount * 20;
    expect(init.arcs.link).toHaveLength(total);
    expect(init.arcs.t0).toHaveLength(total);
    expect(init.arcs.t1).toHaveLength(total);
    for (let i = 0; i < total; i++) {
      expect(init.arcs.link[i]!).toBeLessThan(init.linkCount);
    }
    expect(init.arcs.t0[0]).toBe(0);
    expect(init.arcs.t1[19]).toBe(1);
  });

  it("recomposes hi + lo into the story's mercator position", () => {
    const init = makeWorld().initMessage(NOW);
    for (let i = 0; i < init.storyCount; i++) {
      const [x, y] = lngLatToMercator(init.stories.lng[i]!, init.stories.lat[i]!);
      expect(init.stories.mercHi[i * 2]! + init.stories.mercLo[i * 2]!).toBeCloseTo(x, 6);
      expect(init.stories.mercHi[i * 2 + 1]! + init.stories.mercLo[i * 2 + 1]!).toBeCloseTo(y, 6);
    }
  });

  it("lists in-range top stories and a HUD list", () => {
    const init = makeWorld().initMessage(NOW);
    expect(init.topStories.length).toBeGreaterThan(0);
    for (const index of init.topStories) expect(index).toBeLessThan(init.storyCount);
    expect(init.hudTop.length).toBeGreaterThan(0);
  });

  it("is deterministic for a given seed and clock", () => {
    const a = makeWorld().initMessage(NOW);
    const b = makeWorld().initMessage(NOW);
    for (const key of ["lng", "lat", "category", "sources", "heat"] as const) {
      expect(Array.from(a.stories[key].slice(0, 50))).toEqual(
        Array.from(b.stories[key].slice(0, 50)),
      );
    }
    expect(Array.from(a.links.a.slice(0, 20))).toEqual(Array.from(b.links.a.slice(0, 20)));
    expect(Array.from(a.links.b.slice(0, 20))).toEqual(Array.from(b.links.b.slice(0, 20)));
  });
});

describe("FakeWorld focus", () => {
  it("focuses one story, links its neighbours and leaves the rest ambient", () => {
    const world = makeWorld();
    const graph = graphOf(world);
    const index = candidates(graph)[0]!;
    const patch = world.setFocus(index, false, NOW + 2500, 42);

    expect(patch.focusIndex).toBe(index);
    expect(patch.requestId).toBe(42);
    expect(patch.secondDegree).toBe(false);
    expect(world.currentFocus).toBe(index);

    const init = world.initMessage(NOW);
    const neighbours = graph.neighbours.get(index)!;
    let focused = 0;
    for (let s = 0; s < world.storyCount; s++) {
      const state = init.stories.focus[s]!;
      if (state === FOCUS_FOCUSED) {
        focused++;
        expect(s).toBe(index);
      } else if (neighbours.has(s)) {
        expect(state).toBe(FOCUS_LINKED);
      } else {
        expect(state).toBe(FOCUS_AMBIENT);
      }
    }
    expect(focused).toBe(1);

    const touching = linkThread(graph, index);
    for (let l = 0; l < world.linkCount; l++) {
      expect(init.links.focus[l]).toBe(touching.has(l) ? FOCUS_LINKED : FOCUS_AMBIENT);
    }

    for (const change of patch.storyChanges) {
      expect(change.prev).not.toBe(change.state);
      expect(change.at).toBeCloseTo(2.5, 9);
    }
    for (const change of patch.linkChanges) {
      expect(change.prev).not.toBe(change.state);
      expect(change.at).toBeCloseTo(2.5, 9);
    }
    expect(patch.storyChanges).toHaveLength(neighbours.size + 1);
    expect(patch.linkChanges).toHaveLength(touching.size);

    const hud = patch.hud!;
    expect(hud.story.index).toBe(index);
    for (let i = 1; i < hud.links.length; i++) {
      expect(hud.links[i - 1]!.strength).toBeGreaterThanOrEqual(hud.links[i]!.strength);
    }
    for (const link of hud.links) {
      const a = init.links.a[link.index]!;
      const b = init.links.b[link.index]!;
      expect(link.storyIndex).toBe(a === index ? b : a);
      expect([a, b]).toContain(index);
    }
    expect(hud.points[0]).toEqual([init.stories.lng[index], init.stories.lat[index]]);
  });

  it("switches thread by touching only the old and the new thread", () => {
    const world = makeWorld();
    const graph = graphOf(world);
    const [a, b] = candidates(graph).filter((c) => c !== undefined);
    expect(b).toBeDefined();
    world.setFocus(a!, false, NOW + 1000);
    const patch = world.setFocus(b!, false, NOW + 2000);

    const allowedStories = new Set([...threadOf(graph, a!), ...threadOf(graph, b!)]);
    const allowedLinks = new Set([...linkThread(graph, a!), ...linkThread(graph, b!)]);
    for (const change of patch.storyChanges) expect(allowedStories.has(change.index)).toBe(true);
    for (const change of patch.linkChanges) expect(allowedLinks.has(change.index)).toBe(true);

    const init = world.initMessage(NOW);
    const newThread = threadOf(graph, b!);
    for (let s = 0; s < world.storyCount; s++) {
      const state = init.stories.focus[s]!;
      if (s === b) expect(state).toBe(FOCUS_FOCUSED);
      else if (newThread.has(s)) expect(state).toBe(FOCUS_LINKED);
      else expect(state).toBe(FOCUS_AMBIENT);
    }
    const newLinks = linkThread(graph, b!);
    for (let l = 0; l < world.linkCount; l++) {
      expect(init.links.focus[l]).toBe(newLinks.has(l) ? FOCUS_LINKED : FOCUS_AMBIENT);
    }
    // The old focused story went back to ambient (unless it is in the new thread).
    if (!newThread.has(a!)) {
      expect(patch.storyChanges.find((c) => c.index === a)).toMatchObject({
        state: FOCUS_AMBIENT,
        prev: FOCUS_FOCUSED,
      });
    }
  });

  it("produces no story changes when the same story is focused twice", () => {
    const world = makeWorld();
    const index = candidates(graphOf(world))[0]!;
    world.setFocus(index, false, NOW + 1000);
    const again = world.setFocus(index, false, NOW + 2000);
    expect(again.storyChanges).toEqual([]);
    expect(again.linkChanges).toEqual([]);
    expect(again.focusIndex).toBe(index);
  });

  it("marks second-degree stories and links as state 3", () => {
    const world = makeWorld();
    const graph = graphOf(world);
    const index = candidates(graph).find((c) => secondDegree(graph, c).stories.size > 0);
    expect(index).toBeDefined();
    const before = world.initMessage(NOW).links.strength;
    const patch = world.setFocus(index!, true, NOW + 1000);
    const second = secondDegree(graph, index!);

    expect(patch.hud!.secondDegree).toBe(true);
    const init = world.initMessage(NOW);
    const ring1 = graph.neighbours.get(index!)!;
    for (let s = 0; s < world.storyCount; s++) {
      const state = init.stories.focus[s]!;
      if (s === index) expect(state).toBe(FOCUS_FOCUSED);
      else if (ring1.has(s)) expect(state).toBe(FOCUS_LINKED);
      else if (second.stories.has(s)) expect(state).toBe(FOCUS_SECOND);
      else expect(state).toBe(FOCUS_AMBIENT);
    }
    const direct = linkThread(graph, index!);
    for (let l = 0; l < world.linkCount; l++) {
      if (direct.has(l)) expect(init.links.focus[l]).toBe(FOCUS_LINKED);
      else if (second.links.has(l)) expect(init.links.focus[l]).toBe(FOCUS_SECOND);
      else expect(init.links.focus[l]).toBe(FOCUS_AMBIENT);
    }
    expect(Array.from(init.links.strength)).toEqual(Array.from(before));
  });

  it("resets everything to ambient when focus is cleared", () => {
    const world = makeWorld();
    const index = candidates(graphOf(world))[0]!;
    world.setFocus(index, true, NOW + 1000);
    const patch = world.setFocus(null, false, NOW + 2000, 9);
    expect(patch.hud).toBeNull();
    expect(patch.focusIndex).toBe(-1);
    expect(patch.requestId).toBe(9);
    expect(world.currentFocus).toBeNull();
    const init = world.initMessage(NOW);
    expect(init.stories.focus.every((s) => s === FOCUS_AMBIENT)).toBe(true);
    expect(init.links.focus.every((s) => s === FOCUS_AMBIENT)).toBe(true);
  });
});

describe("FakeWorld stream", () => {
  it("rate-limits ripple spawns to about capacity/lifetime", () => {
    const world = makeWorld();
    world.flush(NOW);
    let rippleWrites = 0;
    for (let frame = 0; frame < 40; frame++) {
      const now = NOW + 1000 + frame * 50;
      world.advance(now, 50);
      const patch = world.flush(now);
      rippleWrites += patch?.ripples.length ?? 0;
    }
    // ~14 spawns/s over 2 s; merges add one write each, so stay well below 40.
    expect(rippleWrites).toBeLessThan(36);
    expect(rippleWrites).toBeGreaterThan(0);
  });

  it("returns a patch with parallel arrays, then null on a second flush", () => {
    const world = makeWorld();
    world.flush(NOW); // drop the boot ripples
    world.advance(NOW + 100, 100);
    const patch = world.flush(NOW + 100)!;
    expect(patch).not.toBeNull();
    expect(patch.storyIndex.length).toBeGreaterThan(0);
    expect(patch.sources).toHaveLength(patch.storyIndex.length);
    expect(patch.heat).toHaveLength(patch.storyIndex.length);
    expect(patch.updatedAt).toHaveLength(patch.storyIndex.length);
    expect(patch.linkColor).toHaveLength(patch.linkIndex.length * 4);
    expect(world.flush(NOW + 100)).toBeNull();
  });

  it("accumulates several advances before a flush", () => {
    const world = makeWorld();
    world.flush(NOW);
    const before = world.initMessage(NOW).stories.sources;
    for (let i = 0; i < 5; i++) world.advance(NOW + i * 100, 100); // 5 articles each
    const patch = world.flush(NOW + 500)!;
    let delta = 0;
    patch.storyIndex.forEach((story, position) => {
      delta += patch.sources[position]! - before[story]!;
    });
    expect(delta).toBe(25);
  });

  it("increases exactly one story's sources by one per article", () => {
    const world = makeWorld();
    world.flush(NOW);
    const before = world.initMessage(NOW).stories.sources;
    world.advance(NOW + 20, 20); // 50/s * 20 ms = exactly 1 article
    const patch = world.flush(NOW + 20)!;
    expect(patch.storyIndex).toHaveLength(1);
    const story = patch.storyIndex[0]!;
    expect(patch.sources[0]).toBe(before[story]! + 1);
    for (const ripple of patch.ripples) {
      expect(ripple.slot).toBeGreaterThanOrEqual(0);
      expect(ripple.slot).toBeLessThan(20);
    }
  });

  it("produces nothing while the stream is disabled", () => {
    const world = makeWorld();
    world.flush(NOW);
    world.setStreamEnabled(false);
    world.advance(NOW + 1000, 250);
    expect(world.flush(NOW + 1000)).toBeNull();
    world.setStreamEnabled(true);
    world.advance(NOW + 1100, 100);
    expect(world.flush(NOW + 1100)).not.toBeNull();
  });
});
