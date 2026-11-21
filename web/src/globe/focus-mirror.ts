import { FOCUS_AMBIENT, FOCUS_FOCUSED, FOCUS_LINKED, FOCUS_SECOND } from "./encoding";
import type {
  FakeInit,
  FocusLinkChange,
  FocusStoryChange,
  HudFocus,
  HudLink,
  HudParty,
  HudStory,
} from "../worker/fake/protocol";

export type FocusMirrorResult = {
  storyChanges: FocusStoryChange[];
  linkChanges: FocusLinkChange[];
  hud: HudFocus | null;
  /** Focused + linked (+ second-degree) positions, in label order. */
  points: [number, number][];
  secondDegree: boolean;
};

/**
 * Main-thread mirror of the worker's CSR adjacency and per-instance focus
 * state.
 *
 * A focus switch on the main thread is O(degree): walk the focused story's
 * adjacency, write the new bytes, and reset every previously lit index that
 * is not in the new thread. That makes stale or dropped worker patches
 * harmless by construction — the lit set is the source of truth for what
 * must be turned off.
 */
export class FocusMirror {
  private readonly storyCount: number;
  private readonly linkCount: number;
  private readonly storyCapacity: number;
  private readonly linkCapacity: number;
  private readonly adjStart: Uint32Array;
  private readonly adjList: Uint16Array;
  private readonly adjLink: Uint16Array;
  private readonly linkA: Uint16Array;
  private readonly linkB: Uint16Array;
  private readonly linkType: Uint8Array;
  private readonly linkStrength: Float32Array;
  private readonly linkUnverified: Uint8Array;
  private readonly linkDirection: Uint8Array;
  private readonly linkConfidence: Float32Array;
  private readonly linkDistance: Float32Array;
  private readonly storyLng: Float64Array;
  private readonly storyLat: Float64Array;
  private readonly storyTitle: string[];
  private readonly storyPlace: string[];
  private readonly storyCategory: Uint8Array;
  private readonly storySources: Uint16Array;
  private readonly storyHeat: Float32Array;
  private readonly partyCount: Uint8Array;
  private readonly partyLng: Float32Array;
  private readonly partyLat: Float32Array;
  private readonly partyLabel: string[];
  private readonly storyActive: Uint8Array;
  private readonly linkActive: Uint8Array;

  private readonly storyState: Uint8Array;
  private readonly linkState: Uint8Array;
  private litStories = new Set<number>();
  private litLinks = new Set<number>();

  constructor(init: FakeInit) {
    this.storyCount = init.storyCount;
    this.linkCount = init.linkCount;
    this.storyCapacity = Math.max(init.storyCount, init.storyCapacity ?? init.storyCount);
    this.linkCapacity = Math.max(init.linkCount, init.linkCapacity ?? init.linkCount);
    this.adjStart = new Uint32Array(this.storyCapacity + 1);
    this.adjStart.set(init.adjacency.adjStart.subarray(0, this.storyCount + 1));
    this.adjList = new Uint16Array(this.linkCapacity * 2);
    this.adjList.set(init.adjacency.adjList);
    this.adjLink = new Uint16Array(this.linkCapacity * 2);
    this.adjLink.set(init.adjacency.adjLink);
    this.linkA = new Uint16Array(this.linkCapacity);
    this.linkA.set(init.links.a);
    this.linkB = new Uint16Array(this.linkCapacity);
    this.linkB.set(init.links.b);
    this.linkType = new Uint8Array(this.linkCapacity);
    this.linkType.set(init.links.type);
    this.linkStrength = new Float32Array(this.linkCapacity);
    this.linkStrength.set(init.links.strength);
    this.linkUnverified = new Uint8Array(this.linkCapacity);
    if (init.links.unverified) this.linkUnverified.set(init.links.unverified);
    this.linkDirection = new Uint8Array(this.linkCapacity);
    if (init.links.direction) this.linkDirection.set(init.links.direction);
    this.linkConfidence = new Float32Array(this.linkCapacity);
    if (init.links.confidence) this.linkConfidence.set(init.links.confidence);
    this.linkDistance = new Float32Array(this.linkCapacity);
    if (init.links.distanceKm) this.linkDistance.set(init.links.distanceKm);
    this.storyLng = new Float64Array(this.storyCapacity);
    this.storyLng.set(init.stories.lng);
    this.storyLat = new Float64Array(this.storyCapacity);
    this.storyLat.set(init.stories.lat);
    this.storyTitle = new Array<string>(this.storyCapacity).fill("");
    init.stories.title.forEach((value, index) => (this.storyTitle[index] = value));
    this.storyPlace = new Array<string>(this.storyCapacity).fill("");
    init.stories.place.forEach((value, index) => (this.storyPlace[index] = value));
    this.storyCategory = new Uint8Array(this.storyCapacity);
    this.storyCategory.set(init.stories.category);
    this.storySources = new Uint16Array(this.storyCapacity);
    this.storySources.set(init.stories.sources);
    this.storyHeat = new Float32Array(this.storyCapacity);
    this.storyHeat.set(init.stories.heat);
    this.partyCount = new Uint8Array(this.storyCapacity);
    this.partyCount.set(init.stories.partyCount);
    this.partyLng = new Float32Array(this.storyCapacity * 6);
    this.partyLng.set(init.stories.partyLng);
    this.partyLat = new Float32Array(this.storyCapacity * 6);
    this.partyLat.set(init.stories.partyLat);
    this.partyLabel = new Array<string>(this.storyCapacity * 6).fill("");
    init.stories.partyLabel.forEach((value, index) => (this.partyLabel[index] = value));
    this.storyState = new Uint8Array(this.storyCapacity);
    this.linkState = new Uint8Array(this.linkCapacity);
    this.storyActive = new Uint8Array(this.storyCapacity);
    this.storyActive.fill(1, 0, this.storyCount);
    this.linkActive = new Uint8Array(this.linkCapacity);
    this.linkActive.fill(1, 0, this.linkCount);
  }

  /** Current lit state for tests and diagnostics. */
  get litStoryCount(): number {
    return this.litStories.size;
  }

  get litLinkCount(): number {
    return this.litLinks.size;
  }

  get litStoryIndices(): ReadonlySet<number> {
    return this.litStories;
  }

  degree(storyIndex: number): number {
    if (storyIndex < 0 || storyIndex >= this.storyCapacity || this.storyActive[storyIndex] === 0) {
      return 0;
    }
    return this.adjStart[storyIndex + 1]! - this.adjStart[storyIndex]!;
  }

  hasNeighbour(a: number, b: number): boolean {
    if (a < 0 || b < 0 || a >= this.storyCapacity || b >= this.storyCapacity) return false;
    for (let edge = this.adjStart[a]!; edge < this.adjStart[a + 1]!; edge++) {
      if (this.adjList[edge] === b) return true;
    }
    return false;
  }

  storyStateOf(index: number): number {
    return this.storyState[index] ?? FOCUS_AMBIENT;
  }

  linkStateOf(index: number): number {
    return this.linkState[index] ?? FOCUS_AMBIENT;
  }

  /** Keep the local HUD metadata in step with streamed worker updates. */
  applyStream(patch: { storyIndex: Uint16Array; sources: Uint16Array; heat: Float32Array }): void {
    for (let i = 0; i < patch.storyIndex.length; i++) {
      const index = patch.storyIndex[i]!;
      if (index >= this.storyCapacity) continue;
      this.storySources[index] = patch.sources[i]!;
      this.storyHeat[index] = patch.heat[i]!;
    }
  }

  /** Whether a story is in the current focused thread. */
  isLit(storyIndex: number): boolean {
    return this.litStories.has(storyIndex);
  }

  focus(index: number | null, secondDegree: boolean, nowSec: number): FocusMirrorResult {
    const nextStories = new Map<number, number>();
    const nextLinks = new Map<number, number>();
    const points: [number, number][] = [];
    const labels: HudFocus["labels"] = [];
    let hud: HudFocus | null = null;

    if (
      index !== null &&
      index >= 0 &&
      index < this.storyCapacity &&
      this.storyActive[index] !== 0
    ) {
      const ring1 = new Set<number>();
      const directLinks: number[] = [];
      for (let edge = this.adjStart[index]!; edge < this.adjStart[index + 1]!; edge++) {
        const other = this.adjList[edge]!;
        const link = this.adjLink[edge]!;
        ring1.add(other);
        directLinks.push(link);
        nextLinks.set(link, FOCUS_LINKED);
      }
      nextStories.set(index, FOCUS_FOCUSED);
      for (const story of ring1) nextStories.set(story, FOCUS_LINKED);

      const secondStories = new Set<number>();
      if (secondDegree) {
        for (const story of ring1) {
          for (let edge = this.adjStart[story]!; edge < this.adjStart[story + 1]!; edge++) {
            const link = this.adjLink[edge]!;
            const other = this.adjList[edge]!;
            if (nextLinks.has(link)) continue;
            nextLinks.set(link, FOCUS_SECOND);
            if (other !== index && !ring1.has(other)) secondStories.add(other);
          }
        }
        for (const story of secondStories) nextStories.set(story, FOCUS_SECOND);
      }

      points.push([this.storyLng[index]!, this.storyLat[index]!]);
      labels.push({
        lng: this.storyLng[index]!,
        lat: this.storyLat[index]!,
        category: this.storyCategory[index]!,
        place: this.storyPlace[index]!,
      });
      for (const story of ring1) {
        points.push([this.storyLng[story]!, this.storyLat[story]!]);
        labels.push({
          lng: this.storyLng[story]!,
          lat: this.storyLat[story]!,
          category: this.storyCategory[story]!,
          place: this.storyPlace[story]!,
        });
      }
      if (secondDegree) {
        for (const story of secondStories) {
          points.push([this.storyLng[story]!, this.storyLat[story]!]);
          labels.push({
            lng: this.storyLng[story]!,
            lat: this.storyLat[story]!,
            category: this.storyCategory[story]!,
            place: this.storyPlace[story]!,
          });
        }
      }

      const links: HudLink[] = directLinks
        .map((link) => {
          const a = this.linkA[link]!;
          const b = this.linkB[link]!;
          const other = a === index ? b : a;
          return {
            index: link,
            storyIndex: other,
            title: this.storyTitle[other]!,
            place: this.storyPlace[other]!,
            type: this.linkType[link]!,
            strength: this.linkStrength[link]!,
            direction: this.linkDirection[link]!,
            confidence: this.linkConfidence[link]!,
            distanceKm: this.linkDistance[link]!,
            unverified: this.linkUnverified[link] === 1,
          };
        })
        .sort((left, right) => right.strength - left.strength)
        .slice(0, 40);

      const parties: HudParty[] = [];
      const count = this.partyCount[index]!;
      for (let party = 0; party < count; party++) {
        parties.push({
          lng: this.partyLng[index * 6 + party]!,
          lat: this.partyLat[index * 6 + party]!,
          label: this.partyLabel[index * 6 + party]!,
        });
      }

      hud = {
        story: this.hudStory(index),
        links,
        points,
        labels,
        parties,
        secondDegree,
      };
    }

    const storyChanges: FocusStoryChange[] = [];
    for (const story of this.litStories) {
      if (!nextStories.has(story)) {
        const prev = this.storyState[story]!;
        this.storyState[story] = FOCUS_AMBIENT;
        storyChanges.push({ index: story, state: FOCUS_AMBIENT, prev, at: nowSec });
      }
    }
    for (const [story, state] of nextStories) {
      const prev = this.storyState[story]!;
      if (prev === state) continue;
      this.storyState[story] = state;
      storyChanges.push({ index: story, state, prev, at: nowSec });
    }
    this.litStories = new Set(nextStories.keys());

    const linkChanges: FocusLinkChange[] = [];
    for (const link of this.litLinks) {
      if (!nextLinks.has(link)) {
        const prev = this.linkState[link]!;
        this.linkState[link] = FOCUS_AMBIENT;
        linkChanges.push({ index: link, state: FOCUS_AMBIENT, prev, at: nowSec });
      }
    }
    for (const [link, state] of nextLinks) {
      const prev = this.linkState[link]!;
      if (prev === state) continue;
      this.linkState[link] = state;
      linkChanges.push({ index: link, state, prev, at: nowSec });
    }
    this.litLinks = new Set(nextLinks.keys());

    return { storyChanges, linkChanges, hud, points, secondDegree };
  }

  /**
   * Keep the mirror in step with a link add/remove. Rare: rebuilding the CSR
   * is O(stories + links), which is still smaller than a frame budget.
   */
  applyLinkDelta(
    linkIndex: number,
    a: number,
    b: number,
    add: boolean,
    type = 0,
    strength = 0.5,
  ): void {
    if (linkIndex < 0 || linkIndex >= this.linkCapacity) return;
    if (add) {
      this.linkA[linkIndex] = a;
      this.linkB[linkIndex] = b;
      this.linkType[linkIndex] = type;
      this.linkStrength[linkIndex] = strength;
      this.linkUnverified[linkIndex] = 0;
      this.linkDirection[linkIndex] = 0;
      this.linkConfidence[linkIndex] = 0;
      this.linkDistance[linkIndex] = 0;
      this.linkActive[linkIndex] = 1;
    } else {
      this.linkA[linkIndex] = 0;
      this.linkB[linkIndex] = 0;
      this.linkType[linkIndex] = 0;
      this.linkStrength[linkIndex] = 0;
      this.linkActive[linkIndex] = 0;
      this.litLinks.delete(linkIndex);
      this.linkState[linkIndex] = FOCUS_AMBIENT;
    }
    this.rebuildAdjacency();
  }

  /** Apply a story insert/remove; structural, so it rebuilds once. */
  applyStoryDelta(
    index: number,
    fields: { lng: number; lat: number; category: number; title: string; place: string; sources: number; heat: number },
    add: boolean,
  ): void {
    if (index < 0 || index >= this.storyCapacity) return;
    this.storyActive[index] = add ? 1 : 0;
    this.storyLng[index] = add ? fields.lng : 0;
    this.storyLat[index] = add ? fields.lat : 0;
    this.storyCategory[index] = add ? fields.category : 0;
    this.storyTitle[index] = add ? fields.title : "";
    this.storyPlace[index] = add ? fields.place : "";
    this.storySources[index] = add ? fields.sources : 0;
    this.storyHeat[index] = add ? fields.heat : 0;
    this.partyCount[index] = 0;
    if (!add) {
      this.litStories.delete(index);
      this.storyState[index] = FOCUS_AMBIENT;
    }
    this.rebuildAdjacency();
  }

  private rebuildAdjacency(): void {
    const counts = new Uint32Array(this.storyCapacity + 1);
    for (let link = 0; link < this.linkCapacity; link++) {
      if (this.linkActive[link] === 0) continue;
      const a = this.linkA[link]!;
      const b = this.linkB[link]!;
      if (a === b || this.storyActive[a] === 0 || this.storyActive[b] === 0) continue;
      counts[a] = (counts[a] ?? 0) + 1;
      counts[b] = (counts[b] ?? 0) + 1;
    }
    let sum = 0;
    for (let story = 0; story < this.storyCapacity; story++) {
      this.adjStart[story] = sum;
      sum += counts[story]!;
    }
    this.adjStart[this.storyCapacity] = sum;
    const cursor = this.adjStart.slice(0, this.storyCapacity);
    for (let link = 0; link < this.linkCapacity; link++) {
      if (this.linkActive[link] === 0) continue;
      const a = this.linkA[link]!;
      const b = this.linkB[link]!;
      if (a === b || this.storyActive[a] === 0 || this.storyActive[b] === 0) continue;
      for (const [from, to] of [
        [a, b],
        [b, a],
      ] as const) {
        const at = cursor[from]!;
        this.adjList[at] = to;
        this.adjLink[at] = link;
        cursor[from] = at + 1;
      }
    }
  }

  private hudStory(index: number): HudStory {
    return {
      index,
      title: this.storyTitle[index]!,
      place: this.storyPlace[index]!,
      category: this.storyCategory[index]!,
      sources: this.storySources[index]!,
      heat: this.storyHeat[index]!,
    };
  }
}
