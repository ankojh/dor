/**
 * Bridges the real dor.v1 protocol into the GPU layer's initial data shape.
 *
 * The M0 renderer was written against a fake-stream `FakeInit`. This module
 * keeps the renderer unchanged while building that shape from real snapshots,
 * and owns the small amount of canonical-coordinate conversion the live path
 * needs.
 */
import type { Link, Story } from "../gen/dor/v1/dor_pb";
import { Role } from "../gen/dor/v1/dor_pb";
import {
  categoryColor,
  encodeArc,
  type FocusState,
  FOCUS_AMBIENT,
} from "./encoding";
import {
  greatCirclePoint,
  lngLatToMercator,
  splitMercator,
  type Vec2,
} from "./mercator";
import { RIPPLE_CAPACITY } from "./ripples";
import { ARC_SEGMENTS, type FakeInit, type HudStory, type TetherRing, type TetherSegment } from "../worker/fake/protocol";

export type LiveStory = {
  story: Story;
  index: number;
  lng: number;
  lat: number;
  place: string;
  category: number;
  color: [number, number, number, number];
  parties: Array<{ lng: number; lat: number; label: string }>;
  degree: number;
  strongestLink: number;
};

export type LiveGlobeData = {
  init: FakeInit;
  storyById: Map<string, number>;
  linkByKey: Map<string, number>;
  stories: LiveStory[];
  /** Stable order used by the wire/panel before the user focuses. */
  topStoryIds: string[];
};

export function linkKey(a: string, b: string): string {
  return a < b ? `${a}\u0000${b}` : `${b}\u0000${a}`;
}

export function storyLocation(story: Story): { lng: number; lat: number; place: string } | null {
  if (story.locations.length === 0) return null;
  const primary = story.locations[story.primary] ?? story.locations[0]!;
  return { lng: primary.lon, lat: primary.lat, place: primary.name || "Unknown" };
}

function storyParties(story: Story): LiveStory["parties"] {
  return story.locations
    .filter((location) => location.role === Role.PARTY)
    .slice(0, 3)
    .map((location) => ({
      lng: location.lon,
      lat: location.lat,
      label: location.name || "Party",
    }));
}

/** Build a full renderer snapshot from the current real stories and links. */
export function buildLiveGlobe(
  sourceStories: Iterable<Story>,
  sourceLinks: Iterable<Link>,
  nowMs = Date.now(),
): LiveGlobeData {
  const located: LiveStory[] = [];
  const storyById = new Map<string, number>();
  const realLinks = Array.from(sourceLinks);
  for (const story of sourceStories) {
    const location = storyLocation(story);
    if (!location) continue;
    const index = located.length;
    const category = story.category >= 0 && story.category <= 8 ? story.category : 0;
    storyById.set(story.id, index);
    located.push({
      story,
      index,
      lng: location.lng,
      lat: location.lat,
      place: location.place,
      category,
      color: [...categoryColor(category), 255] as [number, number, number, number],
      parties: storyParties(story),
      degree: 0,
      strongestLink: 0,
    });
  }

  const storyCount = located.length;
  const storyCapacity = Math.max(2048, storyCount + 1024);
  const linkCapacity = Math.max(1024, realLinks.length + 512);
  const epochMs = nowMs;
  const storyLng = new Float64Array(storyCount);
  const storyLat = new Float64Array(storyCount);
  const mercHi = new Float32Array(storyCount * 2);
  const mercLo = new Float32Array(storyCount * 2);
  const category = new Uint8Array(storyCount);
  const sources = new Uint16Array(storyCount);
  const heat = new Float32Array(storyCount);
  const updatedAt = new Float32Array(storyCount);
  const focus = new Uint8Array(storyCount).fill(FOCUS_AMBIENT as FocusState);
  const prevFocus = new Uint8Array(storyCount).fill(FOCUS_AMBIENT as FocusState);
  const focusAt = new Float32Array(storyCount);
  const color = new Uint8Array(storyCount * 4);
  const title: string[] = new Array(storyCount).fill("");
  const place: string[] = new Array(storyCount).fill("");
  const partyCount = new Uint8Array(storyCount);
  const partyLng = new Float32Array(storyCount * 3);
  const partyLat = new Float32Array(storyCount * 3);
  const partyLabel: string[] = new Array(storyCount * 3).fill("");

  for (const entry of located) {
    const i = entry.index;
    const [mercX, mercY] = lngLatToMercator(entry.lng, entry.lat);
    const [hiX, loX] = splitMercator(mercX);
    const [hiY, loY] = splitMercator(mercY);
    storyLng[i] = entry.lng;
    storyLat[i] = entry.lat;
    mercHi[i * 2] = hiX;
    mercHi[i * 2 + 1] = hiY;
    mercLo[i * 2] = loX;
    mercLo[i * 2 + 1] = loY;
    category[i] = entry.category;
    sources[i] = Math.max(0, Math.min(65535, entry.story.sourceCount));
    heat[i] = Math.max(0, Math.min(1, entry.story.heat));
    updatedAt[i] = (Number(entry.story.updatedAt) * 1000 - nowMs) / 1000;
    title[i] = entry.story.title;
    place[i] = entry.place;
    color.set(entry.color, i * 4);
    partyCount[i] = entry.parties.length;
    entry.parties.forEach((party, p) => {
      partyLng[i * 3 + p] = party.lng;
      partyLat[i * 3 + p] = party.lat;
      partyLabel[i * 3 + p] = party.label;
    });
  }

  const linkA: number[] = [];
  const linkB: number[] = [];
  const linkType: number[] = [];
  const linkStrength: number[] = [];
  const linkWidth: number[] = [];
  const linkUnverified: number[] = [];
  const linkDirection: number[] = [];
  const linkConfidence: number[] = [];
  const linkDistance: number[] = [];
  const linkByKey = new Map<string, number>();
  for (const link of realLinks) {
    const a = storyById.get(link.a);
    const b = storyById.get(link.b);
    if (a === undefined || b === undefined || a === b) continue;
    const key = linkKey(link.a, link.b);
    if (linkByKey.has(key)) continue;
    const index = linkA.length;
    linkByKey.set(key, index);
    linkA.push(a);
    linkB.push(b);
    linkType.push(link.type);
    linkStrength.push(Math.max(0, Math.min(1, link.strength)));
    const encoding = encodeArc(link.type, link.strength);
    linkWidth.push(encoding.widthPx);
    linkUnverified.push(link.unverified ? 1 : 0);
    linkDirection.push(link.dir);
    linkConfidence.push(link.confidence);
    linkDistance.push(
      greatCircleLength(
        [located[a]!.lng, located[a]!.lat],
        [located[b]!.lng, located[b]!.lat],
      ) / 1000,
    );
  }

  const linkCount = linkA.length;
  const links = {
    a: Uint16Array.from(linkA),
    b: Uint16Array.from(linkB),
    type: Uint8Array.from(linkType),
    strength: Float32Array.from(linkStrength),
    focus: new Uint8Array(linkCount).fill(FOCUS_AMBIENT),
    prevFocus: new Uint8Array(linkCount).fill(FOCUS_AMBIENT),
    focusAt: new Float32Array(linkCount),
    color: new Uint8Array(linkCount * 4),
    width: Float32Array.from(linkWidth),
    unverified: Uint8Array.from(linkUnverified),
    direction: Uint8Array.from(linkDirection),
    confidence: Float32Array.from(linkConfidence),
    distanceKm: Float32Array.from(linkDistance),
  };
  for (let i = 0; i < linkCount; i++) {
    const encoding = encodeArc(linkType[i]!, linkStrength[i]!);
    const rgb = linkColorFor(linkType[i]!);
    links.color.set([...rgb, Math.round(encoding.opacity * 255)], i * 4);
  }

  const adjStart = new Uint32Array(storyCount + 1);
  const adjList = new Uint16Array(Math.max(1, linkCount * 2));
  const adjLink = new Uint16Array(Math.max(1, linkCount * 2));
  const counts = new Uint32Array(storyCount + 1);
  for (let i = 0; i < linkCount; i++) {
    counts[linkA[i]!] = (counts[linkA[i]!] ?? 0) + 1;
    counts[linkB[i]!] = (counts[linkB[i]!] ?? 0) + 1;
  }
  let sum = 0;
  for (let i = 0; i < storyCount; i++) {
    adjStart[i] = sum;
    sum += counts[i]!;
  }
  adjStart[storyCount] = sum;
  const cursor = adjStart.slice(0, storyCount);
  for (let i = 0; i < linkCount; i++) {
    const a = linkA[i]!;
    const b = linkB[i]!;
    const atA = cursor[a]!;
    adjList[atA] = b;
    adjLink[atA] = i;
    cursor[a] = atA + 1;
    const atB = cursor[b]!;
    adjList[atB] = a;
    adjLink[atB] = i;
    cursor[b] = atB + 1;
  }

  const top = located
    .slice()
    .sort((left, right) => {
      const leftDegree = adjStart[left.index + 1]! - adjStart[left.index]!;
      const rightDegree = adjStart[right.index + 1]! - adjStart[right.index]!;
      const score = (entry: LiveStory, degree: number): number =>
        entry.story.heat * 2 +
        Math.min(10, degree) * 0.15 +
        Math.min(30, entry.story.sourceCount) * 0.01;
      return score(right, rightDegree) - score(left, leftDegree);
    })
    .slice(0, 300);
  const topStories = Uint16Array.from(top.map((entry) => entry.index));
  const hudTop: HudStory[] = top.slice(0, 24).map((entry) => ({
    index: entry.index,
    title: entry.story.title,
    place: entry.place,
    category: entry.category,
    sources: entry.story.sourceCount,
    heat: entry.story.heat,
  }));

  for (const entry of located) {
    entry.degree = adjStart[entry.index + 1]! - adjStart[entry.index]!;
  }
  for (let i = 0; i < linkCount; i++) {
    const strength = linkStrength[i]!;
    const a = located[linkA[i]!]!;
    const b = located[linkB[i]!]!;
    a.strongestLink = Math.max(a.strongestLink, strength);
    b.strongestLink = Math.max(b.strongestLink, strength);
  }
  const arcs = buildArcArrays(located, linkA, linkB, linkType);
  const ripples = emptyRipples();
  const strengths = linkStrength.filter((strength) => strength >= 0.6).sort((a, b) => b - a);
  const arcRestFloor = Math.max(0.6, strengths[Math.min(strengths.length - 1, 55)] ?? 0.6);

  const init: FakeInit = {
    type: "init",
    now: nowMs,
    epochMs,
    storyCount,
    linkCount,
    storyCapacity,
    linkCapacity,
    arcSegments: ARC_SEGMENTS,
    rippleCapacity: RIPPLE_CAPACITY,
    arcRestFloor,
    stories: {
      lng: storyLng,
      lat: storyLat,
      mercHi,
      mercLo,
      category,
      sources,
      heat,
      updatedAt,
      focus,
      prevFocus,
      focusAt,
      color,
      title,
      place,
      partyCount,
      partyLng,
      partyLat,
      partyLabel,
    },
    links,
    arcs,
    ripples,
    adjacency: { adjStart, adjList, adjLink },
    topStories,
    hudTop,
  };

  return {
    init,
    storyById,
    linkByKey,
    stories: located,
    topStoryIds: top.map((entry) => entry.story.id),
  };
}

function linkColorFor(type: number): readonly [number, number, number] {
  const colors: Record<number, readonly [number, number, number]> = {
    0: [255, 194, 92],
    1: [255, 111, 216],
    2: [94, 242, 255],
  };
  return colors[type] ?? colors[0]!;
}

function buildArcArrays(
  stories: LiveStory[],
  linkA: number[],
  linkB: number[],
  linkType: number[],
): FakeInit["arcs"] {
  const instances = Math.max(1, linkA.length * ARC_SEGMENTS);
  const startHi = new Float32Array(instances * 2);
  const startLo = new Float32Array(instances * 2);
  const endHi = new Float32Array(instances * 2);
  const endLo = new Float32Array(instances * 2);
  const startElev = new Float32Array(instances);
  const endElev = new Float32Array(instances);
  const t0 = new Float32Array(instances);
  const t1 = new Float32Array(instances);
  const link = new Uint16Array(instances);
  const type = new Uint8Array(instances);
  const length = new Float32Array(instances);
  for (let l = 0; l < linkA.length; l++) {
    const aStory = stories[linkA[l]!]!;
    const bStory = stories[linkB[l]!]!;
    const a: Vec2 = [aStory.lng, aStory.lat];
    const b: Vec2 = [bStory.lng, bStory.lat];
    const linkLength = greatCircleLength(a, b);
    let previous = lngLatToMercator(a[0], a[1]);
    let previousX = previous[0];
    let point = a;
    for (let segment = 0; segment < ARC_SEGMENTS; segment++) {
      const tA = segment / ARC_SEGMENTS;
      const tB = (segment + 1) / ARC_SEGMENTS;
      const next = greatCirclePoint(a, b, tB);
      let mercB = lngLatToMercator(next[0], next[1]);
      while (mercB[0] - previousX > 0.5) mercB = [mercB[0] - 1, mercB[1]];
      while (mercB[0] - previousX < -0.5) mercB = [mercB[0] + 1, mercB[1]];
      const [startHiX, startLoX] = splitMercator(previousX);
      const [startHiY, startLoY] = splitMercator(previous[1]);
      const [endHiX, endLoX] = splitMercator(mercB[0]);
      const [endHiY, endLoY] = splitMercator(mercB[1]);
      const base = l * ARC_SEGMENTS + segment;
      startHi[base * 2] = startHiX;
      startHi[base * 2 + 1] = startHiY;
      startLo[base * 2] = startLoX;
      startLo[base * 2 + 1] = startLoY;
      endHi[base * 2] = endHiX;
      endHi[base * 2 + 1] = endHiY;
      endLo[base * 2] = endLoX;
      endLo[base * 2 + 1] = endLoY;
      startElev[base] = elevation(a, b, tA);
      endElev[base] = elevation(a, b, tB);
      t0[base] = tA;
      t1[base] = tB;
      link[base] = l;
      type[base] = linkType[l]!;
      length[base] = linkLength;
      previousX = mercB[0];
      previous = mercB;
      point = next;
    }
    void point;
  }
  return { startHi, startLo, endHi, endLo, startElev, endElev, t0, t1, link, type, length };
}

function emptyRipples(): FakeInit["ripples"] {
  const capacity = RIPPLE_CAPACITY;
  return {
    lng: new Float32Array(capacity),
    lat: new Float32Array(capacity),
    mercHi: new Float32Array(capacity * 2),
    mercLo: new Float32Array(capacity * 2),
    category: new Uint8Array(capacity),
    spawnAt: new Float32Array(capacity).fill(-1e9),
    intensity: new Float32Array(capacity),
    storyIndex: new Int32Array(capacity).fill(-1),
  };
}

function greatCircleLength(a: Vec2, b: Vec2): number {
  const toRad = Math.PI / 180;
  const dLng = (b[0] - a[0]) * toRad;
  const dLat = (b[1] - a[1]) * toRad;
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(a[1] * toRad) * Math.cos(b[1] * toRad) * Math.sin(dLng / 2) ** 2;
  return 2 * 6_371_008.8 * Math.asin(Math.min(1, Math.sqrt(h)));
}

function elevation(a: Vec2, b: Vec2, t: number): number {
  return Math.sin(Math.PI * t) * Math.min(greatCircleLength(a, b) * 0.035, 420_000);
}

/** Build the focused story's party tethers locally (the real data path has no fake worker). */
export function buildLiveTethers(entry: LiveStory): {
  segments: TetherSegment[];
  rings: TetherRing[];
} {
  const segments: TetherSegment[] = [];
  const rings: TetherRing[] = [];
  const origin: Vec2 = [entry.lng, entry.lat];
  for (const party of entry.parties) {
    const target: Vec2 = [party.lng, party.lat];
    let previous = lngLatToMercator(origin[0], origin[1]);
    let previousX = previous[0];
    const steps = 32;
    for (let step = 0; step < steps; step++) {
      const tA = step / steps;
      const tB = (step + 1) / steps;
      const next = greatCirclePoint(origin, target, tB);
      let mercB = lngLatToMercator(next[0], next[1]);
      while (mercB[0] - previousX > 0.5) mercB = [mercB[0] - 1, mercB[1]];
      while (mercB[0] - previousX < -0.5) mercB = [mercB[0] + 1, mercB[1]];
      const [startHiX, startLoX] = splitMercator(previousX);
      const [startHiY, startLoY] = splitMercator(previous[1]);
      const [endHiX, endLoX] = splitMercator(mercB[0]);
      const [endHiY, endLoY] = splitMercator(mercB[1]);
      segments.push({
        startHi: [startHiX, startHiY],
        startLo: [startLoX, startLoY],
        endHi: [endHiX, endHiY],
        endLo: [endLoX, endLoY],
        t0: tA,
        t1: tB,
      });
      previousX = mercB[0];
      previous = mercB;
    }
    const [ringHiX, ringLoX] = splitMercator(previous[0]);
    const [ringHiY, ringLoY] = splitMercator(previous[1]);
    rings.push({ hi: [ringHiX, ringHiY], lo: [ringLoX, ringLoY], label: party.label });
  }
  return { segments, rings };
}
