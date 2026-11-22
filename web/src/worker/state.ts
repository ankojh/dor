/** Authoritative live state and GPU encoding. This module has no DOM dependency. */
import type { Delta, Link, Story, WireRow } from "../gen/dor/v1/dor_pb";
import {
  buildArcArrays,
  buildLiveGlobe,
  linkKey,
  storyLocation,
  type LiveStory,
} from "../globe/live-data";
import { categoryColor, encodeArc } from "../globe/encoding";
import { lngLatToMercator, splitMercator } from "../globe/mercator";
import { RippleRingBuffer } from "../globe/ripples";
import { mergeWireRow } from "../live/wire";
import type { AdjacencyArrays, FakeInit, StreamPatch } from "./fake/protocol";
import type {
  LinkPatch,
  MatchBytes,
  StoryPatch,
  WireFilters,
  WorkerInit,
  WorkerPatch,
} from "./messages";

export const WIRE_LIMIT = 1000;
/** Slot ids are uint16 throughout the GPU and focus protocol. Fail before wrapping. */
export const MAX_SLOTS = 65536;

import { growArray, type Numeric } from "../globe/arrays";
function capacityFor(current: number, required: number): number {
  if (required > MAX_SLOTS) throw new Error("globe slot capacity exceeded");
  return Math.min(MAX_SLOTS, Math.max(current, required, current * 2));
}
function emptyStream(now: number): StreamPatch {
  return {
    type: "stream",
    now,
    storyIndex: new Uint16Array(),
    sources: new Uint16Array(),
    heat: new Float32Array(),
    updatedAt: new Float32Array(),
    linkIndex: new Uint16Array(),
    strength: new Float32Array(),
    width: new Float32Array(),
    linkColor: new Uint8Array(),
    ripples: [],
  };
}
function rowKey(row: WireRow): string {
  return row.articleId || `${row.source}:${row.at}:${row.title}`;
}
function range(map: Map<number, number>): MatchBytes {
  return { indices: Uint16Array.from(map.keys()), dimmed: Uint8Array.from(map.values()) };
}

export class LiveState {
  readonly epochMs: number;
  readonly stories = new Map<string, Story>();
  readonly links = new Map<string, Link>();
  readonly wire = new Map<string, WireRow>();
  private storySlots = new Map<string, number>();
  private linkSlots = new Map<string, number>();
  private freeStories: number[] = [];
  private freeLinks: number[] = [];
  private entries: LiveStory[] = [];
  private init!: FakeInit;
  private dirtyStories = new Set<number>();
  private dirtyGraphStories = new Set<number>();
  private storyOps = new Map<number, StoryPatch>();
  private linkOps = new Map<number, LinkPatch>();
  private dirtyWire = new Map<string, WireRow>();
  private alerts: WorkerPatch["alerts"] = [];
  private topology = false;
  private storyDims = new Map<number, number>();
  private linkDims = new Map<number, number>();
  private dimmedStories = new Set<number>();
  private dimmedLinks = new Set<number>();
  private rippleRows = new Map<string, WireRow>();
  private readonly ripples = new RippleRingBuffer();
  private filters: WireFilters | null = null;
  private prompts: string[] = [];
  /** Positive story matches outlive display-ring eviction, bounded by live
   * stories × active prompts. Resync/expiry/prompt removal prune them. */
  private promptMatches = new Map<string, Set<string>>();
  private retainedMatches = new Map<string, Set<string>>();
  private pendingMatches = new Map<string, WireRow>();
  private filterKey = "";
  private promptKey = "";
  seq = 0;
  lastWireAt: number | null = null;

  constructor(epochMs = Date.now()) {
    this.epochMs = epochMs;
  }

  snapshot(stories: Story[], links: Link[], recentWire: WireRow[], seq: number): WorkerInit {
    this.stories.clear();
    this.links.clear();
    this.wire.clear();
    this.dirtyStories.clear();
    this.dirtyGraphStories.clear();
    this.storyOps.clear();
    this.linkOps.clear();
    this.dirtyWire.clear();
    this.alerts = [];
    this.rippleRows.clear();
    this.freeStories = [];
    this.freeLinks = [];
    this.dimmedStories.clear();
    this.dimmedLinks.clear();
    this.storyDims.clear();
    this.linkDims.clear();
    this.lastWireAt = null;
    this.promptMatches.clear();
    this.retainedMatches.clear();
    this.pendingMatches.clear();
    this.seq = seq;
    stories.forEach((story) => this.stories.set(story.id, story));
    links.forEach((link) => this.links.set(linkKey(link.a, link.b), link));
    const data = buildLiveGlobe(stories, links, this.epochMs);
    this.init = data.init;
    this.growStories(this.init.storyCapacity);
    this.growLinks(this.init.linkCapacity);
    this.init.now = Date.now();
    this.entries = data.stories;
    this.storySlots = data.storyById;
    this.linkSlots = data.linkByKey;
    for (const row of recentWire) this.remember(row);
    this.dirtyWire.clear();
    this.topology = false;
    this.refreshMatches();
    // Copies are transferred; the worker keeps its authoritative typed arrays.
    return {
      type: "init",
      seq,
      init: structuredClone(this.init),
      summaries: structuredClone(this.entries),
      recentWire: [...this.wire.values()],
    };
  }

  setFilters(filters: WireFilters): boolean {
    const key = JSON.stringify({
      ...filters,
      categories: [...filters.categories].sort(),
      regions: [...filters.regions].sort(),
      sources: [...filters.sources].sort(),
      lang: [...filters.lang].sort(),
    });
    if (key === this.filterKey) return false;
    this.filters = filters;
    this.filterKey = key;
    if (this.init) this.refreshMatches();
    return true;
  }
  setPrompts(prompts: string[]): boolean {
    const key = JSON.stringify([...prompts].sort());
    if (key === this.promptKey) return false;
    this.prompts = [...prompts];
    this.promptKey = key;
    const active = new Set(prompts);
    for (const cache of [this.promptMatches, this.retainedMatches])
      for (const [id, matches] of cache) {
        for (const prompt of matches) if (!active.has(prompt)) matches.delete(prompt);
        if (!matches.size) cache.delete(id);
      }
    for (const row of this.wire.values())
      if (this.stories.has(row.storyId)) this.rememberMatches(row);
    if (this.init) this.refreshMatches();
    return true;
  }

  apply(delta: Delta): void {
    this.seq = Math.max(this.seq, Number(delta.seq));
    if (!this.init) return;
    switch (delta.body.case) {
      case "storyUpsert":
        this.upsertStory(delta.body.value);
        break;
      case "storyExpire":
        this.removeStory(delta.body.value);
        break;
      case "linkUpsert":
        this.upsertLink(delta.body.value);
        break;
      case "linkRemove":
        this.removeLink(linkKey(delta.body.value.a, delta.body.value.b));
        break;
      case "wire": {
        const row = this.remember(delta.body.value);
        this.rippleRows.set(rowKey(row), row);
        while (this.rippleRows.size > WIRE_LIMIT)
          this.rippleRows.delete(this.rippleRows.keys().next().value!);
        break;
      }
      case "alert":
        this.alerts.push(delta.body.value);
        if (this.alerts.length > 1000) this.alerts.shift();
        break;
    }
  }

  private remember(row: WireRow): WireRow {
    const key = rowKey(row);
    const old = this.wire.get(key);
    const merged = old ? mergeWireRow(old, row) : row;
    this.wire.set(key, merged);
    this.rememberMatches(merged);
    this.dirtyWire.set(key, merged);
    while (this.wire.size > WIRE_LIMIT) {
      const key = this.wire.keys().next().value!;
      const evicted = this.wire.get(key)!;
      if (this.stories.has(evicted.storyId))
        for (const prompt of this.prompts)
          if ((evicted.promptScores[prompt] ?? 0) >= 0.5) {
            let retained = this.retainedMatches.get(evicted.storyId);
            if (!retained) {
              retained = new Set();
              this.retainedMatches.set(evicted.storyId, retained);
            }
            retained.add(prompt);
          }
      this.wire.delete(key);
    }
    while (this.dirtyWire.size > WIRE_LIMIT)
      this.dirtyWire.delete(this.dirtyWire.keys().next().value!);
    this.lastWireAt = Math.max(this.lastWireAt ?? 0, Number(row.at));
    return merged;
  }

  private rememberMatches(row: WireRow): void {
    if (!this.stories.has(row.storyId)) {
      if (row.storyId) {
        this.pendingMatches.set(rowKey(row), row);
        while (this.pendingMatches.size > WIRE_LIMIT)
          this.pendingMatches.delete(this.pendingMatches.keys().next().value!);
      }
      return;
    }
    // A pending row can outlive the display ring while waiting for its story.
    // Promote it before consuming it so its positive evidence is retained.
    if (!this.wire.has(rowKey(row)))
      for (const prompt of this.prompts)
        if ((row.promptScores[prompt] ?? 0) >= 0.5) {
          let retained = this.retainedMatches.get(row.storyId);
          if (!retained) {
            retained = new Set();
            this.retainedMatches.set(row.storyId, retained);
          }
          retained.add(prompt);
        }
    // In-ring corrections can retract their contribution. Evicted positives
    // retain one aggregate bit per live story/prompt until expiry or reset.
    const rows = [...this.wire.values()].filter((candidate) => candidate.storyId === row.storyId);
    const matches = new Set<string>();
    for (const prompt of this.prompts)
      if (
        this.retainedMatches.get(row.storyId)?.has(prompt) ||
        rows.some((candidate) => (candidate.promptScores[prompt] ?? 0) >= 0.5)
      )
        matches.add(prompt);
    if (matches.size) this.promptMatches.set(row.storyId, matches);
    else this.promptMatches.delete(row.storyId);
  }

  private upsertStory(story: Story): void {
    const wasKnown = this.stories.has(story.id);
    this.stories.set(story.id, story);
    if (!wasKnown)
      for (const [key, row] of this.pendingMatches)
        if (row.storyId === story.id) {
          this.rememberMatches(row);
          this.pendingMatches.delete(key);
        }
    const location = storyLocation(story);
    if (!location) {
      this.removeStorySlot(story.id);
      return;
    }
    let index = this.storySlots.get(story.id);
    if (index === undefined) {
      index = this.freeStories.pop() ?? this.init.storyCount++;
      if (index >= this.init.storyCapacity) this.growStories(index + 1);
      this.storySlots.set(story.id, index);
      this.topology = true;
    }
    const old = this.entries[index];
    const category = Math.max(0, Math.min(8, story.category));
    const color = [...categoryColor(category), 255] as LiveStory["color"];
    const entry: LiveStory = {
      index,
      lng: location.lng,
      lat: location.lat,
      place: location.place,
      category,
      color,
      story: {
        id: story.id,
        title: story.title,
        category: story.category,
        primary: story.primary,
        sourceCount: story.sourceCount,
        heat: story.heat,
        firstSeen: story.firstSeen,
        updatedAt: story.updatedAt,
        locations: story.locations.map(({ name, lat, lon, role, precision }) => ({
          name,
          lat,
          lon,
          role,
          precision,
        })),
      },
      parties: story.locations
        .filter((place) => place.role === 2)
        .slice(0, 6)
        .map((place) => ({ lng: place.lon, lat: place.lat, label: place.name })),
      degree: old?.degree ?? 0,
      strongestLink: old?.strongestLink ?? 0,
    };
    this.entries[index] = entry;
    const [x, y] = lngLatToMercator(entry.lng, entry.lat);
    const [hx, lx] = splitMercator(x);
    const [hy, ly] = splitMercator(y);
    const mercHi = new Float32Array([hx, hy]);
    const mercLo = new Float32Array([lx, ly]);
    const op: StoryPatch = {
      op: "add",
      reset: !old || this.storyOps.get(index)?.reset === true,
      index,
      lng: entry.lng,
      lat: entry.lat,
      category,
      sources: story.sourceCount,
      heat: story.heat,
      updatedAt: (Number(story.updatedAt) * 1000 - this.epochMs) / 1000,
      title: story.title,
      place: entry.place,
      color,
      summary: entry,
      mercHi,
      mercLo,
    };
    this.dirtyStories.add(index);
    if (
      !old ||
      old.lng !== entry.lng ||
      old.lat !== entry.lat ||
      old.category !== entry.category ||
      old.story.title !== entry.story.title ||
      JSON.stringify(old.story.locations) !== JSON.stringify(entry.story.locations)
    )
      this.storyOps.set(index, op);
    else if (this.storyOps.has(index)) {
      const previous = this.storyOps.get(index)!;
      this.storyOps.set(index, { ...op, reset: previous.reset });
    }
    const s = this.init.stories;
    s.lng[index] = entry.lng;
    s.lat[index] = entry.lat;
    s.mercHi.set(mercHi, index * 2);
    s.mercLo.set(mercLo, index * 2);
    s.category[index] = category;
    s.sources[index] = story.sourceCount;
    s.heat[index] = story.heat;
    s.updatedAt[index] = op.updatedAt;
    s.color.set(color, index * 4);
    s.title[index] = story.title;
    s.place[index] = entry.place;
    // Re-encode only incident arcs when a canonical position changed.
    if (!old || old.lng !== entry.lng || old.lat !== entry.lat) {
      for (const link of this.links.values())
        if (link.a === story.id || link.b === story.id) this.upsertLink(link, true);
    }
  }

  private removeStory(id: string): void {
    this.stories.delete(id);
    this.promptMatches.delete(id);
    this.retainedMatches.delete(id);
    for (const [key, row] of this.pendingMatches)
      if (row.storyId === id) this.pendingMatches.delete(key);
    this.removeStorySlot(id);
  }
  private removeStorySlot(id: string): void {
    const index = this.storySlots.get(id);
    if (index === undefined) return;
    for (const [key, link] of this.links) if (link.a === id || link.b === id) this.removeLink(key);
    const old = this.entries[index]!;
    this.storyOps.set(index, {
      op: "remove",
      index,
      lng: old.lng,
      lat: old.lat,
      category: old.category,
      sources: 0,
      heat: 0,
      updatedAt: 0,
      title: "",
      place: "",
      color: [0, 0, 0, 0],
    });
    this.storySlots.delete(id);
    this.dirtyStories.delete(index);
    delete this.entries[index];
    this.freeStories.push(index);
    this.dimmedStories.delete(index);
    this.storyDims.set(index, 0);
    this.topology = true;
  }
  private upsertLink(link: Link, moved = false): void {
    const key = linkKey(link.a, link.b);
    this.links.set(key, link);
    const a = this.storySlots.get(link.a),
      b = this.storySlots.get(link.b);
    if (a === undefined || b === undefined || a === b) return;
    let index = this.linkSlots.get(key);
    const isNew = index === undefined;
    if (index === undefined) {
      index = this.freeLinks.pop() ?? this.init.linkCount++;
      if (index >= this.init.linkCapacity) this.growLinks(index + 1);
      this.linkSlots.set(key, index);
      this.topology = true;
    }
    const previous = this.linkOps.get(index);
    const l = this.init.links;
    if (!isNew) {
      this.dirtyGraphStories.add(l.a[index]!);
      this.dirtyGraphStories.add(l.b[index]!);
    }
    this.dirtyGraphStories.add(a);
    this.dirtyGraphStories.add(b);
    const geometryChanged =
      isNew || moved || l.a[index] !== a || l.b[index] !== b || l.type[index] !== link.type;
    const arcs = geometryChanged
      ? buildArcArrays([this.entries[a]!, this.entries[b]!], [0], [1], [link.type])
      : previous?.arcs;
    arcs?.link.fill(index);
    const distanceKm = arcs ? arcs.length[0]! / 1000 : (l.distanceKm?.[index] ?? 0);
    this.linkOps.set(index, {
      op: "add",
      reset: isNew || previous?.reset === true,
      index,
      a,
      b,
      type: link.type,
      strength: link.strength,
      arcs,
      direction: link.dir,
      confidence: link.confidence,
      distanceKm,
      unverified: link.unverified,
    });
    l.a[index] = a;
    l.b[index] = b;
    l.type[index] = link.type;
    l.strength[index] = link.strength;
    l.width[index] = encodeArc(link.type, link.strength).widthPx;
    if (l.direction) l.direction[index] = link.dir;
    if (l.confidence) l.confidence[index] = link.confidence;
    if (l.distanceKm) l.distanceKm[index] = distanceKm;
    if (l.unverified) l.unverified[index] = link.unverified ? 1 : 0;
  }
  private removeLink(key: string): void {
    this.links.delete(key);
    const index = this.linkSlots.get(key);
    if (index === undefined) return;
    const l = this.init.links;
    this.dirtyGraphStories.add(l.a[index]!);
    this.dirtyGraphStories.add(l.b[index]!);
    this.linkOps.set(index, {
      op: "remove",
      index,
      a: l.a[index]!,
      b: l.b[index]!,
      type: l.type[index]!,
      strength: 0,
    });
    this.linkSlots.delete(key);
    this.freeLinks.push(index);
    l.strength[index] = 0;
    this.dimmedLinks.delete(index);
    this.linkDims.set(index, 0);
    this.topology = true;
  }
  private growStories(required: number): void {
    const capacity =
      required > this.init.storyCapacity
        ? capacityFor(this.init.storyCapacity, required)
        : this.init.storyCapacity;
    this.init.storyCapacity = capacity;
    const s = this.init.stories;
    for (const key of Object.keys(s) as Array<keyof typeof s>) {
      const value = s[key];
      const stride = ["mercHi", "mercLo"].includes(key)
        ? 2
        : key === "color"
          ? 4
          : key.startsWith("party") && key !== "partyCount"
            ? 6
            : 1;
      if (ArrayBuffer.isView(value))
        (s as unknown as Record<string, Numeric>)[key] = growArray(value, capacity * stride);
    }
  }
  private growLinks(required: number): void {
    const capacity =
      required > this.init.linkCapacity
        ? capacityFor(this.init.linkCapacity, required)
        : this.init.linkCapacity;
    this.init.linkCapacity = capacity;
    const l = this.init.links;
    for (const key of Object.keys(l) as Array<keyof typeof l>) {
      const value = l[key];
      if (value)
        (l as unknown as Record<string, Numeric>)[key] = growArray(
          value,
          capacity * (key === "color" ? 4 : 1),
        );
    }
  }
  private adjacency(): AdjacencyArrays {
    const counts = new Uint32Array(this.init.storyCapacity + 1);
    for (const index of this.linkSlots.values()) {
      counts[this.init.links.a[index]!]!++;
      counts[this.init.links.b[index]!]!++;
    }
    const adjStart = new Uint32Array(counts.length);
    for (let i = 0; i < counts.length - 1; i++) adjStart[i + 1] = adjStart[i]! + counts[i]!;
    const adjList = new Uint16Array(adjStart[adjStart.length - 1]!);
    const adjLink = new Uint16Array(adjList.length);
    const cursor = adjStart.slice();
    for (const index of this.linkSlots.values()) {
      const a = this.init.links.a[index]!,
        b = this.init.links.b[index]!;
      const x = cursor[a]!,
        y = cursor[b]!;
      adjList[x] = b;
      adjLink[x] = index;
      adjList[y] = a;
      adjLink[y] = index;
      cursor[a] = x + 1;
      cursor[b] = y + 1;
    }
    for (const entry of this.entries)
      if (entry) {
        entry.degree = counts[entry.index]!;
        entry.strongestLink = 0;
      }
    for (const index of this.linkSlots.values())
      for (const i of [this.init.links.a[index]!, this.init.links.b[index]!]) {
        const entry = this.entries[i]!;
        entry.strongestLink = Math.max(entry.strongestLink, this.init.links.strength[index]!);
      }
    this.init.adjacency = { adjStart, adjList, adjLink };
    return this.init.adjacency;
  }
  private refreshGraphStats(): void {
    for (const i of this.dirtyGraphStories) {
      const entry = this.entries[i];
      if (entry) {
        entry.degree = 0;
        entry.strongestLink = 0;
        this.dirtyStories.add(i);
      }
    }
    for (const index of this.linkSlots.values())
      for (const i of [this.init.links.a[index]!, this.init.links.b[index]!]) {
        if (!this.dirtyGraphStories.has(i)) continue;
        const entry = this.entries[i];
        if (entry) {
          entry.degree++;
          entry.strongestLink = Math.max(entry.strongestLink, this.init.links.strength[index]!);
        }
      }
  }

  private refreshMatches(): void {
    const f = this.filters;
    for (const entry of this.entries) {
      if (!entry) continue;
      const story = entry.story;
      let match =
        (!this.prompts.length || Boolean(this.promptMatches.get(story.id)?.size)) &&
        (!f ||
          ((!f.categories.length || f.categories.includes(story.category)) &&
            story.sourceCount >= f.minSources));
      if (match && f?.regions.length)
        match = f.regions.some((region) =>
          story.locations.some((p) => p.name.toLowerCase().includes(region.toLowerCase())),
        );
      if (match && f && f.radiusKm > 0) {
        const rad = Math.PI / 180;
        const dlat = (entry.lat - f.lat) * rad,
          dlon = (entry.lng - f.lon) * rad;
        const a =
          Math.sin(dlat / 2) ** 2 +
          Math.cos(entry.lat * rad) * Math.cos(f.lat * rad) * Math.sin(dlon / 2) ** 2;
        match = 2 * 6371 * Math.asin(Math.min(1, Math.sqrt(a))) <= f.radiusKm;
      }
      const dim = match ? 0 : 1;
      if (this.dimmedStories.has(entry.index) !== Boolean(dim)) {
        this.storyDims.set(entry.index, dim);
        if (dim) this.dimmedStories.add(entry.index);
        else this.dimmedStories.delete(entry.index);
      }
    }
    for (const index of this.linkSlots.values()) {
      const dim =
        this.dimmedStories.has(this.init.links.a[index]!) ||
        this.dimmedStories.has(this.init.links.b[index]!);
      if (this.dimmedLinks.has(index) !== dim) {
        this.linkDims.set(index, dim ? 1 : 0);
        if (dim) this.dimmedLinks.add(index);
        else this.dimmedLinks.delete(index);
      }
    }
  }
  flush(now = Date.now()): WorkerPatch | null {
    if (!this.init) return null;
    if (
      !this.storyOps.size &&
      !this.dirtyStories.size &&
      !this.linkOps.size &&
      !this.dirtyWire.size &&
      !this.storyDims.size &&
      !this.linkDims.size &&
      !this.alerts.length
    )
      return null;
    this.refreshMatches();
    const adjacency = this.topology ? this.adjacency() : undefined;
    this.refreshGraphStats();
    const top = this.entries
      .filter(Boolean)
      .sort(
        (a, b) =>
          b.story.heat * 2 +
          Math.min(10, b.degree) * 0.15 +
          Math.min(30, b.story.sourceCount) * 0.01 -
          (a.story.heat * 2 +
            Math.min(10, a.degree) * 0.15 +
            Math.min(30, a.story.sourceCount) * 0.01),
      )
      .slice(0, 300);
    const stream = emptyStream(now);
    const changed = [...this.dirtyStories];
    stream.storyIndex = Uint16Array.from(changed);
    stream.sources = Uint16Array.from(changed, (i) => this.init.stories.sources[i]!);
    stream.heat = Float32Array.from(changed, (i) => this.init.stories.heat[i]!);
    stream.updatedAt = Float32Array.from(changed, (i) => this.init.stories.updatedAt[i]!);
    for (const row of this.rippleRows.values()) {
      const index = this.storySlots.get(row.storyId);
      if (index === undefined || this.dimmedStories.has(index)) continue;
      const entry = this.entries[index]!;
      for (const slot of this.ripples.spawn({
        lng: entry.lng,
        lat: entry.lat,
        category: entry.category,
        spawnAt: (now - this.epochMs) / 1000,
        storyIndex: index,
      })) {
        const [x, y] = lngLatToMercator(slot.lng, slot.lat);
        const [hx, lx] = splitMercator(x),
          [hy, ly] = splitMercator(y);
        stream.ripples.push({ ...slot, mercHi: [hx, hy], mercLo: [lx, ly] });
      }
    }
    const patch: WorkerPatch = {
      type: "patch",
      seq: this.seq,
      storyCapacity: this.init.storyCapacity,
      linkCapacity: this.init.linkCapacity,
      storyCount: this.storySlots.size,
      linkCount: this.linkSlots.size,
      stories: structuredClone([...this.storyOps.values()]),
      summaries: structuredClone([...this.dirtyStories].map((i) => this.entries[i]!)),
      links: structuredClone([...this.linkOps.values()]),
      stream,
      storyStates: range(this.storyDims),
      linkStates: range(this.linkDims),
      ...(adjacency ? { adjacency: structuredClone(adjacency) } : {}),
      topStories: Uint16Array.from(top.map((entry) => entry.index)),
      wire: [...this.dirtyWire.values()],
      alerts: this.alerts,
      bounds: {
        stories: this.stories.size,
        links: this.links.size,
        wire: this.wire.size,
        pendingWire: this.dirtyWire.size,
      },
    };
    this.dirtyStories.clear();
    this.dirtyGraphStories.clear();
    this.storyOps.clear();
    this.linkOps.clear();
    this.dirtyWire.clear();
    this.rippleRows.clear();
    this.alerts = [];
    this.storyDims.clear();
    this.linkDims.clear();
    this.topology = false;
    return patch;
  }
}
