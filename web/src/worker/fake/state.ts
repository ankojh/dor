import {
  ARC_MIN_STRENGTH,
  CATEGORY_CLIMATE,
  CATEGORY_CONFLICT,
  CATEGORY_DISASTER,
  CATEGORY_ECONOMY,
  CATEGORY_POLITICS,
  CATEGORY_TECH,
  DEFAULT_STRENGTH_FLOOR,
  FOCUS_AMBIENT,
  FOCUS_FOCUSED,
  FOCUS_LINKED,
  FOCUS_SECOND,
  LINK_CAUSAL,
  LINK_REACTION,
  LINK_THEMATIC,
  categoryColor,
  encodeArc,
  linkColor,
} from "../../globe/encoding";
import {
  angularDistance,
  arcElevation,
  EARTH_RADIUS_M,
  greatCirclePoint,
  lngLatToMercator,
  splitMercator,
  type Vec2,
} from "../../globe/mercator";
import {
  RIPPLE_LIFETIME_SECONDS,
  RippleRingBuffer,
  type RippleSlotState,
} from "../../globe/ripples";
import { CITIES, COUNTRY_POINTS, type City } from "./places";
import { fakeTitle } from "./titles";
import {
  ARC_SEGMENTS,
  ARTICLES_PER_SECOND,
  DEFAULT_LINK_COUNT,
  DEFAULT_STORY_COUNT,
  RIPPLE_CAPACITY,
  type ArcArrays,
  type FakeInit,
  type FocusLinkChange,
  type FocusPatch,
  type FocusStoryChange,
  type HudFocus,
  type HudStory,
  type LinkArrays,
  type LinkOp,
  type RippleArrays,
  type RippleUpdate,
  type StoryArrays,
  type StoryOp,
  type StreamPatch,
  type TetherRing,
  type TetherSegment,
} from "./protocol";

type WorldOptions = {
  storyCount?: number;
  linkCount?: number;
  seed?: number;
  now?: number;
};

type SeedStory = {
  key?: string;
  city: string;
  category: number;
  title?: string;
  sources?: number;
  heat?: number;
  jitter?: number;
  parties?: string[];
};

const SEED_STORIES: SeedStory[] = [
  {
    key: "rot",
    city: "Rotterdam",
    category: CATEGORY_ECONOMY,
    title: "Port strike extends, container backlog grows",
    sources: 28,
    heat: 0.96,
    parties: ["Netherlands", "Belgium"],
  },
  {
    key: "sin",
    city: "Singapore",
    category: CATEGORY_ECONOMY,
    title: "Asia–Europe freight rates climb for a second week",
    sources: 22,
    heat: 0.9,
  },
  {
    key: "sue",
    city: "Suez",
    category: CATEGORY_ECONOMY,
    title: "Canal transits slow amid congestion",
    sources: 18,
    heat: 0.86,
  },
  {
    key: "dxb",
    city: "Dubai",
    category: CATEGORY_ECONOMY,
    title: "Rerouted cargo lifts Gulf port volumes",
    sources: 14,
    heat: 0.8,
  },
  {
    key: "lon",
    city: "London",
    category: CATEGORY_ECONOMY,
    title: "Logistics firms warn on holiday-season stock",
    sources: 12,
    heat: 0.74,
  },
  {
    key: "lag",
    city: "Lagos",
    category: CATEGORY_ECONOMY,
    title: "Fuel import costs rise with freight",
    sources: 10,
    heat: 0.72,
  },
  {
    key: "fra",
    city: "Frankfurt",
    category: CATEGORY_ECONOMY,
    title: "Central bank holds rates, flags supply risks",
    sources: 11,
    heat: 0.7,
  },
  {
    key: "ant",
    city: "Antwerp",
    category: CATEGORY_ECONOMY,
    title: "Shippers reroute cargo to Antwerp",
    sources: 9,
    heat: 0.68,
  },
  {
    key: "deal",
    city: "Geneva",
    category: CATEGORY_POLITICS,
    title: "US and China agree trade deal after Geneva talks",
    sources: 31,
    heat: 1,
    parties: ["United States", "China"],
  },
  {
    key: "mkt",
    city: "New York",
    category: CATEGORY_ECONOMY,
    title: "Stocks rally on trade deal",
    sources: 26,
    heat: 0.95,
  },
  {
    key: "ship",
    city: "Los Angeles",
    category: CATEGORY_ECONOMY,
    title: "Exporters rush to book transpacific freight",
    sources: 19,
    heat: 0.88,
  },
  {
    key: "soy",
    city: "Chicago",
    category: CATEGORY_ECONOMY,
    title: "Soybean futures jump",
    sources: 15,
    heat: 0.82,
  },
  {
    key: "eu",
    city: "Brussels",
    category: CATEGORY_POLITICS,
    title: "EU weighs its own talks",
    sources: 14,
    heat: 0.78,
  },
  {
    key: "tour",
    city: "Bangkok",
    category: CATEGORY_ECONOMY,
    title: "Chinese tourist arrivals expected to rise",
    sources: 9,
    heat: 0.62,
  },
  {
    key: "cny",
    city: "Hong Kong",
    category: CATEGORY_ECONOMY,
    title: "Retailers prepare for New Year spending",
    sources: 8,
    heat: 0.58,
  },
  {
    key: "was",
    city: "Washington",
    category: CATEGORY_POLITICS,
    title: "Senate passes chip export bill",
    sources: 24,
    heat: 0.94,
    parties: ["United States"],
  },
  {
    key: "tpe",
    city: "Taipei",
    category: CATEGORY_TECH,
    title: "Chipmakers weigh response to export rules",
    sources: 21,
    heat: 0.91,
    parties: ["China", "United States"],
  },
  {
    key: "sfo",
    city: "San Francisco",
    category: CATEGORY_TECH,
    title: "Cloud providers warn of GPU shortages",
    sources: 17,
    heat: 0.85,
  },
  {
    key: "sel",
    city: "Seoul",
    category: CATEGORY_TECH,
    title: "Memory chip prices tick up on tight supply",
    sources: 12,
    heat: 0.76,
  },
  {
    key: "blr",
    city: "Bengaluru",
    category: CATEGORY_TECH,
    title: "AI chip startup raises new round",
    sources: 8,
    heat: 0.6,
  },
  {
    key: "tyo",
    city: "Tokyo",
    category: CATEGORY_DISASTER,
    title: "Strong offshore quake; tsunami advisory lifted",
    sources: 22,
    heat: 0.92,
    parties: ["Japan"],
  },
  {
    key: "jkt",
    city: "Jakarta",
    category: CATEGORY_DISASTER,
    title: "Volcanic ash grounds regional flights",
    sources: 11,
    heat: 0.72,
  },
  {
    key: "sao",
    city: "São Paulo",
    category: CATEGORY_CLIMATE,
    title: "Frost hits coffee belt, futures jump",
    sources: 13,
    heat: 0.77,
  },
  {
    key: "nbo",
    city: "Nairobi",
    category: CATEGORY_CLIMATE,
    title: "Climate talks circulate draft text",
    sources: 10,
    heat: 0.69,
  },
  {
    key: "kyv",
    city: "Kyiv",
    category: CATEGORY_CONFLICT,
    title: "Overnight strikes hit regional energy grid",
    sources: 19,
    heat: 0.9,
    parties: ["Ukraine", "Russia"],
  },
  {
    key: "kha",
    city: "Khartoum",
    category: CATEGORY_CONFLICT,
    title: "Ceasefire talks scheduled for next week",
    sources: 9,
    heat: 0.66,
  },
  {
    key: "syd",
    city: "Sydney",
    category: CATEGORY_CLIMATE,
    title: "Heatwave warnings across the east coast",
    sources: 8,
    heat: 0.6,
  },
  {
    key: "mex",
    city: "Mexico City",
    category: CATEGORY_POLITICS,
    title: "Nearshoring plants expand in the north",
    sources: 9,
    heat: 0.64,
  },
  {
    key: "lim",
    city: "Lima",
    category: CATEGORY_CLIMATE,
    title: "Drought cuts river shipping",
    sources: 7,
    heat: 0.56,
  },
  {
    key: "del",
    city: "New Delhi",
    category: CATEGORY_CLIMATE,
    title: "Monsoon delay worries farmers",
    sources: 12,
    heat: 0.71,
  },
  {
    key: "tor",
    city: "Toronto",
    category: CATEGORY_POLITICS,
    title: "Officials weigh response to chip bill",
    sources: 8,
    heat: 0.58,
  },
];

const SEED_LINKS: Array<[string, string, number, number]> = [
  ["rot", "sin", LINK_CAUSAL, 0.86],
  ["sue", "sin", LINK_CAUSAL, 0.74],
  ["sue", "rot", LINK_CAUSAL, 0.7],
  ["rot", "fra", LINK_CAUSAL, 0.52],
  ["rot", "lag", LINK_CAUSAL, 0.61],
  ["sue", "dxb", LINK_CAUSAL, 0.66],
  ["rot", "lon", LINK_THEMATIC, 0.45],
  ["rot", "ant", LINK_CAUSAL, 0.58],
  ["rot", "dxb", LINK_CAUSAL, 0.64],
  ["ship", "sin", LINK_CAUSAL, 0.59],
  ["deal", "mkt", LINK_CAUSAL, 0.88],
  ["deal", "ship", LINK_CAUSAL, 0.81],
  ["deal", "soy", LINK_CAUSAL, 0.7],
  ["deal", "eu", LINK_REACTION, 0.55],
  ["deal", "tour", LINK_THEMATIC, 0.42],
  ["deal", "cny", LINK_THEMATIC, 0.31],
  ["was", "tpe", LINK_REACTION, 0.9],
  ["was", "eu", LINK_REACTION, 0.78],
  ["tpe", "sfo", LINK_CAUSAL, 0.63],
  ["was", "sel", LINK_REACTION, 0.7],
  ["tpe", "blr", LINK_THEMATIC, 0.4],
  ["sao", "nbo", LINK_THEMATIC, 0.38],
  ["nbo", "del", LINK_THEMATIC, 0.42],
  ["sao", "lim", LINK_THEMATIC, 0.35],
  ["tyo", "jkt", LINK_THEMATIC, 0.33],
  ["kyv", "fra", LINK_CAUSAL, 0.58],
  ["kyv", "eu", LINK_REACTION, 0.64],
  ["was", "mex", LINK_THEMATIC, 0.45],
  ["kha", "sue", LINK_THEMATIC, 0.36],
  ["del", "syd", LINK_THEMATIC, 0.32],
  ["was", "tor", LINK_REACTION, 0.55],
  ["tyo", "sfo", LINK_CAUSAL, 0.57],
  ["mkt", "lon", LINK_REACTION, 0.62],
  ["sao", "mkt", LINK_THEMATIC, 0.41],
  ["kyv", "was", LINK_REACTION, 0.67],
];

/** Deterministic PRNG so perf runs and screenshots are comparable. */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function cityByName(name: string): City {
  const city = CITIES.find((entry) => entry.name === name);
  if (!city) throw new Error(`unknown city ${name}`);
  return city;
}

function countryPoint(label: string): { lat: number; lon: number } {
  const point = COUNTRY_POINTS.find((entry) => entry.label === label);
  return point ?? COUNTRY_POINTS[0]!;
}

function toRGBA(
  rgb: readonly [number, number, number],
  alpha = 1,
): [number, number, number, number] {
  return [rgb[0], rgb[1], rgb[2], Math.round(Math.min(1, Math.max(0, alpha)) * 255)];
}

export class FakeWorld {
  readonly storyCount: number;
  readonly linkCount: number;
  readonly storyCapacity: number;
  readonly linkCapacity: number;

  private readonly rng: () => number;
  private readonly storyLng: Float64Array;
  private readonly storyLat: Float64Array;
  private readonly storyMercX: Float64Array;
  private readonly storyMercY: Float64Array;
  private readonly storyCategory: Uint8Array;
  private readonly storySources: Uint16Array;
  private readonly storyHeat: Float32Array;
  private readonly storyUpdatedAt: Float32Array;
  private readonly storyFocus: Uint8Array;
  private readonly storyPrevFocus: Uint8Array;
  private readonly storyFocusAt: Float32Array;
  private readonly storyColor: Uint8Array;
  private readonly storyActive: Uint8Array;
  private readonly storyTitle: string[];
  private readonly storyPlace: string[];
  private readonly partyCount: Uint8Array;
  private readonly partyLng: Float32Array;
  private readonly partyLat: Float32Array;
  private readonly partyLabel: string[];

  private readonly linkA: Uint16Array;
  private readonly linkB: Uint16Array;
  private readonly linkType: Uint8Array;
  private readonly linkStrength: Float32Array;
  private readonly linkFocus: Uint8Array;
  private readonly linkPrevFocus: Uint8Array;
  private readonly linkFocusAt: Float32Array;
  private readonly linkColor: Uint8Array;
  private readonly linkWidth: Float32Array;
  private readonly linkActive: Uint8Array;

  private readonly adjStart: Uint32Array;
  private readonly adjList: Uint16Array;
  private readonly adjLink: Uint16Array;

  private pickPrefix!: Float64Array;
  private pickTotal = 0;

  private readonly ripple: RippleRingBuffer;
  private readonly rippleDirty = new Map<number, RippleSlotState>();
  private rippleBudget = 0;
  private focusThread = new Set<number>();

  private readonly dirtyStoryFlags: Uint8Array;
  private readonly dirtyStoryList: number[] = [];
  private readonly dirtyLinkFlags: Uint8Array;
  private readonly dirtyLinkList: number[] = [];

  private readonly changeStamp: Uint32Array;
  private readonly changeOriginal: Uint8Array;
  private readonly changeFinal: Uint8Array;
  private readonly linkChangeStamp: Uint32Array;
  private readonly linkChangeOriginal: Uint8Array;
  private readonly linkChangeFinal: Uint8Array;
  private stamp = 0;

  private focusIndex: number | null = null;
  private lastThreadStories: number[] = [];
  private lastThreadLinks: number[] = [];
  private accumulator = 0;
  private enabled = true;

  readonly topStories: Uint16Array;
  private readonly hudTop: HudStory[];

  readonly epochMs: number;

  constructor(options: WorldOptions = {}) {
    this.storyCount = options.storyCount ?? DEFAULT_STORY_COUNT;
    this.linkCount = options.linkCount ?? DEFAULT_LINK_COUNT;
    this.storyCapacity =
      this.storyCount + Math.max(32, Math.min(1024, Math.ceil(this.storyCount * 0.05)));
    this.linkCapacity =
      this.linkCount + Math.max(32, Math.min(512, Math.ceil(this.linkCount * 0.1)));
    this.rng = mulberry32(options.seed ?? 0xd02);
    const nowMs = options.now ?? Date.now();
    this.epochMs = nowMs;

    const n = this.storyCount;
    const l = this.linkCount;
    const sc = this.storyCapacity;
    const lc = this.linkCapacity;
    this.storyLng = new Float64Array(sc);
    this.storyLat = new Float64Array(sc);
    this.storyMercX = new Float64Array(sc);
    this.storyMercY = new Float64Array(sc);
    this.storyCategory = new Uint8Array(sc);
    this.storySources = new Uint16Array(sc);
    this.storyHeat = new Float32Array(sc);
    this.storyUpdatedAt = new Float32Array(sc);
    this.storyFocus = new Uint8Array(sc);
    this.storyPrevFocus = new Uint8Array(sc);
    this.storyFocusAt = new Float32Array(sc);
    this.storyColor = new Uint8Array(sc * 4);
    this.storyTitle = new Array<string>(sc).fill("");
    this.storyPlace = new Array<string>(sc).fill("");
    this.partyCount = new Uint8Array(sc);
    this.partyLng = new Float32Array(sc * 3);
    this.partyLat = new Float32Array(sc * 3);
    this.partyLabel = new Array<string>(sc * 3).fill("");
    this.storyActive = new Uint8Array(sc);
    this.storyActive.fill(1, 0, n);

    this.linkA = new Uint16Array(lc);
    this.linkB = new Uint16Array(lc);
    this.linkType = new Uint8Array(lc);
    this.linkStrength = new Float32Array(lc);
    this.linkFocus = new Uint8Array(lc);
    this.linkPrevFocus = new Uint8Array(lc);
    this.linkFocusAt = new Float32Array(lc);
    this.linkColor = new Uint8Array(lc * 4);
    this.linkWidth = new Float32Array(lc);
    this.linkActive = new Uint8Array(lc);
    this.linkActive.fill(1, 0, l);

    this.adjStart = new Uint32Array(sc + 1);
    this.adjList = new Uint16Array(lc * 2);
    this.adjLink = new Uint16Array(lc * 2);

    this.dirtyStoryFlags = new Uint8Array(sc);
    this.dirtyLinkFlags = new Uint8Array(lc);
    this.changeStamp = new Uint32Array(sc);
    this.changeOriginal = new Uint8Array(sc);
    this.changeFinal = new Uint8Array(sc);
    this.linkChangeStamp = new Uint32Array(lc);
    this.linkChangeOriginal = new Uint8Array(lc);
    this.linkChangeFinal = new Uint8Array(lc);

    this.ripple = new RippleRingBuffer(RIPPLE_CAPACITY);

    const seedKeys = new Map<string, number>();
    this.writeSeedStories(seedKeys, (nowMs - this.epochMs) / 1000);
    this.writeSeedLinks(seedKeys);
    this.writeRandomLinks();
    this.buildAdjacency();
    this.fillColors();
    this.buildPickPrefix();

    this.topStories = this.buildTopStories();
    this.hudTop = Array.from(this.topStories.slice(0, 24), (index) => this.hudStory(index));

    // A few live ripples so the boot frame is not empty.
    for (let i = 0; i < 4; i++) {
      const storyIndex = this.topStories[i] ?? 0;
      const slots = this.ripple.spawn({
        lng: this.storyLng[storyIndex]!,
        lat: this.storyLat[storyIndex]!,
        category: this.storyCategory[storyIndex]!,
        spawnAt: (nowMs - this.epochMs) / 1000 - 0.15 - i * 0.22,
        intensity: 0.5,
        storyIndex,
      });
      for (const slot of slots) this.rippleDirty.set(slot.slot, slot);
    }
  }

  setStreamEnabled(enabled: boolean): void {
    this.enabled = enabled;
  }

  /** Find the lowest free runtime slot, optionally requiring a requested one. */
  private freeStorySlot(requested?: number): number {
    if (requested !== undefined) {
      return requested >= 0 && requested < this.storyCapacity && this.storyActive[requested] === 0
        ? requested
        : -1;
    }
    for (let index = 0; index < this.storyCapacity; index++) {
      if (this.storyActive[index] === 0) return index;
    }
    return -1;
  }

  private freeLinkSlot(requested?: number): number {
    if (requested !== undefined) {
      return requested >= 0 && requested < this.linkCapacity && this.linkActive[requested] === 0
        ? requested
        : -1;
    }
    for (let index = 0; index < this.linkCapacity; index++) {
      if (this.linkActive[index] === 0) return index;
    }
    return -1;
  }

  /**
   * Runtime insert used by the spike's structural-change regression and by
   * M2·4. The worker owns the index; the main thread applies the returned op
   * to its mirror, geometry buffers and textures.
   */
  insertLink(
    a: number,
    b: number,
    type: number,
    strength: number,
  ): LinkOp | null {
    if (
      a === b ||
      a < 0 ||
      b < 0 ||
      a >= this.storyCapacity ||
      b >= this.storyCapacity ||
      this.storyActive[a] === 0 ||
      this.storyActive[b] === 0
    ) {
      return null;
    }
    const index = this.freeLinkSlot();
    if (index < 0) return null;
    const s = Math.min(1, Math.max(0, strength));
    this.linkA[index] = a;
    this.linkB[index] = b;
    this.linkType[index] = type;
    this.linkStrength[index] = s;
    this.linkFocus[index] = FOCUS_AMBIENT;
    this.linkPrevFocus[index] = FOCUS_AMBIENT;
    this.linkFocusAt[index] = 0;
    this.linkActive[index] = 1;
    this.applyLinkStyle(index);
    this.buildAdjacency();
    this.buildPickPrefix();
    return { op: "add", index, a, b, type, strength: s };
  }

  removeLink(index: number): LinkOp | null {
    if (index < 0 || index >= this.linkCapacity || this.linkActive[index] === 0) return null;
    const a = this.linkA[index]!;
    const b = this.linkB[index]!;
    const type = this.linkType[index]!;
    const strength = this.linkStrength[index]!;
    this.linkActive[index] = 0;
    this.linkA[index] = 0;
    this.linkB[index] = 0;
    this.linkStrength[index] = 0;
    this.linkFocus[index] = FOCUS_AMBIENT;
    this.linkPrevFocus[index] = FOCUS_AMBIENT;
    this.linkFocusAt[index] = 0;
    this.linkColor.fill(0, index * 4, index * 4 + 4);
    this.linkWidth[index] = 0;
    this.buildAdjacency();
    this.buildPickPrefix();
    return { op: "remove", index, a, b, type, strength };
  }

  insertStory(fields: {
    lng: number;
    lat: number;
    category: number;
    sources: number;
    heat: number;
    title: string;
    place: string;
  }): StoryOp | null {
    const index = this.freeStorySlot();
    if (index < 0) return null;
    const nowSec = (Date.now() - this.epochMs) / 1000;
    this.writeStoryAt(index, { ...fields, updatedAt: nowSec });
    this.storyActive[index] = 1;
    this.partyCount[index] = 0;
    const rgba = toRGBA(categoryColor(fields.category));
    this.storyColor.set(rgba, index * 4);
    this.buildPickPrefix();
    return {
      op: "add",
      index,
      lng: fields.lng,
      lat: fields.lat,
      category: fields.category,
      sources: this.storySources[index]!,
      heat: this.storyHeat[index]!,
      updatedAt: this.storyUpdatedAt[index]!,
      title: fields.title,
      place: fields.place,
      color: rgba as [number, number, number, number],
    };
  }

  removeStory(index: number): StoryOp | null {
    if (index < 0 || index >= this.storyCapacity || this.storyActive[index] === 0) return null;
    const links: LinkOp[] = [];
    for (let link = 0; link < this.linkCapacity; link++) {
      if (this.linkActive[link] === 0) continue;
      if (this.linkA[link] === index || this.linkB[link] === index) {
        const removed = this.removeLink(link);
        if (removed) links.push(removed);
      }
    }
    const op: StoryOp = {
      op: "remove",
      index,
      lng: this.storyLng[index]!,
      lat: this.storyLat[index]!,
      category: this.storyCategory[index]!,
      sources: 0,
      heat: 0,
      updatedAt: 0,
      title: "",
      place: "",
      color: [0, 0, 0, 0],
      links,
    };
    this.storyActive[index] = 0;
    this.storySources[index] = 0;
    this.storyHeat[index] = 0;
    this.storyFocus[index] = FOCUS_AMBIENT;
    this.storyPrevFocus[index] = FOCUS_AMBIENT;
    this.storyColor.fill(0, index * 4, index * 4 + 4);
    this.storyTitle[index] = "";
    this.storyPlace[index] = "";
    this.partyCount[index] = 0;
    this.buildAdjacency();
    this.buildPickPrefix();
    return op;
  }

  get currentFocus(): number | null {
    return this.focusIndex;
  }

  initMessage(nowMs: number = Date.now()): FakeInit {
    const n = this.storyCount;
    const halfN = n * 2;
    const mercHi = new Float32Array(halfN);
    const mercLo = new Float32Array(halfN);
    for (let i = 0; i < n; i++) {
      const [hiX, loX] = splitMercator(this.storyMercX[i]!);
      const [hiY, loY] = splitMercator(this.storyMercY[i]!);
      mercHi[i * 2] = hiX;
      mercHi[i * 2 + 1] = hiY;
      mercLo[i * 2] = loX;
      mercLo[i * 2 + 1] = loY;
    }

    const stories: StoryArrays = {
      // Init arrays stay initial-count sized; capacity is carried separately
      // and the main thread allocates spare slots from it.
      lng: this.storyLng.slice(0, n),
      lat: this.storyLat.slice(0, n),
      mercHi,
      mercLo,
      category: this.storyCategory.slice(0, n),
      sources: this.storySources.slice(0, n),
      heat: this.storyHeat.slice(0, n),
      updatedAt: this.storyUpdatedAt.slice(0, n),
      focus: this.storyFocus.slice(0, n),
      prevFocus: this.storyPrevFocus.slice(0, n),
      focusAt: this.storyFocusAt.slice(0, n),
      color: this.storyColor.slice(0, n * 4),
      title: this.storyTitle.slice(0, n),
      place: this.storyPlace.slice(0, n),
      partyCount: this.partyCount.slice(0, n),
      partyLng: this.partyLng.slice(0, n * 3),
      partyLat: this.partyLat.slice(0, n * 3),
      partyLabel: this.partyLabel.slice(0, n * 3),
    };

    const links: LinkArrays = {
      a: this.linkA.slice(0, this.linkCount),
      b: this.linkB.slice(0, this.linkCount),
      type: this.linkType.slice(0, this.linkCount),
      strength: this.linkStrength.slice(0, this.linkCount),
      focus: this.linkFocus.slice(0, this.linkCount),
      prevFocus: this.linkPrevFocus.slice(0, this.linkCount),
      focusAt: this.linkFocusAt.slice(0, this.linkCount),
      color: this.linkColor.slice(0, this.linkCount * 4),
      width: this.linkWidth.slice(0, this.linkCount),
    };

    const arcs = this.buildArcArrays();
    const ripples = this.buildRippleArrays();

    return {
      type: "init",
      now: nowMs,
      epochMs: this.epochMs,
      storyCount: n,
      linkCount: this.linkCount,
      storyCapacity: this.storyCapacity,
      linkCapacity: this.linkCapacity,
      arcSegments: ARC_SEGMENTS,
      rippleCapacity: RIPPLE_CAPACITY,
      arcRestFloor: this.computeArcRestFloor(),
      stories,
      links,
      arcs,
      ripples,
      adjacency: {
        adjStart: this.adjStart.slice(0, n + 1),
        adjList: this.adjList.slice(0, this.linkCount * 2),
        adjLink: this.adjLink.slice(0, this.linkCount * 2),
      },
      topStories: this.topStories.slice(),
      hudTop: this.hudTop.map((entry) => ({ ...entry })),
    };
  }

  /**
   * At rest the arc budget goes to the strongest links first. The effective
   * floor is the 55th strongest link above the nominal 0.6 floor.
   */
  private computeArcRestFloor(budget = 55): number {
    const strengths: number[] = [];
    for (let i = 0; i < this.linkCapacity; i++) {
      if (this.linkActive[i] === 0) continue;
      const strength = this.linkStrength[i]!;
      if (strength >= DEFAULT_STRENGTH_FLOOR) strengths.push(strength);
    }
    if (strengths.length === 0) return DEFAULT_STRENGTH_FLOOR;
    strengths.sort((a, b) => b - a);
    return Math.max(DEFAULT_STRENGTH_FLOOR, strengths[Math.min(strengths.length - 1, budget)]!);
  }

  /** One stream tick per rendered frame, ≤ 1 patch per frame. */
  advance(nowMs: number, dtMs: number): void {
    if (!this.enabled) {
      return;
    }
    const dt = Math.min(250, Math.max(0, dtMs));
    this.accumulator += (ARTICLES_PER_SECOND * dt) / 1000;
    const count = Math.min(12, Math.floor(this.accumulator));
    this.accumulator -= count;
    this.rippleBudget = Math.min(
      2,
      this.rippleBudget + (dt * (RIPPLE_CAPACITY / RIPPLE_LIFETIME_SECONDS)) / 1000,
    );
    for (let i = 0; i < count; i++) this.arriveArticle(nowMs);
  }

  flush(nowMs: number): StreamPatch | null {
    if (
      this.dirtyStoryList.length === 0 &&
      this.dirtyLinkList.length === 0 &&
      this.rippleDirty.size === 0
    ) {
      return null;
    }
    const storyIndex = new Uint16Array(this.dirtyStoryList.length);
    const sources = new Uint16Array(this.dirtyStoryList.length);
    const heat = new Float32Array(this.dirtyStoryList.length);
    const updatedAt = new Float32Array(this.dirtyStoryList.length);
    this.dirtyStoryList.forEach((index, position) => {
      storyIndex[position] = index;
      sources[position] = this.storySources[index]!;
      heat[position] = this.storyHeat[index]!;
      updatedAt[position] = this.storyUpdatedAt[index]!;
      this.dirtyStoryFlags[index] = 0;
    });
    this.dirtyStoryList.length = 0;

    const linkIndex = new Uint16Array(this.dirtyLinkList.length);
    const strength = new Float32Array(this.dirtyLinkList.length);
    const width = new Float32Array(this.dirtyLinkList.length);
    const linkColorBytes = new Uint8Array(this.dirtyLinkList.length * 4);
    this.dirtyLinkList.forEach((index, position) => {
      linkIndex[position] = index;
      strength[position] = this.linkStrength[index]!;
      width[position] = this.linkWidth[index]!;
      const color = this.linkColor.subarray(index * 4, index * 4 + 4);
      linkColorBytes.set(color, position * 4);
      this.dirtyLinkFlags[index] = 0;
    });
    this.dirtyLinkList.length = 0;

    const ripples: RippleUpdate[] = [];
    for (const slot of this.rippleDirty.values()) {
      ripples.push(this.rippleUpdate(slot));
    }
    this.rippleDirty.clear();

    return {
      type: "stream",
      now: nowMs,
      storyIndex,
      sources,
      heat,
      updatedAt,
      linkIndex,
      strength,
      width,
      linkColor: linkColorBytes,
      ripples,
    };
  }

  setFocus(index: number | null, secondDegree: boolean, nowMs: number, requestId = 0): FocusPatch {
    const nowSec = (nowMs - this.epochMs) / 1000;
    this.stamp += 1;

    if (this.focusIndex !== null) {
      for (const story of this.lastThreadStories) this.touchStory(story, FOCUS_AMBIENT);
      for (const link of this.lastThreadLinks) this.touchLink(link, FOCUS_AMBIENT);
    }
    this.lastThreadStories = [];
    this.lastThreadLinks = [];

    this.focusIndex = index;
    this.focusThread = new Set<number>();
    let hud: HudFocus | null = null;
    let tethers: TetherSegment[] = [];
    let tetherRings: TetherRing[] = [];

    if (index !== null && index >= 0 && index < this.storyCapacity && this.storyActive[index] !== 0) {
      const ring1 = new Set<number>();
      const directLinks: number[] = [];
      for (let edge = this.adjStart[index]!; edge < this.adjStart[index + 1]!; edge++) {
        const link = this.adjLink[edge]!;
        const other = this.adjList[edge]!;
        ring1.add(other);
        directLinks.push(link);
        this.touchLink(link, FOCUS_LINKED);
      }
      this.touchStory(index, FOCUS_FOCUSED);
      this.focusThread.add(index);
      for (const story of ring1) {
        this.touchStory(story, FOCUS_LINKED);
        this.focusThread.add(story);
      }

      const secondStories = new Set<number>();
      const secondLinks = new Set<number>();
      if (secondDegree) {
        for (const story of ring1) {
          for (let edge = this.adjStart[story]!; edge < this.adjStart[story + 1]!; edge++) {
            const link = this.adjLink[edge]!;
            const other = this.adjList[edge]!;
            if (other === index) continue;
            if (ring1.has(other)) {
              if (!directLinks.includes(link)) secondLinks.add(link);
              continue;
            }
            secondStories.add(other);
            secondLinks.add(link);
          }
        }
        for (const story of secondStories) {
          this.touchStory(story, FOCUS_SECOND);
          this.focusThread.add(story);
        }
        for (const link of secondLinks) this.touchLink(link, FOCUS_SECOND);
      }

      this.lastThreadStories = [index, ...ring1, ...(secondDegree ? [...secondStories] : [])];
      this.lastThreadLinks = [...directLinks, ...(secondDegree ? [...secondLinks] : [])];

      const links: HudFocus["links"] = directLinks
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
          };
        })
        .sort((left, right) => right.strength - left.strength)
        .slice(0, 40);

      const points: [number, number][] = [
        [this.storyLng[index]!, this.storyLat[index]!],
        ...[...ring1].map((story): [number, number] => [
          this.storyLng[story]!,
          this.storyLat[story]!,
        ]),
      ];
      const labels: HudFocus["labels"] = [
        {
          lng: this.storyLng[index]!,
          lat: this.storyLat[index]!,
          category: this.storyCategory[index]!,
          place: this.storyPlace[index]!,
        },
        ...[...ring1].map((story) => ({
          lng: this.storyLng[story]!,
          lat: this.storyLat[story]!,
          category: this.storyCategory[story]!,
          place: this.storyPlace[story]!,
        })),
      ];
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

      const parties: HudFocus["parties"] = [];
      const tether = this.buildTethers(index);
      tethers = tether.segments;
      tetherRings = tether.rings;
      parties.push(...tether.parties);

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
    for (let i = 0; i < this.storyCapacity; i++) {
      if (this.storyActive[i] === 0 || this.changeStamp[i] !== this.stamp) continue;
      const prev = this.changeOriginal[i]!;
      const state = this.changeFinal[i]!;
      if (prev === state) continue;
      this.storyPrevFocus[i] = prev;
      this.storyFocus[i] = state;
      this.storyFocusAt[i] = nowSec;
      storyChanges.push({ index: i, state, prev, at: nowSec });
    }

    const linkChanges: FocusLinkChange[] = [];
    for (let i = 0; i < this.linkCapacity; i++) {
      if (this.linkActive[i] === 0 || this.linkChangeStamp[i] !== this.stamp) continue;
      const prev = this.linkChangeOriginal[i]!;
      const state = this.linkChangeFinal[i]!;
      if (prev === state) continue;
      this.linkPrevFocus[i] = prev;
      this.linkFocus[i] = state;
      this.linkFocusAt[i] = nowSec;
      linkChanges.push({ index: i, state, prev, at: nowSec });
    }

    return {
      type: "focus",
      now: nowMs,
      requestId,
      focusIndex: index ?? -1,
      secondDegree,
      storyChanges,
      linkChanges,
      tethers,
      tetherRings,
      hud,
    };
  }

  private touchStory(index: number, state: number): void {
    if (this.changeStamp[index] !== this.stamp) {
      this.changeStamp[index] = this.stamp;
      this.changeOriginal[index] = this.storyFocus[index]!;
    }
    this.changeFinal[index] = state;
  }

  private touchLink(index: number, state: number): void {
    if (this.linkChangeStamp[index] !== this.stamp) {
      this.linkChangeStamp[index] = this.stamp;
      this.linkChangeOriginal[index] = this.linkFocus[index]!;
    }
    this.linkChangeFinal[index] = state;
  }

  private arriveArticle(nowMs: number): void {
    const index = this.pickStory();
    const nowSec = (nowMs - this.epochMs) / 1000;
    this.storySources[index] = Math.min(65535, this.storySources[index]! + 1);
    this.storyHeat[index] = Math.min(1, this.storyHeat[index]! + 0.012 + this.rng() * 0.01);
    this.storyUpdatedAt[index] = nowSec;
    this.markStoryDirty(index);

    // Rate-limit ripples to the ring buffer's capacity/lifetime (~14/s), and
    // mute stories outside the focused thread.
    if (this.rippleBudget >= 1 && (this.focusIndex === null || this.focusThread.has(index))) {
      this.rippleBudget -= 1;
      const slots = this.ripple.spawn({
        lng: this.storyLng[index]!,
        lat: this.storyLat[index]!,
        category: this.storyCategory[index]!,
        spawnAt: nowSec,
        intensity: 0.45 + this.rng() * 0.3,
        storyIndex: index,
      });
      for (const slot of slots) this.rippleDirty.set(slot.slot, slot);
    }

    if (this.rng() < 0.35 && this.adjStart[index]! < this.adjStart[index + 1]!) {
      const start = this.adjStart[index]!;
      const end = this.adjStart[index + 1]!;
      const link = this.adjLink[start + Math.floor(this.rng() * (end - start))]!;
      const before = this.linkStrength[link]!;
      if (before < 0.985) {
        this.linkStrength[link] = Math.min(0.985, before + 0.0015 + this.rng() * 0.002);
        this.applyLinkStyle(link);
        this.markLinkDirty(link);
      }
    }
  }

  private pickStory(): number {
    if (this.pickTotal <= 0) return 0;
    const target = this.rng() * this.pickTotal;
    let low = 0;
    let high = this.pickPrefix.length - 1;
    while (low < high) {
      const mid = (low + high) >> 1;
      if ((this.pickPrefix[mid] ?? 0) < target) low = mid + 1;
      else high = mid;
    }
    return low;
  }

  private markStoryDirty(index: number): void {
    if (this.dirtyStoryFlags[index] !== 0) return;
    this.dirtyStoryFlags[index] = 1;
    this.dirtyStoryList.push(index);
  }

  private markLinkDirty(index: number): void {
    if (this.dirtyLinkFlags[index] !== 0) return;
    this.dirtyLinkFlags[index] = 1;
    this.dirtyLinkList.push(index);
  }

  private writeSeedStories(keys: Map<string, number>, nowSec: number): void {
    let cursor = 0;
    const writeStory = (seed: SeedStory, jitter: number): number => {
      const city = cityByName(seed.city);
      const lat = city.lat + (this.rng() - 0.5) * jitter;
      const lon = city.lon + (this.rng() - 0.5) * jitter * 1.6;
      const index = cursor++;
      this.writeStoryAt(index, {
        lng: lon,
        lat,
        category: seed.category,
        title: seed.title ?? fakeTitle(seed.category, city.name, this.rng),
        place: city.name,
        sources: seed.sources ?? 1 + Math.floor(this.rng() * 8),
        heat: seed.heat ?? 0.3 + this.rng() * 0.3,
        updatedAt: nowSec - this.rng() * 90,
      });
      if (seed.parties?.length) this.writeParties(index, seed.parties);
      return index;
    };

    for (const seed of SEED_STORIES) {
      if (cursor >= this.storyCount) break;
      const index = writeStory(seed, 0.16);
      if (seed.key) keys.set(seed.key, index);
    }

    // Dense city clusters: street zoom needs overlapping dots at a venue.
    const clusters: Array<{ key: string; city: string; count: number; jitter: number }> = [
      { key: "rot", city: "Rotterdam", count: 22, jitter: 0.03 },
      { key: "rot", city: "Rotterdam", count: 12, jitter: 0.007 },
      { key: "deal", city: "Geneva", count: 16, jitter: 0.05 },
      { key: "was", city: "Washington", count: 10, jitter: 0.05 },
      { key: "tyo", city: "Tokyo", count: 10, jitter: 0.05 },
    ];
    const clusterLinks: Array<[string, number, number]> = [];
    for (const cluster of clusters) {
      const parent = keys.get(cluster.key);
      if (parent === undefined) continue;
      const categories = [
        CATEGORY_ECONOMY,
        CATEGORY_POLITICS,
        CATEGORY_TECH,
        CATEGORY_CLIMATE,
        CATEGORY_CONFLICT,
        CATEGORY_DISASTER,
      ];
      for (let i = 0; i < cluster.count && cursor < this.storyCount; i++) {
        const city = cityByName(cluster.city);
        const category = categories[Math.floor(this.rng() * categories.length)]!;
        const index = cursor++;
        this.writeStoryAt(index, {
          lng: city.lon + (this.rng() - 0.5) * cluster.jitter * 1.6,
          lat: city.lat + (this.rng() - 0.5) * cluster.jitter,
          category,
          title: fakeTitle(category, city.name, this.rng),
          place: city.name,
          sources: 1 + Math.floor(this.rng() * 6),
          heat: 0.25 + this.rng() * 0.35,
          updatedAt: nowSec - this.rng() * 60,
        });
        this.copyParties(parent, index);
        clusterLinks.push([cluster.key, index, 0.35 + this.rng() * 0.35]);
      }
    }

    // Fill the rest from the weighted city list.
    const totalWeight = CITIES.reduce((sum, city) => sum + city.weight, 0);
    while (cursor < this.storyCount) {
      let roll = this.rng() * totalWeight;
      let city = CITIES[0]!;
      for (const candidate of CITIES) {
        roll -= candidate.weight;
        if (roll <= 0) {
          city = candidate;
          break;
        }
      }
      const category = [
        CATEGORY_CONFLICT,
        CATEGORY_POLITICS,
        CATEGORY_ECONOMY,
        CATEGORY_ECONOMY,
        CATEGORY_TECH,
        CATEGORY_CLIMATE,
        CATEGORY_DISASTER,
      ][Math.floor(this.rng() * 7)]!;
      const index = cursor++;
      const sources = 1 + Math.floor(Math.pow(this.rng(), 2.2) * 45);
      this.writeStoryAt(index, {
        lng: city.lon + (this.rng() - 0.5) * 0.9,
        lat: city.lat + (this.rng() - 0.5) * 0.6,
        category,
        title: fakeTitle(category, city.name, this.rng),
        place: city.name,
        sources,
        heat: 0.18 + 0.55 * Math.sqrt(sources / 40) + this.rng() * 0.12,
        updatedAt: nowSec - this.rng() * 600,
      });
      if (this.rng() < 0.3) {
        const party = COUNTRY_POINTS[Math.floor(this.rng() * COUNTRY_POINTS.length)]!;
        this.writeParties(index, [party.label]);
      }
    }

    // Cluster links are stored separately; append them with the curated set.
    for (const [key, index, strength] of clusterLinks) {
      const parent = keys.get(key);
      if (parent === undefined) continue;
      this.seedExtraLinks.push([parent, index, LINK_CAUSAL, strength]);
    }
  }

  private readonly seedExtraLinks: Array<[number, number, number, number]> = [];

  private writeStoryAt(
    index: number,
    fields: {
      lng: number;
      lat: number;
      category: number;
      title: string;
      place: string;
      sources: number;
      heat: number;
      updatedAt: number;
    },
  ): void {
    const [mercX, mercY] = lngLatToMercator(fields.lng, fields.lat);
    this.storyLng[index] = fields.lng;
    this.storyLat[index] = fields.lat;
    this.storyMercX[index] = mercX;
    this.storyMercY[index] = mercY;
    this.storyCategory[index] = fields.category;
    this.storyTitle[index] = fields.title;
    this.storyPlace[index] = fields.place;
    this.storySources[index] = Math.max(1, Math.round(fields.sources));
    this.storyHeat[index] = Math.min(1, Math.max(0, fields.heat));
    this.storyUpdatedAt[index] = fields.updatedAt;
    this.storyFocus[index] = FOCUS_AMBIENT;
    this.storyPrevFocus[index] = FOCUS_AMBIENT;
    this.storyFocusAt[index] = 0;
  }

  private writeParties(index: number, labels: string[]): void {
    const count = Math.min(3, labels.length);
    this.partyCount[index] = count;
    for (let i = 0; i < count; i++) {
      const label = labels[i]!;
      const point = countryPoint(label);
      this.partyLng[index * 3 + i] = point.lon;
      this.partyLat[index * 3 + i] = point.lat;
      this.partyLabel[index * 3 + i] = label;
    }
  }

  private copyParties(from: number, to: number): void {
    const count = this.partyCount[from]!;
    this.partyCount[to] = count;
    for (let i = 0; i < count; i++) {
      this.partyLng[to * 3 + i] = this.partyLng[from * 3 + i]!;
      this.partyLat[to * 3 + i] = this.partyLat[from * 3 + i]!;
      this.partyLabel[to * 3 + i] = this.partyLabel[from * 3 + i]!;
    }
  }

  private writeSeedLinks(keys: Map<string, number>): void {
    let cursor = 0;
    const written = new Set<string>();
    const push = (a: number, b: number, type: number, strength: number): void => {
      if (cursor >= this.linkCount || a === b) return;
      const pair = a < b ? `${a}:${b}` : `${b}:${a}`;
      if (written.has(pair)) return;
      written.add(pair);
      this.linkA[cursor] = a;
      this.linkB[cursor] = b;
      this.linkType[cursor] = type;
      this.linkStrength[cursor] = Math.min(1, Math.max(0, strength));
      this.linkFocus[cursor] = FOCUS_AMBIENT;
      this.linkPrevFocus[cursor] = FOCUS_AMBIENT;
      this.linkFocusAt[cursor] = 0;
      this.applyLinkStyle(cursor);
      cursor++;
    };

    for (const [aKey, bKey, type, strength] of SEED_LINKS) {
      const a = keys.get(aKey);
      const b = keys.get(bKey);
      if (a === undefined || b === undefined) continue;
      push(a, b, type, strength);
    }
    for (const [a, b, type, strength] of this.seedExtraLinks) push(a, b, type, strength);
    this.linkCursor = cursor;
    this.writtenPairs = written;
  }

  private linkCursor = 0;
  private writtenPairs = new Set<string>();

  private writeRandomLinks(): void {
    const n = this.storyCount;
    let guard = 0;
    while (this.linkCursor < this.linkCount && guard < this.linkCount * 50) {
      guard++;
      const a = Math.floor(this.rng() * n);
      let b: number;
      if (this.rng() < 0.65) {
        // Prefer a nearby story so threads look geographical.
        const origin: Vec2 = [this.storyLng[a]!, this.storyLat[a]!];
        let best = -1;
        let bestDistance = 0.35;
        for (let attempt = 0; attempt < 24; attempt++) {
          const candidate = Math.floor(this.rng() * n);
          if (candidate === a) continue;
          const distance = angularDistance(origin, [
            this.storyLng[candidate]!,
            this.storyLat[candidate]!,
          ]);
          if (distance < bestDistance) {
            best = candidate;
            bestDistance = distance;
          }
        }
        b = best >= 0 ? best : Math.floor(this.rng() * n);
      } else {
        b = Math.floor(this.rng() * n);
      }
      if (b === a) continue;
      const pair = a < b ? `${a}:${b}` : `${b}:${a}`;
      if (this.writtenPairs.has(pair)) continue;
      this.writtenPairs.add(pair);

      const roll = this.rng();
      const strength =
        roll < 0.1
          ? 0.12 + this.rng() * 0.16
          : roll < 0.4
            ? 0.3 + this.rng() * 0.2
            : roll < 0.75
              ? 0.5 + this.rng() * 0.25
              : 0.75 + this.rng() * 0.22;
      const typeRoll = this.rng();
      const type = typeRoll < 0.45 ? LINK_CAUSAL : typeRoll < 0.75 ? LINK_REACTION : LINK_THEMATIC;
      const index = this.linkCursor++;
      this.linkA[index] = a;
      this.linkB[index] = b;
      this.linkType[index] = type;
      this.linkStrength[index] = strength;
      this.linkFocus[index] = FOCUS_AMBIENT;
      this.linkPrevFocus[index] = FOCUS_AMBIENT;
      this.linkFocusAt[index] = 0;
      this.applyLinkStyle(index);
    }
    // If the pair filter starved the buffer, fill the remainder deterministically.
    while (this.linkCursor < this.linkCount) {
      const index = this.linkCursor++;
      const a = index % n;
      const b = (index * 7919 + 17) % n;
      this.linkA[index] = a;
      this.linkB[index] = b === a ? (b + 1) % n : b;
      this.linkType[index] = index % 3;
      this.linkStrength[index] = ARC_MIN_STRENGTH + 0.05;
      this.applyLinkStyle(index);
    }
  }

  private buildAdjacency(): void {
    const counts = new Uint32Array(this.storyCapacity + 1);
    for (let link = 0; link < this.linkCapacity; link++) {
      if (this.linkActive[link] === 0) continue;
      const a = this.linkA[link]!;
      const b = this.linkB[link]!;
      counts[a] = (counts[a] ?? 0) + 1;
      counts[b] = (counts[b] ?? 0) + 1;
    }
    let sum = 0;
    for (let story = 0; story < this.storyCapacity; story++) {
      const count = counts[story]!;
      this.adjStart[story] = sum;
      sum += count;
    }
    this.adjStart[this.storyCapacity] = sum;
    const cursor = this.adjStart.slice(0, this.storyCapacity);
    for (let link = 0; link < this.linkCapacity; link++) {
      if (this.linkActive[link] === 0) continue;
      const a = this.linkA[link]!;
      const b = this.linkB[link]!;
      const atA = cursor[a]!;
      this.adjList[atA] = b;
      this.adjLink[atA] = link;
      cursor[a] = atA + 1;
      const atB = cursor[b]!;
      this.adjList[atB] = a;
      this.adjLink[atB] = link;
      cursor[b] = atB + 1;
    }
  }

  private fillColors(): void {
    for (let story = 0; story < this.storyCount; story++) {
      const rgba = toRGBA(categoryColor(this.storyCategory[story]!));
      this.storyColor.set(rgba, story * 4);
    }
  }

  private applyLinkStyle(index: number): void {
    const strength = this.linkStrength[index]!;
    const type = this.linkType[index]!;
    const encoding = encodeArc(type, strength);
    this.linkWidth[index] = encoding.widthPx;
    this.linkColor.set(toRGBA(linkColor(type), encoding.opacity), index * 4);
  }

  private buildPickPrefix(): void {
    this.pickPrefix = new Float64Array(this.storyCapacity);
    let sum = 0;
    for (let story = 0; story < this.storyCapacity; story++) {
      if (this.storyActive[story] === 0) {
        this.pickPrefix[story] = sum;
        continue;
      }
      const degree = this.adjStart[story + 1]! - this.adjStart[story]!;
      sum += 0.4 + this.storyHeat[story]! * 2 + Math.min(6, degree) * 0.3;
      this.pickPrefix[story] = sum;
    }
    this.pickTotal = sum;
  }

  private buildTopStories(): Uint16Array {
    const candidates: number[] = [];
    for (let story = 0; story < this.storyCount; story++) {
      const degree = this.adjStart[story + 1]! - this.adjStart[story]!;
      if (degree > 0) candidates.push(story);
    }
    candidates.sort((left, right) => {
      const score = (index: number): number =>
        this.storyHeat[index]! * 2 +
        Math.min(10, this.adjStart[index + 1]! - this.adjStart[index]!) * 0.15 +
        Math.min(30, this.storySources[index]!) * 0.01;
      return score(right) - score(left);
    });
    return Uint16Array.from(candidates.slice(0, 300));
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

  private buildArcArrays(): ArcArrays {
    const instances = this.linkCount * ARC_SEGMENTS;
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

    for (let l = 0; l < this.linkCount; l++) {
      const a: Vec2 = [this.storyLng[this.linkA[l]!]!, this.storyLat[this.linkA[l]!]!];
      const b: Vec2 = [this.storyLng[this.linkB[l]!]!, this.storyLat[this.linkB[l]!]!];
      const linkLength = angularDistance(a, b) * EARTH_RADIUS_M;
      const previous = lngLatToMercator(a[0], a[1]);
      let previousX = previous[0];
      let point = a;
      for (let segment = 0; segment < ARC_SEGMENTS; segment++) {
        const tA = segment / ARC_SEGMENTS;
        const tB = (segment + 1) / ARC_SEGMENTS;
        const next = greatCirclePoint(a, b, tB);
        let mercB = lngLatToMercator(next[0], next[1]);
        // Unwrap across the antimeridian so a segment never spans the world.
        while (mercB[0] - previousX > 0.5) mercB = [mercB[0] - 1, mercB[1]];
        while (mercB[0] - previousX < -0.5) mercB = [mercB[0] + 1, mercB[1]];
        const [startHiX, startLoX] = splitMercator(previousX);
        const [startHiY, startLoY] = splitMercator(lngLatToMercator(point[0], point[1])[1]);
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
        startElev[base] = arcElevation(a, b, tA);
        endElev[base] = arcElevation(a, b, tB);
        t0[base] = tA;
        t1[base] = tB;
        link[base] = l;
        type[base] = this.linkType[l]!;
        length[base] = linkLength;
        previousX = mercB[0];
        point = next;
      }
    }

    return { startHi, startLo, endHi, endLo, startElev, endElev, t0, t1, link, type, length };
  }

  private buildRippleArrays(): RippleArrays {
    const capacity = this.ripple.capacity;
    const lng = new Float32Array(capacity);
    const lat = new Float32Array(capacity);
    const mercHi = new Float32Array(capacity * 2);
    const mercLo = new Float32Array(capacity * 2);
    const category = new Uint8Array(capacity);
    const spawnAt = new Float32Array(capacity).fill(-1e9);
    const intensity = new Float32Array(capacity);
    const storyIndex = new Int32Array(capacity).fill(-1);
    const arrays = { lng, lat, mercHi, mercLo, category, spawnAt, intensity, storyIndex };
    this.ripple.snapshot().forEach((slot, index) => {
      this.writeRippleSlot(slot, index, arrays);
    });
    return arrays;
  }

  private writeRippleSlot(slot: RippleSlotState, index: number, arrays: RippleArrays): void {
    const [mercX, mercY] = lngLatToMercator(slot.lng, slot.lat);
    const [hiX, loX] = splitMercator(mercX);
    const [hiY, loY] = splitMercator(mercY);
    arrays.lng[index] = slot.lng;
    arrays.lat[index] = slot.lat;
    arrays.mercHi[index * 2] = hiX;
    arrays.mercHi[index * 2 + 1] = hiY;
    arrays.mercLo[index * 2] = loX;
    arrays.mercLo[index * 2 + 1] = loY;
    arrays.category[index] = slot.category;
    arrays.spawnAt[index] = slot.spawnAt;
    arrays.intensity[index] = slot.intensity;
    arrays.storyIndex[index] = slot.storyIndex;
  }

  private rippleUpdate(slot: RippleSlotState): RippleUpdate {
    const [mercX, mercY] = lngLatToMercator(slot.lng, slot.lat);
    const [hiX, loX] = splitMercator(mercX);
    const [hiY, loY] = splitMercator(mercY);
    return {
      slot: slot.slot,
      lng: slot.lng,
      lat: slot.lat,
      mercHi: [hiX, hiY],
      mercLo: [loX, loY],
      category: slot.category,
      spawnAt: slot.spawnAt,
      intensity: slot.intensity,
      storyIndex: slot.storyIndex,
    };
  }

  private buildTethers(index: number): {
    segments: TetherSegment[];
    rings: TetherRing[];
    parties: HudFocus["parties"];
  } {
    const segments: TetherSegment[] = [];
    const rings: TetherRing[] = [];
    const parties: HudFocus["parties"] = [];
    const count = this.partyCount[index]!;
    if (count === 0) return { segments, rings, parties };
    const origin: Vec2 = [this.storyLng[index]!, this.storyLat[index]!];
    const perTether = 32;
    for (let party = 0; party < count; party++) {
      const target: Vec2 = [this.partyLng[index * 3 + party]!, this.partyLat[index * 3 + party]!];
      const label = this.partyLabel[index * 3 + party]!;
      parties.push({ lng: target[0], lat: target[1], label });
      let previous = lngLatToMercator(origin[0], origin[1]);
      let previousX = previous[0];
      for (let step = 0; step < perTether; step++) {
        const tA = step / perTether;
        const tB = (step + 1) / perTether;
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
      rings.push({ hi: [ringHiX, ringHiY], lo: [ringLoX, ringLoY], label });
    }
    return { segments, rings, parties };
  }
}
