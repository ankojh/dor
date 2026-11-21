/**
 * Message protocol between the fake-stream data worker and the main thread.
 *
 * The worker owns every story, link, adjacency list and ripple slot. It builds
 * typed arrays and transfers them; it never touches the DOM. Buffers are
 * posted at most once per animation frame (the main thread sends a `frame`
 * ack), and a focus switch is an adjacency lookup that rewrites only the focus
 * bytes of the old and the new thread.
 */

export const DEFAULT_STORY_COUNT = 5000;
export const DEFAULT_LINK_COUNT = 2000;
export const ARC_SEGMENTS = 20;
export const ARTICLES_PER_SECOND = 50;
export const RIPPLE_CAPACITY = 20;

/** Runtime add/remove operations. Indices are authoritative worker slots. */
export type LinkOp = {
  op: "add" | "remove";
  index: number;
  a: number;
  b: number;
  type: number;
  strength: number;
};

export type StoryOp = {
  op: "add" | "remove";
  index: number;
  lng: number;
  lat: number;
  category: number;
  sources: number;
  heat: number;
  updatedAt: number;
  title: string;
  place: string;
  color: [number, number, number, number];
  /** Links removed with the story, so the renderer can hide their arcs too. */
  links?: LinkOp[];
};

export type HudStory = {
  index: number;
  title: string;
  place: string;
  category: number;
  sources: number;
  heat: number;
};

export type HudLink = {
  index: number;
  storyIndex: number;
  title: string;
  place: string;
  type: number;
  strength: number;
  /** Similarity-only relation, not confirmed by the relation judge. */
  unverified?: boolean;
};

export type HudParty = { lng: number; lat: number; label: string };

export type HudLabel = { lng: number; lat: number; category: number; place: string };

export type HudFocus = {
  story: HudStory;
  /** Direct links, strongest first. */
  links: HudLink[];
  /** Focused + linked (+ second-degree) positions, for the camera fit. */
  points: [number, number][];
  /** One label per position, in the same order. */
  labels: HudLabel[];
  parties: HudParty[];
  secondDegree: boolean;
};

export type StoryArrays = {
  /** Canonical coordinates. Float64 keeps MapLibre's project() and our
   * double-precision mercator path on the same source, so street-zoom
   * alignment is not measured against a pre-rounded Float32 coordinate. */
  lng: Float64Array;
  lat: Float64Array;
  /** Coarse mercator, two floats per story. */
  mercHi: Float32Array;
  /** Residual mercator, two floats per story. */
  mercLo: Float32Array;
  category: Uint8Array;
  sources: Uint16Array;
  heat: Float32Array;
  /** Wall-clock seconds (Date.now() / 1000). */
  updatedAt: Float32Array;
  focus: Uint8Array;
  prevFocus: Uint8Array;
  focusAt: Float32Array;
  /** RGB(A) for deck.gl binary attributes. */
  color: Uint8Array;
  title: string[];
  place: string[];
  /** Up to three party places per story. */
  partyCount: Uint8Array;
  partyLng: Float32Array;
  partyLat: Float32Array;
  partyLabel: string[];
};

export type LinkArrays = {
  a: Uint16Array;
  b: Uint16Array;
  type: Uint8Array;
  strength: Float32Array;
  focus: Uint8Array;
  prevFocus: Uint8Array;
  focusAt: Float32Array;
  color: Uint8Array;
  width: Float32Array;
  /** Optional: 1 when a real link is similarity-only/unverified. */
  unverified?: Uint8Array;
};

export type ArcArrays = {
  startHi: Float32Array;
  startLo: Float32Array;
  endHi: Float32Array;
  endLo: Float32Array;
  startElev: Float32Array;
  endElev: Float32Array;
  t0: Float32Array;
  t1: Float32Array;
  link: Uint16Array;
  type: Uint8Array;
  /** Great-circle length in metres per segment (same for a link's segments). */
  length: Float32Array;
};

export type RippleArrays = {
  lng: Float32Array;
  lat: Float32Array;
  mercHi: Float32Array;
  mercLo: Float32Array;
  category: Uint8Array;
  spawnAt: Float32Array;
  intensity: Float32Array;
  /** Story that spawned the ripple; -1 for an empty slot. */
  storyIndex: Int32Array;
};

/** CSR adjacency, copied to the main thread for synchronous focus switches. */
export type AdjacencyArrays = {
  adjStart: Uint32Array;
  adjList: Uint16Array;
  adjLink: Uint16Array;
};

export type FakeInit = {
  type: "init";
  now: number;
  /** Shared epoch for shader time: (Date.now() - epochMs) / 1000. */
  epochMs: number;
  storyCount: number;
  linkCount: number;
  /** Total slots pre-allocated for runtime inserts (>= storyCount). */
  storyCapacity: number;
  /** Total slots pre-allocated for runtime inserts (>= linkCount). */
  linkCapacity: number;
  arcSegments: number;
  rippleCapacity: number;
  /** Effective strength floor at rest after spending the arc budget. */
  arcRestFloor: number;
  stories: StoryArrays;
  links: LinkArrays;
  arcs: ArcArrays;
  ripples: RippleArrays;
  adjacency: AdjacencyArrays;
  /** Story indices for keyboard navigation, hottest first. */
  topStories: Uint16Array;
  hudTop: HudStory[];
};

export type RippleUpdate = {
  slot: number;
  lng: number;
  lat: number;
  mercHi: [number, number];
  mercLo: [number, number];
  category: number;
  spawnAt: number;
  intensity: number;
  storyIndex: number;
};

export type StreamPatch = {
  type: "stream";
  now: number;
  storyIndex: Uint16Array;
  sources: Uint16Array;
  heat: Float32Array;
  updatedAt: Float32Array;
  linkIndex: Uint16Array;
  strength: Float32Array;
  width: Float32Array;
  /** RGBA per changed link (4 bytes each), for deck.gl. */
  linkColor: Uint8Array;
  ripples: RippleUpdate[];
  /** Rare structural changes; initial-count streams leave these undefined. */
  linkOps?: LinkOp[];
  storyOps?: StoryOp[];
};

export type FocusStoryChange = {
  index: number;
  state: number;
  prev: number;
  at: number;
};

export type FocusLinkChange = {
  index: number;
  state: number;
  prev: number;
  at: number;
};

export type TetherSegment = {
  startHi: [number, number];
  startLo: [number, number];
  endHi: [number, number];
  endLo: [number, number];
  t0: number;
  t1: number;
};

export type TetherRing = {
  hi: [number, number];
  lo: [number, number];
  label: string;
};

export type FocusPatch = {
  type: "focus";
  now: number;
  requestId: number;
  focusIndex: number;
  secondDegree: boolean;
  storyChanges: FocusStoryChange[];
  linkChanges: FocusLinkChange[];
  /** Surface tethers for the focused story. */
  tethers: TetherSegment[];
  tetherRings: TetherRing[];
  hud: HudFocus | null;
};

export type FrameCommand = { type: "frame"; now: number };
export type FocusCommand = {
  type: "focus";
  index: number | null;
  secondDegree: boolean;
  requestId: number;
};
export type StreamCommand = { type: "stream"; enabled: boolean };
export type LinkCommand =
  | {
      type: "link";
      action: "add";
      a: number;
      b: number;
      linkType: number;
      strength: number;
      index?: number;
    }
  | { type: "link"; action: "remove"; index: number };
export type StoryCommand =
  | {
      type: "story";
      action: "add";
      lng: number;
      lat: number;
      category: number;
      sources: number;
      heat: number;
      title: string;
      place: string;
      index?: number;
    }
  | { type: "story"; action: "remove"; index: number };

export type WorkerCommand =
  | FrameCommand
  | FocusCommand
  | StreamCommand
  | LinkCommand
  | StoryCommand;
export type WorkerResponse = FakeInit | StreamPatch | FocusPatch;

/** Collects the transferable buffers of a response. */
export function transferablesOf(response: WorkerResponse): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  const add = (value: unknown): void => {
    if (ArrayBuffer.isView(value)) buffers.add(value.buffer as ArrayBuffer);
  };
  if (response.type === "init") {
    Object.values(response.stories).forEach((value) => {
      if (
        value instanceof Float32Array ||
        value instanceof Float64Array ||
        value instanceof Uint8Array ||
        value instanceof Uint16Array
      ) {
        add(value);
      }
    });
    Object.values(response.links).forEach((value) => {
      if (
        value instanceof Float32Array ||
        value instanceof Float64Array ||
        value instanceof Uint8Array ||
        value instanceof Uint16Array
      ) {
        add(value);
      }
    });
    Object.values(response.arcs).forEach(add);
    Object.values(response.ripples).forEach(add);
    Object.values(response.adjacency).forEach(add);
    add(response.topStories);
  } else if (response.type === "stream") {
    add(response.storyIndex);
    add(response.sources);
    add(response.heat);
    add(response.updatedAt);
    add(response.linkIndex);
    add(response.strength);
    add(response.width);
    add(response.linkColor);
  }
  return [...buffers];
}
