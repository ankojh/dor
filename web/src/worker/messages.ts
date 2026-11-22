import type { LiveStory } from "../globe/live-data";
import type {
  AdjacencyArrays,
  ArcArrays,
  FakeInit,
  LinkOp,
  StoryOp,
  StreamPatch,
} from "./fake/protocol";
import type { Delta, Link, Story, WireRow } from "../gen/dor/v1/dor_pb";

/** Kept for the old worker-probe test; no longer used by the app. */
export type WorkerReady = {
  type: "ready";
  protoStoryId: string;
  storyBytes: number;
  category: string;
};

export type ConnectionState = "connecting" | "online" | "reconnecting" | "offline";

export type SourceHealth = {
  /** null when /api/health is unavailable. */
  ok: boolean | null;
  sourcesOk: number;
  sourcesTotal: number;
  detail: string;
};

export type WorkerStatus = {
  type: "status";
  connection: ConnectionState;
  lastSeq: number;
  /** Unix seconds of the newest wire row the worker has seen. */
  lastWireAt: number | null;
  reconnectInMs: number | null;
  health: SourceHealth;
  error?: string;
};

export type WorkerSnapshot = {
  type: "snapshot";
  seq: number;
  builtAt: number;
  stories: Story[];
  links: Link[];
  /** Cold-start wire history from Snapshot.recent_wire; no seq semantics. */
  recentWire?: WireRow[];
};

export type WorkerDeltas = {
  type: "deltas";
  seq: number;
  deltas: Delta[];
};

export type WorkerResync = {
  type: "resync";
  seq: number;
  reason: string;
};

/** The only full upload. Generated off-thread on snapshot/resync. */
export type WorkerInit = {
  type: "init";
  seq: number;
  init: FakeInit;
  summaries: LiveStory[];
  recentWire: WireRow[];
};
export type StoryPatch = StoryOp & {
  reset?: boolean;
  summary?: LiveStory;
  mercHi?: Float32Array;
  mercLo?: Float32Array;
};
export type LinkPatch = LinkOp & {
  reset?: boolean;
  arcs?: ArcArrays;
  direction?: number;
  confidence?: number;
  distanceKm?: number;
  unverified?: boolean;
};
export type MatchBytes = { indices: Uint16Array; dimmed: Uint8Array };
export type WorkerPatch = {
  type: "patch";
  seq: number;
  storyCapacity: number;
  linkCapacity: number;
  storyCount: number;
  linkCount: number;
  stories: StoryPatch[];
  summaries: LiveStory[];
  links: LinkPatch[];
  stream: StreamPatch;
  storyStates: MatchBytes;
  linkStates: MatchBytes;
  adjacency?: AdjacencyArrays;
  topStories: Uint16Array;
  wire: WireRow[];
  bounds?: { stories: number; links: number; wire: number; pendingWire: number };
  alerts: Array<Extract<Delta["body"], { case: "alert" }>["value"]>;
};
export type WorkerMessage =
  WorkerStatus | WorkerInit | WorkerPatch | WorkerResync | { type: "frameDone" };
/** Input adapters used only inside the worker (including the dev mock). */
export type FeedMessage = WorkerStatus | WorkerSnapshot | WorkerDeltas | WorkerResync;

/** Recursively gather unique buffers, including nested geometry ranges. */
export function transferablesOf(message: WorkerInit | WorkerPatch): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  const visit = (value: unknown): void => {
    if (ArrayBuffer.isView(value)) buffers.add(value.buffer as ArrayBuffer);
    else if (value && typeof value === "object") Object.values(value).forEach(visit);
  };
  visit(message);
  return [...buffers];
}

export type WireFilters = {
  categories: number[];
  regions: string[];
  minSources: number;
  lang: string[];
  sources: string[];
  lat: number;
  lon: number;
  radiusKm: number;
};

/** Main -> worker controls. */
export type WorkerCommand =
  | { type: "start"; mode: "live" | "mock" | "perf" }
  | { type: "frame" }
  | { type: "focus"; storyId: string }
  | { type: "filters"; filters: WireFilters }
  | { type: "prompts"; prompts: string[] };
