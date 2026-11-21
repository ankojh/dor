import type { Delta, Link, Story } from "../gen/dor/v1/dor_pb";

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

export type WorkerMessage = WorkerStatus | WorkerSnapshot | WorkerDeltas | WorkerResync;
