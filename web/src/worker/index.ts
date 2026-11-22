/**
 * Real data worker: owns the gateway socket, protobuf decoding, resume and
 * the bounded wire ring. It never touches the DOM.
 *
 * Contract: binary dor.v1 ClientMessage/ServerMessage on ws://127.0.0.1:8080/ws
 * (configurable through VITE_WS_URL; Vite proxies /ws in dev).
 */
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  AckSchema,
  ClientMessageSchema,
  FocusSchema,
  HelloSchema,
  SetFiltersSchema,
  SetPromptsSchema,
  ServerMessageSchema,
  type ClientMessage,
  type Delta,
} from "../gen/dor/v1/dor_pb";
import type {
  SourceHealth,
  WireFilters,
  WorkerCommand,
  WorkerMessage,
  WorkerStatus,
} from "./messages";
import { LiveState } from "./state";
import { transferablesOf, type FeedMessage } from "./messages";
import { DeltaSequence } from "./sequence";
import { resolveSocketUrl } from "./url";

const statusScope = globalThis as unknown as {
  postMessage(message: WorkerMessage, transfer?: ArrayBuffer[]): void;
  __dorWorker?: Record<string, unknown>;
};
const debug: Record<string, unknown> = {
  raw: 0,
  snapshots: 0,
  batches: 0,
  deltas: 0,
  duplicates: 0,
  gaps: 0,
  resyncRequests: 0,
  wires: 0,
  lastType: null,
  lastError: null,
  socket: "init",
};
statusScope.__dorWorker = debug;

const env = import.meta.env as Record<string, string | undefined>;
const socketUrl = resolveSocketUrl(env.VITE_WS_URL, location);
const apiBase = (env.VITE_API_URL || "").replace(/\/$/, "");
const healthUrl = env.VITE_HEALTH_URL || `${apiBase}/api/health`;

/** A missing promised snapshot retires the socket; never restart recovery with Hello on it. */
export const SNAPSHOT_RECOVERY_TIMEOUT_MS = 30_000;

const state = new LiveState();
const sequencing = new DeltaSequence();
let lastSeq = 0;
let activeFilters: WireFilters | null = null;
let activePrompts: string[] = [];
let socket: WebSocket | null = null;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let snapshotTimer: ReturnType<typeof setTimeout> | null = null;
let awaitingSnapshot = false;
let reconnectAttempt = 0;
let reconnectAt = 0;
let stopped = false;
let health: SourceHealth = {
  ok: null,
  sourcesOk: 0,
  sourcesTotal: 0,
  detail: "health unavailable",
};

function postStatus(connection: WorkerStatus["connection"], error?: string): void {
  statusScope.postMessage({
    type: "status",
    connection,
    lastSeq,
    lastWireAt: state.lastWireAt,
    reconnectInMs: reconnectAt > Date.now() ? reconnectAt - Date.now() : null,
    health,
    ...(error ? { error } : {}),
  });
}

function sendClient(body: ClientMessage["body"]): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  const message = create(ClientMessageSchema, { body });
  socket.send(toBinary(ClientMessageSchema, message));
}

function sendHello(): void {
  // Prompts and filters must be in the gateway's state before it builds the
  // resume snapshot, otherwise the client's own cached prompt scores are
  // stripped from that snapshot.
  sendFilters();
  sendPrompts();
  sendClient({
    case: "hello",
    value: create(HelloSchema, { lastSeq: BigInt(lastSeq), clientVersion: "dor-web/0.1" }),
  });
}

function sendFilters(): void {
  if (!activeFilters) return;
  sendClient({
    case: "setFilters",
    value: create(SetFiltersSchema, {
      categories: activeFilters.categories as never[],
      regions: activeFilters.regions,
      minSources: activeFilters.minSources,
      lang: activeFilters.lang,
      sources: activeFilters.sources,
      lat: activeFilters.lat,
      lon: activeFilters.lon,
      radiusKm: activeFilters.radiusKm,
    }),
  });
}

function sendPrompts(): void {
  // Always send, including an empty list: removing the last prompt must reach
  // the matcher so it stops spending jev calls on a filter nobody watches.
  sendClient({
    case: "setPrompts",
    value: create(SetPromptsSchema, { prompts: activePrompts }),
  });
}

function sendAck(): void {
  sendClient({ case: "ack", value: create(AckSchema, { seq: BigInt(lastSeq) }) });
}

function clearSnapshotTimer(): void {
  if (snapshotTimer !== null) clearTimeout(snapshotTimer);
  snapshotTimer = null;
}

function awaitSnapshot(connection: WebSocket): void {
  awaitingSnapshot = true;
  debug.recovery = "awaiting snapshot";
  // Repeated Resync frames cannot extend the first recovery deadline.
  if (snapshotTimer !== null) return;
  snapshotTimer = setTimeout(() => {
    snapshotTimer = null;
    if (socket !== connection || !awaitingSnapshot || stopped) return;
    // Retire before close: buffered frames and delayed close callbacks from
    // this connection must not mutate state or affect a replacement socket.
    socket = null;
    debug.socket = "snapshot timeout";
    connection.close(4000, "snapshot recovery timed out");
    scheduleReconnect();
  }, SNAPSHOT_RECOVERY_TIMEOUT_MS);
}

/**
 * Apply one delta batch in order, dropping duplicates and out-of-order frames.
 * The gateway keeps the global sequence contiguous by replacing per-client
 * filtered deltas with seq-only no-ops, so a gap always means frames were lost:
 * reconnect from the last contiguous seq so the gateway can replay them or
 * answer with Resync + snapshot.
 */
function handleDeltaBatch(connection: WebSocket, batch: Delta[]): void {
  let applied = 0;
  let gapAt: number | null = null;
  for (const delta of batch) {
    const seq = Number(delta.seq);
    const disposition = sequencing.classify(seq);
    if (disposition === "duplicate") {
      debug.duplicates = Number(debug.duplicates) + 1;
      continue;
    }
    if (disposition === "gap") {
      gapAt = seq;
      break;
    }
    state.apply(delta);
    sequencing.commit(seq);
    applied++;
  }
  lastSeq = sequencing.lastSeq;
  debug.deltas = Number(debug.deltas) + applied;
  debug.wires = state.wire.size;
  if (applied > 0) sendAck();
  if (gapAt !== null) {
    debug.gaps = Number(debug.gaps) + 1;
    requestResync(connection, `delta gap at ${gapAt}`);
    return;
  }
  postStatus("online");
}

/**
 * Ask the gateway for a resync by resuming from the last contiguous sequence.
 * Hello(last_seq) makes the hub replay the missing tail, or answer with
 * Resync + snapshot when the ring no longer covers it. The old socket is
 * retired first so its close callback cannot disturb the replacement.
 */
function requestResync(connection: WebSocket, reason: string): void {
  if (socket !== connection) return;
  debug.recovery = reason;
  debug.resyncRequests = Number(debug.resyncRequests) + 1;
  socket = null;
  clearSnapshotTimer();
  awaitingSnapshot = false;
  connection.close(4002, reason);
  scheduleReconnect(true);
}

async function handleBinary(data: Blob | ArrayBuffer, connection: WebSocket): Promise<void> {
  if (socket !== connection) return;
  debug.raw = Number(debug.raw) + 1;
  const bytes =
    data instanceof Blob ? new Uint8Array(await data.arrayBuffer()) : new Uint8Array(data);
  if (socket !== connection) return;
  let message;
  try {
    message = fromBinary(ServerMessageSchema, bytes);
  } catch (error) {
    debug.lastError = String(error);
    postStatus(socket?.readyState === WebSocket.OPEN ? "online" : "offline", String(error));
    return;
  }
  debug.lastType = message.body.case ?? null;
  switch (message.body.case) {
    case "snapshot": {
      const snapshot = message.body.value;
      const seq = Number(snapshot.seq);
      const init = state.snapshot(snapshot.stories, snapshot.links, snapshot.recentWire, seq);
      lastSeq = seq;
      sequencing.reset(seq);
      awaitingSnapshot = false;
      clearSnapshotTimer();
      debug.recovery = "ready";
      debug.snapshots = Number(debug.snapshots) + 1;
      statusScope.postMessage(init, transferablesOf(init));
      sendAck();
      postStatus("online");
      break;
    }
    case "deltas": {
      // The recovery snapshot is authoritative. No tail/live delta may be
      // applied or ACKed until it arrives, including unexpected stale frames.
      if (awaitingSnapshot) return;
      const batch = message.body.value.deltas;
      debug.batches = Number(debug.batches) + 1;
      handleDeltaBatch(connection, batch);
      break;
    }
    case "resync": {
      lastSeq = 0;
      state.seq = 0;
      sequencing.reset(0);
      awaitSnapshot(connection);
      statusScope.postMessage({
        type: "resync",
        seq: Number(message.body.value.seq),
        reason: message.body.value.reason,
      });
      postStatus("online");
      break;
    }
  }
}

function scheduleReconnect(immediate = false): void {
  if (stopped) return;
  if (reconnectTimer !== null) clearTimeout(reconnectTimer);
  const delay = immediate ? 0 : Math.min(10_000, 500 * 2 ** reconnectAttempt);
  reconnectAttempt += 1;
  reconnectAt = Date.now() + delay;
  postStatus("reconnecting");
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, delay);
}

/**
 * Drop the current socket and request a fresh authoritative snapshot, keeping
 * the app's focus. Used after a GPU context restore: the renderer rebuilt its
 * buffers and needs the worker's current state rather than the next delta.
 */
function refreshFromGateway(): void {
  if (stopped || !liveMode) return;
  const previous = socket;
  socket = null;
  clearSnapshotTimer();
  awaitingSnapshot = false;
  if (previous) previous.close(4001, "client refresh");
  sequencing.reset(0);
  lastSeq = 0;
  state.seq = 0;
  reconnectAttempt = 0;
  reconnectAt = 0;
  connect();
}

function connect(): void {
  if (stopped) return;
  if (
    socket &&
    (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)
  ) {
    return;
  }
  postStatus(reconnectAttempt > 0 ? "reconnecting" : "connecting");
  try {
    socket = new WebSocket(socketUrl);
  } catch (error) {
    postStatus("offline", String(error));
    scheduleReconnect();
    return;
  }
  const connection = socket;
  connection.binaryType = "arraybuffer";
  debug.socket = "connecting";
  connection.onopen = () => {
    if (socket !== connection) return;
    debug.socket = "open";
    reconnectAttempt = 0;
    reconnectAt = 0;
    postStatus("online");
    if (lastSeq === 0) awaitSnapshot(connection);
    sendHello();
  };
  connection.onmessage = (event) => {
    if (typeof event.data === "string") return;
    void handleBinary(event.data as Blob | ArrayBuffer, connection);
  };
  connection.onerror = () => {
    if (socket !== connection) return;
    postStatus("reconnecting", "gateway socket error");
  };
  connection.onclose = (event) => {
    if (socket !== connection) return;
    clearSnapshotTimer();
    debug.socket = `closed:${event.code}`;
    socket = null;
    if (stopped) return;
    postStatus("reconnecting", event.reason || `socket closed (${event.code})`);
    scheduleReconnect();
  };
}

async function pollHealth(): Promise<void> {
  try {
    const response = await fetch(healthUrl, {
      credentials: "include",
      headers: { accept: "application/json" },
    });
    if (!response.ok) throw new Error(`health ${response.status}`);
    const payload = (await response.json()) as {
      ok?: boolean;
      status?: string;
      count?: number;
      sources?: Array<{
        source?: string;
        name?: string;
        id?: string;
        ok?: boolean;
        status?: string;
        health?: { state?: string; source_id?: string };
      }>;
      source_count?: number;
      healthy_sources?: number;
    };
    const list = Array.isArray(payload.sources) ? payload.sources : [];
    const stateOf = (source: NonNullable<typeof payload.sources>[number]): string =>
      (
        source.health?.state ??
        source.status ??
        (source.ok === false ? "down" : "ok")
      ).toLowerCase();
    const sourcesOk = list.filter((source) => stateOf(source) === "ok").length;
    const sourcesTotal = payload.count ?? list.length ?? payload.source_count ?? 0;
    health = {
      ok:
        sourcesTotal > 0
          ? sourcesOk === sourcesTotal
          : (payload.ok ?? (payload.status ? payload.status === "ok" : null)),
      sourcesOk,
      sourcesTotal,
      detail:
        sourcesTotal > 0
          ? `${sourcesOk}/${sourcesTotal} sources healthy`
          : "health endpoint reachable",
    };
  } catch {
    health = { ok: null, sourcesOk: 0, sourcesTotal: 0, detail: "health unavailable" };
  }
  postStatus(socket?.readyState === WebSocket.OPEN ? "online" : "offline");
}

statusScope.postMessage({
  type: "status",
  connection: "connecting",
  lastSeq: 0,
  lastWireAt: null,
  reconnectInMs: 0,
  health,
});
let started = false;
let liveMode = false;
let stopFeed: (() => void) | null = null;
let healthTimer: ReturnType<typeof setInterval> | null = null;
function mockMessage(message: FeedMessage): void {
  if (message.type === "snapshot") {
    const init = state.snapshot(
      message.stories,
      message.links,
      message.recentWire ?? [],
      message.seq,
    );
    statusScope.postMessage(init, transferablesOf(init));
  } else if (message.type === "deltas") message.deltas.forEach((delta) => state.apply(delta));
  else statusScope.postMessage(message);
}
async function start(mode: "live" | "mock" | "perf" | "wirefixture"): Promise<void> {
  if (started) return;
  started = true;
  liveMode = mode === "live";
  if (mode === "mock") {
    const { startMock } = await import("../live/mock");
    stopFeed = startMock(mockMessage).stop;
  } else if (mode === "wirefixture") {
    const { startWireFixture } = await import("../live/wire-fixture");
    stopFeed = startWireFixture(mockMessage).stop;
  } else if (mode === "perf") {
    const { startPerfFeed } = await import("./perf-feed");
    stopFeed = startPerfFeed(mockMessage);
  } else {
    connect();
    void pollHealth();
    healthTimer = setInterval(() => void pollHealth(), 15_000);
  }
}

const commandScope = globalThis as unknown as {
  onmessage: ((event: MessageEvent<WorkerCommand>) => void) | null;
};
commandScope.onmessage = (event) => {
  const command = event.data;
  if (command?.type === "start") {
    void start(command.mode);
  } else if (command?.type === "refresh") {
    refreshFromGateway();
  } else if (command?.type === "frame") {
    const patch = awaitingSnapshot ? null : state.flush();
    if (patch) statusScope.postMessage(patch, transferablesOf(patch));
    statusScope.postMessage({ type: "frameDone" });
  } else if (command?.type === "focus" && command.storyId) {
    sendClient({ case: "focus", value: create(FocusSchema, { storyId: command.storyId }) });
  } else if (command?.type === "filters") {
    activeFilters = command.filters;
    if (state.setFilters(activeFilters)) sendFilters();
  } else if (command?.type === "prompts") {
    activePrompts = command.prompts;
    if (state.setPrompts(activePrompts)) sendPrompts();
  }
};

// Vite worker termination handles cleanup; this keeps the module visibly
// stopped if a host ever posts a stop command.
self.addEventListener("beforeunload", () => {
  stopped = true;
  if (reconnectTimer !== null) clearTimeout(reconnectTimer);
  clearSnapshotTimer();
  socket?.close();
  stopFeed?.();
  if (healthTimer) clearInterval(healthTimer);
});
