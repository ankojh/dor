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
  type Link,
  type Story,
  type WireRow,
} from "../gen/dor/v1/dor_pb";
import type {
  SourceHealth,
  WireFilters,
  WorkerCommand,
  WorkerMessage,
  WorkerStatus,
} from "./messages";
import { mergeWireRow } from "../live/wire";

const WIRE_LIMIT = 1000;
const statusScope = globalThis as unknown as {
  postMessage(message: WorkerMessage): void;
  __dorWorker?: Record<string, unknown>;
};
const debug: Record<string, unknown> = {
  raw: 0,
  snapshots: 0,
  batches: 0,
  deltas: 0,
  wires: 0,
  lastType: null,
  lastError: null,
  socket: "init",
};
statusScope.__dorWorker = debug;

const env = import.meta.env as Record<string, string | undefined>;
const configuredUrl = env.VITE_WS_URL;
const socketUrl =
  configuredUrl && configuredUrl.length > 0
    ? configuredUrl
    : import.meta.env.DEV
      ? `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`
      : "ws://127.0.0.1:8080/ws";
const apiBase = (env.VITE_API_URL || "").replace(/\/$/, "");
const healthUrl = env.VITE_HEALTH_URL || `${apiBase}/api/health`;

const stories = new Map<string, Story>();
const links = new Map<string, Link>();
const wire: WireRow[] = [];
const seenArticles = new Set<string>();
let lastSeq = 0;
let lastWireAt: number | null = null;
let activeFilters: WireFilters | null = null;
let activePrompts: string[] = [];
let socket: WebSocket | null = null;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
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
    lastWireAt,
    reconnectInMs: reconnectAt > Date.now() ? reconnectAt - Date.now() : null,
    health,
    ...(error ? { error } : {}),
  });
}

function wireKey(row: WireRow): string {
  return row.articleId || `${row.source}:${row.at}:${row.title}`;
}

function rememberWire(row: WireRow): void {
  const key = wireKey(row);
  const existing = wire.find((candidate) => wireKey(candidate) === key);
  if (existing) {
    // article.clean arrives first; article.placed later adds story_id,
    // category and join. Update the row in place instead of dropping it.
    Object.assign(existing, mergeWireRow(existing, row));
    lastWireAt = Math.max(lastWireAt ?? 0, Number(row.at));
    return;
  }
  seenArticles.add(key);
  wire.unshift(row);
  if (wire.length > WIRE_LIMIT) {
    const removed = wire.pop();
    if (removed) seenArticles.delete(wireKey(removed));
  }
  lastWireAt = Math.max(lastWireAt ?? 0, Number(row.at));
}

function resetState(): void {
  stories.clear();
  links.clear();
  wire.length = 0;
  seenArticles.clear();
  lastWireAt = null;
}

function sendClient(body: ClientMessage["body"]): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  const message = create(ClientMessageSchema, { body });
  socket.send(toBinary(ClientMessageSchema, message));
}

function sendHello(): void {
  sendClient({
    case: "hello",
    value: create(HelloSchema, { lastSeq: BigInt(lastSeq), clientVersion: "dor-web/0.1" }),
  });
  sendFilters();
  sendPrompts();
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
  if (activePrompts.length === 0) return;
  sendClient({
    case: "setPrompts",
    value: create(SetPromptsSchema, { prompts: activePrompts }),
  });
}

function sendAck(): void {
  sendClient({ case: "ack", value: create(AckSchema, { seq: BigInt(lastSeq) }) });
}

function applyDelta(delta: Delta): void {
  const seq = Number(delta.seq);
  if (seq > lastSeq) lastSeq = seq;
  switch (delta.body.case) {
    case "storyUpsert":
      stories.set(delta.body.value.id, delta.body.value);
      break;
    case "storyExpire": {
      const id = delta.body.value;
      stories.delete(id);
      for (const [key, link] of links) {
        if (link.a === id || link.b === id) links.delete(key);
      }
      break;
    }
    case "linkUpsert":
      links.set(linkKey(delta.body.value.a, delta.body.value.b), delta.body.value);
      break;
    case "linkRemove":
      links.delete(linkKey(delta.body.value.a, delta.body.value.b));
      break;
    case "wire":
      rememberWire(delta.body.value);
      break;
    default:
      break;
  }
}

function linkKey(a: string, b: string): string {
  return a < b ? `${a}\u0000${b}` : `${b}\u0000${a}`;
}

async function handleBinary(data: Blob | ArrayBuffer): Promise<void> {
  debug.raw = Number(debug.raw) + 1;
  const bytes = data instanceof Blob ? new Uint8Array(await data.arrayBuffer()) : new Uint8Array(data);
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
      resetState();
      for (const story of snapshot.stories) stories.set(story.id, story);
      for (const link of snapshot.links) links.set(linkKey(link.a, link.b), link);
      const recentWire = snapshot.recentWire;
      for (const row of recentWire) rememberWire(row);
      lastSeq = Number(snapshot.seq);
      debug.snapshots = Number(debug.snapshots) + 1;
      debug.wires = wire.length;
      statusScope.postMessage({
        type: "snapshot",
        seq: lastSeq,
        builtAt: Number(snapshot.builtAt),
        stories: [...stories.values()],
        links: [...links.values()],
        ...(recentWire.length > 0 ? { recentWire } : {}),
      });
      sendAck();
      postStatus("online");
      break;
    }
    case "deltas": {
      const batch = message.body.value.deltas;
      debug.batches = Number(debug.batches) + 1;
      debug.deltas = Number(debug.deltas) + batch.length;
      debug.wires = wire.length;
      for (const delta of batch) applyDelta(delta);
      statusScope.postMessage({
        type: "deltas",
        seq: lastSeq,
        deltas: batch,
      });
      sendAck();
      postStatus("online");
      break;
    }
    case "resync": {
      resetState();
      lastSeq = 0;
      statusScope.postMessage({
        type: "resync",
        seq: Number(message.body.value.seq),
        reason: message.body.value.reason,
      });
      sendHello();
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

function connect(): void {
  if (stopped) return;
  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
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
  socket.binaryType = "arraybuffer";
  debug.socket = "connecting";
  socket.onopen = () => {
    debug.socket = "open";
    reconnectAttempt = 0;
    reconnectAt = 0;
    postStatus("online");
    sendHello();
  };
  socket.onmessage = (event) => {
    if (typeof event.data === "string") return;
    void handleBinary(event.data as Blob | ArrayBuffer);
  };
  socket.onerror = () => {
    postStatus("reconnecting", "gateway socket error");
  };
  socket.onclose = (event) => {
    debug.socket = `closed:${event.code}`;
    socket = null;
    if (stopped) return;
    postStatus("reconnecting", event.reason || `socket closed (${event.code})`);
    scheduleReconnect();
  };
}

async function pollHealth(): Promise<void> {
  try {
    const response = await fetch(healthUrl, { headers: { accept: "application/json" } });
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
      (source.health?.state ?? source.status ?? (source.ok === false ? "down" : "ok")).toLowerCase();
    const sourcesOk = list.filter((source) => stateOf(source) === "ok").length;
    const sourcesTotal = payload.count ?? list.length ?? payload.source_count ?? 0;
    health = {
      ok:
        sourcesTotal > 0
          ? sourcesOk === sourcesTotal
          : payload.ok ?? (payload.status ? payload.status === "ok" : null),
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

statusScope.postMessage({ type: "status", connection: "connecting", lastSeq: 0, lastWireAt: null, reconnectInMs: 0, health });
connect();
void pollHealth();
const healthTimer = setInterval(() => void pollHealth(), 15_000);
void healthTimer;

const commandScope = globalThis as unknown as {
  onmessage: ((event: MessageEvent<WorkerCommand>) => void) | null;
};
commandScope.onmessage = (event) => {
  const command = event.data;
  if (command?.type === "focus" && command.storyId) {
    sendClient({ case: "focus", value: create(FocusSchema, { storyId: command.storyId }) });
  } else if (command?.type === "filters") {
    activeFilters = command.filters;
    sendFilters();
  } else if (command?.type === "prompts") {
    activePrompts = command.prompts;
    sendPrompts();
  }
};

// Vite worker termination handles cleanup; this keeps the module visibly
// stopped if a host ever posts a stop command.
self.addEventListener("beforeunload", () => {
  stopped = true;
  if (reconnectTimer !== null) clearTimeout(reconnectTimer);
  socket?.close();
});
