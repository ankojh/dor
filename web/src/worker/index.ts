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
  HelloSchema,
  ServerMessageSchema,
  type ClientMessage,
  type Delta,
  type Link,
  type Story,
  type WireRow,
} from "../gen/dor/v1/dor_pb";
import type { SourceHealth, WorkerMessage, WorkerStatus } from "./messages";

const WIRE_LIMIT = 1000;
const statusScope = globalThis as unknown as { postMessage(message: WorkerMessage): void };

const env = import.meta.env as Record<string, string | undefined>;
const configuredUrl = env.VITE_WS_URL;
const socketUrl =
  configuredUrl && configuredUrl.length > 0
    ? configuredUrl
    : import.meta.env.DEV
      ? `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`
      : "ws://127.0.0.1:8080/ws";
const healthUrl = env.VITE_HEALTH_URL || "/api/health";

const stories = new Map<string, Story>();
const links = new Map<string, Link>();
const wire: WireRow[] = [];
const seenArticles = new Set<string>();
let lastSeq = 0;
let lastWireAt: number | null = null;
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
  if (seenArticles.has(key)) return;
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
  const bytes = data instanceof Blob ? new Uint8Array(await data.arrayBuffer()) : new Uint8Array(data);
  let message;
  try {
    message = fromBinary(ServerMessageSchema, bytes);
  } catch (error) {
    postStatus(socket?.readyState === WebSocket.OPEN ? "online" : "offline", String(error));
    return;
  }
  switch (message.body.case) {
    case "snapshot": {
      const snapshot = message.body.value;
      resetState();
      for (const story of snapshot.stories) stories.set(story.id, story);
      for (const link of snapshot.links) links.set(linkKey(link.a, link.b), link);
      lastSeq = Number(snapshot.seq);
      statusScope.postMessage({
        type: "snapshot",
        seq: lastSeq,
        builtAt: Number(snapshot.builtAt),
        stories: [...stories.values()],
        links: [...links.values()],
      });
      sendAck();
      postStatus("online");
      break;
    }
    case "deltas": {
      const batch = message.body.value.deltas;
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
  socket.onopen = () => {
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
      sources?: Array<{ ok?: boolean; status?: string; name?: string; id?: string }>;
      source_count?: number;
      healthy_sources?: number;
    };
    const list = Array.isArray(payload.sources) ? payload.sources : [];
    const sourcesOk = list.filter((source) => source.ok !== false && source.status !== "down").length;
    const sourcesTotal = list.length || payload.source_count || 0;
    health = {
      ok: payload.ok ?? (payload.status ? payload.status === "ok" : null),
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

// Vite worker termination handles cleanup; this keeps the module visibly
// stopped if a host ever posts a stop command.
self.addEventListener("beforeunload", () => {
  stopped = true;
  if (reconnectTimer !== null) clearTimeout(reconnectTimer);
  socket?.close();
});
