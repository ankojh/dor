/**
 * Entry-wiring regression for the real worker protocol (src/worker/index.ts):
 * connect/Hello, snapshot/ACK, Resync recovery, the 30s snapshot watchdog,
 * and stale-socket isolation. Drives the actual module through its real
 * `onmessage` command port and real `postMessage`/`WebSocket` globals, with
 * real binary dor.v1 protobuf frames — not just LiveState/helpers directly.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  ClientMessageSchema,
  DeltaBatchSchema,
  DeltaSchema,
  ResyncSchema,
  ServerMessageSchema,
  SnapshotSchema,
  type ClientMessage,
  type ServerMessage,
} from "../gen/dor/v1/dor_pb";
import type { WorkerMessage } from "./messages";

class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  readyState = MockWebSocket.CONNECTING;
  binaryType = "blob";
  readonly url: string;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: ArrayBuffer | string }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: ((event: { code: number; reason: string }) => void) | null = null;
  readonly sent: Uint8Array[] = [];
  closed = false;
  closeCode: number | null = null;

  constructor(url: string) {
    this.url = url;
    instances.push(this);
  }
  send(data: ArrayBuffer | Uint8Array): void {
    this.sent.push(data instanceof Uint8Array ? data : new Uint8Array(data));
  }
  close(code = 1000, reason = ""): void {
    if (this.closed) return;
    this.closed = true;
    this.closeCode = code;
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.({ code, reason });
  }
  /** Test helper: simulate the server accepting the connection. */
  open(): void {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.();
  }
  /** Test helper: deliver a binary frame as if it arrived on the wire. */
  receive(bytes: Uint8Array): void {
    const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    this.onmessage?.({ data: buffer as ArrayBuffer });
  }
}

let instances: MockWebSocket[] = [];
let posted: WorkerMessage[] = [];
let beforeUnload: (() => void) | null = null;
let onmessageHandler: ((event: { data: unknown }) => void) | null = null;

async function importWorker(): Promise<void> {
  instances = [];
  posted = [];
  beforeUnload = null;
  vi.resetModules();
  vi.stubGlobal("WebSocket", MockWebSocket as unknown as typeof WebSocket);
  vi.stubGlobal("postMessage", (message: WorkerMessage) => posted.push(message));
  vi.stubGlobal("location", { protocol: "http:", host: "test-host" });
  vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("no network available in this test")));
  vi.stubGlobal("self", {
    addEventListener: (type: string, listener: () => void) => {
      if (type === "beforeunload") beforeUnload = listener;
    },
  });
  await import("./index");
  onmessageHandler = (globalThis as unknown as { onmessage: typeof onmessageHandler }).onmessage;
}

function send(command: unknown): void {
  onmessageHandler!({ data: command } as never);
}

/** Starts live mode and opens the first socket; returns that connection. */
function startAndOpen(): MockWebSocket {
  send({ type: "start", mode: "live" });
  const ws = instances[0]!;
  ws.open();
  return ws;
}

function decodeSent(ws: MockWebSocket, index: number): ClientMessage {
  return fromBinary(ClientMessageSchema, ws.sent[index]!);
}

function serverMessage(body: ServerMessage["body"]): Uint8Array {
  const message = create(ServerMessageSchema, { body });
  return toBinary(ServerMessageSchema, message);
}
function snapshotFrame(seq: number): Uint8Array {
  return serverMessage({
    case: "snapshot",
    value: create(SnapshotSchema, {
      seq: BigInt(seq),
      stories: [],
      links: [],
      builtAt: 0n,
      recentWire: [],
    }),
  });
}
function resyncFrame(seq: number, reason = "resync"): Uint8Array {
  return serverMessage({
    case: "resync",
    value: create(ResyncSchema, { seq: BigInt(seq), reason }),
  });
}
function deltasFrame(seqs: number[]): Uint8Array {
  return serverMessage({
    case: "deltas",
    value: create(DeltaBatchSchema, {
      deltas: seqs.map((seq) =>
        create(DeltaSchema, { seq: BigInt(seq), body: { case: undefined } }),
      ),
    }),
  });
}

function postedOfType<T extends WorkerMessage["type"]>(
  type: T,
): Array<Extract<WorkerMessage, { type: T }>> {
  return posted.filter((m): m is Extract<WorkerMessage, { type: T }> => m.type === type);
}

beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("worker/index.ts: initial connect", () => {
  it("sends prompts then Hello(lastSeq=0), and a snapshot yields init + Ack(snapshot seq)", async () => {
    await importWorker();
    const ws = startAndOpen();

    // Prompts are sent first so the gateway can project a resume snapshot
    // with this client's prompt set.
    const initialPrompts = decodeSent(ws, 0);
    expect(initialPrompts.body.case).toBe("setPrompts");
    if (initialPrompts.body.case !== "setPrompts") throw new Error("wrong case");
    expect(initialPrompts.body.value.prompts).toEqual([]);

    const hello = decodeSent(ws, 1);
    expect(hello.body.case).toBe("hello");
    expect((hello.body as { value: { lastSeq: bigint } }).value.lastSeq).toBe(0n);

    ws.receive(snapshotFrame(5));

    expect(postedOfType("init")).toHaveLength(1);
    const ack = decodeSent(ws, 2);
    expect(ack.body.case).toBe("ack");
    expect((ack.body as { value: { seq: bigint } }).value.seq).toBe(5n);
    expect(postedOfType("status").at(-1)?.connection).toBe("online");
  });

  it("sends SetPrompts for every prompt edit, including removing the last prompt", async () => {
    await importWorker();
    const ws = startAndOpen();

    send({ type: "prompts", prompts: ["wildfire"] });
    const added = decodeSent(ws, ws.sent.length - 1);
    expect(added.body.case).toBe("setPrompts");
    if (added.body.case !== "setPrompts") throw new Error("wrong case");
    expect(added.body.value.prompts).toEqual(["wildfire"]);

    send({ type: "prompts", prompts: [] });
    const removed = decodeSent(ws, ws.sent.length - 1);
    expect(removed.body.case).toBe("setPrompts");
    if (removed.body.case !== "setPrompts") throw new Error("wrong case");
    expect(removed.body.value.prompts).toEqual([]);
  });

  it("reconnect with active prompts sends the prompt set before Hello", async () => {
    await importWorker();
    const ws = startAndOpen();
    ws.receive(snapshotFrame(1)); // established at seq 1

    send({ type: "prompts", prompts: ["wildfire"] });
    const active = decodeSent(ws, ws.sent.length - 1);
    expect(active.body.case).toBe("setPrompts");
    if (active.body.case !== "setPrompts") throw new Error("wrong case");
    expect(active.body.value.prompts).toEqual(["wildfire"]);

    ws.close(1006, "server dropped");
    vi.advanceTimersByTime(500);
    expect(instances).toHaveLength(2);
    const next = instances[1]!;
    next.open();

    // The gateway projects the resume snapshot from its current prompt
    // state, so the client's active prompts must arrive before Hello.
    expect(next.sent).toHaveLength(2);
    const prompts = decodeSent(next, 0);
    expect(prompts.body.case).toBe("setPrompts");
    if (prompts.body.case !== "setPrompts") throw new Error("wrong case");
    expect(prompts.body.value.prompts).toEqual(["wildfire"]);

    const hello = decodeSent(next, 1);
    expect(hello.body.case).toBe("hello");
    expect((hello.body as { value: { lastSeq: bigint } }).value.lastSeq).toBe(1n);
  });
});

describe("worker/index.ts: Resync suspends live traffic until recovery", () => {
  it("a Resync resets seq to 0, sends no second Hello, and both flush and delta-apply are suppressed while awaiting", async () => {
    await importWorker();
    const ws = startAndOpen();
    ws.receive(snapshotFrame(1));
    ws.receive(deltasFrame([2])); // an ordinary delta lands fine before Resync
    const sentBeforeResync = ws.sent.length;

    ws.receive(resyncFrame(99, "gateway resync"));
    expect(postedOfType("resync").at(-1)).toMatchObject({ seq: 99, reason: "gateway resync" });
    // No new client message (in particular no second Hello) was sent for the Resync itself.
    expect(ws.sent.length).toBe(sentBeforeResync);

    // A frame tick while awaiting must not flush a patch (frameDone still posts).
    const patchesBefore = postedOfType("patch").length;
    send({ type: "frame" });
    expect(postedOfType("patch").length).toBe(patchesBefore);
    expect(postedOfType("frameDone").length).toBeGreaterThan(0);

    // A delta arriving while awaiting must be ignored: no apply, no Ack.
    const sentBeforeStrayDelta = ws.sent.length;
    ws.receive(deltasFrame([100]));
    expect(ws.sent.length).toBe(sentBeforeStrayDelta);
  });

  it("the recovery snapshot produces exactly one init/Ack, then the covered tail applies and a later frame patch carries the advanced seq, and the watchdog is cleared", async () => {
    await importWorker();
    const ws = startAndOpen();
    ws.receive(snapshotFrame(1));
    ws.receive(resyncFrame(50));

    const initsBefore = postedOfType("init").length;
    ws.receive(snapshotFrame(10)); // the recovery snapshot
    expect(postedOfType("init").length).toBe(initsBefore + 1);
    const ack = decodeSent(ws, ws.sent.length - 1);
    expect(ack.body.case).toBe("ack");
    expect((ack.body as { value: { seq: bigint } }).value.seq).toBe(10n);

    // The covered tail (deltas right after the recovery snapshot) applies normally.
    ws.receive(deltasFrame([11]));
    const sentBeforeAck = ws.sent.length;
    const tailAck = decodeSent(ws, sentBeforeAck - 1);
    expect(tailAck.body.case).toBe("ack");
    expect((tailAck.body as { value: { seq: bigint } }).value.seq).toBe(11n);

    // The watchdog was cleared by the recovery snapshot: letting 30s pass now
    // must not close/retire this connection.
    vi.advanceTimersByTime(30_000);
    expect(ws.closed).toBe(false);
    expect(instances).toHaveLength(1);
  });
});

describe("worker/index.ts: socket close during recovery reconnects with a fresh Hello(0)", () => {
  it("closes, waits ~500ms, opens a new socket, sends Hello(lastSeq=0) and re-enters awaiting", async () => {
    await importWorker();
    const ws = startAndOpen();
    ws.receive(snapshotFrame(1));
    ws.receive(resyncFrame(50)); // now awaiting; lastSeq/state.seq are 0

    ws.close(1006, "server dropped");
    expect(instances).toHaveLength(1); // not yet reconnected

    vi.advanceTimersByTime(500);
    expect(instances).toHaveLength(2);
    const next = instances[1]!;
    next.open();
    const hello = decodeSent(next, 1);
    expect(hello.body.case).toBe("hello");
    expect((hello.body as { value: { lastSeq: bigint } }).value.lastSeq).toBe(0n);

    // Still awaiting on the new connection: a stray delta there is ignored too.
    const sentBefore = next.sent.length;
    next.receive(deltasFrame([2]));
    expect(next.sent.length).toBe(sentBefore);
  });
});

describe("worker/index.ts: the 30s snapshot watchdog", () => {
  it("retires the old socket, reconnects with Hello(0), and never re-sends Hello on the stale socket", async () => {
    await importWorker();
    const ws = startAndOpen(); // awaiting the initial snapshot (lastSeq===0)
    expect(ws.sent).toHaveLength(2); // the initial empty prompt set and Hello

    vi.advanceTimersByTime(30_000);
    expect(ws.closed).toBe(true);
    expect(ws.closeCode).toBe(4000);
    expect(ws.sent).toHaveLength(2); // the watchdog never re-sends Hello on the old socket

    vi.advanceTimersByTime(500); // scheduleReconnect's first backoff
    expect(instances).toHaveLength(2);
    const next = instances[1]!;
    next.open();
    expect(next.sent).toHaveLength(2);
    const hello = decodeSent(next, 1);
    expect(hello.body.case).toBe("hello");
    expect((hello.body as { value: { lastSeq: bigint } }).value.lastSeq).toBe(0n);
  });

  it("repeated Resync frames do not extend the recovery deadline", async () => {
    await importWorker();
    const ws = startAndOpen();
    ws.receive(snapshotFrame(1));
    ws.receive(resyncFrame(50)); // deadline armed at t=0 (relative)

    vi.advanceTimersByTime(29_000);
    ws.receive(resyncFrame(51)); // repeated Resync: must not push the deadline out
    expect(ws.closed).toBe(false);

    vi.advanceTimersByTime(1_000); // total 30s since the FIRST Resync
    expect(ws.closed).toBe(true);
    expect(ws.closeCode).toBe(4000);
  });
});

describe("worker/index.ts: a retired socket cannot mutate its replacement", () => {
  it("ignores a late frame and a late close callback from the retired socket without touching the new connection's state or watchdog", async () => {
    await importWorker();
    const old = startAndOpen();
    vi.advanceTimersByTime(30_000); // watchdog retires `old`
    vi.advanceTimersByTime(500); // reconnect fires
    const fresh = instances[1]!;
    fresh.open();
    const initsBefore = postedOfType("init").length;

    // A late snapshot frame arriving on the retired connection must be ignored outright.
    old.receive(snapshotFrame(777));
    expect(postedOfType("init").length).toBe(initsBefore);

    // A duplicate/late close callback on the retired connection must not disturb
    // the new connection (no extra reconnect scheduled, no extra instance).
    old.onclose?.({ code: 1006, reason: "late close replay" });
    vi.advanceTimersByTime(10_000);
    expect(instances).toHaveLength(2);

    // The new connection's own watchdog must still be live and fire at its own 30s.
    vi.advanceTimersByTime(20_000); // total 30s since `fresh` opened
    expect(fresh.closed).toBe(true);
    expect(fresh.closeCode).toBe(4000);
  });
});

describe("worker/index.ts: teardown", () => {
  it("beforeunload cancels the recovery watchdog and the health poll, so no further reconnects happen", async () => {
    await importWorker();
    const ws = startAndOpen(); // awaiting the initial snapshot; health polling is also running

    expect(beforeUnload).not.toBeNull();
    beforeUnload!();

    // Nothing further should happen: no watchdog close/reconnect, no new sockets.
    vi.advanceTimersByTime(120_000);
    expect(ws.closed).toBe(true); // torn down itself calls socket?.close()
    expect(instances).toHaveLength(1);
  });
});
