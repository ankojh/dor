/**
 * Fake-stream data worker entry.
 *
 * Owns all stories, links, adjacency lists and ripple slots. It posts
 * transferable typed arrays, at most one stream patch per frame ack, and
 * answers focus switches with an adjacency lookup that rewrites only the old
 * and new thread's focus bytes.
 */
import { FakeWorld } from "./state";
import {
  transferablesOf,
  type LinkOp,
  type StoryOp,
  type StreamPatch,
  type WorkerCommand,
  type WorkerResponse,
} from "./protocol";

function opsPatch(now: number, linkOps?: LinkOp[], storyOps?: StoryOp[]): StreamPatch {
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
    ...(linkOps?.length ? { linkOps } : {}),
    ...(storyOps?.length ? { storyOps } : {}),
  };
}

const scope = globalThis as unknown as {
  postMessage(message: WorkerResponse, transfer?: ArrayBuffer[]): void;
  onmessage: ((event: MessageEvent<WorkerCommand>) => void) | null;
};

const world = new FakeWorld({ now: Date.now() });
const init = world.initMessage(Date.now());
scope.postMessage(init, transferablesOf(init));

let lastFrameAt = Date.now();

scope.onmessage = (event: MessageEvent<WorkerCommand>) => {
  const message = event.data;
  switch (message.type) {
    case "frame": {
      const now = Number.isFinite(message.now) ? message.now : Date.now();
      const dt = Math.max(0, now - lastFrameAt);
      lastFrameAt = now;
      world.advance(now, dt);
      const patch = world.flush(now);
      if (patch) scope.postMessage(patch, transferablesOf(patch));
      break;
    }
    case "focus": {
      const patch = world.setFocus(
        message.index,
        message.secondDegree,
        Date.now(),
        message.requestId,
      );
      scope.postMessage(patch, transferablesOf(patch));
      break;
    }
    case "stream": {
      world.setStreamEnabled(message.enabled);
      break;
    }
    case "link": {
      const op =
        message.action === "add"
          ? world.insertLink(message.a, message.b, message.linkType, message.strength)
          : world.removeLink(message.index);
      if (op) {
        const patch = opsPatch(Date.now(), [op]);
        scope.postMessage(patch, transferablesOf(patch));
      }
      break;
    }
    case "story": {
      if (message.action === "add") {
        const op = world.insertStory(message);
        if (op) {
          const patch = opsPatch(Date.now(), undefined, [op]);
          scope.postMessage(patch, transferablesOf(patch));
        }
      } else {
        const op = world.removeStory(message.index);
        if (op) {
          const patch = opsPatch(Date.now(), op.links, [op]);
          scope.postMessage(patch, transferablesOf(patch));
        }
      }
      break;
    }
  }
};
