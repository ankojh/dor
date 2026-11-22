import { describe, expect, it } from "vitest";
import { GlobeGpuData } from "./buffers";
import { FakeWorld } from "../../worker/fake/state";

const NOW = 1_700_000_000_000;

/**
 * A minimal WebGL2 stand-in that backs every buffer/texture with real bytes
 * instead of GPU memory, so `ensureCapacity`'s grow-by-copy sequence
 * (bindBuffer → getBufferParameter(BUFFER_SIZE) → create scratch → bufferData
 * → copyBufferSubData → swap back) can be exercised and its result inspected
 * from the test. Every gl.* call buffers.ts makes is covered; nothing here
 * tries to be a real WebGL2RenderingContext beyond that.
 */
function createMockGl(): WebGL2RenderingContext {
  let nextId = 1;
  const TEXTURE_2D = 1,
    TEXTURE_MIN_FILTER = 2,
    TEXTURE_MAG_FILTER = 3,
    TEXTURE_WRAP_S = 4,
    TEXTURE_WRAP_T = 5,
    NEAREST = 6,
    CLAMP_TO_EDGE = 7,
    RGBA32F = 8,
    RGBA = 9,
    FLOAT = 10,
    UNSIGNED_BYTE = 11,
    ARRAY_BUFFER = 12,
    COPY_READ_BUFFER = 13,
    COPY_WRITE_BUFFER = 14,
    STATIC_DRAW = 15,
    DYNAMIC_DRAW = 16,
    STREAM_COPY = 17,
    BUFFER_SIZE = 18;

  type MockBuffer = { id: number; bytes: Uint8Array };
  const bindings = new Map<number, MockBuffer | null>();

  function toBytes(data: ArrayBufferView): Uint8Array {
    return new Uint8Array(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength));
  }

  const gl = {
    TEXTURE_2D,
    TEXTURE_MIN_FILTER,
    TEXTURE_MAG_FILTER,
    TEXTURE_WRAP_S,
    TEXTURE_WRAP_T,
    NEAREST,
    CLAMP_TO_EDGE,
    RGBA32F,
    RGBA,
    FLOAT,
    UNSIGNED_BYTE,
    ARRAY_BUFFER,
    COPY_READ_BUFFER,
    COPY_WRITE_BUFFER,
    STATIC_DRAW,
    DYNAMIC_DRAW,
    STREAM_COPY,
    BUFFER_SIZE,

    createBuffer: (): MockBuffer => ({ id: nextId++, bytes: new Uint8Array(0) }),
    createTexture: (): { id: number } => ({ id: nextId++ }),
    createVertexArray: (): { id: number } => ({ id: nextId++ }),
    deleteBuffer: (): void => {},
    deleteTexture: (): void => {},
    deleteVertexArray: (): void => {},

    bindBuffer: (target: number, buffer: MockBuffer | null): void => {
      bindings.set(target, buffer);
    },
    bindTexture: (): void => {},
    bindVertexArray: (): void => {},
    texParameteri: (): void => {},
    texImage2D: (): void => {},
    texSubImage2D: (): void => {},
    enableVertexAttribArray: (): void => {},
    vertexAttribPointer: (): void => {},
    vertexAttribDivisor: (): void => {},

    bufferData: (target: number, sizeOrData: number | ArrayBufferView, _usage: number): void => {
      const buffer = bindings.get(target);
      if (!buffer) return;
      buffer.bytes =
        typeof sizeOrData === "number" ? new Uint8Array(sizeOrData) : toBytes(sizeOrData);
    },
    bufferSubData: (
      target: number,
      dstByteOffset: number,
      srcData: ArrayBufferView,
      srcOffset = 0,
      length = 0,
    ): void => {
      const buffer = bindings.get(target);
      if (!buffer) return;
      const elementSize = (srcData as { BYTES_PER_ELEMENT?: number }).BYTES_PER_ELEMENT ?? 1;
      const full = toBytes(srcData);
      const start = srcOffset * elementSize;
      const slice =
        length > 0 ? full.subarray(start, start + length * elementSize) : full.subarray(start);
      buffer.bytes.set(slice, dstByteOffset);
    },
    getBufferParameter: (target: number, pname: number): number => {
      if (pname !== BUFFER_SIZE) return 0;
      return bindings.get(target)?.bytes.byteLength ?? 0;
    },
    copyBufferSubData: (
      readTarget: number,
      writeTarget: number,
      readOffset: number,
      writeOffset: number,
      size: number,
    ): void => {
      const read = bindings.get(readTarget);
      const write = bindings.get(writeTarget);
      if (!read || !write) return;
      write.bytes.set(read.bytes.subarray(readOffset, readOffset + size), writeOffset);
    },
  };
  return gl as unknown as WebGL2RenderingContext;
}

function buildInit(storyCount: number, linkCount: number) {
  const world = new FakeWorld({ storyCount, linkCount, seed: 11, now: NOW });
  return world.initMessage(NOW);
}

describe("GlobeGpuData.ensureCapacity grows GPU + CPU buffers without losing data", () => {
  it("preserves per-instance dot buffer bytes and focus data for existing slots after growth", () => {
    const init = buildInit(40, 10);
    const gl = createMockGl();
    const gpu = new GlobeGpuData(gl, init);
    const before = gpu.counts;

    // Snapshot the first story's hi/lo/color bytes and its focus data before growth.
    const dotBuffers = (
      gpu as unknown as {
        dotBuffers: {
          hi: { bytes: Uint8Array };
          lo: { bytes: Uint8Array };
          color: { bytes: Uint8Array };
        };
      }
    ).dotBuffers;
    const hiBefore = dotBuffers.hi.bytes.slice(0, 16); // first 2 stories' worth (2 floats each)
    const colorBefore = dotBuffers.color.bytes.slice(0, 8);
    const focusDataBefore = (gpu as unknown as { focusData: Float32Array }).focusData.slice(0, 4);
    const storyActiveBefore = Array.from(
      (gpu as unknown as { storyActive: Uint8Array }).storyActive.slice(0, before.stories),
    );

    const grownStories = before.stories * 2;
    const grownLinks = before.links * 2;
    expect(() => gpu.ensureCapacity(grownStories, grownLinks)).not.toThrow();

    expect(gpu.counts.stories).toBe(grownStories);
    expect(gpu.counts.links).toBe(grownLinks);
    expect(gpu.storyPool.capacity).toBe(grownStories);
    expect(gpu.linkPool.capacity).toBe(grownLinks);

    // The same buffer objects grew in place; their original bytes must survive the copy.
    expect(dotBuffers.hi.bytes.byteLength).toBeGreaterThan(hiBefore.byteLength);
    expect(dotBuffers.hi.bytes.slice(0, 16)).toEqual(hiBefore);
    expect(dotBuffers.color.bytes.slice(0, 8)).toEqual(colorBefore);

    // Focus/active state for pre-existing slots is untouched by a pure capacity grow.
    const focusDataAfter = (gpu as unknown as { focusData: Float32Array }).focusData;
    expect(Array.from(focusDataAfter.slice(0, 4))).toEqual(Array.from(focusDataBefore));
    const storyActiveAfter = (gpu as unknown as { storyActive: Uint8Array }).storyActive;
    expect(Array.from(storyActiveAfter.slice(0, before.stories))).toEqual(storyActiveBefore);
    // Newly grown slots start inactive.
    expect(
      Array.from(storyActiveAfter.slice(before.stories, grownStories)).every((v) => v === 0),
    ).toBe(true);
  });

  it("is a no-op when asked to shrink or hold the current capacity", () => {
    const init = buildInit(40, 10);
    const gl = createMockGl();
    const gpu = new GlobeGpuData(gl, init);
    const before = gpu.counts;
    gpu.ensureCapacity(1, 1);
    expect(gpu.counts).toEqual(before);
  });
});

describe("GlobeGpuData keeps a dimmed link dimmed through later strength-only writes", () => {
  it("survives both applyLinkOp and updateStream strength updates via the independent linkDimmed byte", () => {
    const init = buildInit(10, 5);
    const gl = createMockGl();
    const gpu = new GlobeGpuData(gl, init);
    const arcData = gpu as unknown as { arcData: Float32Array; linkDimmed: Uint8Array };

    gpu.updateMatches(
      { indices: new Uint16Array(), dimmed: new Uint8Array() },
      { indices: Uint16Array.from([0]), dimmed: Uint8Array.from([1]) },
    );
    expect(arcData.linkDimmed[0]).toBe(1);
    expect(arcData.arcData[0 * 4 + 3]).toBeLessThan(0);

    // A metadata/strength-only link op (not a reset) must not clear the dim bit.
    gpu.applyLinkOp({
      op: "add",
      index: 0,
      a: 0,
      b: 1,
      type: 0,
      strength: 0.8,
      reset: false,
    });
    expect(arcData.linkDimmed[0]).toBe(1);
    expect(arcData.arcData[0 * 4 + 3]).toBeLessThan(0);

    // A stream-driven strength write (e.g. a live strength change) must also respect the dim bit.
    gpu.updateStream({
      type: "stream",
      now: NOW,
      storyIndex: new Uint16Array(),
      sources: new Uint16Array(),
      heat: new Float32Array(),
      updatedAt: new Float32Array(),
      linkIndex: Uint16Array.from([0]),
      strength: Float32Array.from([0.8]),
      width: new Float32Array(),
      linkColor: new Uint8Array(),
      ripples: [],
    });
    expect(arcData.arcData[0 * 4 + 3]).toBeLessThan(0);
  });

  it("clears the dim bit only on an explicit reset", () => {
    const init = buildInit(10, 5);
    const gl = createMockGl();
    const gpu = new GlobeGpuData(gl, init);
    const internals = gpu as unknown as { arcData: Float32Array; linkDimmed: Uint8Array };

    gpu.updateMatches(
      { indices: new Uint16Array(), dimmed: new Uint8Array() },
      { indices: Uint16Array.from([0]), dimmed: Uint8Array.from([1]) },
    );
    expect(internals.linkDimmed[0]).toBe(1);

    gpu.applyLinkOp({ op: "add", index: 0, a: 0, b: 1, type: 0, strength: 0.5, reset: true });
    expect(internals.linkDimmed[0]).toBe(0);
    expect(internals.arcData[0 * 4 + 3]).toBeGreaterThan(0);
  });
});
