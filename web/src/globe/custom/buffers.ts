import { growArray } from "../arrays";
import type { StoryPatch, LinkPatch, MatchBytes } from "../../worker/messages";
import { dotSizeFactor } from "../encoding";
import { CapacityPool, TEXTURE_WIDTH, texelOf, textureRows } from "../gpu-layout";
import {
  angularDistance,
  arcElevation,
  EARTH_RADIUS_M,
  greatCirclePoint,
  lngLatToMercator,
  mercatorToLngLat,
  splitMercator,
  type Vec2,
} from "../mercator";
import type {
  FakeInit,
  FocusPatch,
  StreamPatch,
  TetherRing,
  TetherSegment,
} from "../../worker/fake/protocol";

const QUAD_STRIP = new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]);
const QUAD_TRIANGLES = new Float32Array([0, -1, 1, -1, 0, 1, 1, -1, 1, 1, 0, 1]);

type TextureInfo = { texture: WebGLTexture; width: number; height: number };

function createTexture(gl: WebGL2RenderingContext, count: number, data: Float32Array): TextureInfo {
  const width = Math.max(1, Math.min(TEXTURE_WIDTH, count));
  const height = textureRows(count, width);
  const texture = gl.createTexture();
  if (!texture) throw new Error("texture allocation failed");
  gl.bindTexture(gl.TEXTURE_2D, texture);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
  const required = width * height * 4;
  let upload = data;
  if (data.length < required) {
    upload = new Float32Array(required);
    upload.set(data);
  }
  gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA32F, width, height, 0, gl.RGBA, gl.FLOAT, upload);
  return { texture, width, height };
}

type Mesh = {
  vao: WebGLVertexArrayObject;
  buffers: WebGLBuffer[];
  instanceCount: number;
};

type TetherBuffers = {
  startHi: WebGLBuffer;
  startLo: WebGLBuffer;
  endHi: WebGLBuffer;
  endLo: WebGLBuffer;
  t0: WebGLBuffer;
  t1: WebGLBuffer;
};

type RingBuffers = { hi: WebGLBuffer; lo: WebGLBuffer };

type ArcBufferSet = {
  startHi: WebGLBuffer;
  startLo: WebGLBuffer;
  endHi: WebGLBuffer;
  endLo: WebGLBuffer;
  startElev: WebGLBuffer;
  endElev: WebGLBuffer;
  t0: WebGLBuffer;
  t1: WebGLBuffer;
  link: WebGLBuffer;
  type: WebGLBuffer;
  length: WebGLBuffer;
};

type DotBufferSet = { hi: WebGLBuffer; lo: WebGLBuffer; color: WebGLBuffer; index: WebGLBuffer };

/**
 * GPU-side data for the custom layer: one VAO per primitive, three 2D
 * RGBA32F textures (width ≤ 2048, rows for capacity) for per-instance
 * focus/heat/strength. Updates are 1-texel sub-range writes plus ≤ 20
 * ripple-attribute writes; a focus switch writes only the old and new
 * thread's texels. Buffers are pre-allocated to capacity; `CapacityPool`
 * carries a free list for insert/remove.
 */
export class GlobeGpuData {
  readonly storyTex: WebGLTexture;
  readonly focusTex: WebGLTexture;
  readonly arcFocusTex: WebGLTexture;
  storyCapacity: number;
  linkCapacity: number;
  readonly storyPool: CapacityPool;
  readonly linkPool: CapacityPool;
  readonly dotMesh: Mesh;
  readonly arcMesh: Mesh;
  readonly rippleMesh: Mesh;
  tetherMesh: Mesh | null = null;
  ringMesh: Mesh | null = null;

  private readonly gl: WebGL2RenderingContext;
  private readonly storyCount: number;
  private readonly linkCount: number;
  private storyTexWidth: number;
  private storyTexHeight: number;
  private linkTexWidth: number;
  private storyData: Float32Array;
  private focusData: Float32Array;
  private arcData: Float32Array;
  private storyMercX: Float64Array;
  private storyMercY: Float64Array;
  private storyActive: Uint8Array;
  private linkActive: Uint8Array;
  private linkDimmed: Uint8Array;
  private readonly arcSegments: number;
  private arcBuffers!: ArcBufferSet;
  private dotBuffers!: DotBufferSet;
  private readonly rippleBuffers: {
    hi: WebGLBuffer;
    lo: WebGLBuffer;
    category: WebGLBuffer;
    spawnAt: WebGLBuffer;
    intensity: WebGLBuffer;
    storyIndex: WebGLBuffer;
  };
  private readonly rippleData = {
    hi: new Float32Array(40),
    lo: new Float32Array(40),
    category: new Float32Array(20),
    spawnAt: new Float32Array(20),
    intensity: new Float32Array(20),
    storyIndex: new Float32Array(20).fill(-1),
  };
  private readonly tetherBuffers: TetherBuffers | null;
  private readonly ringBuffers: RingBuffers | null;
  private readonly pixel = new Float32Array(4);
  private readonly arcPixel = new Float32Array(4);
  private readonly tetherScratch = {
    startHi: new Float32Array(384),
    startLo: new Float32Array(384),
    endHi: new Float32Array(384),
    endLo: new Float32Array(384),
    t0: new Float32Array(192),
    t1: new Float32Array(192),
    ringHi: new Float32Array(12),
    ringLo: new Float32Array(12),
  };

  constructor(gl: WebGL2RenderingContext, init: FakeInit) {
    this.gl = gl;
    this.storyCount = init.storyCount;
    this.linkCount = init.linkCount;
    this.storyCapacity = Math.max(2048, this.storyCount + 1024, init.storyCapacity ?? 0);
    this.linkCapacity = Math.max(1024, this.linkCount + 512, init.linkCapacity ?? 0);
    this.arcSegments = init.arcSegments;
    this.storyPool = new CapacityPool(this.storyCapacity, this.storyCount);
    this.linkPool = new CapacityPool(this.linkCapacity, this.linkCount);
    const stories = init.stories;
    const links = init.links;
    this.storyMercX = new Float64Array(this.storyCapacity);
    this.storyMercY = new Float64Array(this.storyCapacity);
    this.storyActive = new Uint8Array(this.storyCapacity);
    this.storyActive.fill(1, 0, this.storyCount);
    this.linkActive = new Uint8Array(this.linkCapacity);
    this.linkDimmed = new Uint8Array(this.linkCapacity);
    this.linkActive.fill(1, 0, this.linkCount);
    for (let i = 0; i < this.storyCount; i++) {
      const [x, y] = lngLatToMercator(stories.lng[i]!, stories.lat[i]!);
      this.storyMercX[i] = x;
      this.storyMercY[i] = y;
    }

    this.storyData = new Float32Array(this.storyCapacity * 4);
    this.focusData = new Float32Array(this.storyCapacity * 4);
    this.arcData = new Float32Array(this.linkCapacity * 4);
    for (let i = 0; i < this.storyCount; i++) {
      this.storyData[i * 4] = dotSizeFactor(stories.sources[i]!, stories.heat[i]!);
      this.storyData[i * 4 + 1] = stories.heat[i]!;
      this.storyData[i * 4 + 2] = stories.updatedAt[i]!;
      this.focusData[i * 4] = stories.focus[i]!;
      this.focusData[i * 4 + 1] = stories.prevFocus[i]!;
      this.focusData[i * 4 + 2] = stories.focusAt[i]!;
    }
    for (let i = 0; i < this.linkCount; i++) {
      this.arcData[i * 4] = links.focus[i]!;
      this.arcData[i * 4 + 1] = links.prevFocus[i]!;
      this.arcData[i * 4 + 2] = links.focusAt[i]!;
      this.arcData[i * 4 + 3] = links.strength[i]!;
    }

    const storyTexture = createTexture(gl, this.storyCapacity, this.storyData);
    const focusTexture = createTexture(gl, this.storyCapacity, this.focusData);
    const arcTexture = createTexture(gl, this.linkCapacity, this.arcData);
    this.storyTex = storyTexture.texture;
    this.focusTex = focusTexture.texture;
    this.arcFocusTex = arcTexture.texture;
    this.storyTexWidth = storyTexture.width;
    this.storyTexHeight = storyTexture.height;
    this.linkTexWidth = arcTexture.width;
    void this.storyTexHeight;

    this.dotMesh = this.createDotMesh(init);
    this.arcMesh = this.createArcMesh(init);

    const ripples = init.ripples;
    for (let i = 0; i < init.rippleCapacity; i++) {
      this.rippleData.hi[i * 2] = ripples.mercHi[i * 2]!;
      this.rippleData.hi[i * 2 + 1] = ripples.mercHi[i * 2 + 1]!;
      this.rippleData.lo[i * 2] = ripples.mercLo[i * 2]!;
      this.rippleData.lo[i * 2 + 1] = ripples.mercLo[i * 2 + 1]!;
      this.rippleData.category[i] = ripples.category[i]!;
      this.rippleData.spawnAt[i] = ripples.spawnAt[i]!;
      this.rippleData.intensity[i] = ripples.intensity[i]!;
      this.rippleData.storyIndex[i] = ripples.storyIndex[i]!;
    }
    const rippleHi = this.createBuffer(this.rippleData.hi, gl.DYNAMIC_DRAW);
    const rippleLo = this.createBuffer(this.rippleData.lo, gl.DYNAMIC_DRAW);
    const rippleCategory = this.createBuffer(this.rippleData.category, gl.DYNAMIC_DRAW);
    const rippleSpawn = this.createBuffer(this.rippleData.spawnAt, gl.DYNAMIC_DRAW);
    const rippleIntensity = this.createBuffer(this.rippleData.intensity, gl.DYNAMIC_DRAW);
    const rippleStory = this.createBuffer(this.rippleData.storyIndex, gl.DYNAMIC_DRAW);
    this.rippleBuffers = {
      hi: rippleHi,
      lo: rippleLo,
      category: rippleCategory,
      spawnAt: rippleSpawn,
      intensity: rippleIntensity,
      storyIndex: rippleStory,
    };
    const rippleVao = gl.createVertexArray();
    if (!rippleVao) throw new Error("VAO allocation failed");
    gl.bindVertexArray(rippleVao);
    const corner = this.createBuffer(QUAD_STRIP, gl.STATIC_DRAW);
    this.attrib(corner, 5, 2, gl.FLOAT, false, 0, 0, 0);
    this.attrib(rippleHi, 0, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(rippleLo, 1, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(rippleCategory, 2, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(rippleSpawn, 3, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(rippleIntensity, 4, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(rippleStory, 6, 1, gl.FLOAT, false, 0, 0, 1);
    gl.bindVertexArray(null);
    this.rippleMesh = {
      vao: rippleVao,
      buffers: [
        corner,
        rippleHi,
        rippleLo,
        rippleCategory,
        rippleSpawn,
        rippleIntensity,
        rippleStory,
      ],
      instanceCount: init.rippleCapacity,
    };

    this.tetherBuffers = {
      startHi: this.createBuffer(new Float32Array(384), gl.DYNAMIC_DRAW),
      startLo: this.createBuffer(new Float32Array(384), gl.DYNAMIC_DRAW),
      endHi: this.createBuffer(new Float32Array(384), gl.DYNAMIC_DRAW),
      endLo: this.createBuffer(new Float32Array(384), gl.DYNAMIC_DRAW),
      t0: this.createBuffer(new Float32Array(192), gl.DYNAMIC_DRAW),
      t1: this.createBuffer(new Float32Array(192), gl.DYNAMIC_DRAW),
    };
    this.ringBuffers = {
      hi: this.createBuffer(new Float32Array(12), gl.DYNAMIC_DRAW),
      lo: this.createBuffer(new Float32Array(12), gl.DYNAMIC_DRAW),
    };
    this.tetherMesh = this.createTetherMesh(this.tetherBuffers);
    this.ringMesh = this.createRingMesh(this.ringBuffers);
  }

  storyAnimation(index: number) {
    return {
      state: this.focusData[index * 4],
      prev: this.focusData[index * 4 + 1],
      at: this.focusData[index * 4 + 2],
      dimmed: this.focusData[index * 4 + 3],
    };
  }

  /** Grow storage while preserving buffers, focus transitions and ripple slots. */
  ensureCapacity(stories: number, links: number): void {
    const gl = this.gl;
    const extendBuffer = (buffer: WebGLBuffer, bytes: number): void => {
      gl.bindBuffer(gl.COPY_READ_BUFFER, buffer);
      const old = gl.getBufferParameter(gl.COPY_READ_BUFFER, gl.BUFFER_SIZE) as number;
      if (bytes <= old) return;
      const scratch = gl.createBuffer();
      if (!scratch) throw new Error("buffer growth failed");
      gl.bindBuffer(gl.COPY_WRITE_BUFFER, scratch);
      gl.bufferData(gl.COPY_WRITE_BUFFER, old, gl.STREAM_COPY);
      gl.copyBufferSubData(gl.COPY_READ_BUFFER, gl.COPY_WRITE_BUFFER, 0, 0, old);
      gl.bindBuffer(gl.COPY_READ_BUFFER, scratch);
      gl.bindBuffer(gl.COPY_WRITE_BUFFER, buffer);
      gl.bufferData(gl.COPY_WRITE_BUFFER, bytes, gl.DYNAMIC_DRAW);
      gl.copyBufferSubData(gl.COPY_READ_BUFFER, gl.COPY_WRITE_BUFFER, 0, 0, old);
      gl.deleteBuffer(scratch);
    };
    const texture = (target: WebGLTexture, capacity: number, data: Float32Array): number => {
      const width = Math.min(TEXTURE_WIDTH, capacity),
        height = textureRows(capacity, width);
      const padded = new Float32Array(width * height * 4);
      padded.set(data);
      gl.bindTexture(gl.TEXTURE_2D, target);
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA32F, width, height, 0, gl.RGBA, gl.FLOAT, padded);
      return width;
    };
    if (stories > this.storyCapacity) {
      this.storyData = growArray(this.storyData, stories * 4);
      this.focusData = growArray(this.focusData, stories * 4);
      this.storyMercX = growArray(this.storyMercX, stories);
      this.storyMercY = growArray(this.storyMercY, stories);
      this.storyActive = growArray(this.storyActive, stories);
      this.storyPool.grow(stories);
      for (const key of ["hi", "lo"] as const) extendBuffer(this.dotBuffers[key], stories * 2 * 4);
      extendBuffer(this.dotBuffers.color, stories * 4);
      extendBuffer(this.dotBuffers.index, stories * 4);
      const indices = Float32Array.from(
        { length: stories - this.storyCapacity },
        (_, i) => i + this.storyCapacity,
      );
      gl.bindBuffer(gl.ARRAY_BUFFER, this.dotBuffers.index);
      gl.bufferSubData(gl.ARRAY_BUFFER, this.storyCapacity * 4, indices);
      this.storyTexWidth = texture(this.storyTex, stories, this.storyData);
      texture(this.focusTex, stories, this.focusData);
      this.storyCapacity = stories;
      this.dotMesh.instanceCount = stories;
    }
    if (links > this.linkCapacity) {
      this.arcData = growArray(this.arcData, links * 4);
      this.linkActive = growArray(this.linkActive, links);
      this.linkDimmed = growArray(this.linkDimmed, links);
      this.linkPool.grow(links);
      for (const [key, buffer] of Object.entries(this.arcBuffers))
        extendBuffer(
          buffer,
          links *
            this.arcSegments *
            4 *
            (["startHi", "startLo", "endHi", "endLo"].includes(key) ? 2 : 1),
        );
      this.linkTexWidth = texture(this.arcFocusTex, links, this.arcData);
      this.linkCapacity = links;
      this.arcMesh.instanceCount = links * this.arcSegments;
    }
    gl.bindBuffer(gl.COPY_READ_BUFFER, null);
    gl.bindBuffer(gl.COPY_WRITE_BUFFER, null);
  }

  private uploadArcRange(index: number, arcs: NonNullable<LinkPatch["arcs"]>): void {
    const gl = this.gl;
    for (const [key, source] of Object.entries(arcs)) {
      const buffer = this.arcBuffers[key as keyof ArcBufferSet];
      const data = source instanceof Float32Array ? source : Float32Array.from(source);
      gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
      gl.bufferSubData(gl.ARRAY_BUFFER, index * data.byteLength, data);
    }
  }

  updateMatches(stories: MatchBytes, links: MatchBytes): void {
    const gl = this.gl;
    gl.bindTexture(gl.TEXTURE_2D, this.focusTex);
    for (let i = 0; i < stories.indices.length; i++) {
      const index = stories.indices[i]!;
      this.focusData[index * 4 + 3] = stories.dimmed[i]!;
      this.uploadFocusTexel(
        index,
        this.focusData[index * 4]!,
        this.focusData[index * 4 + 1]!,
        this.focusData[index * 4 + 2]!,
      );
    }
    for (let i = 0; i < links.indices.length; i++) {
      const index = links.indices[i]!;
      this.linkDimmed[index] = links.dimmed[i]!;
      const strength = Math.abs(this.arcData[index * 4 + 3]!) * (links.dimmed[i] ? -1 : 1);
      this.arcData[index * 4 + 3] = strength;
      this.uploadArcTexel(
        index,
        this.arcData[index * 4]!,
        this.arcData[index * 4 + 1]!,
        this.arcData[index * 4 + 2]!,
        strength,
      );
    }
  }

  /** Free every buffer, VAO and texture. Called on remove and before re-init. */
  dispose(): void {
    const gl = this.gl;
    for (const mesh of [
      this.dotMesh,
      this.arcMesh,
      this.rippleMesh,
      this.tetherMesh,
      this.ringMesh,
    ]) {
      if (!mesh) continue;
      for (const buffer of mesh.buffers) gl.deleteBuffer(buffer);
      gl.deleteVertexArray(mesh.vao);
    }
    gl.deleteTexture(this.storyTex);
    gl.deleteTexture(this.focusTex);
    gl.deleteTexture(this.arcFocusTex);
    this.tetherMesh = null;
    this.ringMesh = null;
  }

  updateStream(patch: StreamPatch): void {
    const gl = this.gl;
    gl.bindTexture(gl.TEXTURE_2D, this.storyTex);
    const storyPixel = this.pixel;
    for (let i = 0; i < patch.storyIndex.length; i++) {
      const index = patch.storyIndex[i]!;
      if (index >= this.storyCapacity || this.storyActive[index] === 0) continue;
      const size = dotSizeFactor(patch.sources[i]!, patch.heat[i]!);
      storyPixel[0] = size;
      storyPixel[1] = patch.heat[i]!;
      storyPixel[2] = patch.updatedAt[i]!;
      storyPixel[3] = 0;
      this.storyData[index * 4] = size;
      this.storyData[index * 4 + 1] = patch.heat[i]!;
      this.storyData[index * 4 + 2] = patch.updatedAt[i]!;
      const texel = texelOf(index, this.storyTexWidth);
      gl.texSubImage2D(gl.TEXTURE_2D, 0, texel.x, texel.y, 1, 1, gl.RGBA, gl.FLOAT, storyPixel);
    }

    if (patch.linkIndex.length > 0) {
      gl.bindTexture(gl.TEXTURE_2D, this.arcFocusTex);
      const linkPixel = this.pixel;
      for (let i = 0; i < patch.linkIndex.length; i++) {
        const index = patch.linkIndex[i]!;
        if (index >= this.linkCapacity || this.linkActive[index] === 0) continue;
        this.arcData[index * 4 + 3] = this.linkDimmed[index]
          ? -patch.strength[i]!
          : patch.strength[i]!;
        linkPixel[0] = this.arcData[index * 4]!;
        linkPixel[1] = this.arcData[index * 4 + 1]!;
        linkPixel[2] = this.arcData[index * 4 + 2]!;
        linkPixel[3] = this.arcData[index * 4 + 3]!;
        const texel = texelOf(index, this.linkTexWidth);
        gl.texSubImage2D(gl.TEXTURE_2D, 0, texel.x, texel.y, 1, 1, gl.RGBA, gl.FLOAT, linkPixel);
      }
    }

    if (patch.ripples.length > 0) {
      this.writeRipples(patch.ripples);
    }
  }

  /**
   * Structural story insert/remove. Slots are claimed/freed in the pool,
   * position/color buffers and both textures are written, and an empty slot
   * is hidden by the dot/pick shader (`story.x <= 0`).
   */
  applyStoryOp(op: StoryPatch): void {
    if (op.index < 0 || op.index >= this.storyCapacity) return;
    const index = op.index;
    if (op.op === "add") {
      const wasActive = this.storyActive[index] !== 0 && !op.reset;
      if (this.storyActive[index] === 0) {
        if (!this.storyPool.claim(index)) return;
        this.storyActive[index] = 1;
      }
      const [mercX, mercY] =
        op.mercHi && op.mercLo
          ? [op.mercHi[0]! + op.mercLo[0]!, op.mercHi[1]! + op.mercLo[1]!]
          : lngLatToMercator(op.lng, op.lat);
      this.storyMercX[index] = mercX;
      this.storyMercY[index] = mercY;
      const [hiX, loX] = splitMercator(mercX);
      const [hiY, loY] = splitMercator(mercY);
      const gl = this.gl;
      gl.bindBuffer(gl.ARRAY_BUFFER, this.dotBuffers.hi);
      gl.bufferSubData(gl.ARRAY_BUFFER, index * 2 * 4, new Float32Array([hiX, hiY]));
      gl.bindBuffer(gl.ARRAY_BUFFER, this.dotBuffers.lo);
      gl.bufferSubData(gl.ARRAY_BUFFER, index * 2 * 4, new Float32Array([loX, loY]));
      gl.bindBuffer(gl.ARRAY_BUFFER, this.dotBuffers.color);
      gl.bufferSubData(gl.ARRAY_BUFFER, index * 4, new Uint8Array(op.color));
      const size = dotSizeFactor(op.sources, op.heat);
      this.storyData[index * 4] = size;
      this.storyData[index * 4 + 1] = op.heat;
      this.storyData[index * 4 + 2] = op.updatedAt;
      this.storyData[index * 4 + 3] = 0;
      if (!wasActive) this.focusData.fill(0, index * 4, index * 4 + 4);
      gl.bindTexture(gl.TEXTURE_2D, this.storyTex);
      this.uploadStoryTexel(index, size, op.heat, op.updatedAt);
      gl.bindTexture(gl.TEXTURE_2D, this.focusTex);
      this.uploadFocusTexel(
        index,
        this.focusData[index * 4]!,
        this.focusData[index * 4 + 1]!,
        this.focusData[index * 4 + 2]!,
      );
    } else {
      this.storyActive[index] = 0;
      this.storyPool.free(index);
      this.storyData[index * 4] = 0;
      this.storyData[index * 4 + 1] = 0;
      this.storyData[index * 4 + 2] = 0;
      this.focusData[index * 4] = 0;
      this.focusData[index * 4 + 1] = 0;
      this.focusData[index * 4 + 2] = 0;
      const gl = this.gl;
      gl.bindBuffer(gl.ARRAY_BUFFER, this.dotBuffers.color);
      gl.bufferSubData(gl.ARRAY_BUFFER, index * 4, new Uint8Array(4));
      gl.bindTexture(gl.TEXTURE_2D, this.storyTex);
      this.uploadStoryTexel(index, 0, 0, 0);
      gl.bindTexture(gl.TEXTURE_2D, this.focusTex);
      this.uploadFocusTexel(index, 0, 0, 0);
      for (const link of op.links ?? []) this.applyLinkOp(link);
    }
  }

  /**
   * Structural link insert/remove. A live link writes its 20 arc segments and
   * its texture texel; a removed link is zero-strength (hidden by the arc
   * shader) and its slot is returned to the pool.
   */
  applyLinkOp(op: LinkPatch): void {
    if (op.index < 0 || op.index >= this.linkCapacity) return;
    const index = op.index;
    if (op.op === "add") {
      if (op.reset) {
        this.arcData.fill(0, index * 4, index * 4 + 4);
        this.linkDimmed[index] = 0;
      }
      if (this.linkActive[index] === 0) {
        if (!this.linkPool.claim(index)) return;
        this.linkActive[index] = 1;
      }
      const strength = this.linkDimmed[index] ? -op.strength : op.strength;
      this.arcData[index * 4 + 3] = strength;
      if (op.arcs) this.uploadArcRange(index, op.arcs);
      else if (op.reset === undefined || op.reset) this.writeArcLink(index, op.a, op.b, op.type);
      this.uploadArcTexel(
        index,
        this.arcData[index * 4]!,
        this.arcData[index * 4 + 1]!,
        this.arcData[index * 4 + 2]!,
        strength,
      );
    } else {
      this.linkActive[index] = 0;
      this.linkPool.free(index);
      this.arcData[index * 4] = 0;
      this.arcData[index * 4 + 1] = 0;
      this.arcData[index * 4 + 2] = 0;
      this.arcData[index * 4 + 3] = 0;
      this.uploadArcTexel(index, 0, 0, 0, 0);
    }
  }

  private uploadStoryTexel(index: number, size: number, heat: number, updatedAt: number): void {
    const texel = texelOf(index, this.storyTexWidth);
    const pixel = this.pixel;
    pixel[0] = size;
    pixel[1] = heat;
    pixel[2] = updatedAt;
    pixel[3] = this.focusData[index * 4 + 3]!;
    this.gl.texSubImage2D(
      this.gl.TEXTURE_2D,
      0,
      texel.x,
      texel.y,
      1,
      1,
      this.gl.RGBA,
      this.gl.FLOAT,
      pixel,
    );
  }

  private uploadFocusTexel(index: number, state: number, prev: number, at: number): void {
    const texel = texelOf(index, this.storyTexWidth);
    const pixel = this.pixel;
    pixel[0] = state;
    pixel[1] = prev;
    pixel[2] = at;
    pixel[3] = this.focusData[index * 4 + 3]!;
    this.gl.texSubImage2D(
      this.gl.TEXTURE_2D,
      0,
      texel.x,
      texel.y,
      1,
      1,
      this.gl.RGBA,
      this.gl.FLOAT,
      pixel,
    );
  }

  private uploadArcTexel(
    index: number,
    state: number,
    prev: number,
    at: number,
    strength: number,
  ): void {
    const texel = texelOf(index, this.linkTexWidth);
    const pixel = this.arcPixel;
    pixel[0] = state;
    pixel[1] = prev;
    pixel[2] = at;
    pixel[3] = strength;
    this.gl.bindTexture(this.gl.TEXTURE_2D, this.arcFocusTex);
    this.gl.texSubImage2D(
      this.gl.TEXTURE_2D,
      0,
      texel.x,
      texel.y,
      1,
      1,
      this.gl.RGBA,
      this.gl.FLOAT,
      pixel,
    );
  }

  /** Build and upload the 20 segments for one runtime link. */
  private writeArcLink(index: number, aIndex: number, bIndex: number, type: number): void {
    const segments = this.arcSegments;
    const a: Vec2 = [this.storyMercX[aIndex]!, this.storyMercY[aIndex]!];
    const b: Vec2 = [this.storyMercX[bIndex]!, this.storyMercY[bIndex]!];
    const lngLatA = mercatorToLngLat(a[0], a[1]);
    const lngLatB = mercatorToLngLat(b[0], b[1]);
    const length = angularDistance(lngLatA, lngLatB) * EARTH_RADIUS_M;
    const startHi = new Float32Array(segments * 2);
    const startLo = new Float32Array(segments * 2);
    const endHi = new Float32Array(segments * 2);
    const endLo = new Float32Array(segments * 2);
    const startElev = new Float32Array(segments);
    const endElev = new Float32Array(segments);
    const t0 = new Float32Array(segments);
    const t1 = new Float32Array(segments);
    const link = new Float32Array(segments);
    const types = new Float32Array(segments);
    const lengths = new Float32Array(segments);
    let previous = a;
    let previousX = previous[0];
    let point = lngLatA;
    for (let segment = 0; segment < segments; segment++) {
      const tA = segment / segments;
      const tB = (segment + 1) / segments;
      const next = greatCirclePoint(lngLatA, lngLatB, tB);
      let mercB = lngLatToMercator(next[0], next[1]);
      while (mercB[0] - previousX > 0.5) mercB = [mercB[0] - 1, mercB[1]];
      while (mercB[0] - previousX < -0.5) mercB = [mercB[0] + 1, mercB[1]];
      const [startHiX, startLoX] = splitMercator(previousX);
      const [startHiY, startLoY] = splitMercator(previous[1]);
      const [endHiX, endLoX] = splitMercator(mercB[0]);
      const [endHiY, endLoY] = splitMercator(mercB[1]);
      startHi[segment * 2] = startHiX;
      startHi[segment * 2 + 1] = startHiY;
      startLo[segment * 2] = startLoX;
      startLo[segment * 2 + 1] = startLoY;
      endHi[segment * 2] = endHiX;
      endHi[segment * 2 + 1] = endHiY;
      endLo[segment * 2] = endLoX;
      endLo[segment * 2 + 1] = endLoY;
      startElev[segment] = arcElevation(lngLatA, lngLatB, tA);
      endElev[segment] = arcElevation(lngLatA, lngLatB, tB);
      t0[segment] = tA;
      t1[segment] = tB;
      link[segment] = index;
      types[segment] = type;
      lengths[segment] = length;
      previousX = mercB[0];
      previous = mercB;
      point = next;
    }
    void point;
    const gl = this.gl;
    const byteOffset = index * segments * 4;
    const upload = (buffer: WebGLBuffer, data: Float32Array, offset: number): void => {
      gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
      gl.bufferSubData(gl.ARRAY_BUFFER, offset, data);
    };
    upload(this.arcBuffers.startHi, startHi, byteOffset * 2);
    upload(this.arcBuffers.startLo, startLo, byteOffset * 2);
    upload(this.arcBuffers.endHi, endHi, byteOffset * 2);
    upload(this.arcBuffers.endLo, endLo, byteOffset * 2);
    upload(this.arcBuffers.startElev, startElev, byteOffset);
    upload(this.arcBuffers.endElev, endElev, byteOffset);
    upload(this.arcBuffers.t0, t0, byteOffset);
    upload(this.arcBuffers.t1, t1, byteOffset);
    upload(this.arcBuffers.link, link, byteOffset);
    upload(this.arcBuffers.type, types, byteOffset);
    upload(this.arcBuffers.length, lengths, byteOffset);
  }

  /** Write only the focus bytes; used by the main-thread focus mirror. */
  updateFocusChanges(
    storyChanges: FocusPatch["storyChanges"],
    linkChanges: FocusPatch["linkChanges"],
  ): void {
    const gl = this.gl;
    gl.bindTexture(gl.TEXTURE_2D, this.focusTex);
    const pixel = this.pixel;
    for (const change of storyChanges) {
      const i = change.index;
      this.focusData[i * 4] = change.state;
      this.focusData[i * 4 + 1] = change.prev;
      this.focusData[i * 4 + 2] = change.at;
      pixel[0] = change.state;
      pixel[1] = change.prev;
      pixel[2] = change.at;
      pixel[3] = this.focusData[i * 4 + 3]!;
      const texel = texelOf(i, this.storyTexWidth);
      gl.texSubImage2D(gl.TEXTURE_2D, 0, texel.x, texel.y, 1, 1, gl.RGBA, gl.FLOAT, pixel);
    }
    if (linkChanges.length > 0) {
      gl.bindTexture(gl.TEXTURE_2D, this.arcFocusTex);
      for (const change of linkChanges) {
        const i = change.index;
        this.arcData[i * 4] = change.state;
        this.arcData[i * 4 + 1] = change.prev;
        this.arcData[i * 4 + 2] = change.at;
        // strength stays at *4 + 3
        const texel = texelOf(i, this.linkTexWidth);
        this.arcPixel[0] = this.arcData[i * 4]!;
        this.arcPixel[1] = this.arcData[i * 4 + 1]!;
        this.arcPixel[2] = this.arcData[i * 4 + 2]!;
        this.arcPixel[3] = this.arcData[i * 4 + 3]!;
        gl.texSubImage2D(
          gl.TEXTURE_2D,
          0,
          texel.x,
          texel.y,
          1,
          1,
          gl.RGBA,
          gl.FLOAT,
          this.arcPixel,
        );
      }
    }
  }

  updateTethers(segments: TetherSegment[], rings: TetherRing[]): void {
    const count = Math.min(192, segments.length);
    const startHi = this.tetherScratch.startHi;
    const startLo = this.tetherScratch.startLo;
    const endHi = this.tetherScratch.endHi;
    const endLo = this.tetherScratch.endLo;
    const t0 = this.tetherScratch.t0;
    const t1 = this.tetherScratch.t1;
    segments.slice(0, count).forEach((segment, index) => {
      startHi[index * 2] = segment.startHi[0];
      startHi[index * 2 + 1] = segment.startHi[1];
      startLo[index * 2] = segment.startLo[0];
      startLo[index * 2 + 1] = segment.startLo[1];
      endHi[index * 2] = segment.endHi[0];
      endHi[index * 2 + 1] = segment.endHi[1];
      endLo[index * 2] = segment.endLo[0];
      endLo[index * 2 + 1] = segment.endLo[1];
      t0[index] = segment.t0;
      t1[index] = segment.t1;
    });
    if (!this.tetherBuffers || !this.ringBuffers) return;
    const gl = this.gl;
    gl.bindBuffer(gl.ARRAY_BUFFER, this.tetherBuffers.startHi);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, startHi, 0, count * 2);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.tetherBuffers.startLo);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, startLo, 0, count * 2);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.tetherBuffers.endHi);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, endHi, 0, count * 2);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.tetherBuffers.endLo);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, endLo, 0, count * 2);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.tetherBuffers.t0);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, t0, 0, count);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.tetherBuffers.t1);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, t1, 0, count);

    const ringCount = Math.min(6, rings.length);
    const ringHi = this.tetherScratch.ringHi;
    const ringLo = this.tetherScratch.ringLo;
    rings.slice(0, ringCount).forEach((ring, index) => {
      ringHi[index * 2] = ring.hi[0];
      ringHi[index * 2 + 1] = ring.hi[1];
      ringLo[index * 2] = ring.lo[0];
      ringLo[index * 2 + 1] = ring.lo[1];
    });
    gl.bindBuffer(gl.ARRAY_BUFFER, this.ringBuffers.hi);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, ringHi, 0, ringCount * 2);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.ringBuffers.lo);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, ringLo, 0, ringCount * 2);

    if (this.tetherMesh) this.tetherMesh.instanceCount = count;
    if (this.ringMesh) this.ringMesh.instanceCount = ringCount;
  }

  private writeRipples(ripples: StreamPatch["ripples"]): void {
    for (const ripple of ripples) {
      const slot = ripple.slot;
      this.rippleData.hi[slot * 2] = ripple.mercHi[0];
      this.rippleData.hi[slot * 2 + 1] = ripple.mercHi[1];
      this.rippleData.lo[slot * 2] = ripple.mercLo[0];
      this.rippleData.lo[slot * 2 + 1] = ripple.mercLo[1];
      this.rippleData.category[slot] = ripple.category;
      this.rippleData.spawnAt[slot] = ripple.spawnAt;
      this.rippleData.intensity[slot] = ripple.intensity;
      // The owner id is a shader input (off-thread muting): it must be
      // uploaded with the rest of the slot, not only stored on the CPU.
      this.rippleData.storyIndex[slot] = ripple.storyIndex;
      this.subUpload(this.rippleBuffers.hi, slot * 2, 2, this.rippleData.hi);
      this.subUpload(this.rippleBuffers.lo, slot * 2, 2, this.rippleData.lo);
      this.subUpload(this.rippleBuffers.category, slot, 1, this.rippleData.category);
      this.subUpload(this.rippleBuffers.spawnAt, slot, 1, this.rippleData.spawnAt);
      this.subUpload(this.rippleBuffers.intensity, slot, 1, this.rippleData.intensity);
      this.subUpload(this.rippleBuffers.storyIndex, slot, 1, this.rippleData.storyIndex);
    }
  }

  private createDotMesh(init: FakeInit): Mesh {
    const gl = this.gl;
    const stories = init.stories;
    const vao = gl.createVertexArray();
    if (!vao) throw new Error("VAO allocation failed");
    gl.bindVertexArray(vao);
    const corner = this.createBuffer(QUAD_STRIP, gl.STATIC_DRAW);
    this.attrib(corner, 4, 2, gl.FLOAT, false, 0, 0, 0);
    const hiData = new Float32Array(this.storyCapacity * 2);
    const loData = new Float32Array(this.storyCapacity * 2);
    hiData.set(stories.mercHi);
    loData.set(stories.mercLo);
    const colorData = new Uint8Array(this.storyCapacity * 4);
    colorData.set(stories.color);
    const hi = this.createBuffer(hiData, gl.STATIC_DRAW);
    const lo = this.createBuffer(loData, gl.STATIC_DRAW);
    const index = new Float32Array(this.storyCapacity);
    for (let i = 0; i < this.storyCapacity; i++) index[i] = i;
    const indexBuffer = this.createBuffer(index, gl.STATIC_DRAW);
    const color = this.createBuffer(colorData, gl.STATIC_DRAW);
    this.dotBuffers = { hi, lo, color, index: indexBuffer };
    this.attrib(hi, 0, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(lo, 1, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(indexBuffer, 2, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(color, 3, 3, gl.UNSIGNED_BYTE, true, 4, 0, 1);
    gl.bindVertexArray(null);
    return {
      vao,
      buffers: [corner, hi, lo, indexBuffer, color],
      instanceCount: this.storyCapacity,
    };
  }

  private createArcMesh(init: FakeInit): Mesh {
    const gl = this.gl;
    const arcs = init.arcs;
    const capacityInstances = this.linkCapacity * init.arcSegments;
    const vao = gl.createVertexArray();
    if (!vao) throw new Error("VAO allocation failed");
    gl.bindVertexArray(vao);
    const corner = this.createBuffer(QUAD_TRIANGLES, gl.STATIC_DRAW);
    this.attrib(corner, 10, 2, gl.FLOAT, false, 0, 0, 0);
    const padded = <T extends Float32Array | Uint16Array | Uint8Array>(
      source: T,
      size: number,
    ): T => {
      const out = new (source.constructor as new (length: number) => T)(size);
      out.set(source);
      return out;
    };
    const startHi = this.createBuffer(padded(arcs.startHi, capacityInstances * 2), gl.STATIC_DRAW);
    const startLo = this.createBuffer(padded(arcs.startLo, capacityInstances * 2), gl.STATIC_DRAW);
    const endHi = this.createBuffer(padded(arcs.endHi, capacityInstances * 2), gl.STATIC_DRAW);
    const endLo = this.createBuffer(padded(arcs.endLo, capacityInstances * 2), gl.STATIC_DRAW);
    const startElev = this.createBuffer(padded(arcs.startElev, capacityInstances), gl.STATIC_DRAW);
    const endElev = this.createBuffer(padded(arcs.endElev, capacityInstances), gl.STATIC_DRAW);
    const t0 = this.createBuffer(padded(arcs.t0, capacityInstances), gl.STATIC_DRAW);
    const t1 = this.createBuffer(padded(arcs.t1, capacityInstances), gl.STATIC_DRAW);
    const link = new Float32Array(capacityInstances);
    for (let i = 0; i < arcs.link.length; i++) link[i] = arcs.link[i]!;
    const linkBuffer = this.createBuffer(link, gl.STATIC_DRAW);
    const type = new Float32Array(capacityInstances);
    for (let i = 0; i < arcs.type.length; i++) type[i] = arcs.type[i]!;
    const typeBuffer = this.createBuffer(type, gl.STATIC_DRAW);
    const lengthBuffer = this.createBuffer(padded(arcs.length, capacityInstances), gl.STATIC_DRAW);

    this.attrib(startHi, 0, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(startLo, 1, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(endHi, 2, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(endLo, 3, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(startElev, 4, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(endElev, 5, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(t0, 6, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(t1, 7, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(linkBuffer, 8, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(typeBuffer, 9, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(lengthBuffer, 11, 1, gl.FLOAT, false, 0, 0, 1);
    this.arcBuffers = {
      startHi,
      startLo,
      endHi,
      endLo,
      startElev,
      endElev,
      t0,
      t1,
      link: linkBuffer,
      type: typeBuffer,
      length: lengthBuffer,
    };
    gl.bindVertexArray(null);
    return {
      vao,
      buffers: [
        corner,
        startHi,
        startLo,
        endHi,
        endLo,
        startElev,
        endElev,
        t0,
        t1,
        linkBuffer,
        typeBuffer,
        lengthBuffer,
      ],
      instanceCount: capacityInstances,
    };
  }

  private createTetherMesh(buffers: TetherBuffers): Mesh {
    const gl = this.gl;
    const vao = gl.createVertexArray();
    if (!vao) throw new Error("VAO allocation failed");
    gl.bindVertexArray(vao);
    const corner = this.createBuffer(QUAD_TRIANGLES, gl.STATIC_DRAW);
    this.attrib(corner, 6, 2, gl.FLOAT, false, 0, 0, 0);
    this.attrib(buffers.startHi, 0, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(buffers.startLo, 1, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(buffers.endHi, 2, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(buffers.endLo, 3, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(buffers.t0, 4, 1, gl.FLOAT, false, 0, 0, 1);
    this.attrib(buffers.t1, 5, 1, gl.FLOAT, false, 0, 0, 1);
    gl.bindVertexArray(null);
    return {
      vao,
      buffers: [corner, ...Object.values(buffers)],
      instanceCount: 0,
    };
  }

  private createRingMesh(buffers: RingBuffers): Mesh {
    const gl = this.gl;
    const vao = gl.createVertexArray();
    if (!vao) throw new Error("VAO allocation failed");
    gl.bindVertexArray(vao);
    const corner = this.createBuffer(QUAD_STRIP, gl.STATIC_DRAW);
    this.attrib(corner, 2, 2, gl.FLOAT, false, 0, 0, 0);
    this.attrib(buffers.hi, 0, 2, gl.FLOAT, false, 0, 0, 1);
    this.attrib(buffers.lo, 1, 2, gl.FLOAT, false, 0, 0, 1);
    gl.bindVertexArray(null);
    return { vao, buffers: [corner, buffers.hi, buffers.lo], instanceCount: 0 };
  }

  private createBuffer(data: ArrayBufferView, usage: number): WebGLBuffer {
    const gl = this.gl;
    const buffer = gl.createBuffer();
    if (!buffer) throw new Error("buffer allocation failed");
    gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
    gl.bufferData(gl.ARRAY_BUFFER, data as ArrayBufferView<ArrayBuffer>, usage);
    return buffer;
  }

  private subUpload(buffer: WebGLBuffer, offset: number, length: number, data: Float32Array): void {
    const gl = this.gl;
    gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
    gl.bufferSubData(
      gl.ARRAY_BUFFER,
      offset * Float32Array.BYTES_PER_ELEMENT,
      data.subarray(offset, offset + length),
    );
  }

  private attrib(
    buffer: WebGLBuffer,
    location: number,
    size: number,
    type: number,
    normalized: boolean,
    stride: number,
    offset: number,
    divisor: number,
  ): void {
    const gl = this.gl;
    gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
    gl.enableVertexAttribArray(location);
    gl.vertexAttribPointer(location, size, type, normalized, stride, offset);
    gl.vertexAttribDivisor(location, divisor);
  }

  /** Draw/pick occupancy is capacity-sized; empty slots are shader-hidden. */
  get counts(): { stories: number; links: number } {
    return { stories: this.storyCapacity, links: this.linkCapacity };
  }

  get textureWidths(): { story: number; link: number } {
    return { story: this.storyTexWidth, link: this.linkTexWidth };
  }
}
