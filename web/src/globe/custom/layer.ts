import type { StoryPatch, LinkPatch, MatchBytes } from "../../worker/messages";
import type {
  CustomLayerInterface,
  CustomRenderMethodInput,
  Map as MapLibreMap,
} from "maplibre-gl";
import { CATEGORY_COLORS, DEFAULT_STRENGTH_FLOOR, LINK_COLORS } from "../encoding";
import { choosePick, decodePickId, type PickHit } from "../pick";
import { lngLatToMercator, matrixTranslate, splitMercator } from "../mercator";
import type { FakeInit, FocusPatch, StreamPatch } from "../../worker/fake/protocol";
import { GlobeGpuData } from "./buffers";
import {
  ARC_FRAG,
  ARC_VERT,
  DOT_FRAG,
  DOT_VERT,
  RING_FRAG,
  RING_VERT,
  RIPPLE_FRAG,
  RIPPLE_VERT,
  TETHER_FRAG,
  TETHER_VERT,
  PICK_FRAG,
  PICK_VERT,
  PICK_ARC_FRAG,
  PICK_ARC_VERT,
} from "./shaders";

type ProgramInfo = {
  program: WebGLProgram;
  uniforms: Map<string, WebGLUniformLocation | null>;
};

const IDENTITY = new Float32Array([1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1]);

function compile(
  gl: WebGL2RenderingContext,
  type: number,
  source: string,
  label: string,
): WebGLShader {
  const shader = gl.createShader(type);
  if (!shader) throw new Error(`shader allocation failed (${label})`);
  gl.shaderSource(shader, source);
  gl.compileShader(shader);
  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
    const log = gl.getShaderInfoLog(shader) ?? "unknown error";
    gl.deleteShader(shader);
    throw new Error(`shader compile failed (${label}): ${log}`);
  }
  return shader;
}

function createProgram(
  gl: WebGL2RenderingContext,
  vertex: string,
  fragment: string,
  label: string,
  uniformNames: string[],
): ProgramInfo {
  const program = gl.createProgram();
  if (!program) throw new Error("program allocation failed");
  const vs = compile(gl, gl.VERTEX_SHADER, vertex, `${label}.vert`);
  const fs = compile(gl, gl.FRAGMENT_SHADER, fragment, `${label}.frag`);
  gl.attachShader(program, vs);
  gl.attachShader(program, fs);
  gl.linkProgram(program);
  gl.deleteShader(vs);
  gl.deleteShader(fs);
  if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
    const log = gl.getProgramInfoLog(program) ?? "unknown error";
    gl.deleteProgram(program);
    throw new Error(`program link failed (${label}): ${log}`);
  }
  const uniforms = new Map<string, WebGLUniformLocation | null>();
  for (const name of uniformNames) uniforms.set(name, gl.getUniformLocation(program, name));
  return { program, uniforms };
}

function withPrelude(body: string, shaderData: CustomRenderMethodInput["shaderData"]): string {
  const marker = "#version 300 es";
  const index = body.indexOf(marker);
  if (index < 0)
    return `${marker}\n${shaderData.vertexShaderPrelude}\n${shaderData.define}\n${body}`;
  const afterVersion = index + marker.length;
  return `${body.slice(0, afterVersion)}\n${shaderData.vertexShaderPrelude}\n${shaderData.define}\n${body.slice(afterVersion)}`;
}

const COMMON_UNIFORMS = [
  "u_projection_matrix",
  "u_projection_tile_mercator_coords",
  "u_projection_clipping_plane",
  "u_projection_transition",
  "u_projection_fallback_matrix",
  "uGlobe",
  "uZoom",
  "uAnchorMatrix",
  "uAnchorHi",
  "uAnchorLo",
  "uViewport",
  "uTime",
  "uHasFocus",
  "uFloor",
  "uStoryTexWidth",
  "uLinkTexWidth",
  "uDropGlow",
  "uDropPulses",
  "uDropThematic",
  "uHeatOn",
];

/**
 * The custom WebGL path: one instanced draw per primitive, static attributes
 * in GPU buffers, focus/heat/strength in small float textures, MapLibre's
 * projection prelude for globe geometry and a double-precision anchored
 * matrix at street zoom.
 */
export class CustomGlobeLayer implements CustomLayerInterface {
  readonly id = "dor-custom-globe";
  readonly type = "custom" as const;
  readonly renderingMode = "2d" as const;

  private gl: WebGL2RenderingContext | null = null;
  private map: MapLibreMap | null = null;
  private data: GlobeGpuData | null = null;
  private init: FakeInit | null = null;
  private hasFocus = false;
  private floor = DEFAULT_STRENGTH_FLOOR;
  private linesVisible = true;
  private tethersVisible = true;
  private heatVisible = true;
  private quality = 0;

  private dotProgram: ProgramInfo | null = null;
  private arcProgram: ProgramInfo | null = null;
  private rippleProgram: ProgramInfo | null = null;
  private tetherProgram: ProgramInfo | null = null;
  private ringProgram: ProgramInfo | null = null;
  private pickProgram: ProgramInfo | null = null;
  private pickArcProgram: ProgramInfo | null = null;

  private pickFramebuffer: WebGLFramebuffer | null = null;
  private pickTexture: WebGLTexture | null = null;
  private pickWidth = 0;
  private pickHeight = 0;
  private readonly pickPixel = new Uint8Array(4);
  private pickPassCount = 0;
  private pickReadCount = 0;

  private readonly renderState = {
    main: new Float32Array(16),
    fallback: new Float32Array(16),
    anchor: new Float32Array(16),
    tile: new Float32Array(4),
    clip: new Float32Array(4),
    transition: 0,
    globe: true,
    zoom: 0,
    time: 0,
    hasFocus: 0,
    floor: DEFAULT_STRENGTH_FLOOR,
    storyTexWidth: 2048,
    linkTexWidth: 2048,
    dropGlow: 0,
    dropPulses: 0,
    dropThematic: 0,
    heatOn: 1,
    anchorHiX: 0,
    anchorHiY: 0,
    anchorLoX: 0,
    anchorLoY: 0,
    width: 1,
    height: 1,
  };
  private readonly categoryColors = new Float32Array(11 * 3);
  private readonly linkColors = new Float32Array(3 * 3);

  private lastGlobe = true;
  private variantName: string | null = null;
  private frames = 0;
  private globeFrames = 0;
  private mercatorFrames = 0;
  /** Freezes animation for still screenshots. */
  timeOverride: number | null = null;

  onAdd(map: MapLibreMap, gl: WebGLRenderingContext | WebGL2RenderingContext): void {
    if (!(gl instanceof WebGL2RenderingContext)) {
      throw new Error("Dor globe custom layer needs WebGL2");
    }
    this.map = map;
    this.gl = gl;
    // A restored context hands us a new GL object: no program, VAO, texture or
    // framebuffer from the old one is valid. Rebuild them lazily on the next
    // frame, and rebuild the GPU data from the retained init if we have one.
    this.variantName = null;
    this.disposePrograms();
    this.pickFramebuffer = null;
    this.pickTexture = null;
    this.pickWidth = 0;
    this.pickHeight = 0;
    this.data?.dispose();
    this.data = null;
    for (let i = 0; i < 11; i++) {
      const rgb = CATEGORY_COLORS[i] ?? CATEGORY_COLORS[0]!;
      this.categoryColors[i * 3] = rgb[0] / 255;
      this.categoryColors[i * 3 + 1] = rgb[1] / 255;
      this.categoryColors[i * 3 + 2] = rgb[2] / 255;
    }
    for (let i = 0; i < 3; i++) {
      const rgb = LINK_COLORS[i] ?? LINK_COLORS[0]!;
      this.linkColors[i * 3] = rgb[0] / 255;
      this.linkColors[i * 3 + 1] = rgb[1] / 255;
      this.linkColors[i * 3 + 2] = rgb[2] / 255;
    }
    if (this.init) this.data = new GlobeGpuData(gl, this.init);
  }

  /**
   * Programs are built on the first frame because MapLibre hands us the
   * projection prelude (and its variant name) in the render arguments.
   */
  private ensurePrograms(
    gl: WebGL2RenderingContext,
    shaderData: CustomRenderMethodInput["shaderData"],
  ): void {
    if (this.variantName === shaderData.variantName && this.dotProgram) return;
    this.disposePrograms();
    this.variantName = shaderData.variantName;
    const vertex = (body: string): string => withPrelude(body, shaderData);
    this.dotProgram = createProgram(gl, vertex(DOT_VERT), DOT_FRAG, "dots", [
      ...COMMON_UNIFORMS,
      "uStoryTex",
      "uFocusTex",
    ]);
    this.arcProgram = createProgram(gl, vertex(ARC_VERT), ARC_FRAG, "arcs", [
      ...COMMON_UNIFORMS,
      "uArcFocusTex",
      "uLinkColors[0]",
    ]);
    this.rippleProgram = createProgram(gl, vertex(RIPPLE_VERT), RIPPLE_FRAG, "ripples", [
      ...COMMON_UNIFORMS,
      "uCategoryColors[0]",
      "uFocusTex",
    ]);
    this.tetherProgram = createProgram(gl, vertex(TETHER_VERT), TETHER_FRAG, "tethers", [
      ...COMMON_UNIFORMS,
    ]);
    this.ringProgram = createProgram(gl, vertex(RING_VERT), RING_FRAG, "rings", [
      ...COMMON_UNIFORMS,
    ]);
    this.pickProgram = createProgram(gl, vertex(PICK_VERT), PICK_FRAG, "pick", [
      ...COMMON_UNIFORMS,
      "uStoryTex",
    ]);
    this.pickArcProgram = createProgram(gl, vertex(PICK_ARC_VERT), PICK_ARC_FRAG, "pick-arcs", [
      ...COMMON_UNIFORMS,
      "uArcFocusTex",
    ]);
  }

  private disposePrograms(): void {
    const gl = this.gl;
    if (!gl) return;
    for (const info of [
      this.dotProgram,
      this.arcProgram,
      this.rippleProgram,
      this.tetherProgram,
      this.ringProgram,
      this.pickProgram,
      this.pickArcProgram,
    ]) {
      if (info) gl.deleteProgram(info.program);
    }
    this.dotProgram = null;
    this.arcProgram = null;
    this.rippleProgram = null;
    this.tetherProgram = null;
    this.ringProgram = null;
    this.pickProgram = null;
    this.pickArcProgram = null;
  }

  onRemove(): void {
    this.disposePrograms();
    this.data?.dispose();
    this.data = null;
    this.disposePickTarget();
    // Drop the dead context reference. setInit() may still arrive while the
    // layer is detached; onAdd() rebuilds from the retained init later.
    this.gl = null;
    this.variantName = null;
  }

  /** Called when the canvas reports context loss, before the style is torn down. */
  handleContextLost(): void {
    this.disposePrograms();
    this.data?.dispose();
    this.data = null;
    this.pickFramebuffer = null;
    this.pickTexture = null;
    this.pickWidth = 0;
    this.pickHeight = 0;
    this.gl = null;
    this.variantName = null;
  }

  setInit(init: FakeInit): void {
    // Retain the init even while the context is lost so onAdd can rebuild the
    // GPU data when MapLibre hands us the restored context.
    this.init = init;
    if (!this.gl) return;
    this.data?.dispose();
    this.data = new GlobeGpuData(this.gl, init);
  }

  /**
   * Pick under a CSS pixel. The id pass is *not* drawn per frame: both passes
   * are rendered lazily here, on the click, then a single pixel is read back.
   * Dots are read first and win over arcs, which is what "click a dot focuses
   * its story" means when a link crosses the dot.
   */
  pickNow(clientX: number, clientY: number): PickHit | null {
    if (!this.gl || !this.data || !this.map) return null;
    const story = this.pickPass(clientX, clientY, (gl) => this.renderStoryPickPass(gl));
    if (story !== null) return choosePick(story, null);
    const link = this.pickPass(clientX, clientY, (gl) => this.renderArcPickPass(gl));
    return choosePick(null, link);
  }

  private pickPass(
    clientX: number,
    clientY: number,
    draw: (gl: WebGL2RenderingContext) => void,
  ): number | null {
    const gl = this.gl;
    const map = this.map;
    if (!gl || !map) return null;
    const width = gl.drawingBufferWidth;
    const height = gl.drawingBufferHeight;
    this.ensurePickTarget(width, height);
    if (!this.pickFramebuffer) return null;
    gl.bindFramebuffer(gl.FRAMEBUFFER, this.pickFramebuffer);
    gl.viewport(0, 0, width, height);
    gl.disable(gl.BLEND);
    gl.disable(gl.DEPTH_TEST);
    gl.disable(gl.SCISSOR_TEST);
    gl.colorMask(true, true, true, true);
    gl.clearColor(0, 0, 0, 0);
    gl.clear(gl.COLOR_BUFFER_BIT);
    draw(gl);
    gl.bindVertexArray(null);
    this.pickPassCount++;
    const canvas = map.getCanvas();
    const cssWidth = canvas.clientWidth || 1;
    const dpr = width / cssWidth;
    const pixelX = Math.max(0, Math.min(width - 1, Math.floor(clientX * dpr)));
    const pixelY = Math.max(0, Math.min(height - 1, height - 1 - Math.floor(clientY * dpr)));
    gl.readPixels(pixelX, pixelY, 1, 1, gl.RGBA, gl.UNSIGNED_BYTE, this.pickPixel);
    this.pickReadCount++;
    gl.bindFramebuffer(gl.FRAMEBUFFER, null);
    gl.viewport(0, 0, width, height);
    const id = decodePickId(this.pickPixel[0]!, this.pickPixel[1]!, this.pickPixel[2]!);
    return id > 0 ? id - 1 : null;
  }

  private renderStoryPickPass(gl: WebGL2RenderingContext): void {
    const data = this.data;
    const pick = this.pickProgram;
    if (!data || !pick) return;
    gl.useProgram(pick.program);
    this.applyCommon(gl, pick);
    gl.activeTexture(gl.TEXTURE0);
    gl.bindTexture(gl.TEXTURE_2D, data.storyTex);
    const storyLocation = pick.uniforms.get("uStoryTex");
    if (storyLocation) gl.uniform1i(storyLocation, 0);
    gl.bindVertexArray(data.dotMesh.vao);
    gl.drawArraysInstanced(gl.TRIANGLE_STRIP, 0, 4, data.counts.stories);
    gl.bindVertexArray(null);
  }

  private renderArcPickPass(gl: WebGL2RenderingContext): void {
    const data = this.data;
    const pick = this.pickArcProgram;
    if (!data || !pick || !this.linesVisible) return;
    gl.useProgram(pick.program);
    this.applyCommon(gl, pick);
    gl.activeTexture(gl.TEXTURE0);
    gl.bindTexture(gl.TEXTURE_2D, data.arcFocusTex);
    const linkLocation = pick.uniforms.get("uArcFocusTex");
    if (linkLocation) gl.uniform1i(linkLocation, 0);
    gl.bindVertexArray(data.arcMesh.vao);
    gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, data.arcMesh.instanceCount);
    gl.bindVertexArray(null);
  }

  private ensurePickTarget(width: number, height: number): void {
    const gl = this.gl;
    if (!gl) return;
    if (this.pickFramebuffer && this.pickWidth === width && this.pickHeight === height) return;
    this.disposePickTarget();
    const texture = gl.createTexture();
    const framebuffer = gl.createFramebuffer();
    if (!texture || !framebuffer) return;
    gl.bindTexture(gl.TEXTURE_2D, texture);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, width, height, 0, gl.RGBA, gl.UNSIGNED_BYTE, null);
    gl.bindFramebuffer(gl.FRAMEBUFFER, framebuffer);
    gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, texture, 0);
    gl.bindFramebuffer(gl.FRAMEBUFFER, null);
    this.pickTexture = texture;
    this.pickFramebuffer = framebuffer;
    this.pickWidth = width;
    this.pickHeight = height;
  }

  private disposePickTarget(): void {
    const gl = this.gl;
    if (!gl) return;
    if (this.pickFramebuffer) gl.deleteFramebuffer(this.pickFramebuffer);
    if (this.pickTexture) gl.deleteTexture(this.pickTexture);
    this.pickFramebuffer = null;
    this.pickTexture = null;
    this.pickWidth = 0;
    this.pickHeight = 0;
  }

  private applyCommon(gl: WebGL2RenderingContext, info: ProgramInfo): void {
    const state = this.renderState;
    const set = (name: string): WebGLUniformLocation | null => info.uniforms.get(name) ?? null;
    const main = set("u_projection_matrix");
    if (main) gl.uniformMatrix4fv(main, false, state.main);
    const tile = set("u_projection_tile_mercator_coords");
    if (tile) gl.uniform4fv(tile, state.tile);
    const clip = set("u_projection_clipping_plane");
    if (clip) gl.uniform4fv(clip, state.clip);
    const transition = set("u_projection_transition");
    if (transition) gl.uniform1f(transition, state.transition);
    const fallback = set("u_projection_fallback_matrix");
    if (fallback) gl.uniformMatrix4fv(fallback, false, state.fallback);
    const globe = set("uGlobe");
    if (globe) gl.uniform1f(globe, state.globe ? 1 : 0);
    const zoom = set("uZoom");
    if (zoom) gl.uniform1f(zoom, state.zoom);
    const anchor = set("uAnchorMatrix");
    if (anchor) gl.uniformMatrix4fv(anchor, false, state.anchor);
    const anchorHi = set("uAnchorHi");
    if (anchorHi) gl.uniform2f(anchorHi, state.anchorHiX, state.anchorHiY);
    const anchorLo = set("uAnchorLo");
    if (anchorLo) gl.uniform2f(anchorLo, state.anchorLoX, state.anchorLoY);
    const viewport = set("uViewport");
    if (viewport) gl.uniform2f(viewport, state.width, state.height);
    const time = set("uTime");
    if (time) gl.uniform1f(time, state.time);
    const hasFocus = set("uHasFocus");
    if (hasFocus) gl.uniform1f(hasFocus, state.hasFocus);
    const floor = set("uFloor");
    if (floor) gl.uniform1f(floor, state.floor);
    const storyWidth = set("uStoryTexWidth");
    if (storyWidth) gl.uniform1f(storyWidth, state.storyTexWidth);
    const linkWidth = set("uLinkTexWidth");
    if (linkWidth) gl.uniform1f(linkWidth, state.linkTexWidth);
    const dropGlow = set("uDropGlow");
    if (dropGlow) gl.uniform1f(dropGlow, state.dropGlow);
    const dropPulses = set("uDropPulses");
    if (dropPulses) gl.uniform1f(dropPulses, state.dropPulses);
    const dropThematic = set("uDropThematic");
    if (dropThematic) gl.uniform1f(dropThematic, state.dropThematic);
    const heatOn = set("uHeatOn");
    if (heatOn) gl.uniform1f(heatOn, state.heatOn);
  }

  storyAnimation(index: number) {
    return this.data?.storyAnimation(index);
  }

  ensureCapacity(stories: number, links: number): void {
    this.data?.ensureCapacity(stories, links);
  }
  updateMatches(stories: MatchBytes, links: MatchBytes): void {
    this.data?.updateMatches(stories, links);
  }

  stream(patch: StreamPatch): void {
    this.data?.updateStream(patch);
  }

  /** Runtime structural changes, applied to geometry, textures and pools. */
  applyLinkOp(op: LinkPatch): void {
    this.data?.applyLinkOp(op);
  }

  applyStoryOp(op: StoryPatch): void {
    this.data?.applyStoryOp(op);
  }

  /** Write the focus bytes synchronously (main-thread mirror). */
  writeFocus(
    storyChanges: FocusPatch["storyChanges"],
    linkChanges: FocusPatch["linkChanges"],
    focusActive: boolean,
  ): void {
    this.hasFocus = focusActive;
    this.data?.updateFocusChanges(storyChanges, linkChanges);
  }

  /** Worker-followed data: surface tethers for the focused story. */
  applyTethers(segments: FocusPatch["tethers"], rings: FocusPatch["tetherRings"]): void {
    this.data?.updateTethers(segments, rings);
  }

  setStrengthFloor(floor: number): void {
    this.floor = floor;
    this.map?.triggerRepaint();
  }

  setLinesVisible(visible: boolean): void {
    this.linesVisible = visible;
    this.map?.triggerRepaint();
  }

  setTethersVisible(visible: boolean): void {
    this.tethersVisible = visible;
    this.map?.triggerRepaint();
  }

  setHeatVisible(visible: boolean): void {
    this.heatVisible = visible;
    this.map?.triggerRepaint();
  }

  /** 0 = full, 1 = drop glow, 2 = drop pulses, 3 = drop thematic arcs. */
  setQuality(level: number): void {
    this.quality = Math.max(0, Math.min(3, Math.round(level)));
    this.map?.triggerRepaint();
  }

  /**
   * On-screen projection check against the attributes that were actually
   * uploaded. `mercHi/Lo` are read from the init arrays (the same values
   * passed to the dot buffers) and every operation is emulated in float32.
   * The reference is MapLibre's project() on the canonical Float64 lng/lat,
   * which is also what labels and camera fits use.
   */
  precisionCheck(samples = 5000): {
    maxPx: number;
    meanPx: number;
    samples: number;
    sampled: number;
    zoom: number;
    globe: boolean;
    attributeSource: string;
    referenceSource: string;
    debug?: unknown;
  } {
    const map = this.map;
    const init = this.init;
    const state = this.renderState;
    if (!map || !init || state.globe) {
      return {
        maxPx: 0,
        meanPx: 0,
        samples: 0,
        sampled: 0,
        zoom: map?.getZoom() ?? 0,
        globe: state.globe,
        attributeSource: "not available on globe",
        referenceSource: "canonical Float64 lng/lat",
      };
    }
    const width = state.width;
    const height = state.height;
    const m = state.anchor;
    const f = Math.fround;
    let maxPx = 0;
    let sumPx = 0;
    let count = 0;
    let sampled = 0;
    let debug: unknown = null;
    const step = Math.max(1, Math.floor(init.storyCount / samples));
    for (let i = 0; i < init.storyCount; i += step) {
      sampled++;
      const lng = init.stories.lng[i]!;
      const lat = init.stories.lat[i]!;
      // Actual uploaded attributes, not a freshly recomputed coordinate.
      const hiX = init.stories.mercHi[i * 2]!;
      const loX = init.stories.mercLo[i * 2]!;
      const hiY = init.stories.mercHi[i * 2 + 1]!;
      const loY = init.stories.mercLo[i * 2 + 1]!;
      const dx = f(f(hiX - f(state.anchorHiX)) + f(loX - f(state.anchorLoX)));
      const dy = f(f(hiY - f(state.anchorHiY)) + f(loY - f(state.anchorLoY)));
      // Emulate float32 MADs in the anchor-matrix multiply.
      const clipX = f(f(f(m[0]! * dx) + f(m[4]! * dy)) + m[12]!);
      const clipY = f(f(f(m[1]! * dx) + f(m[5]! * dy)) + m[13]!);
      const clipW = f(f(f(m[3]! * dx) + f(m[7]! * dy)) + m[15]!);
      if (clipW <= 1e-6) continue;
      const screenX = f((f(clipX / clipW) * 0.5 + 0.5) * width);
      const screenY = f((1 - (f(clipY / clipW) * 0.5 + 0.5)) * height);
      if (screenX < 0 || screenX > width || screenY < 0 || screenY > height) continue;
      const reference = map.project([lng, lat]);
      if (reference.x < 0 || reference.x > width || reference.y < 0 || reference.y > height) {
        continue;
      }
      const error = Math.hypot(screenX - reference.x, screenY - reference.y);
      if (debug === null) {
        debug = {
          lng,
          lat,
          hiX,
          loX,
          hiY,
          loY,
          dx,
          dy,
          clipX,
          clipY,
          clipW,
          screenX,
          screenY,
          reference: [reference.x, reference.y],
          matrix: Array.from(m),
        };
      }
      maxPx = Math.max(maxPx, error);
      sumPx += error;
      count++;
    }
    return {
      maxPx,
      meanPx: count > 0 ? sumPx / count : 0,
      samples: count,
      sampled,
      zoom: map.getZoom(),
      globe: state.globe,
      attributeSource: "uploaded mercHi/mercLo, float32 emulation",
      referenceSource: "MapLibre project() on canonical Float64 lng/lat",
      debug,
    };
  }

  get stats(): {
    frames: number;
    globeFrames: number;
    mercatorFrames: number;
    globe: boolean;
    pickPasses: number;
    pickReads: number;
  } {
    return {
      frames: this.frames,
      globeFrames: this.globeFrames,
      mercatorFrames: this.mercatorFrames,
      globe: this.lastGlobe,
      pickPasses: this.pickPassCount,
      pickReads: this.pickReadCount,
    };
  }

  render(gl: WebGLRenderingContext | WebGL2RenderingContext, args: CustomRenderMethodInput): void {
    if (!(gl instanceof WebGL2RenderingContext)) return;
    const data = this.data;
    if (!data || !this.map) return;
    if (!this.dotProgram || this.variantName !== args.shaderData.variantName) {
      this.ensurePrograms(gl, args.shaderData);
    }

    this.frames += 1;
    const projection = args.defaultProjectionData;
    const state = this.renderState;
    const globe = projection.projectionTransition > 0;
    this.lastGlobe = globe;
    state.globe = globe;
    if (globe) this.globeFrames += 1;
    else this.mercatorFrames += 1;

    const center = this.map.getCenter();
    const [mercX, mercY] = lngLatToMercator(center.lng, center.lat);
    const [anchorHiX, anchorLoX] = splitMercator(mercX);
    const [anchorHiY, anchorLoY] = splitMercator(mercY);
    if (!globe) {
      matrixTranslate(projection.mainMatrix as ArrayLike<number>, mercX, mercY, 0, state.anchor);
    } else {
      state.anchor.set(IDENTITY);
    }
    const canvas = this.map.getCanvas();
    state.width = canvas.clientWidth || gl.drawingBufferWidth || 1;
    state.height = canvas.clientHeight || gl.drawingBufferHeight || 1;
    state.anchorHiX = anchorHiX;
    state.anchorHiY = anchorHiY;
    state.anchorLoX = anchorLoX;
    state.anchorLoY = anchorLoY;
    state.main.set(projection.mainMatrix);
    state.fallback.set(projection.fallbackMatrix);
    state.tile.set(projection.tileMercatorCoords);
    state.clip.set(projection.clippingPlane);
    state.transition = projection.projectionTransition;
    state.zoom = this.map.getZoom();
    state.time = this.timeOverride ?? (Date.now() - (this.init?.epochMs ?? 0)) / 1000;
    state.hasFocus = this.hasFocus ? 1 : 0;
    state.floor = this.floor;
    const widths = data.textureWidths;
    state.storyTexWidth = widths.story;
    state.linkTexWidth = widths.link;
    state.dropGlow = this.quality >= 1 ? 1 : 0;
    state.dropPulses = this.quality >= 2 ? 1 : 0;
    state.dropThematic = this.quality >= 3 ? 1 : 0;
    state.heatOn = this.heatVisible ? 1 : 0;

    gl.enable(gl.BLEND);
    gl.blendFuncSeparate(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA, gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
    gl.disable(gl.CULL_FACE);
    gl.disable(gl.STENCIL_TEST);
    // Our primitives are screen-space and use MapLibre's globe clipping; the
    // depth test would only z-fight with the globe surface.
    gl.disable(gl.DEPTH_TEST);

    if (this.rippleProgram && data.rippleMesh.instanceCount > 0) {
      gl.useProgram(this.rippleProgram.program);
      this.applyCommon(gl, this.rippleProgram);
      const categoryLocation = this.rippleProgram.uniforms.get("uCategoryColors[0]");
      if (categoryLocation) gl.uniform3fv(categoryLocation, this.categoryColors);
      gl.activeTexture(gl.TEXTURE0);
      gl.bindTexture(gl.TEXTURE_2D, data.focusTex);
      const focusLocation = this.rippleProgram.uniforms.get("uFocusTex");
      if (focusLocation) gl.uniform1i(focusLocation, 0);
      gl.bindVertexArray(data.rippleMesh.vao);
      gl.drawArraysInstanced(gl.TRIANGLE_STRIP, 0, 4, data.rippleMesh.instanceCount);
    }

    if (
      this.tethersVisible &&
      this.tetherProgram &&
      data.tetherMesh &&
      data.tetherMesh.instanceCount > 0
    ) {
      gl.useProgram(this.tetherProgram.program);
      this.applyCommon(gl, this.tetherProgram);
      gl.bindVertexArray(data.tetherMesh.vao);
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, data.tetherMesh.instanceCount);
    }
    if (
      this.tethersVisible &&
      this.ringProgram &&
      data.ringMesh &&
      data.ringMesh.instanceCount > 0
    ) {
      gl.useProgram(this.ringProgram.program);
      this.applyCommon(gl, this.ringProgram);
      gl.bindVertexArray(data.ringMesh.vao);
      gl.drawArraysInstanced(gl.TRIANGLE_STRIP, 0, 4, data.ringMesh.instanceCount);
    }

    if (this.linesVisible && this.arcProgram) {
      gl.useProgram(this.arcProgram.program);
      this.applyCommon(gl, this.arcProgram);
      gl.activeTexture(gl.TEXTURE0);
      gl.bindTexture(gl.TEXTURE_2D, data.arcFocusTex);
      const textureLocation = this.arcProgram.uniforms.get("uArcFocusTex");
      if (textureLocation) gl.uniform1i(textureLocation, 0);
      const linkColorLocation = this.arcProgram.uniforms.get("uLinkColors[0]");
      if (linkColorLocation) gl.uniform3fv(linkColorLocation, this.linkColors);
      gl.bindVertexArray(data.arcMesh.vao);
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, data.arcMesh.instanceCount);
    }

    if (this.dotProgram) {
      gl.useProgram(this.dotProgram.program);
      this.applyCommon(gl, this.dotProgram);
      gl.activeTexture(gl.TEXTURE0);
      gl.bindTexture(gl.TEXTURE_2D, data.storyTex);
      const storyLocation = this.dotProgram.uniforms.get("uStoryTex");
      if (storyLocation) gl.uniform1i(storyLocation, 0);
      gl.activeTexture(gl.TEXTURE1);
      gl.bindTexture(gl.TEXTURE_2D, data.focusTex);
      const focusLocation = this.dotProgram.uniforms.get("uFocusTex");
      if (focusLocation) gl.uniform1i(focusLocation, 1);
      gl.bindVertexArray(data.dotMesh.vao);
      gl.drawArraysInstanced(gl.TRIANGLE_STRIP, 0, 4, data.dotMesh.instanceCount);
      gl.activeTexture(gl.TEXTURE0);
    }

    gl.bindVertexArray(null);
  }
}
