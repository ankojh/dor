/* eslint-disable @typescript-eslint/no-explicit-any -- deck.gl v9 shader injections, binary attribute data and accessor props are untyped */
import { MapboxOverlay } from "@deck.gl/mapbox";
import { ArcLayer, ScatterplotLayer } from "@deck.gl/layers";
import type { Layer } from "@deck.gl/core";
import type { Map as MapLibreMap } from "maplibre-gl";
import {
  ARC_MIN_STRENGTH,
  ARC_PULSE_1,
  ARC_PULSE_2,
  ARC_WIDTH_MIN,
  DEFAULT_STRENGTH_FLOOR,
  FOCUS_AMBIENT_ALPHA,
  FOCUS_FADE_SECONDS,
  FOCUS_FOCUSED_ALPHA,
  FOCUS_LINKED_ALPHA,
  FOCUS_OFF_THREAD_ALPHA,
  FOCUS_SECOND_ALPHA,
  dotSizeFactor,
} from "../encoding";
import type { FakeInit, FocusPatch, StreamPatch } from "../../worker/fake/protocol";

/** Shared clock + mode uniforms for the deck.gl shader injections. */
const clock = { time: 0, focusActive: 0, floor: DEFAULT_STRENGTH_FLOOR };

const CLOCK_MODULE = {
  name: "dorClock",
  vs: `layout(std140) uniform dorClockUniforms {
  float time;
  float focusActive;
  float floor;
} dorClock;`,
  fs: `layout(std140) uniform dorClockUniforms {
  float time;
  float focusActive;
  float floor;
} dorClock;`,
  uniformTypes: { time: "f32", focusActive: "f32", floor: "f32" },
} as const;

export const DECK_FOCUS_HELPERS = `
float dorFocusOpacity(float state) {
  if (state > 1.5 && state < 2.5) return ${FOCUS_FOCUSED_ALPHA};
  if (state > 0.5 && state < 1.5) return ${FOCUS_LINKED_ALPHA};
  if (state > 2.5) return ${FOCUS_SECOND_ALPHA};
  return mix(1.0, ${FOCUS_AMBIENT_ALPHA}, dorClock.focusActive);
}
`;

const FOCUS_DECL = `
in float instanceFocus;
in float instancePrevFocus;
in float instanceFocusAt;
${DECK_FOCUS_HELPERS}
`;

const FOCUS_COLOR = `
float dorPrev = dorFocusOpacity(instancePrevFocus);
float dorNext = dorFocusOpacity(instanceFocus);
float dorFade = clamp((dorClock.time - instanceFocusAt) / ${FOCUS_FADE_SECONDS}, 0.0, 1.0);
color.a *= mix(dorPrev, dorNext, dorFade);
`;

type FocusLayerProps = { beforeId?: string };

/** Scatterplot dot with the 200 ms per-instance focus fade. */
class FocusDotLayer extends ScatterplotLayer<unknown, FocusLayerProps> {
  static layerName = "FocusDotLayer";

  getShaders(): any {
    const shaders = super.getShaders();
    return {
      ...shaders,
      modules: [...(shaders.modules ?? []), CLOCK_MODULE],
      inject: {
        ...shaders.inject,
        "vs:#decl": `${shaders.inject?.["vs:#decl"] ?? ""}${FOCUS_DECL}`,
        "vs:DECKGL_FILTER_COLOR": `${shaders.inject?.["vs:DECKGL_FILTER_COLOR"] ?? ""}${FOCUS_COLOR}`,
      },
    };
  }

  initializeState(): void {
    super.initializeState();
    this.getAttributeManager()!.addInstanced({
      instanceFocus: { size: 1, accessor: "getFocus" },
      instancePrevFocus: { size: 1, accessor: "getPrevFocus" },
      instanceFocusAt: { size: 1, accessor: "getFocusAt" },
    });
  }

  draw(opts: any): void {
    this.state.model?.shaderInputs.setProps({ dorClock: clock });
    super.draw(opts);
  }
}

/** Soft halo under a dot: same quads, fragment falloff. */
class GlowLayer extends FocusDotLayer {
  static layerName = "GlowLayer";

  getShaders(): any {
    const shaders = super.getShaders();
    return {
      ...shaders,
      inject: {
        ...shaders.inject,
        "fs:#main-end": "fragColor.a *= 1.0 - smoothstep(0.0, 1.0, length(unitPosition));",
      },
    };
  }
}

/** Arc with strength, focus fade, dashes and travelling pulses. */
class DorArcLayer extends ArcLayer<unknown, FocusLayerProps> {
  static layerName = "DorArcLayer";

  getShaders(): any {
    const shaders = super.getShaders();
    return {
      ...shaders,
      modules: [...(shaders.modules ?? []), CLOCK_MODULE],
      inject: {
        ...shaders.inject,
        "vs:#decl": `${shaders.inject?.["vs:#decl"] ?? ""}${FOCUS_DECL}
in float instanceStrength;
in float instanceDashed;
out float vDorStrength;
out float vDorDashed;
out float vDorPulse;
out float vDorSeed;
`,
        "vs:DECKGL_FILTER_COLOR": `${shaders.inject?.["vs:DECKGL_FILTER_COLOR"] ?? ""}${FOCUS_COLOR}
vDorStrength = instanceStrength;
vDorDashed = instanceDashed;
vDorPulse = (1.0 - instanceDashed) * (1.0 - step(2.5, instanceFocus)) *
  step(${ARC_PULSE_1}, instanceStrength) *
  (1.0 - (1.0 - step(0.5, instanceFocus)) * dorClock.focusActive);
vDorSeed = fract(sin(float(gl_InstanceID) * 3.71) * 43758.5453);
if (instanceStrength < ${ARC_MIN_STRENGTH}) color.a = 0.0;
if (dorClock.focusActive < 0.5 && instanceStrength < dorClock.floor) color.a = 0.0;
if (instanceFocus < 0.5 && dorClock.focusActive > 0.5) color.a *= ${FOCUS_OFF_THREAD_ALPHA};
`,
        "fs:#decl": `
in float vDorStrength;
in float vDorDashed;
in float vDorPulse;
in float vDorSeed;
${DECK_FOCUS_HELPERS}
`,
        "fs:DECKGL_FILTER_COLOR": `
if (vDorDashed > 0.5 && fract(geometry.uv.x * 55.0 + vDorSeed) > 0.62) discard;
float dorPulse = 0.0;
if (vDorPulse > 0.5) {
  float dorSpeed = 0.15 + 0.35 * vDorStrength;
  float dorCount = vDorStrength >= ${ARC_PULSE_2} ? 2.0 : 1.0;
  for (int i = 0; i < 2; i++) {
    if (float(i) >= dorCount) break;
    float p = fract(dorClock.time * dorSpeed + vDorSeed + float(i) / dorCount);
    float delta = abs(geometry.uv.x - p);
    delta = min(delta, 1.0 - delta);
    dorPulse = max(dorPulse, 1.0 - smoothstep(0.004, 0.014, delta));
  }
}
color.rgb = mix(color.rgb, vec3(1.0), dorPulse);
color.a = max(color.a, dorPulse * color.a);
`,
      },
    };
  }

  initializeState(): void {
    super.initializeState();
    this.getAttributeManager()!.addInstanced({
      instanceFocus: { size: 1, accessor: "getFocus" },
      instancePrevFocus: { size: 1, accessor: "getPrevFocus" },
      instanceFocusAt: { size: 1, accessor: "getFocusAt" },
      instanceStrength: { size: 1, accessor: "getStrength" },
      instanceDashed: { size: 1, accessor: "getDashed" },
    });
  }

  draw(opts: any): void {
    this.state.model?.shaderInputs.setProps({ dorClock: clock });
    super.draw(opts);
  }
}

type BinaryData = {
  length: number;
  attributes: Record<string, { value: ArrayBufferView; size: number; normalized?: boolean }>;
};

function binary(length: number, attributes: BinaryData["attributes"]): BinaryData {
  return { length, attributes };
}

function interleavePositions(lng: ArrayLike<number>, lat: ArrayLike<number>): Float32Array {
  const out = new Float32Array(lng.length * 3);
  for (let i = 0; i < lng.length; i++) {
    out[i * 3] = lng[i]!;
    out[i * 3 + 1] = lat[i]!;
    out[i * 3 + 2] = 0;
  }
  return out;
}

/**
 * Core deck.gl path: ScatterplotLayer dots and ArcLayer arcs with binary
 * attributes over MapboxOverlay.
 *
 * On MapLibre's globe the dots draw, but deck.gl 9.4.1's ArcLayer (and
 * LineLayer) do not: a two-arc minimal page on the same globe view produced
 * nothing, while the same layers render in mercator. That is recorded in
 * docs/globe-spike.md; the spike measures dots on the globe and dots+arcs in
 * mercator for the like-for-like renderer comparison.
 */
export class DeckGlobeRenderer {
  private readonly map: MapLibreMap;
  private readonly beforeId: string;
  private overlay: MapboxOverlay | null = null;
  private initData: FakeInit | null = null;
  private dirty = true;

  private positions!: Float32Array;
  private dotRadius!: Float32Array;
  private glowRadius!: Float32Array;
  private coreRadius!: Float32Array;
  private categoryColor!: Uint8Array;
  private glowColor!: Uint8Array;
  private whiteColor!: Uint8Array;
  private focusValues!: Float32Array;
  private prevFocusValues!: Float32Array;
  private focusAtValues!: Float32Array;

  private sourcePositions!: Float32Array;
  private targetPositions!: Float32Array;
  private arcColors!: Uint8Array;
  private arcWidths!: Float32Array;
  private arcStrength!: Float32Array;
  private arcDashed!: Float32Array;
  private arcFocus!: Float32Array;
  private arcPrevFocus!: Float32Array;
  private arcFocusAt!: Float32Array;

  constructor(map: MapLibreMap, beforeId: string) {
    this.map = map;
    this.beforeId = beforeId;
  }

  init(init: FakeInit): void {
    this.initData = init;
    const n = init.storyCount;
    const l = init.linkCount;
    const stories = init.stories;
    const links = init.links;
    this.positions = interleavePositions(stories.lng, stories.lat);
    this.dotRadius = new Float32Array(n);
    this.glowRadius = new Float32Array(n);
    this.coreRadius = new Float32Array(n);
    this.categoryColor = stories.color.slice();
    this.glowColor = new Uint8Array(n * 4);
    this.whiteColor = new Uint8Array(n * 4);
    this.focusValues = Float32Array.from(stories.focus);
    this.prevFocusValues = Float32Array.from(stories.prevFocus);
    this.focusAtValues = Float32Array.from(stories.focusAt);
    for (let i = 0; i < n; i++) {
      this.updateDotSize(i, stories.sources[i]!, stories.heat[i]!);
      this.glowColor[i * 4] = stories.color[i * 4]!;
      this.glowColor[i * 4 + 1] = stories.color[i * 4 + 1]!;
      this.glowColor[i * 4 + 2] = stories.color[i * 4 + 2]!;
      this.glowColor[i * 4 + 3] = 34;
      this.whiteColor[i * 4] = 255;
      this.whiteColor[i * 4 + 1] = 255;
      this.whiteColor[i * 4 + 2] = 255;
      this.whiteColor[i * 4 + 3] = 235;
    }

    const sourceLng = new Float32Array(l);
    const sourceLat = new Float32Array(l);
    const targetLng = new Float32Array(l);
    const targetLat = new Float32Array(l);
    for (let i = 0; i < l; i++) {
      const a = links.a[i]!;
      const b = links.b[i]!;
      sourceLng[i] = stories.lng[a]!;
      sourceLat[i] = stories.lat[a]!;
      targetLng[i] = stories.lng[b]!;
      targetLat[i] = stories.lat[b]!;
    }
    this.sourcePositions = interleavePositions(sourceLng, sourceLat);
    this.targetPositions = interleavePositions(targetLng, targetLat);
    this.arcColors = links.color.slice();
    this.arcWidths = links.width.slice();
    this.arcStrength = links.strength.slice();
    this.arcDashed = Float32Array.from(links.type, (type) => (type === 2 ? 1 : 0));
    this.arcFocus = Float32Array.from(links.focus);
    this.arcPrevFocus = Float32Array.from(links.prevFocus);
    this.arcFocusAt = Float32Array.from(links.focusAt);

    this.overlay = new MapboxOverlay({ interleaved: true, layers: [] });
    this.map.addControl(this.overlay);
    this.dirty = true;
    this.flush();
  }

  private updateDotSize(index: number, sources: number, heat: number): void {
    const radius = 2 + 4.6 * dotSizeFactor(sources, heat);
    this.dotRadius[index] = radius;
    this.glowRadius[index] = radius * 1.55;
    this.coreRadius[index] = Math.max(0.8, radius * 0.38);
  }

  stream(patch: StreamPatch): void {
    if (!this.initData) return;
    for (let i = 0; i < patch.storyIndex.length; i++) {
      const index = patch.storyIndex[i]!;
      this.updateDotSize(index, patch.sources[i]!, patch.heat[i]!);
    }
    for (let i = 0; i < patch.linkIndex.length; i++) {
      const index = patch.linkIndex[i]!;
      this.arcStrength[index] = patch.strength[i]!;
      this.arcWidths[index] = patch.width[i]!;
      this.arcColors.set(patch.linkColor.subarray(i * 4, i * 4 + 4), index * 4);
    }
    this.dirty = true;
  }

  writeFocus(
    storyChanges: FocusPatch["storyChanges"],
    linkChanges: FocusPatch["linkChanges"],
    focusActive: boolean,
  ): void {
    for (const change of storyChanges) {
      this.focusValues[change.index] = change.state;
      this.prevFocusValues[change.index] = change.prev;
      this.focusAtValues[change.index] = change.at;
    }
    for (const change of linkChanges) {
      this.arcFocus[change.index] = change.state;
      this.arcPrevFocus[change.index] = change.prev;
      this.arcFocusAt[change.index] = change.at;
    }
    clock.focusActive = focusActive ? 1 : 0;
    this.dirty = true;
    this.flush();
  }

  applyTethers(): void {
    // Tethers are a custom-layer encoding; the deck comparison is core-only.
  }

  tick(time: number): void {
    clock.time = time;
  }

  setStrengthFloor(floor: number): void {
    clock.floor = floor;
  }

  flush(): void {
    if (!this.overlay || !this.initData || !this.dirty) return;
    this.dirty = false;
    this.overlay.setProps({ layers: this.buildLayers() });
  }

  private buildLayers(): Layer[] {
    if (!this.initData) return [];
    const n = this.initData.storyCount;
    const l = this.initData.linkCount;
    const focusAttributes = {
      getFocus: { value: this.focusValues, size: 1 },
      getPrevFocus: { value: this.prevFocusValues, size: 1 },
      getFocusAt: { value: this.focusAtValues, size: 1 },
    };
    const dotData = binary(n, {
      getPosition: { value: this.positions, size: 3 },
      getRadius: { value: this.dotRadius, size: 1 },
      getFillColor: { value: this.categoryColor, size: 4, normalized: true },
      ...focusAttributes,
    });
    const glowData = binary(n, {
      getPosition: { value: this.positions, size: 3 },
      getRadius: { value: this.glowRadius, size: 1 },
      getFillColor: { value: this.glowColor, size: 4, normalized: true },
      ...focusAttributes,
    });
    const coreData = binary(n, {
      getPosition: { value: this.positions, size: 3 },
      getRadius: { value: this.coreRadius, size: 1 },
      getFillColor: { value: this.whiteColor, size: 4, normalized: true },
      ...focusAttributes,
    });
    const arcData = binary(l, {
      getSourcePosition: { value: this.sourcePositions, size: 3 },
      getTargetPosition: { value: this.targetPositions, size: 3 },
      getSourceColor: { value: this.arcColors, size: 4, normalized: true },
      getTargetColor: { value: this.arcColors, size: 4, normalized: true },
      getWidth: { value: this.arcWidths, size: 1 },
      getStrength: { value: this.arcStrength, size: 1 },
      getDashed: { value: this.arcDashed, size: 1 },
      getFocus: { value: this.arcFocus, size: 1 },
      getPrevFocus: { value: this.arcPrevFocus, size: 1 },
      getFocusAt: { value: this.arcFocusAt, size: 1 },
    });

    const depthless = {
      cullMode: "none" as const,
      depthCompare: "always" as const,
      depthWriteEnabled: false,
    };
    return [
      new GlowLayer({
        id: "dor-deck-glow",
        beforeId: this.beforeId,
        data: glowData as any,
        getPosition: (d: any) => d.position,
        getRadius: (d: any) => d.radius,
        radiusUnits: "pixels",
        getFillColor: (d: any) => d.color,
        billboard: true,
        pickable: false,
        parameters: depthless,
      }),
      new DorArcLayer({
        id: "dor-deck-arcs",
        beforeId: this.beforeId,
        data: arcData as any,
        greatCircle: true,
        numSegments: 64,
        widthUnits: "pixels",
        widthMinPixels: ARC_WIDTH_MIN,
        widthMaxPixels: 6,
        getHeight: 0.12,
        getSourcePosition: (d: any) => d.sourcePosition,
        getTargetPosition: (d: any) => d.targetPosition,
        getSourceColor: (d: any) => d.color,
        getTargetColor: (d: any) => d.color,
        getWidth: (d: any) => d.width,
        pickable: false,
        parameters: depthless,
      }),
      new FocusDotLayer({
        id: "dor-deck-dots",
        beforeId: this.beforeId,
        data: dotData as any,
        getPosition: (d: any) => d.position,
        getRadius: (d: any) => d.radius,
        radiusUnits: "pixels",
        getFillColor: (d: any) => d.color,
        billboard: true,
        pickable: false,
        parameters: depthless,
      }),
      new FocusDotLayer({
        id: "dor-deck-cores",
        beforeId: this.beforeId,
        data: coreData as any,
        getPosition: (d: any) => d.position,
        getRadius: (d: any) => d.radius,
        radiusUnits: "pixels",
        getFillColor: (d: any) => d.color,
        billboard: true,
        pickable: false,
        parameters: depthless,
      }),
    ];
  }
}

export { clock as deckClock };
