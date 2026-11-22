/**
 * Production globe runtime.
 *
 * Reuses the M0 custom layer, GPU data and focus mirror, but drives them from
 * real dor.v1 snapshot/delta messages. The fake stream is never imported here.
 */
import { Map as MapLibreMap, ScaleControl, type GeoJSONSource } from "maplibre-gl";
import type { WorkerInit, WorkerPatch } from "../worker/messages";
import { growArray } from "./arrays";
import { PatchHold } from "./gestures";
import { PerfMeter, type PerfSnapshot } from "./perf";
import { CameraController, type CameraTarget } from "./camera";
import { loadGlobeConfig } from "./config";
import { CustomGlobeLayer } from "./custom/layer";
import { FocusMirror } from "./focus-mirror";
import { StoryLabels, type AmbientLabel } from "./labels";
import {
  buildLiveTethers,
  type LiveGlobeData,
  type LiveStory,
  type StorySummary,
} from "./live-data";
import { greatCirclePoint } from "./mercator";
import { shouldIgnoreGlobeKeys } from "../keyboard";
import { buildLabelsOnlyStyle } from "./style/dor-labels";
import type { HudFocus } from "../worker/fake/protocol";

const DEFAULT_VIEW: CameraTarget = { center: [6, 22], zoom: 1.55, bearing: 0, pitch: 0 };
const LINK_LABEL_SOURCE = "dor-link-labels";
const LINK_LABEL_LAYER = "dor-link-labels";
const AGG_SOURCE = "dor-aggregates";
const AGG_CIRCLE_LAYER = "dor-aggregate-circles";
const AGG_COUNT_LAYER = "dor-aggregate-count";
const HEX_SOURCE = "dor-aggregate-hex";
const HEX_LAYER = "dor-aggregate-hex-fill";
const AREA_SOURCE = "dor-story-areas";
const AREA_LAYER = "dor-story-areas";
const RING_SOURCE = "dor-distance-rings";
const RING_LAYER = "dor-distance-rings";
const RING_LABEL_SOURCE = "dor-distance-labels";

export type LinkInfo = {
  index: number;
  type: number;
  direction: number;
  strength: number;
  confidence: number;
  distanceKm: number;
  unverified: boolean;
  aId: string;
  bId: string;
  aTitle: string;
  bTitle: string;
  aPlace: string;
  bPlace: string;
};

export type EdgeMarker = {
  key: string;
  label: string;
  count: number;
  center: [number, number];
  zoom: number;
};

export type FocusDetails = {
  storyId: string;
  story: StorySummary;
  hud: HudFocus;
  live: LiveStory;
  threadStoryIds: string[];
  secondDegree: boolean;
};

export type GlobeView = { center: [number, number]; zoom: number };

export type GlobeState = {
  ready: boolean;
  storyCount: number;
  linkCount: number;
  focused: FocusDetails | null;
  topStoryIds: string[];
  topStories: LiveStory[];
  edgeMarkers: EdgeMarker[];
  secondDegree: boolean;
  quality: number;
  tileFailed: boolean;
  perf?: PerfSnapshot;
  initCount?: number;
  patchCount?: number;
  rippleCount?: number;
  focusLatencyMs?: number;
};

type RuntimeOptions = {
  onState?: (state: GlobeState) => void;
  onFocus?: (focus: FocusDetails | null) => void;
  onLinkClick?: (link: LinkInfo) => void;
  onView?: (view: GlobeView) => void;
  /** Fired after a lost WebGL context is rebuilt (request authoritative data). */
  onContextRestored?: () => void;
};

type AggregateFeature = {
  type: "Feature";
  geometry: { type: "Point"; coordinates: [number, number] };
  properties: { count: number; color: string };
};

function linkColor(type: number): string {
  return type === 0 ? "#ffc25c" : type === 1 ? "#ff6fd8" : "#5ef2ff";
}

export class LiveGlobe {
  readonly map: MapLibreMap;

  private readonly layer = new CustomGlobeLayer();
  private readonly camera: CameraController;
  private readonly labels: StoryLabels;
  private readonly options: RuntimeOptions;
  readonly perf = new PerfMeter();
  private readonly patches = new PatchHold<WorkerPatch[]>((older, newer) => older.concat(newer));
  private initCount = 0;
  private patchCount = 0;
  private rippleCount = 0;
  private focusLatencyMs = 0;
  private focusStartedAt: number | null = null;
  private lastPerfAt = 0;
  private queuedPatches = 0;
  private maxQueuedPatches = 0;
  private patchTimes: number[] = [];
  private annotationsDirty = false;
  private lastAnnotationAt = Number.NEGATIVE_INFINITY;
  private activeStoryCount = 0;
  private activeLinkCount = 0;

  private data: LiveGlobeData | null = null;
  private mirror: FocusMirror | null = null;
  private focusId: string | null = null;
  private focused: FocusDetails | null = null;
  private ready = false;
  private pending: WorkerInit | null = null;
  private destroyed = false;
  private contextLost = false;
  private readonly storyDimState = new Map<number, number>();
  private readonly linkDimState = new Map<number, number>();
  private secondDegree = false;
  private edgeMarkers: EdgeMarker[] = [];
  private labelsVisible = true;
  private linesVisible = true;
  private tethersVisible = true;
  private heatVisible = true;
  private quality = 0;
  private floor = 0.6;
  private tileFailed = false;

  private frameSamples: number[] = [];
  private lastFrameAt = 0;
  private goodSince = 0;
  private frameHandle: number | null = null;
  private restoreTimer: ReturnType<typeof setTimeout> | null = null;
  private restoreAttempts = 0;

  constructor(container: HTMLElement, options: RuntimeOptions = {}) {
    this.options = options;
    if (new URLSearchParams(location.search).has("diagnostics")) {
      (window as unknown as { __dorLive?: LiveGlobe }).__dorLive = this;
    }
    const config = loadGlobeConfig();
    this.map = new MapLibreMap({
      container,
      style: config.styleUrl || buildLabelsOnlyStyle(config),
      center: DEFAULT_VIEW.center,
      zoom: DEFAULT_VIEW.zoom,
      minZoom: 0.3,
      maxZoom: 20,
      maxPitch: 60,
      attributionControl: false,
      fadeDuration: 0,
      // Explicit DPR cap: retina displays never multiply the fill cost by >4.
      pixelRatio: Math.min(window.devicePixelRatio || 1, 2),
      renderWorldCopies: true,
    });
    this.camera = new CameraController(this.map);
    // The app owns ↑/↓ (walk the wire) and Esc (clear focus); MapLibre's
    // canvas keyboard handler would pan the camera on the same keys and mark
    // them defaultPrevented before the window-level handler sees them.
    this.map.keyboard.disable();
    this.map.addControl(new ScaleControl({ unit: "metric", maxWidth: 110 }), "bottom-right");
    this.labels = new StoryLabels(this.map);
    this.map.on("load", () => this.onLoad());
    this.map.on("error", (event) => {
      const message = (event as { error?: Error }).error?.message ?? "";
      if (message && !message.includes("Failed to fetch")) console.warn(`map: ${message}`);
      if (!this.tileFailed && /tile|vector|glyph|sprite|fetch|network|404|5\d\d/i.test(message)) {
        this.tileFailed = true;
        this.map.getContainer().classList.add("tile-failed");
        void this.addFallbackCities();
        this.emit();
      }
    });
    this.map.on("render", () => {
      if (this.focusStartedAt !== null) {
        this.focusLatencyMs = performance.now() - this.focusStartedAt;
        this.focusStartedAt = null;
      }
    });
    this.installInteractions();
    this.map.on("webglcontextlost", this.onContextLost);
    this.map.on("webglcontextrestored", this.onContextRestored);
    this.frameHandle = requestAnimationFrame(this.sampleFrame);
  }

  private onLoad(): void {
    if (this.destroyed) return;
    const firstSymbol = (this.map.getStyle().layers ?? []).find(
      (candidate) => candidate.type === "symbol",
    )?.id;
    this.addAreaLayer(firstSymbol);
    this.map.addLayer(this.layer, firstSymbol);
    this.layer.setLinesVisible(this.linesVisible);
    this.layer.setTethersVisible(this.tethersVisible);
    this.layer.setHeatVisible(this.heatVisible);
    this.layer.setQuality(this.quality);
    this.labels.add();
    this.addAnnotations();
    this.ready = true;
    this.map.on("movestart", () => this.patches.setGestureActive(true));
    this.map.on("moveend", () => {
      this.patches.setGestureActive(false);
      this.applyPendingPatches();
      this.onMoveEnd();
    });
    if (this.pending) {
      const pending = this.pending;
      this.pending = null;
      this.setInit(pending, false);
    }
    this.emit();
  }

  private addAreaLayer(beforeId: string | undefined): void {
    if (!this.map.getSource(AREA_SOURCE)) {
      this.map.addSource(AREA_SOURCE, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      this.map.addLayer(
        {
          id: AREA_LAYER,
          type: "circle",
          source: AREA_SOURCE,
          maxzoom: 12,
          paint: {
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 0, 12, 4, 18, 8, 26, 14, 36],
            "circle-color": ["get", "color"],
            "circle-opacity": 0.07,
            "circle-stroke-color": ["get", "color"],
            "circle-stroke-opacity": 0.22,
            "circle-stroke-width": 1,
          },
        },
        beforeId,
      );
    }
  }

  private addAnnotations(): void {
    if (!this.map.getSource(LINK_LABEL_SOURCE)) {
      this.map.addSource(LINK_LABEL_SOURCE, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      this.map.addLayer({
        id: LINK_LABEL_LAYER,
        type: "symbol",
        source: LINK_LABEL_SOURCE,
        layout: {
          "text-field": ["get", "label"],
          "text-font": ["Noto Sans Regular"],
          "text-size": 10,
          "text-letter-spacing": 0.08,
          "text-allow-overlap": true,
          "text-offset": [0, -0.7],
        },
        paint: {
          "text-color": ["get", "color"],
          "text-halo-color": "rgba(3,5,11,0.96)",
          "text-halo-width": 1.4,
        },
      });
    }
    if (!this.map.getSource(AGG_SOURCE)) {
      this.map.addSource(AGG_SOURCE, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      this.map.addLayer({
        id: AGG_CIRCLE_LAYER,
        type: "circle",
        source: AGG_SOURCE,
        paint: {
          "circle-radius": ["interpolate", ["linear"], ["get", "count"], 1, 5, 30, 22],
          "circle-color": ["get", "color"],
          "circle-opacity": 0.16,
          "circle-stroke-width": 1,
          "circle-stroke-color": ["get", "color"],
          "circle-stroke-opacity": 0.36,
        },
      });
      this.map.addLayer({
        id: AGG_COUNT_LAYER,
        type: "symbol",
        source: AGG_SOURCE,
        layout: {
          "text-field": ["to-string", ["get", "count"]],
          "text-font": ["Noto Sans Regular"],
          "text-size": 10,
          "text-allow-overlap": true,
        },
        paint: {
          "text-color": "#e8f3ff",
          "text-halo-color": "rgba(3,5,11,0.9)",
          "text-halo-width": 1,
        },
      });
    }
    if (!this.map.getSource(HEX_SOURCE)) {
      this.map.addSource(HEX_SOURCE, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      this.map.addLayer({
        id: HEX_LAYER,
        type: "fill",
        source: HEX_SOURCE,
        paint: {
          "fill-color": ["get", "color"],
          "fill-opacity": 0.1,
          "fill-outline-color": ["get", "color"],
        },
      });
    }
    if (!this.map.getSource(RING_SOURCE)) {
      this.map.addSource(RING_SOURCE, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      this.map.addLayer({
        id: RING_LAYER,
        type: "line",
        source: RING_SOURCE,
        paint: {
          "line-color": "rgba(159,179,207,0.38)",
          "line-width": 0.8,
          "line-dasharray": [2, 3],
        },
      });
      this.map.addSource(RING_LABEL_SOURCE, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      this.map.addLayer({
        id: RING_LABEL_SOURCE,
        type: "symbol",
        source: RING_LABEL_SOURCE,
        layout: {
          "text-field": ["get", "label"],
          "text-font": ["Noto Sans Regular"],
          "text-size": 9,
          "text-allow-overlap": true,
        },
        paint: {
          "text-color": "rgba(159,179,207,0.75)",
          "text-halo-color": "rgba(3,5,11,0.9)",
          "text-halo-width": 1,
        },
      });
    }
  }

  private installInteractions(): void {
    this.map.on("click", (event) => {
      if (this.contextLost) return;
      // Dots first: an arc crossing a dot must not steal the dot's click.
      const pick = this.layer.pickNow(event.point.x, event.point.y);
      if (pick?.kind === "story") {
        const entry = this.data?.stories[pick.index];
        if (entry) this.focusStoryId(entry.story.id);
        return;
      }
      if (this.map.getLayer(LINK_LABEL_LAYER)) {
        const hit = this.map.queryRenderedFeatures(event.point, { layers: [LINK_LABEL_LAYER] })[0];
        if (hit) {
          const index = Number(hit.properties?.linkIndex);
          const info = this.linkInfo(index);
          if (info) this.options.onLinkClick?.(info);
          return;
        }
      }
      if (pick?.kind === "link") {
        const info = this.linkInfo(pick.index);
        if (info) this.options.onLinkClick?.(info);
      }
    });
    window.addEventListener("keydown", this.onKeyDown);
  }

  private readonly onKeyDown = (event: KeyboardEvent): void => {
    // Esc is a globe shortcut only when the user is not typing and no dialog
    // or popover owns the keyboard (typing "q"/Esc in the prompt box must not
    // clear the focused story).
    if (event.key !== "Escape") return;
    if (shouldIgnoreGlobeKeys(event, document)) return;
    this.clearFocus();
  };

  private readonly onContextLost = (): void => {
    // MapLibre destroys the style (and our custom layer) on loss. Keep the
    // runtime data; the layer retains its init for the restore path.
    this.contextLost = true;
    this.layer.handleContextLost();
    this.map.getContainer().classList.add("gl-context-lost");
    this.emit();
  };

  private readonly onContextRestored = (): void => {
    this.contextLost = false;
    this.map.getContainer().classList.remove("gl-context-lost");
    // Ask for authoritative data first; the layer rebuild can wait for the
    // restored style to become serializable.
    this.options.onContextRestored?.();
    this.restoreLayer();
    this.emit();
  };

  /**
   * Re-add the custom layer after MapLibre restores the style. Custom layers
   * cannot be serialized, so MapLibre drops them from the restored style; the
   * layer's onAdd rebuilds its programs and buffers from the retained init.
   */
  private restoreLayer(): void {
    if (this.destroyed || !this.ready || !this.data) return;
    let style: ReturnType<MapLibreMap["getStyle"]> | undefined;
    try {
      style = this.map.getStyle();
    } catch {
      style = undefined;
    }
    // A style restored from a lost context is not serializable until its
    // sources finish loading; retry instead of throwing on the first frame.
    if (!style) {
      this.scheduleLayerRestore();
      return;
    }
    const firstSymbol = (style.layers ?? []).find((candidate) => candidate.type === "symbol")?.id;
    try {
      this.addAreaLayer(firstSymbol);
      if (!this.map.getLayer(this.layer.id)) this.map.addLayer(this.layer, firstSymbol);
      this.labels.add();
      this.addAnnotations();
    } catch (error) {
      console.warn(`globe: layer restore failed (${String(error)})`);
      this.scheduleLayerRestore();
      return;
    }
    this.restoreAttempts = 0;
    this.layer.setLinesVisible(this.linesVisible);
    this.layer.setTethersVisible(this.tethersVisible);
    this.layer.setHeatVisible(this.heatVisible);
    this.layer.setQuality(this.quality);
    this.layer.setStrengthFloor(this.floor);
    const nowSec = (Date.now() - this.data.init.epochMs) / 1000;
    if (this.mirror) {
      const snapshot = this.mirror.snapshot(nowSec);
      this.layer.writeFocus(snapshot.storyChanges, snapshot.linkChanges, this.focused !== null);
    }
    this.reapplyMatchState();
    if (this.focused && this.tethersVisible) {
      const tethers = buildLiveTethers(this.focused.live);
      this.layer.applyTethers(tethers.segments, tethers.rings);
    } else {
      this.layer.applyTethers([], []);
    }
    this.map.triggerRepaint();
  }

  private scheduleLayerRestore(): void {
    if (this.restoreTimer !== null || this.destroyed) return;
    if (this.restoreAttempts >= 50) return;
    this.restoreAttempts += 1;
    this.restoreTimer = setTimeout(() => {
      this.restoreTimer = null;
      this.restoreLayer();
    }, 100);
  }

  private reapplyMatchState(): void {
    if (!this.ready || this.storyDimState.size === 0) return;
    const storyIndices = Uint16Array.from(this.storyDimState.keys());
    const storyDims = Uint8Array.from(this.storyDimState.values());
    const linkIndices = Uint16Array.from(this.linkDimState.keys());
    const linkDims = Uint8Array.from(this.linkDimState.values());
    this.layer.updateMatches(
      { indices: storyIndices, dimmed: storyDims },
      { indices: linkIndices, dimmed: linkDims },
    );
  }

  private onMoveEnd(): void {
    if (this.annotationsDirty) {
      this.updateAreas();
      this.annotationsDirty = false;
    }
    if (!this.focused) this.setAmbientLabels();
    this.updateAggregates();
    this.updateEdgeMarkers();
    this.updateDistanceRings();
    this.options.onView?.(this.view);
    this.emit();
  }

  /** Full uploads occur only on worker init (snapshot/resync), never a delta. */
  setInit(message: WorkerInit, resetQueue = true): void {
    if (resetQueue) {
      this.patches.flush();
      this.queuedPatches = 0;
    }
    if (!this.ready) {
      this.pending = message;
      return;
    }
    const { init, summaries } = message;
    this.data = {
      init,
      stories: summaries,
      storyById: new Map(summaries.filter(Boolean).map((entry) => [entry.story.id, entry.index])),
      linkByKey: new Map(),
      topStoryIds: [],
    };
    this.activeStoryCount = summaries.filter(Boolean).length;
    this.activeLinkCount = init.linkCount;
    this.storyDimState.clear();
    this.linkDimState.clear();
    this.updateTop(init.topStories);
    this.layer.setInit(init);
    this.initCount++;
    this.layer.setStrengthFloor(this.floor);
    this.layer.setLinesVisible(this.linesVisible);
    this.layer.setTethersVisible(this.tethersVisible);
    this.layer.setHeatVisible(this.heatVisible);
    this.layer.setQuality(this.quality);
    this.mirror = new FocusMirror(init);
    this.setAmbientLabels();
    this.updateAreas();
    this.updateAggregates();
    this.updateEdgeMarkers();
    this.updateDistanceRings();
    if (this.focusId) {
      const index = this.data.storyById.get(this.focusId);
      if (index !== undefined) this.applyFocus(index, false);
      else this.clearFocus();
    }
    this.emit();
    this.map.triggerRepaint();
  }

  queuePatch(patch: WorkerPatch): void {
    this.patches.push([patch]);
    this.queuedPatches++;
    this.maxQueuedPatches = Math.max(this.maxQueuedPatches, this.queuedPatches);
  }
  private updateTop(indices: Uint16Array): void {
    if (!this.data) return;
    this.data.init.topStories = indices;
    this.data.topStoryIds = Array.from(indices, (i) => this.data!.stories[i]?.story.id).filter(
      (id): id is string => Boolean(id),
    );
  }
  private applyPendingPatches(): void {
    if (!this.ready || !this.data || !this.mirror) return;
    const patches = this.patches.take(performance.now());
    if (!patches) return;
    this.queuedPatches = 0;
    const patchStarted = performance.now();
    const data = this.data,
      mirror = this.mirror;
    let focusDirty = false;
    for (const patch of patches) {
      this.layer.ensureCapacity(patch.storyCapacity, patch.linkCapacity);
      mirror.ensureCapacity(patch.storyCapacity, patch.linkCapacity);
      const links = data.init.links;
      for (const key of Object.keys(links) as Array<keyof typeof links>) {
        const value = links[key];
        if (value)
          (links as unknown as Record<string, typeof value>)[key] = growArray(
            value,
            patch.linkCapacity * (key === "color" ? 4 : 1),
          );
      }
      data.init.storyCapacity = patch.storyCapacity;
      data.init.linkCapacity = patch.linkCapacity;
      if (patch.stories.length) this.annotationsDirty = true;
      for (const op of patch.stories) {
        const previous = data.stories[op.index];
        if (previous && previous.story.id !== op.summary?.story.id)
          data.storyById.delete(previous.story.id);
        if (op.op === "add" && op.summary) {
          data.stories[op.index] = op.summary;
          data.storyById.set(op.summary.story.id, op.index);
        } else delete data.stories[op.index];
        if (op.reset) mirror.applyStoryDelta(op.index, op, false, true);
        this.layer.applyStoryOp(op);
        mirror.applyStoryDelta(op.index, op, op.op === "add", true);
        mirror.applyStoryMetadata(op);
      }
      for (const entry of patch.summaries) {
        data.stories[entry.index] = entry;
        data.storyById.set(entry.story.id, entry.index);
      }
      for (const op of patch.links) {
        if (op.reset)
          mirror.applyLinkDelta(op.index, op.a, op.b, false, op.type, op.strength, true);
        this.layer.applyLinkOp(op);
        mirror.applyLinkDelta(op.index, op.a, op.b, op.op === "add", op.type, op.strength, true);
        mirror.applyLinkMetadata(op);
        links.a[op.index] = op.a;
        links.b[op.index] = op.b;
        links.type[op.index] = op.type;
        links.strength[op.index] = op.op === "add" ? op.strength : 0;
        if (links.direction) links.direction[op.index] = op.direction ?? 0;
        if (links.confidence) links.confidence[op.index] = op.confidence ?? 0;
        if (links.distanceKm) links.distanceKm[op.index] = op.distanceKm ?? 0;
        if (links.unverified) links.unverified[op.index] = op.unverified ? 1 : 0;
        data.init.linkCount = Math.max(data.init.linkCount, op.index + 1);
      }
      if (patch.adjacency) mirror.applyAdjacency(patch.adjacency);
      mirror.applyStream(patch.stream);
      this.layer.stream(patch.stream);
      this.layer.updateMatches(patch.storyStates, patch.linkStates);
      for (let i = 0; i < patch.storyStates.indices.length; i++)
        this.storyDimState.set(patch.storyStates.indices[i]!, patch.storyStates.dimmed[i]!);
      for (let i = 0; i < patch.linkStates.indices.length; i++)
        this.linkDimState.set(patch.linkStates.indices[i]!, patch.linkStates.dimmed[i]!);
      this.rippleCount += patch.stream.ripples.length;
      this.patchCount++;
      this.activeStoryCount = patch.storyCount;
      this.activeLinkCount = patch.linkCount;
      this.updateTop(patch.topStories);
      focusDirty ||=
        patch.stories.length > 0 || patch.summaries.length > 0 || patch.links.length > 0;
    }
    if (this.focusId && focusDirty) {
      const index = data.storyById.get(this.focusId);
      if (index === undefined) this.clearFocus();
      else this.applyFocus(index, false);
    }
    this.refreshAnnotations();
    this.patchTimes.push(performance.now() - patchStarted);
    if (this.patchTimes.length > 600) this.patchTimes.shift();
    this.emit();
    this.map.triggerRepaint();
  }

  private refreshAnnotations(): void {
    if (
      !this.annotationsDirty ||
      this.patches.isGestureActive() ||
      performance.now() - this.lastAnnotationAt < 1000
    )
      return;
    this.annotationsDirty = false;
    this.lastAnnotationAt = performance.now();
    if (!this.focused) this.setAmbientLabels();
    this.updateAreas();
    this.updateAggregates();
    this.updateEdgeMarkers();
  }

  get diagnostics() {
    const sorted = [...this.patchTimes].sort((a, b) => a - b);
    const index = this.focusId ? this.data?.storyById.get(this.focusId) : undefined;
    return {
      epochMs: this.data?.init.epochMs,
      initCount: this.initCount,
      patchCount: this.patchCount,
      rippleCount: this.rippleCount,
      queuedPatches: this.queuedPatches,
      maxQueuedPatches: this.maxQueuedPatches,
      patchMs: {
        p95: sorted[Math.floor(sorted.length * 0.95)] ?? 0,
        max: sorted.at(-1) ?? 0,
        mean: sorted.reduce((a, b) => a + b, 0) / Math.max(1, sorted.length),
      },
      focusLatencyMs: this.focusLatencyMs,
      focusedStoryId: this.focusId,
      focusState: index === undefined ? null : this.layer.storyAnimation(index),
      picks: this.layer.stats,
      contextLost: this.contextLost,
    };
  }

  focusStoryId(storyId: string): void {
    const data = this.data;
    if (!data) {
      // Map/data is still loading (deep link); rebuild applies this once ready.
      this.focusId = storyId;
      return;
    }
    const index = data.storyById.get(storyId);
    if (index === undefined) return;
    // Re-selecting the same story (an adjacent wire row of the same story)
    // must not refly the camera or re-activate the panel.
    if (this.focusId === storyId && this.focused) return;
    this.focusId = storyId;
    this.focusStartedAt = performance.now();
    this.applyFocus(index, true);
  }

  clearFocus(): void {
    if (!this.mirror || !this.data) return;
    const nowSec = (Date.now() - this.data.init.epochMs) / 1000;
    const result = this.mirror.focus(null, false, nowSec);
    this.layer.writeFocus(result.storyChanges, result.linkChanges, false);
    this.layer.applyTethers([], []);
    this.labels.setFocus(null);
    this.clearLinkLabels();
    this.camera.cancel();
    this.focusId = null;
    this.focused = null;
    this.updateDistanceRings();
    this.updateEdgeMarkers();
    this.setAmbientLabels();
    this.options.onFocus?.(null);
    this.emit();
    this.map.triggerRepaint();
  }

  private applyFocus(index: number, fly: boolean): void {
    const data = this.data;
    const mirror = this.mirror;
    if (!data || !mirror) return;
    const entry = data.stories[index];
    if (!entry) return;
    const nowSec = (Date.now() - data.init.epochMs) / 1000;
    const result = mirror.focus(index, this.secondDegree, nowSec);
    if (!result.hud) return;
    this.layer.writeFocus(result.storyChanges, result.linkChanges, true);
    if (this.tethersVisible) {
      const tethers = buildLiveTethers(entry);
      this.layer.applyTethers(tethers.segments, tethers.rings);
    } else {
      this.layer.applyTethers([], []);
    }
    this.labels.setFocus(result.hud);
    if (fly && result.points.length > 0) this.camera.flyTo(this.camera.fitThread(result.points));
    const threadStoryIds = [...mirror.litStoryIndices]
      .map((storyIndex) => data.stories[storyIndex]?.story.id)
      .filter((id): id is string => id !== undefined);
    this.focused = {
      storyId: entry.story.id,
      story: entry.story,
      hud: result.hud,
      live: entry,
      threadStoryIds,
      secondDegree: this.secondDegree,
    };
    this.updateDistanceRings();
    this.updateLinkLabels();
    this.updateEdgeMarkers();
    this.options.onFocus?.(this.focused);
    this.emit();
    this.map.triggerRepaint();
  }

  private async addFallbackCities(): Promise<void> {
    if (this.map.getSource("dor-fallback-cities")) return;
    try {
      const apiBase = (
        (import.meta.env as Record<string, string | undefined>).VITE_API_URL || ""
      ).replace(/\/$/, "");
      const response = await fetch(`${apiBase}/api/places?limit=20`);
      if (!response.ok) return;
      const payload = (await response.json()) as {
        places?: Array<{ name: string; lat: number; lon: number }>;
      };
      const features = (payload.places ?? []).map((place) => ({
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: [place.lon, place.lat] },
        properties: { label: place.name.toUpperCase() },
      }));
      this.map.addSource("dor-fallback-cities", {
        type: "geojson",
        data: { type: "FeatureCollection", features },
      });
      this.map.addLayer({
        id: "dor-fallback-cities",
        type: "symbol",
        source: "dor-fallback-cities",
        layout: {
          "text-field": ["get", "label"],
          "text-font": ["Noto Sans Regular"],
          "text-size": 10,
          "text-letter-spacing": 0.08,
        },
        paint: {
          "text-color": "rgba(159,179,207,.7)",
          "text-halo-color": "rgba(3,5,11,.9)",
          "text-halo-width": 1,
        },
      });
    } catch {
      /* dark background and story dots remain; no fabricated land */
    }
  }

  private setAmbientLabels(): void {
    const data = this.data;
    if (!data) return;
    const zoom = this.map.getZoom();
    const budget = zoom < 3 ? 6 : zoom < 7 ? 10 : 8;
    const points: AmbientLabel[] = [];
    for (const index of data.init.topStories.slice(0, budget)) {
      const entry = data.stories[index];
      if (!entry) continue;
      points.push({
        lng: entry.lng,
        lat: entry.lat,
        category: entry.category,
        place: entry.place,
      });
    }
    this.labels.setAmbient(points);
  }

  private updateLinkLabels(): void {
    if (!this.ready || !this.focused) {
      this.clearLinkLabels();
      return;
    }
    const source = this.map.getSource(LINK_LABEL_SOURCE) as GeoJSONSource | undefined;
    if (!source) return;
    const data = this.data;
    if (!data) return;
    const features = this.focused.hud.links
      .filter((link) => link.strength >= 0.3)
      .map((link) => {
        const a = data.init.links.a[link.index];
        const b = data.init.links.b[link.index];
        if (a === undefined || b === undefined) return null;
        const start = data.stories[a];
        const end = data.stories[b];
        if (!start || !end) return null;
        const mid = greatCirclePoint([start.lng, start.lat], [end.lng, end.lat], 0.5);
        return {
          type: "Feature" as const,
          geometry: { type: "Point" as const, coordinates: mid },
          properties: {
            label: `${link.strength.toFixed(2)} · ${(link.distanceKm ?? 0).toFixed(0)} km`,
            color: linkColor(link.type),
            linkIndex: link.index,
          },
        };
      })
      .filter((feature): feature is NonNullable<typeof feature> => feature !== null);
    source.setData({ type: "FeatureCollection", features });
    this.map.setLayoutProperty(
      LINK_LABEL_LAYER,
      "visibility",
      this.labelsVisible ? "visible" : "none",
    );
  }

  private clearLinkLabels(): void {
    if (!this.ready) return;
    const source = this.map.getSource(LINK_LABEL_SOURCE) as GeoJSONSource | undefined;
    source?.setData({ type: "FeatureCollection", features: [] });
  }

  private updateAreas(): void {
    if (!this.ready || !this.data) return;
    const source = this.map.getSource(AREA_SOURCE) as GeoJSONSource | undefined;
    if (!source) return;
    const features = this.data.stories
      .filter((entry) => {
        const location = entry.story.locations[entry.story.primary] ?? entry.story.locations[0];
        return location !== undefined && location.precision <= 2;
      })
      .map((entry) => {
        const location = entry.story.locations[entry.story.primary] ?? entry.story.locations[0]!;
        return {
          type: "Feature" as const,
          geometry: {
            type: "Point" as const,
            coordinates: [location.lon, location.lat] as [number, number],
          },
          properties: {
            color: `#${entry.color
              .slice(0, 3)
              .map((value) => value.toString(16).padStart(2, "0"))
              .join("")}`,
            precision: location.precision,
          },
        };
      });
    source.setData({ type: "FeatureCollection", features });
  }

  private updateDistanceRings(): void {
    if (!this.ready) return;
    const source = this.map.getSource(RING_SOURCE) as GeoJSONSource | undefined;
    const labels = this.map.getSource(RING_LABEL_SOURCE) as GeoJSONSource | undefined;
    if (!source || !labels) return;
    const center = this.focused
      ? [this.focused.live.lng, this.focused.live.lat]
      : [this.map.getCenter().lng, this.map.getCenter().lat];
    const zoom = this.map.getZoom();
    const radii =
      zoom < 4 ? [1000, 5000] : zoom < 8 ? [200, 1000] : zoom < 12 ? [50, 200] : [1, 5, 25];
    const ringFeatures = radii.map((km) => {
      const coordinates: [number, number][] = [];
      for (let i = 0; i <= 64; i++) {
        const bearing = (i / 64) * 360;
        coordinates.push(destinationPoint(center[1]!, center[0]!, km, bearing));
      }
      return {
        type: "Feature" as const,
        geometry: { type: "Polygon" as const, coordinates: [coordinates] },
        properties: { km },
      };
    });
    const labelFeatures = radii.map((km) => ({
      type: "Feature" as const,
      geometry: {
        type: "Point" as const,
        coordinates: destinationPoint(center[1]!, center[0]!, km, 90),
      },
      properties: { label: `${km} km` },
    }));
    source.setData({ type: "FeatureCollection", features: ringFeatures });
    labels.setData({ type: "FeatureCollection", features: labelFeatures });
  }

  private updateAggregates(): void {
    if (!this.ready) return;
    const zoom = this.map.getZoom();
    const source = this.map.getSource(AGG_SOURCE) as GeoJSONSource | undefined;
    if (!source) return;
    const show = zoom < 4.2 && (this.data?.stories.length ?? 0) > 0;
    this.map.setLayoutProperty(AGG_CIRCLE_LAYER, "visibility", "none");
    this.map.setLayoutProperty(AGG_COUNT_LAYER, "visibility", show ? "visible" : "none");
    this.map.setLayoutProperty(HEX_LAYER, "visibility", show ? "visible" : "none");
    const hexSource = this.map.getSource(HEX_SOURCE) as GeoJSONSource | undefined;
    if (!show || !this.data) {
      source.setData({ type: "FeatureCollection", features: [] });
      hexSource?.setData({ type: "FeatureCollection", features: [] });
      return;
    }
    const cell = zoom < 2 ? 18 : 8;
    const buckets = new Map<
      string,
      { count: number; lng: number; lat: number; category: number }
    >();
    for (const entry of this.data.stories) {
      if (!entry) continue;
      const key = `${Math.floor(entry.lng / cell)}:${Math.floor(entry.lat / cell)}`;
      const bucket = buckets.get(key) ?? { count: 0, lng: 0, lat: 0, category: entry.category };
      bucket.count++;
      bucket.lng += entry.lng;
      bucket.lat += entry.lat;
      buckets.set(key, bucket);
    }
    const features: AggregateFeature[] = [...buckets.values()].map((bucket) => ({
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [bucket.lng / bucket.count, bucket.lat / bucket.count],
      },
      properties: { count: bucket.count, color: linkColor(bucket.category) },
    }));
    source.setData({ type: "FeatureCollection", features });
    const hexFeatures = [...buckets.values()].map((bucket) => {
      const centerLng = bucket.lng / bucket.count;
      const centerLat = bucket.lat / bucket.count;
      const radius = cell * 0.55;
      const ring: [number, number][] = [];
      for (let i = 0; i <= 6; i++) {
        const angle = (i / 6) * Math.PI * 2;
        ring.push([
          centerLng +
            (radius * Math.cos(angle)) / Math.max(0.2, Math.cos((centerLat * Math.PI) / 180)),
          centerLat + radius * Math.sin(angle),
        ]);
      }
      return {
        type: "Feature" as const,
        geometry: { type: "Polygon" as const, coordinates: [ring] },
        properties: { count: bucket.count, color: linkColor(bucket.category) },
      };
    });
    hexSource?.setData({ type: "FeatureCollection", features: hexFeatures });
  }

  private updateEdgeMarkers(): void {
    if (!this.ready || !this.data || this.data.stories.length === 0) {
      this.edgeMarkers = [];
      return;
    }
    if (this.focused) {
      const canvas = this.map.getCanvas();
      const linked: EdgeMarker[] = [];
      for (const link of this.focused.hud.links) {
        const entry = this.data.stories[link.storyIndex];
        if (!entry) continue;
        const point = this.map.project([entry.lng, entry.lat]);
        const visible =
          point.x >= 40 &&
          point.x <= canvas.clientWidth - 40 &&
          point.y >= 80 &&
          point.y <= canvas.clientHeight - 80;
        if (visible) continue;
        linked.push({
          key: `link-${link.index}`,
          label: `${link.place.toUpperCase()} · ${(link.distanceKm ?? 0).toFixed(0)} km`,
          count: 1,
          center: [entry.lng, entry.lat],
          zoom: Math.max(5, this.map.getZoom()),
        });
      }
      this.edgeMarkers = linked.slice(0, 4);
      return;
    }
    const regions = [
      { key: "asia", label: "ASIA", center: [92, 24] as [number, number], zoom: 1.9 },
      { key: "americas", label: "AMERICAS", center: [-80, 18] as [number, number], zoom: 1.9 },
      { key: "europe", label: "EUROPE", center: [16, 48] as [number, number], zoom: 2.4 },
      { key: "africa", label: "AFRICA", center: [20, 5] as [number, number], zoom: 2.1 },
      { key: "oceania", label: "OCEANIA", center: [140, -25] as [number, number], zoom: 2.1 },
    ];
    const counts = new Map<string, number>();
    for (const entry of this.data.stories) {
      if (!entry) continue;
      const point = this.map.project([entry.lng, entry.lat]);
      const canvas = this.map.getCanvas();
      const visible =
        point.x >= 40 &&
        point.x <= canvas.clientWidth - 40 &&
        point.y >= 80 &&
        point.y <= canvas.clientHeight - 80;
      if (visible) continue;
      let key = "asia";
      if (entry.lng < -30) key = "americas";
      else if (entry.lng >= -25 && entry.lng < 45 && entry.lat >= 34) key = "europe";
      else if (entry.lng >= -25 && entry.lng < 55 && entry.lat < 34) key = "africa";
      else if (entry.lng >= 110 && entry.lat < -8) key = "oceania";
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    this.edgeMarkers = regions
      .map((region) => ({
        key: region.key,
        label: region.label,
        count: counts.get(region.key) ?? 0,
        center: region.center,
        zoom: region.zoom,
      }))
      .filter((marker) => marker.count > 0)
      .sort((a, b) => b.count - a.count)
      .slice(0, 3);
  }

  private sampleFrame = (time: number): void => {
    if (this.destroyed) return;
    this.frameHandle = requestAnimationFrame(this.sampleFrame);
    this.perf.frame(time);
    this.applyPendingPatches();
    this.refreshAnnotations();
    if (time - this.lastPerfAt > 1000) {
      this.lastPerfAt = time;
      this.emit();
    }
    if (document.visibilityState !== "visible") return;
    if (this.lastFrameAt > 0) {
      const delta = time - this.lastFrameAt;
      if (delta > 0 && delta < 200) this.frameSamples.push(delta);
    }
    this.lastFrameAt = time;
    if (this.frameSamples.length < 90) return;
    const sorted = [...this.frameSamples].sort((a, b) => a - b);
    const p95 = sorted[Math.floor(sorted.length * 0.95)] ?? 16;
    this.frameSamples = [];
    if (p95 > 20 && this.quality < 3) {
      this.quality++;
      this.layer.setQuality(this.quality);
      this.goodSince = time;
      this.emit();
    } else if (p95 < 14 && this.quality > 0 && time - this.goodSince > 8000) {
      this.quality--;
      this.layer.setQuality(this.quality);
      this.goodSince = time;
      this.emit();
    } else if (p95 < 14) {
      if (this.goodSince === 0) this.goodSince = time;
    }
  };

  // ---- Public controls -------------------------------------------------
  zoomIn(): void {
    this.map.zoomIn();
  }
  zoomOut(): void {
    this.map.zoomOut();
  }
  resetView(): void {
    this.camera.flyTo(DEFAULT_VIEW);
  }

  flyTo(center: [number, number], zoom: number): void {
    this.camera.flyTo({ center, zoom, bearing: 0, pitch: 0 });
  }

  setSecondDegree(on: boolean): void {
    this.secondDegree = on;
    if (this.focusId) {
      const index = this.data?.storyById.get(this.focusId);
      if (index !== undefined) this.applyFocus(index, false);
    }
    this.emit();
  }

  setLinesVisible(visible: boolean): void {
    this.linesVisible = visible;
    this.layer.setLinesVisible(visible);
    if (!this.ready) return;
    if (!visible) this.clearLinkLabels();
    else this.updateLinkLabels();
    this.map.setLayoutProperty(
      LINK_LABEL_LAYER,
      "visibility",
      visible && this.labelsVisible ? "visible" : "none",
    );
  }

  setTethersVisible(visible: boolean): void {
    this.tethersVisible = visible;
    this.layer.setTethersVisible(visible);
    if (this.focusId) {
      const index = this.data?.storyById.get(this.focusId);
      if (index !== undefined) this.applyFocus(index, false);
    }
  }

  setHeatVisible(visible: boolean): void {
    this.heatVisible = visible;
    this.layer.setHeatVisible(visible);
  }

  setLabelsVisible(visible: boolean): void {
    this.labelsVisible = visible;
    this.labels.setVisible(visible);
    if (!this.ready) return;
    this.map.setLayoutProperty(
      LINK_LABEL_LAYER,
      "visibility",
      visible && this.linesVisible ? "visible" : "none",
    );
  }

  setStrengthFloor(floor: number): void {
    this.floor = Math.max(0.3, Math.min(0.95, floor));
    this.layer.setStrengthFloor(this.floor);
  }

  get isReady(): boolean {
    return this.ready;
  }

  get focus(): FocusDetails | null {
    return this.focused;
  }

  get view(): GlobeView {
    const center = this.map.getCenter();
    return { center: [center.lng, center.lat], zoom: this.map.getZoom() };
  }

  setView(center: [number, number], zoom: number): void {
    this.camera.jumpTo({ center, zoom, bearing: 0, pitch: 0 });
  }

  searchStories(query: string): LiveStory[] {
    const normalized = query.trim().toLowerCase();
    if (!normalized || !this.data) return [];
    return this.data.stories
      .filter((entry) => {
        if (entry.story.title.toLowerCase().includes(normalized)) return true;
        if (entry.place.toLowerCase().includes(normalized)) return true;
        return entry.story.locations.some((location) =>
          location.name.toLowerCase().includes(normalized),
        );
      })
      .slice(0, 20);
  }

  linkInfo(index: number): LinkInfo | null {
    const data = this.data;
    if (!data) return null;
    const a = data.init.links.a[index];
    const b = data.init.links.b[index];
    if (a === undefined || b === undefined) return null;
    const start = data.stories[a];
    const end = data.stories[b];
    if (!start || !end) return null;
    return {
      index,
      type: data.init.links.type[index] ?? 0,
      direction: data.init.links.direction?.[index] ?? 0,
      strength: data.init.links.strength[index] ?? 0,
      confidence: data.init.links.confidence?.[index] ?? 0,
      distanceKm: data.init.links.distanceKm?.[index] ?? 0,
      unverified: data.init.links.unverified?.[index] === 1,
      aId: start.story.id,
      bId: end.story.id,
      aTitle: start.story.title,
      bTitle: end.story.title,
      aPlace: start.place,
      bPlace: end.place,
    };
  }

  nearbyStories(lat: number, lon: number, limit = 12): Array<{ entry: LiveStory; km: number }> {
    if (!this.data) return [];
    return this.data.stories
      .map((entry) => ({ entry, km: haversineKm(lat, lon, entry.lat, entry.lng) }))
      .sort((a, b) => a.km - b.km)
      .slice(0, limit);
  }

  get state(): GlobeState {
    return {
      ready: this.ready,
      storyCount: this.activeStoryCount,
      linkCount: this.activeLinkCount,
      focused: this.focused,
      topStoryIds: this.data?.topStoryIds ?? [],
      topStories: this.data
        ? this.data.topStoryIds
            .map((id) => {
              const index = this.data!.storyById.get(id);
              return index === undefined ? undefined : this.data!.stories[index];
            })
            .filter((entry): entry is LiveStory => entry !== undefined)
            .slice(0, 12)
        : [],
      edgeMarkers: this.edgeMarkers,
      secondDegree: this.secondDegree,
      quality: this.quality,
      tileFailed: this.tileFailed,
      perf: this.perf.snapshot(),
      initCount: this.initCount,
      patchCount: this.patchCount,
      rippleCount: this.rippleCount,
      focusLatencyMs: this.focusLatencyMs,
    };
  }

  private emit(): void {
    this.options.onState?.(this.state);
  }

  destroy(): void {
    this.destroyed = true;
    if (this.frameHandle !== null) cancelAnimationFrame(this.frameHandle);
    if (this.restoreTimer !== null) clearTimeout(this.restoreTimer);
    window.removeEventListener("keydown", this.onKeyDown);
    this.map.off("webglcontextlost", this.onContextLost);
    this.map.off("webglcontextrestored", this.onContextRestored);
    this.perf.destroy();
    this.map.remove();
  }
}

function destinationPoint(
  lat: number,
  lon: number,
  km: number,
  bearingDegrees: number,
): [number, number] {
  const radius = 6371;
  const angular = km / radius;
  const bearing = (bearingDegrees * Math.PI) / 180;
  const lat1 = (lat * Math.PI) / 180;
  const lon1 = (lon * Math.PI) / 180;
  const lat2 = Math.asin(
    Math.sin(lat1) * Math.cos(angular) + Math.cos(lat1) * Math.sin(angular) * Math.cos(bearing),
  );
  const lon2 =
    lon1 +
    Math.atan2(
      Math.sin(bearing) * Math.sin(angular) * Math.cos(lat1),
      Math.cos(angular) - Math.sin(lat1) * Math.sin(lat2),
    );
  return [(lon2 * 180) / Math.PI, (lat2 * 180) / Math.PI];
}

function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const toRad = Math.PI / 180;
  const dLat = (lat2 - lat1) * toRad;
  const dLon = (lon2 - lon1) * toRad;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * toRad) * Math.cos(lat2 * toRad) * Math.sin(dLon / 2) ** 2;
  return 2 * 6371 * Math.asin(Math.min(1, Math.sqrt(a)));
}
