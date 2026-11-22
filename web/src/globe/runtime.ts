/**
 * Production globe runtime.
 *
 * Reuses the M0 custom layer, GPU data and focus mirror, but drives them from
 * real dor.v1 snapshot/delta messages. The fake stream is never imported here.
 */
import { Map as MapLibreMap, type GeoJSONSource, type MapMouseEvent } from "maplibre-gl";
import type { WorkerInit, WorkerPatch } from "../worker/messages";
import { growArray } from "./arrays";
import { PatchHold } from "./gestures";
import { PerfMeter, type PerfSnapshot } from "./perf";
import { CameraController, type CameraTarget } from "./camera";
import { loadGlobeConfig } from "./config";
import { CustomGlobeLayer } from "./custom/layer";
import { FocusMirror } from "./focus-mirror";
import { StoryLabels, STORY_LABEL_LAYER, type AmbientLabel } from "./labels";
import {
  buildLiveTethers,
  type LiveGlobeData,
  type LiveStory,
  type StorySummary,
} from "./live-data";
import { greatCirclePoint } from "./mercator";
import { categoryColor, linkColor } from "./encoding";
import { shouldIgnoreGlobeKeys } from "../keyboard";
import { buildLabelsOnlyStyle } from "./style/dor-labels";
import { TilePrefetcher } from "./tile-prefetch";
import { WORLD } from "./world";
import { countryAt, type WorldFeature } from "./topojson";
import { regionFor } from "./regions";
import type { HudFocus } from "../worker/fake/protocol";

const DEFAULT_VIEW: CameraTarget = { center: [6, 22], zoom: 1.55, bearing: 0, pitch: 0 };
const LINK_LABEL_SOURCE = "dor-link-labels";
const LINK_LABEL_LAYER = "dor-link-labels";
const LINK_HOVER_SOURCE = "dor-link-hover-label";
const LINK_HOVER_LAYER = "dor-link-hover-label";
const DISTANCE_CHIP_SOURCE = "dor-distance-chips";
const DISTANCE_CHIP_LAYER = "dor-distance-chips";
const COUNTRY_SOURCE = "dor-country-tint";
const COUNTRY_FILL_LAYER = "dor-country-tint-fill";
const COUNTRY_LINE_LAYER = "dor-country-tint-line";
const WORLD_SOURCE = "dor-world-land";
const WORLD_FILL_LAYER = "dor-world-land-fill";
const WORLD_LINE_LAYER = "dor-world-land-line";
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

const SCALE_STEPS = [
  0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000,
];

/** Zoom at which a story becomes an area instead of a precise pin (D5). */
const AREA_ZOOM: Readonly<Record<number, number>> = { 0: 3.8, 1: 5.8, 2: 9.5, 3: 14.2 };

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

export type FocusThreadStory = {
  storyId: string;
  title: string;
  place: string;
  category: number;
  firstSeen: number;
  updatedAt: number;
};

export type FocusDetails = {
  storyId: string;
  story: StorySummary;
  hud: HudFocus;
  live: LiveStory;
  threadStoryIds: string[];
  /** The focused story followed by every lit story, with real event times. */
  thread: FocusThreadStory[];
  secondDegree: boolean;
};

export type GlobeView = { center: [number, number]; zoom: number };

export type GlobeScale = { km: number; px: number };

export type MatchSummary = { active: boolean; matched: number; total: number };

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
  scale: GlobeScale | null;
  match: MatchSummary;
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

function hexColor(rgb: readonly [number, number, number]): string {
  return `#${rgb.map((value) => value.toString(16).padStart(2, "0")).join("")}`;
}

const REGIONS: ReadonlyArray<{
  key: string;
  label: string;
  center: [number, number];
  zoom: number;
}> = [
  { key: "asia", label: "ASIA", center: [92, 24], zoom: 1.9 },
  { key: "americas", label: "AMERICAS", center: [-80, 18], zoom: 1.9 },
  { key: "europe", label: "EUROPE", center: [16, 48], zoom: 2.4 },
  { key: "africa", label: "AFRICA", center: [20, 5], zoom: 2.1 },
  { key: "oceania", label: "OCEANIA", center: [140, -25], zoom: 2.1 },
];

/** Riyadh is Asia: the Middle East gets its own test before Africa/Europe. */

function frontHemisphere(lng: number, lat: number, centerLng: number, centerLat: number): boolean {
  const toRad = Math.PI / 180;
  const cosine =
    Math.sin(lat * toRad) * Math.sin(centerLat * toRad) +
    Math.cos(lat * toRad) * Math.cos(centerLat * toRad) * Math.cos((lng - centerLng) * toRad);
  return cosine > 0;
}

function formatKm(km: number): string {
  if (km >= 1000) return Math.round(km).toLocaleString("en-US");
  if (km >= 100) return km.toFixed(0);
  if (km >= 10) return km.toFixed(1);
  return km.toFixed(2);
}

function compactPlace(place: string): string {
  return place.split(",")[0]?.trim() || place;
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
  private coastlineVisible = true;
  private quality = 0;
  private floor = 0.6;
  private tileFailed = false;
  private scale: GlobeScale | null = null;
  private match: MatchSummary = { active: false, matched: 0, total: 0 };
  private hoverLink: number | null = null;
  private hoverRaf: number | null = null;
  private readonly tiles: TilePrefetcher;

  private frameSamples: number[] = [];
  private lastFrameAt = 0;
  private goodSince = 0;
  private frameHandle: number | null = null;
  private restoreTimer: ReturnType<typeof setTimeout> | null = null;
  private restoreAttempts = 0;
  private fadeUntil = 0;

  constructor(container: HTMLElement, options: RuntimeOptions = {}) {
    this.options = options;
    if (new URLSearchParams(location.search).has("diagnostics")) {
      (window as unknown as { __dorLive?: LiveGlobe }).__dorLive = this;
    }
    const config = loadGlobeConfig();
    this.tiles = new TilePrefetcher(config.tilesUrl);
    this.tiles.preloadGlyphs(config.glyphsUrl);
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
    this.labels = new StoryLabels(this.map);
    this.map.on("load", () => this.onLoad());
    this.map.on("error", (event) => {
      const message = (event as { error?: Error }).error?.message ?? "";
      if (message && !message.includes("Failed to fetch")) console.warn(`map: ${message}`);
      if (!this.tileFailed && /tile|vector|glyph|sprite|fetch|network|404|5\d\d/i.test(message)) {
        this.tileFailed = true;
        this.map.getContainer().classList.add("tile-failed");
        this.showWorldFallback(true);
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
    this.addWorldFallback(firstSymbol);
    this.addCountryLayer(firstSymbol);
    this.addAreaLayer(firstSymbol);
    this.map.addLayer(this.layer, firstSymbol);
    this.layer.setLinesVisible(this.linesVisible);
    this.layer.setTethersVisible(this.tethersVisible);
    this.layer.setHeatVisible(this.heatVisible);
    this.layer.setQuality(this.quality);
    this.addAnnotations();
    this.labels.add();
    this.setCoastlineVisible(this.coastlineVisible);
    this.ready = true;
    this.map.on("movestart", () => this.patches.setGestureActive(true));
    this.map.on("moveend", () => {
      this.patches.setGestureActive(false);
      this.applyPendingPatches();
      this.onMoveEnd();
    });
    this.map.on("mousemove", this.onMouseMove);
    this.map.getCanvas().addEventListener("mouseleave", this.onMouseLeave);
    if (this.pending) {
      const pending = this.pending;
      this.pending = null;
      this.setInit(pending, false);
    }
    this.updateScale();
    this.emit();
  }

  /** Bundled Natural Earth land, visible at low zoom and when tiles fail. */
  private addWorldFallback(beforeId: string | undefined): void {
    if (this.map.getSource(WORLD_SOURCE)) return;
    this.map.addSource(WORLD_SOURCE, {
      type: "geojson",
      data: WORLD,
    });
    this.map.addLayer(
      {
        id: WORLD_FILL_LAYER,
        type: "fill",
        source: WORLD_SOURCE,
        maxzoom: 5.5,
        paint: {
          "fill-color": "#0b1c33",
          "fill-opacity": 0.9,
        },
      },
      beforeId,
    );
    this.map.addLayer(
      {
        id: WORLD_LINE_LAYER,
        type: "line",
        source: WORLD_SOURCE,
        maxzoom: 5.5,
        paint: {
          "line-color": "rgba(126,214,236,0.32)",
          "line-width": ["interpolate", ["linear"], ["zoom"], 0, 0.4, 5, 0.7],
        },
      },
      beforeId,
    );
  }

  private showWorldFallback(allZooms: boolean): void {
    if (!this.map.getLayer(WORLD_FILL_LAYER)) return;
    this.map.setLayerZoomRange(WORLD_FILL_LAYER, 0, allZooms ? 24 : 5.5);
    this.map.setLayerZoomRange(WORLD_LINE_LAYER, 0, allZooms ? 24 : 5.5);
  }

  /** Party countries under the custom dots, above the basemap. */
  private addCountryLayer(beforeId: string | undefined): void {
    if (this.map.getSource(COUNTRY_SOURCE)) return;
    this.map.addSource(COUNTRY_SOURCE, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
    });
    this.map.addLayer(
      {
        id: COUNTRY_FILL_LAYER,
        type: "fill",
        source: COUNTRY_SOURCE,
        paint: {
          "fill-color": "#5ef2ff",
          "fill-opacity": 0.1,
        },
      },
      beforeId,
    );
    this.map.addLayer(
      {
        id: COUNTRY_LINE_LAYER,
        type: "line",
        source: COUNTRY_SOURCE,
        paint: {
          "line-color": "rgba(94,242,255,0.52)",
          "line-width": 0.8,
        },
      },
      beforeId,
    );
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
          paint: {
            "circle-radius": [
              "interpolate",
              ["linear"],
              ["zoom"],
              0,
              12,
              4,
              18,
              8,
              26,
              14,
              36,
              17,
              48,
            ],
            "circle-color": ["get", "color"],
            // D5: only features at/after their precision threshold are in the
            // source (updateAreas filters by zoom), so no zoom expression is
            // needed inside paint.
            "circle-opacity": 0.18,
            "circle-stroke-color": ["get", "color"],
            "circle-stroke-opacity": 0.46,
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
          "text-allow-overlap": false,
          "text-optional": true,
          "text-padding": 3,
          "text-offset": [0, -0.7],
          "symbol-sort-key": ["get", "priority"],
        },
        paint: {
          "text-color": ["get", "color"],
          "text-halo-color": "rgba(3,5,11,0.96)",
          "text-halo-width": 1.4,
        },
      });
    }
    if (!this.map.getSource(LINK_HOVER_SOURCE)) {
      this.map.addSource(LINK_HOVER_SOURCE, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      this.map.addLayer({
        id: LINK_HOVER_LAYER,
        type: "symbol",
        source: LINK_HOVER_SOURCE,
        layout: {
          "text-field": ["get", "label"],
          "text-font": ["Noto Sans Regular"],
          "text-size": 10.5,
          "text-letter-spacing": 0.06,
          "text-allow-overlap": true,
          "text-offset": [0, -0.9],
        },
        paint: {
          "text-color": ["get", "color"],
          "text-halo-color": "rgba(3,5,11,0.98)",
          "text-halo-width": 1.6,
        },
      });
    }
    if (!this.map.getSource(DISTANCE_CHIP_SOURCE)) {
      this.map.addSource(DISTANCE_CHIP_SOURCE, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      this.map.addLayer({
        id: DISTANCE_CHIP_LAYER,
        type: "symbol",
        source: DISTANCE_CHIP_SOURCE,
        layout: {
          "text-field": ["get", "label"],
          "text-font": ["Noto Sans Regular"],
          "text-size": 9.5,
          "text-letter-spacing": 0.06,
          "text-allow-overlap": false,
          "text-optional": true,
          "text-offset": [0, 1.2],
          "text-anchor": "top",
          "text-padding": 2,
          "symbol-sort-key": ["get", "priority"],
        },
        paint: {
          "text-color": "rgba(199,226,255,0.92)",
          "text-halo-color": "rgba(3,5,11,0.95)",
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
      this.addWorldFallback(firstSymbol);
      this.addCountryLayer(firstSymbol);
      this.addAreaLayer(firstSymbol);
      if (!this.map.getLayer(this.layer.id)) this.map.addLayer(this.layer, firstSymbol);
      this.addAnnotations();
      this.labels.add();
      this.setCoastlineVisible(this.coastlineVisible);
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
    // Precision areas are zoom-gated in the source, so rebuild them on every
    // settled view (not only when story metadata changed).
    this.updateAreas();
    this.annotationsDirty = false;
    if (this.focused) this.setFocusLabels();
    else this.setAmbientLabels();
    this.updateAggregates();
    this.updateEdgeMarkers();
    this.updateDistanceRings();
    this.updateDistanceChips();
    this.updateScale();
    const center = this.map.getCenter();
    this.tiles.prefetch([center.lng, center.lat], this.map.getZoom());
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
    this.match = message.match ?? {
      active: false,
      matched: summaries.filter(Boolean).length,
      total: summaries.filter(Boolean).length,
    };
    this.setAmbientLabels();
    this.updateAreas();
    this.updateAggregates();
    this.updateEdgeMarkers();
    this.updateDistanceRings();
    this.updateDistanceChips();
    this.updateCountryTint();
    this.updateScale();
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
      this.match = patch.match;
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
    if (this.focused) this.setFocusLabels();
    else this.setAmbientLabels();
    this.updateAreas();
    this.updateAggregates();
    this.updateEdgeMarkers();
    this.updateDistanceChips();
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
    this.clearHoverLabel();
    this.camera.cancel();
    this.focusId = null;
    this.focused = null;
    this.fadeUntil = performance.now() + 240;
    this.updateDistanceRings();
    this.updateDistanceChips();
    this.updateCountryTint();
    this.updateAggregates();
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
    this.fadeUntil = performance.now() + 240;
    this.layer.writeFocus(result.storyChanges, result.linkChanges, true);
    if (this.tethersVisible) {
      const tethers = buildLiveTethers(entry);
      this.layer.applyTethers(tethers.segments, tethers.rings);
    } else {
      this.layer.applyTethers([], []);
    }
    this.setFocusLabels(result.hud);
    if (fly && result.points.length > 0) this.camera.flyTo(this.camera.fitThread(result.points));
    const threadStoryIds = [...mirror.litStoryIndices]
      .map((storyIndex) => data.stories[storyIndex]?.story.id)
      .filter((id): id is string => id !== undefined);
    const thread: FocusThreadStory[] = [...mirror.litStoryIndices]
      .map((storyIndex) => data.stories[storyIndex])
      .filter((entry): entry is LiveStory => entry !== undefined)
      .map((entry) => ({
        storyId: entry.story.id,
        title: entry.story.title,
        place: entry.place,
        category: entry.category,
        firstSeen: Number(entry.story.firstSeen),
        updatedAt: Number(entry.story.updatedAt),
      }));
    this.focused = {
      storyId: entry.story.id,
      story: entry.story,
      hud: result.hud,
      live: entry,
      threadStoryIds,
      thread,
      secondDegree: this.secondDegree,
    };
    this.updateDistanceRings();
    this.updateDistanceChips();
    this.updateCountryTint();
    this.updateAggregates();
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

  /** Focus labels follow the zoom label budget, strongest links first. */
  private setFocusLabels(hud = this.focused?.hud): void {
    const data = this.data;
    const focused = this.focused;
    if (!data || !focused || !hud) return;
    const zoom = this.map.getZoom();
    const budget = zoom < 4 ? 6 : zoom < 8 ? 10 : 1;
    const indices = new Set<number>();
    const focusIndex = data.storyById.get(focused.storyId);
    if (focusIndex !== undefined) indices.add(focusIndex);
    for (const link of hud.links.slice(0, Math.max(0, budget - 1))) {
      indices.add(link.storyIndex);
    }
    const labels = [...indices]
      .map((index) => data.stories[index])
      .filter((entry): entry is LiveStory => entry !== undefined)
      .map((entry) => ({
        lng: entry.lng,
        lat: entry.lat,
        category: entry.category,
        place: entry.place,
      }));
    this.labels.setFocus({ ...hud, labels });
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
    // Declutter: label only the strongest handful; MapLibre drops the rest on
    // collision. Any other link reveals its label on hover.
    const labelled = this.focused.hud.links
      .filter((link) => link.strength >= 0.3)
      .sort((left, right) => right.strength - left.strength)
      .slice(0, 7);
    const features = labelled
      .map((link, rank) => {
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
            color: hexColor(linkColor(link.type)),
            linkIndex: link.index,
            priority: rank,
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

  private updateHoverLabel(index: number | null): void {
    const source = this.map.getSource(LINK_HOVER_SOURCE) as GeoJSONSource | undefined;
    if (!source) return;
    if (index === null || !this.focused || !this.data) {
      this.hoverLink = null;
      source.setData({ type: "FeatureCollection", features: [] });
      return;
    }
    const link = this.focused.hud.links.find((candidate) => candidate.index === index);
    const a = this.data.init.links.a[index];
    const b = this.data.init.links.b[index];
    if (!link || a === undefined || b === undefined) {
      source.setData({ type: "FeatureCollection", features: [] });
      return;
    }
    const start = this.data.stories[a];
    const end = this.data.stories[b];
    if (!start || !end) return;
    const mid = greatCirclePoint([start.lng, start.lat], [end.lng, end.lat], 0.5);
    this.hoverLink = index;
    source.setData({
      type: "FeatureCollection",
      features: [
        {
          type: "Feature",
          geometry: { type: "Point", coordinates: mid },
          properties: {
            label: `${link.strength.toFixed(2)} · ${(link.distanceKm ?? 0).toFixed(0)} km`,
            color: hexColor(linkColor(link.type)),
          },
        },
      ],
    });
  }

  private clearHoverLabel(): void {
    this.updateHoverLabel(null);
  }

  private readonly onMouseLeave = (): void => {
    if (this.hoverRaf !== null) cancelAnimationFrame(this.hoverRaf);
    this.hoverRaf = null;
    this.clearHoverLabel();
  };

  private readonly onMouseMove = (event: MapMouseEvent): void => {
    if (!this.focused || !this.linesVisible) return;
    if (this.hoverRaf !== null) cancelAnimationFrame(this.hoverRaf);
    const point = event.point;
    this.hoverRaf = requestAnimationFrame(() => {
      this.hoverRaf = null;
      if (!this.focused || !this.linesVisible || this.contextLost) return;
      const indices = this.focused.hud.links.map((link) => link.index).sort((a, b) => a - b);
      const index = this.layer.hoverFocusedArc(indices, point.x, point.y);
      if (index !== this.hoverLink) this.updateHoverLabel(index);
    });
  };

  private clearLinkLabels(): void {
    if (!this.ready) return;
    const source = this.map.getSource(LINK_LABEL_SOURCE) as GeoJSONSource | undefined;
    source?.setData({ type: "FeatureCollection", features: [] });
  }

  private updateAreas(): void {
    if (!this.ready || !this.data) return;
    const source = this.map.getSource(AREA_SOURCE) as GeoJSONSource | undefined;
    if (!source) return;
    const zoom = this.map.getZoom();
    const features = this.data.stories
      .filter((entry) => {
        const location = entry.story.locations[entry.story.primary] ?? entry.story.locations[0];
        if (location === undefined || location.precision > 3) return false;
        return zoom >= (AREA_ZOOM[location.precision] ?? 3.8);
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
            color: hexColor(categoryColor(entry.category)),
            precision: location.precision,
            areaZoom: AREA_ZOOM[location.precision] ?? 3.8,
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
    // Ambient mode has no anchor; rings are focus-only.
    if (!this.focused) {
      source.setData({ type: "FeatureCollection", features: [] });
      labels.setData({ type: "FeatureCollection", features: [] });
      return;
    }
    const center: [number, number] = [this.focused.live.lng, this.focused.live.lat];
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

  private updateDistanceChips(): void {
    if (!this.ready || !this.data) return;
    const source = this.map.getSource(DISTANCE_CHIP_SOURCE) as GeoJSONSource | undefined;
    if (!source) return;
    const zoom = this.map.getZoom();
    if (!this.focused && zoom < 3.5) {
      source.setData({ type: "FeatureCollection", features: [] });
      return;
    }
    const anchor = this.focused
      ? ([this.focused.live.lng, this.focused.live.lat] as [number, number])
      : ([this.map.getCenter().lng, this.map.getCenter().lat] as [number, number]);
    const budget = zoom < 8 ? 4 : zoom < 12 ? 6 : 8;
    const candidates: Array<{ entry: LiveStory; rank: number }> = [];
    if (this.focused) {
      this.focused.hud.links.forEach((link, rank) => {
        const entry = this.data!.stories[link.storyIndex];
        if (entry) candidates.push({ entry, rank });
      });
    } else {
      for (const index of this.data.init.topStories.slice(0, budget * 2)) {
        const entry = this.data.stories[index];
        if (entry) candidates.push({ entry, rank: index });
      }
    }
    const seen = new Set<number>();
    const features = candidates
      .filter(({ entry }) => {
        if (seen.has(entry.index)) return false;
        seen.add(entry.index);
        return true;
      })
      .slice(0, budget)
      .map(({ entry, rank }) => {
        const km = haversineKm(anchor[1], anchor[0], entry.lat, entry.lng);
        if (km < 0.5) return null;
        return {
          type: "Feature" as const,
          geometry: {
            type: "Point" as const,
            coordinates: [entry.lng, entry.lat] as [number, number],
          },
          properties: {
            label: `${formatKm(km)} km`,
            priority: rank,
          },
        };
      })
      .filter((feature): feature is NonNullable<typeof feature> => feature !== null);
    source.setData({ type: "FeatureCollection", features });
  }

  private updateScale(): void {
    if (!this.ready) {
      this.scale = null;
      return;
    }
    const canvas = this.map.getCanvas();
    const width = canvas.clientWidth || 800;
    const height = canvas.clientHeight || 600;
    // Measure the projection itself instead of assuming 256-px Mercator
    // tiles: MapLibre's globe and 512-px zoom scale both change metres/pixel.
    const referencePx = Math.min(110, Math.max(24, width / 4));
    const centerX = width / 2;
    const centerY = height / 2;
    const start = this.map.unproject([centerX, centerY]);
    const end = this.map.unproject([centerX + referencePx, centerY]);
    const referenceMeters = start.distanceTo(end);
    if (!Number.isFinite(referenceMeters) || referenceMeters <= 0) {
      this.scale = null;
      return;
    }
    const referenceKm = referenceMeters / 1000;
    let km = SCALE_STEPS[0]!;
    for (const candidate of SCALE_STEPS) {
      if (candidate <= referenceKm * 1.2) km = candidate;
      else break;
    }
    const px = Math.max(12, Math.min(150, (referencePx * km) / referenceKm));
    this.scale = { km, px };
  }

  private updateCountryTint(): void {
    if (!this.ready) return;
    const source = this.map.getSource(COUNTRY_SOURCE) as GeoJSONSource | undefined;
    if (!source) return;
    const parties = this.focused?.hud.parties ?? [];
    // The boards keep the globe readable beyond six parties: the panel marks
    // the story multi-region instead of drawing a ring and tint per country.
    if (!this.focused || parties.length === 0 || (this.focused.live.partyTotal ?? 0) > 6) {
      source.setData({ type: "FeatureCollection", features: [] });
      return;
    }
    const seen = new Set<string>();
    const features: WorldFeature[] = [];
    for (const party of parties) {
      const country = countryAt(WORLD.features, party.lng, party.lat);
      if (!country) continue;
      const key = country.id ?? country.properties.name;
      if (seen.has(key)) continue;
      seen.add(key);
      features.push(country);
    }
    source.setData({ type: "FeatureCollection", features });
  }

  private updateAggregates(): void {
    if (!this.ready) return;
    const zoom = this.map.getZoom();
    const source = this.map.getSource(AGG_SOURCE) as GeoJSONSource | undefined;
    if (!source) return;
    const show = !this.focused && zoom < 4.2 && (this.data?.stories.length ?? 0) > 0;
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
      { count: number; lng: number; lat: number; categories: Float32Array }
    >();
    for (const entry of this.data.stories) {
      if (!entry) continue;
      const key = `${Math.floor(entry.lng / cell)}:${Math.floor(entry.lat / cell)}`;
      const bucket = buckets.get(key) ?? {
        count: 0,
        lng: 0,
        lat: 0,
        categories: new Float32Array(11),
      };
      bucket.count++;
      bucket.lng += entry.lng;
      bucket.lat += entry.lat;
      bucket.categories[entry.category] = (bucket.categories[entry.category] ?? 0) + 1;
      buckets.set(key, bucket);
    }
    const dominant = (categories: Float32Array): number => {
      let best = 0;
      for (let category = 1; category < categories.length; category++) {
        if ((categories[category] ?? 0) > (categories[best] ?? 0)) best = category;
      }
      return best;
    };
    const features: AggregateFeature[] = [...buckets.values()].map((bucket) => ({
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [bucket.lng / bucket.count, bucket.lat / bucket.count],
      },
      properties: {
        count: bucket.count,
        color: hexColor(categoryColor(dominant(bucket.categories))),
      },
    }));
    source.setData({ type: "FeatureCollection", features });
    const hexFeatures = [...buckets.values()].map((bucket) => {
      const centerLng = bucket.lng / bucket.count;
      const centerLat = bucket.lat / bucket.count;
      const radius = cell * 0.55;
      const color = hexColor(categoryColor(dominant(bucket.categories)));
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
        properties: { count: bucket.count, color },
      };
    });
    hexSource?.setData({ type: "FeatureCollection", features: hexFeatures });
  }

  private updateEdgeMarkers(): void {
    if (!this.ready || !this.data || this.data.stories.length === 0) {
      this.edgeMarkers = [];
      return;
    }
    const canvas = this.map.getCanvas();
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    const zoom = this.map.getZoom();
    const center = this.map.getCenter();
    const visible = (lng: number, lat: number, padX = 40, padTop = 80, padBottom = 80): boolean => {
      // A far-side story can project into the visible canvas on a globe, so
      // screen bounds alone are not enough: require the front hemisphere.
      if (zoom < 5 && !frontHemisphere(lng, lat, center.lng, center.lat)) return false;
      const point = this.map.project([lng, lat]);
      return (
        point.x >= padX &&
        point.x <= width - padX &&
        point.y >= padTop &&
        point.y <= height - padBottom
      );
    };
    if (this.focused) {
      if (zoom >= 8) {
        this.edgeMarkers = [];
        return;
      }
      const linked: EdgeMarker[] = [];
      for (const link of this.focused.hud.links) {
        const entry = this.data.stories[link.storyIndex];
        if (!entry) continue;
        if (visible(entry.lng, entry.lat)) continue;
        linked.push({
          key: `link-${link.index}`,
          label: `${compactPlace(link.place).toUpperCase()} · ${(link.distanceKm ?? 0).toFixed(0)} km`,
          count: 1,
          center: [entry.lng, entry.lat],
          zoom: Math.max(5, this.map.getZoom()),
        });
      }
      this.edgeMarkers = linked.slice(0, 4);
      return;
    }
    const counts = new Map<string, number>();
    for (const entry of this.data.stories) {
      if (!entry) continue;
      if (visible(entry.lng, entry.lat)) continue;
      const key = regionFor(entry.lng, entry.lat);
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    this.edgeMarkers = REGIONS.map((region) => ({
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
    if (performance.now() < this.fadeUntil) this.map.triggerRepaint();
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

  /** Zoom rail: anchor on the focused story, otherwise the screen centre. */
  flyToBand(label: "GLOBE" | "REGION" | "CITY" | "STREET", zoom?: number): void {
    const targetZoom = zoom ?? { GLOBE: 1.55, REGION: 6, CITY: 11, STREET: 15 }[label];
    const center = this.focused
      ? ([this.focused.live.lng, this.focused.live.lat] as [number, number])
      : ([this.map.getCenter().lng, this.map.getCenter().lat] as [number, number]);
    this.tiles.prefetch(center, targetZoom);
    this.camera.flyTo({ center, zoom: targetZoom, bearing: 0, pitch: 0 });
  }

  flyTo(center: [number, number], zoom: number): void {
    this.tiles.prefetch(center, zoom);
    this.camera.flyTo({ center, zoom, bearing: 0, pitch: 0 });
  }

  setCoastlineVisible(visible: boolean): void {
    this.coastlineVisible = visible;
    if (!this.ready) return;
    for (const layer of ["coastline", "waterway"]) {
      if (this.map.getLayer(layer)) {
        this.map.setLayoutProperty(layer, "visibility", visible ? "visible" : "none");
      }
    }
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
    if (!visible) {
      this.clearLinkLabels();
      this.clearHoverLabel();
    } else this.updateLinkLabels();
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
    this.map.setLayoutProperty(LINK_HOVER_LAYER, "visibility", visible ? "visible" : "none");
    this.map.setLayoutProperty(DISTANCE_CHIP_LAYER, "visibility", visible ? "visible" : "none");
  }

  /** A zero-match filter dims the whole globe, labels included. */
  setLabelsDimmed(dimmed: boolean): void {
    if (!this.ready) return;
    const opacity = dimmed ? 0.2 : 1;
    for (const [layer, property] of [
      [STORY_LABEL_LAYER, "text-opacity"],
      [LINK_LABEL_LAYER, "text-opacity"],
      [LINK_HOVER_LAYER, "text-opacity"],
      [DISTANCE_CHIP_LAYER, "text-opacity"],
      [RING_LABEL_SOURCE, "text-opacity"],
    ] as const) {
      if (this.map.getLayer(layer)) this.map.setPaintProperty(layer, property, opacity);
    }
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
    this.updateScale();
    this.updateDistanceChips();
    this.updateDistanceRings();
    this.updateEdgeMarkers();
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
      scale: this.scale,
      match: this.match,
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
    if (this.hoverRaf !== null) cancelAnimationFrame(this.hoverRaf);
    window.removeEventListener("keydown", this.onKeyDown);
    this.map.off("webglcontextlost", this.onContextLost);
    this.map.off("webglcontextrestored", this.onContextRestored);
    this.map.getCanvas().removeEventListener("mouseleave", this.onMouseLeave);
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
