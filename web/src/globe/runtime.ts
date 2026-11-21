/**
 * Production globe runtime.
 *
 * Reuses the M0 custom layer, GPU data and focus mirror, but drives them from
 * real dor.v1 snapshot/delta messages. The fake stream is never imported here.
 */
import { Map as MapLibreMap, ScaleControl, type GeoJSONSource } from "maplibre-gl";
import type { Delta, Link, Story, WireRow } from "../gen/dor/v1/dor_pb";
import { CameraController, type CameraTarget } from "./camera";
import { loadGlobeConfig } from "./config";
import { CustomGlobeLayer } from "./custom/layer";
import { FocusMirror } from "./focus-mirror";
import { StoryLabels, type AmbientLabel } from "./labels";
import {
  buildLiveGlobe,
  buildLiveTethers,
  linkKey,
  storyLocation,
  type LiveGlobeData,
  type LiveStory,
} from "./live-data";
import { greatCirclePoint } from "./mercator";
import { RippleRingBuffer, type RippleSlotState } from "./ripples";
import { buildLabelsOnlyStyle } from "./style/dor-labels";
import type { HudFocus, StreamPatch } from "../worker/fake/protocol";

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
  story: Story;
  hud: HudFocus;
  live: LiveStory;
  threadStoryIds: string[];
  secondDegree: boolean;
};

export type GlobeView = { center: [number, number]; zoom: number };

export type StoryFilter = {
  categories: Set<number>;
  regions: string[];
  sources: string[];
  minSources: number;
  lat: number;
  lon: number;
  radiusKm: number;
  promptStoryIds: Set<string>;
};

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
};

type RuntimeOptions = {
  onState?: (state: GlobeState) => void;
  onFocus?: (focus: FocusDetails | null) => void;
  onLinkClick?: (link: LinkInfo) => void;
  onView?: (view: GlobeView) => void;
};

type AggregateFeature = {
  type: "Feature";
  geometry: { type: "Point"; coordinates: [number, number] };
  properties: { count: number; color: string };
};

function emptyPatch(ripples: StreamPatch["ripples"]): StreamPatch {
  return {
    type: "stream",
    now: Date.now(),
    storyIndex: new Uint16Array(),
    sources: new Uint16Array(),
    heat: new Float32Array(),
    updatedAt: new Float32Array(),
    linkIndex: new Uint16Array(),
    strength: new Float32Array(),
    width: new Float32Array(),
    linkColor: new Uint8Array(),
    ripples,
  };
}

function linkColor(type: number): string {
  return type === 0 ? "#ffc25c" : type === 1 ? "#ff6fd8" : "#5ef2ff";
}

export class LiveGlobe {
  readonly map: MapLibreMap;

  private readonly layer = new CustomGlobeLayer();
  private readonly camera: CameraController;
  private readonly labels: StoryLabels;
  private readonly options: RuntimeOptions;
  private readonly ripples = new RippleRingBuffer();

  private stories = new Map<string, Story>();
  private links = new Map<string, Link>();
  private data: LiveGlobeData | null = null;
  private mirror: FocusMirror | null = null;
  private focusId: string | null = null;
  private focused: FocusDetails | null = null;
  private ready = false;
  private pending: { stories: Story[]; links: Link[] } | null = null;
  private lastPick: number | null = null;
  private lastPickAt = 0;
  private lastPickPoint: { x: number; y: number } | null = null;
  private destroyed = false;
  private secondDegree = false;
  private edgeMarkers: EdgeMarker[] = [];
  private labelsVisible = true;
  private linesVisible = true;
  private tethersVisible = true;
  private heatVisible = true;
  private quality = 0;
  private floor = 0.6;
  private tileFailed = false;
  private storyFilter: StoryFilter | null = null;

  private frameSamples: number[] = [];
  private lastFrameAt = 0;
  private goodSince = 0;
  private frameHandle: number | null = null;
  private idleHandle: number | null = null;
  private lastInteractionAt = Date.now();

  constructor(container: HTMLElement, options: RuntimeOptions = {}) {
    this.options = options;
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
    this.map.addControl(new ScaleControl({ unit: "metric", maxWidth: 110 }), "bottom-right");
    this.labels = new StoryLabels(this.map);
    this.map.on("load", () => this.onLoad());
    this.map.on("error", (event) => {
      const message = (event as { error?: Error }).error?.message ?? "";
      if (message && !message.includes("Failed to fetch")) console.warn(`map: ${message}`);
      if (
        !this.tileFailed &&
        /tile|vector|glyph|sprite|fetch|network|404|5\d\d/i.test(message)
      ) {
        this.tileFailed = true;
        this.map.getContainer().classList.add("tile-failed");
        void this.addFallbackCities();
        this.emit();
      }
    });
    this.installInteractions();
    this.frameHandle = requestAnimationFrame(this.sampleFrame);
    this.idleHandle = window.setInterval(() => {
      if (this.destroyed || this.focused || this.map.isMoving()) return;
      if (Date.now() - this.lastInteractionAt < 30_000) return;
      this.map.panBy([0.012, 0], { duration: 0 });
    }, 1000);
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
    this.map.on("moveend", () => this.onMoveEnd());
    if (this.pending) {
      const pending = this.pending;
      this.pending = null;
      this.rebuild(pending.stories, pending.links, false);
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
            ],
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
    const remember = (event: { point: { x: number; y: number }; originalEvent?: unknown }) => {
      if (!event.originalEvent) return;
      this.lastPick = this.layer.pickNow(event.point.x, event.point.y);
      this.lastPickAt = performance.now();
      this.lastPickPoint = { x: event.point.x, y: event.point.y };
    };
    this.map.on("mousedown", remember);
    this.map.on("touchstart", remember);
    for (const eventName of ["mousedown", "touchstart", "wheel", "dragstart", "zoomstart"] as const) {
      this.map.on(eventName, () => {
        this.lastInteractionAt = Date.now();
      });
    }
    this.map.on("click", (event) => {
      if (this.map.getLayer(LINK_LABEL_LAYER)) {
        const hit = this.map.queryRenderedFeatures(event.point, { layers: [LINK_LABEL_LAYER] })[0];
        if (hit) {
          const index = Number(hit.properties?.linkIndex);
          const info = this.linkInfo(index);
          if (info) this.options.onLinkClick?.(info);
          return;
        }
      }
      const now = performance.now();
      const cached =
        this.lastPick !== null &&
        this.lastPickPoint !== null &&
        now - this.lastPickAt < 1000 &&
        Math.hypot(
          this.lastPickPoint.x - event.point.x,
          this.lastPickPoint.y - event.point.y,
        ) < 8;
      const arc = this.findArcAt(event.point.x, event.point.y);
      if (arc !== null) {
        const info = this.linkInfo(arc);
        if (info) {
          this.options.onLinkClick?.(info);
          return;
        }
      }
      const picked = cached ? this.lastPick : this.layer.pickNow(event.point.x, event.point.y);
      if (picked !== null && picked >= 0) {
        const entry = this.data?.stories[picked];
        if (entry) this.focusStoryId(entry.story.id);
      }
    });
    window.addEventListener("keydown", this.onKeyDown);
  }

  private readonly onKeyDown = (event: KeyboardEvent): void => {
    if (event.key === "Escape") this.clearFocus();
  };

  private onMoveEnd(): void {
    if (!this.focused) this.setAmbientLabels();
    this.updateAggregates();
    this.updateEdgeMarkers();
    this.updateDistanceRings();
    this.options.onView?.(this.view);
    this.emit();
  }

  setSnapshot(stories: Story[], links: Link[]): void {
    this.stories = new Map(stories.map((story) => [story.id, story]));
    this.links = new Map(links.map((link) => [linkKey(link.a, link.b), link]));
    if (!this.ready) {
      this.pending = { stories: [...this.stories.values()], links: [...this.links.values()] };
      return;
    }
    this.rebuild([...this.stories.values()], [...this.links.values()], true);
  }

  /** Wire-only batches stay on the hot path; structural changes rebuild once. */
  applyDeltas(deltas: Delta[]): void {
    let structural = false;
    const wire: WireRow[] = [];
    for (const delta of deltas) {
      switch (delta.body.case) {
        case "wire":
          wire.push(delta.body.value);
          break;
        case "storyUpsert":
          this.stories.set(delta.body.value.id, delta.body.value);
          structural = true;
          break;
        case "storyExpire": {
          const expired = delta.body.value;
          this.stories.delete(expired);
          for (const [key, link] of this.links) {
            if (link.a === expired || link.b === expired) this.links.delete(key);
          }
          structural = true;
          break;
        }
        case "linkUpsert":
          this.links.set(linkKey(delta.body.value.a, delta.body.value.b), delta.body.value);
          structural = true;
          break;
        case "linkRemove":
          this.links.delete(linkKey(delta.body.value.a, delta.body.value.b));
          structural = true;
          break;
        default:
          break;
      }
    }
    if (structural) this.rebuild([...this.stories.values()], [...this.links.values()], false);
    if (wire.length > 0) this.spawnRipples(wire);
  }

  private rebuild(stories: Story[], links: Link[], flyToFocus: boolean): void {
    const visible = this.storyFilter ? stories.filter((story) => this.matchesStory(story)) : stories;
    const visibleIds = new Set(visible.map((story) => story.id));
    const visibleLinks = links.filter(
      (link) => visibleIds.has(link.a) && visibleIds.has(link.b),
    );
    const data = buildLiveGlobe(visible, visibleLinks);
    this.data = data;
    this.layer.setInit(data.init);
    this.layer.setStrengthFloor(this.floor);
    this.layer.setLinesVisible(this.linesVisible);
    this.layer.setTethersVisible(this.tethersVisible);
    this.layer.setHeatVisible(this.heatVisible);
    this.layer.setQuality(this.quality);
    this.mirror = new FocusMirror(data.init);
    this.setAmbientLabels();
    this.updateAreas();
    this.updateAggregates();
    this.updateEdgeMarkers();
    this.updateDistanceRings();
    const focusId = this.focusId;
    if (focusId) {
      const index = data.storyById.get(focusId);
      if (index !== undefined) this.applyFocus(index, flyToFocus);
      else {
        this.focusId = null;
        this.focused = null;
        this.clearLinkLabels();
        this.options.onFocus?.(null);
      }
    }
    this.emit();
    this.map.triggerRepaint();
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
    this.focusId = storyId;
    this.lastInteractionAt = Date.now();
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
      const apiBase = ((import.meta.env as Record<string, string | undefined>).VITE_API_URL || "").replace(/\/$/, "");
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

  private spawnRipples(rows: WireRow[]): void {
    const data = this.data;
    if (!data) return;
    const nowSec = (Date.now() - data.init.epochMs) / 1000;
    const updates: StreamPatch["ripples"] = [];
    for (const row of rows) {
      if (!row.storyId) continue;
      const story = this.stories.get(row.storyId);
      if (!story || story.locations.length === 0) continue;
      if (this.storyFilter && !this.matchesStory(story)) continue;
      const location = story.locations[story.primary] ?? story.locations[0]!;
      const slots = this.ripples.spawn({
        lng: location.lon,
        lat: location.lat,
        category: story.category,
        spawnAt: nowSec,
        intensity: 0.5,
        storyIndex: data.storyById.get(row.storyId) ?? -1,
      });
      for (const slot of slots) updates.push(toRippleUpdate(slot, story.category));
    }
    if (updates.length > 0) this.layer.stream(emptyPatch(updates));
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
    this.map.setLayoutProperty(LINK_LABEL_LAYER, "visibility", this.labelsVisible ? "visible" : "none");
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

  private findArcAt(x: number, y: number): number | null {
    const data = this.data;
    if (!data || !this.linesVisible) return null;
    let best: number | null = null;
    let bestDistance = 7;
    for (let index = 0; index < data.init.linkCount; index++) {
      if ((data.init.links.strength[index] ?? 0) < this.floor) continue;
      const a = data.stories[data.init.links.a[index]!];
      const b = data.stories[data.init.links.b[index]!];
      if (!a || !b) continue;
      const start = this.map.project([a.lng, a.lat]);
      const end = this.map.project([b.lng, b.lat]);
      const distance = distanceToSegment(x, y, start.x, start.y, end.x, end.y);
      if (distance < bestDistance) {
        bestDistance = distance;
        best = index;
      }
    }
    return best;
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
    const buckets = new Map<string, { count: number; lng: number; lat: number; category: number }>();
    for (const entry of this.data.stories) {
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
          centerLng + (radius * Math.cos(angle)) / Math.max(0.2, Math.cos((centerLat * Math.PI) / 180)),
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
  zoomIn(): void { this.map.zoomIn(); }
  zoomOut(): void { this.map.zoomOut(); }
  resetView(): void { this.camera.flyTo(DEFAULT_VIEW); }

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
    this.map.setLayoutProperty(LINK_LABEL_LAYER, "visibility", visible && this.labelsVisible ? "visible" : "none");
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
    this.map.setLayoutProperty(LINK_LABEL_LAYER, "visibility", visible && this.linesVisible ? "visible" : "none");
  }

  setStrengthFloor(floor: number): void {
    this.floor = Math.max(0.3, Math.min(0.95, floor));
    this.layer.setStrengthFloor(this.floor);
  }

  setStoryFilter(filter: StoryFilter | null): void {
    this.storyFilter = filter;
    if (!this.ready) return;
    this.rebuild([...this.stories.values()], [...this.links.values()], false);
  }

  private matchesStory(story: Story): boolean {
    const filter = this.storyFilter;
    if (!filter) return true;
    if (filter.categories.size > 0 && !filter.categories.has(story.category)) return false;
    if (story.sourceCount < filter.minSources) return false;
    if (filter.promptStoryIds.size > 0 && !filter.promptStoryIds.has(story.id)) return false;
    if (filter.regions.length > 0) {
      const haystack = story.locations.map((location) => location.name.toLowerCase());
      const matches = filter.regions.some((region) =>
        haystack.some((name) => name.includes(region)),
      );
      if (!matches) return false;
    }
    if (filter.radiusKm > 0) {
      const location = storyLocation(story);
      if (!location) return false;
      if (haversineKm(filter.lat, filter.lon, location.lat, location.lng) > filter.radiusKm) {
        return false;
      }
    }
    return true;
  }

  get focus(): FocusDetails | null { return this.focused; }

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
      storyCount: this.data?.init.storyCount ?? 0,
      linkCount: this.data?.init.linkCount ?? 0,
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
    };
  }

  private emit(): void {
    this.options.onState?.(this.state);
  }

  destroy(): void {
    this.destroyed = true;
    if (this.frameHandle !== null) cancelAnimationFrame(this.frameHandle);
    if (this.idleHandle !== null) clearInterval(this.idleHandle);
    window.removeEventListener("keydown", this.onKeyDown);
    this.map.remove();
  }
}

function distanceToSegment(
  px: number,
  py: number,
  ax: number,
  ay: number,
  bx: number,
  by: number,
): number {
  const dx = bx - ax;
  const dy = by - ay;
  const length = dx * dx + dy * dy;
  if (length === 0) return Math.hypot(px - ax, py - ay);
  const t = Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / length));
  return Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
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
    Math.sin(lat1) * Math.cos(angular) +
      Math.cos(lat1) * Math.sin(angular) * Math.cos(bearing),
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

function toRippleUpdate(slot: RippleSlotState, category: number): StreamPatch["ripples"][number] {
  const toRad = Math.PI / 180;
  const x = (slot.lng + 180) / 360;
  const clamped = Math.max(-85.05112878, Math.min(85.05112878, slot.lat));
  const y = 0.5 - Math.log((1 + Math.sin(clamped * toRad)) / (1 - Math.sin(clamped * toRad))) / (4 * Math.PI);
  const hiX = Math.round(x * 8192) / 8192;
  const hiY = Math.round(y * 8192) / 8192;
  return {
    slot: slot.slot,
    lng: slot.lng,
    lat: slot.lat,
    mercHi: [hiX, hiY],
    mercLo: [x - hiX, y - hiY],
    category,
    spawnAt: slot.spawnAt,
    intensity: slot.intensity,
    storyIndex: slot.storyIndex,
  };
}
