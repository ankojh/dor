/**
 * Production globe runtime.
 *
 * Reuses the M0 custom layer, GPU data and focus mirror, but drives them from
 * real dor.v1 snapshot/delta messages. The fake stream is not imported here.
 */
import { Map as MapLibreMap } from "maplibre-gl";
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
  type LiveGlobeData,
  type LiveStory,
} from "./live-data";
import { RippleRingBuffer, type RippleSlotState } from "./ripples";
import { buildLabelsOnlyStyle } from "./style/dor-labels";
import type { HudFocus, StreamPatch } from "../worker/fake/protocol";

const DEFAULT_VIEW: CameraTarget = { center: [6, 22], zoom: 1.55, bearing: 0, pitch: 0 };

export type FocusDetails = {
  storyId: string;
  story: Story;
  hud: HudFocus;
  live: LiveStory;
};

export type GlobeState = {
  ready: boolean;
  storyCount: number;
  linkCount: number;
  focused: FocusDetails | null;
  topStoryIds: string[];
  topStories: LiveStory[];
};

type RuntimeOptions = {
  onState?: (state: GlobeState) => void;
  onFocus?: (focus: FocusDetails | null) => void;
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
      pixelRatio: Math.min(window.devicePixelRatio || 1, 2),
      renderWorldCopies: true,
    });
    this.camera = new CameraController(this.map);
    this.labels = new StoryLabels(this.map);
    this.map.on("load", () => this.onLoad());
    this.map.on("error", (event) => {
      // Tile/glyph failures are non-fatal; the layer still renders.
      const message = (event as { error?: Error }).error?.message;
      if (message && !message.includes("Failed to fetch")) {
        console.warn(`map: ${message}`);
      }
    });
    this.installInteractions();
  }

  private onLoad(): void {
    if (this.destroyed) return;
    const firstSymbol = (this.map.getStyle().layers ?? []).find(
      (candidate) => candidate.type === "symbol",
    )?.id;
    this.map.addLayer(this.layer, firstSymbol);
    this.labels.add();
    this.ready = true;
    if (this.pending) {
      const pending = this.pending;
      this.pending = null;
      this.rebuild(pending.stories, pending.links, false);
    }
    this.emit();
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
    this.map.on("click", (event) => {
      const now = performance.now();
      const cached =
        this.lastPick !== null &&
        this.lastPickPoint !== null &&
        now - this.lastPickAt < 1000 &&
        Math.hypot(
          this.lastPickPoint.x - event.point.x,
          this.lastPickPoint.y - event.point.y,
        ) < 8;
      const picked = cached
        ? this.lastPick
        : this.layer.pickNow(event.point.x, event.point.y);
      if (picked !== null && picked >= 0) {
        const entry = this.data?.stories[picked];
        if (entry) this.focusStoryId(entry.story.id);
      }
    });
    window.addEventListener("keydown", this.onKeyDown);
  }

  private readonly onKeyDown = (event: KeyboardEvent): void => {
    if (event.key === "Escape") {
      this.clearFocus();
    }
  };

  setSnapshot(stories: Story[], links: Link[]): void {
    this.stories = new Map(stories.map((story) => [story.id, story]));
    this.links = new Map(links.map((link) => [linkKey(link.a, link.b), link]));
    if (!this.ready) {
      this.pending = { stories: [...this.stories.values()], links: [...this.links.values()] };
      return;
    }
    this.rebuild([...this.stories.values()], [...this.links.values()], true);
  }

  /**
   * Apply a batch. Wire-only batches stay on the hot path (ripple + panel);
   * structural changes rebuild the small renderer snapshot once per batch.
   */
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
    if (structural) {
      this.rebuild([...this.stories.values()], [...this.links.values()], false);
    }
    if (wire.length > 0) this.spawnRipples(wire);
  }

  private rebuild(stories: Story[], links: Link[], flyToFocus: boolean): void {
    const data = buildLiveGlobe(stories, links);
    this.data = data;
    this.layer.setInit(data.init);
    this.layer.setStrengthFloor(data.init.arcRestFloor);
    this.mirror = new FocusMirror(data.init);
    this.setAmbientLabels();
    const focusId = this.focusId;
    if (focusId) {
      const index = data.storyById.get(focusId);
      if (index !== undefined) {
        this.applyFocus(index, flyToFocus);
      } else {
        this.focusId = null;
        this.focused = null;
        this.options.onFocus?.(null);
      }
    }
    this.emit();
    this.map.triggerRepaint();
  }

  focusStoryId(storyId: string): void {
    const data = this.data;
    if (!data) return;
    const index = data.storyById.get(storyId);
    if (index === undefined) return;
    this.focusId = storyId;
    this.applyFocus(index, true);
  }

  clearFocus(): void {
    if (!this.mirror || !this.data) return;
    const nowSec = (Date.now() - this.data.init.epochMs) / 1000;
    const result = this.mirror.focus(null, false, nowSec);
    this.layer.writeFocus(result.storyChanges, result.linkChanges, false);
    this.layer.applyTethers([], []);
    this.labels.setFocus(null);
    this.camera.cancel();
    this.focusId = null;
    this.focused = null;
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
    const result = mirror.focus(index, false, nowSec);
    this.layer.writeFocus(result.storyChanges, result.linkChanges, true);
    const tethers = buildLiveTethers(entry);
    this.layer.applyTethers(tethers.segments, tethers.rings);
    this.labels.setFocus(result.hud);
    if (fly && result.points.length > 0) {
      this.camera.flyTo(this.camera.fitThread(result.points));
    }
    this.focused = {
      storyId: entry.story.id,
      story: entry.story,
      hud: result.hud!,
      live: entry,
    };
    this.options.onFocus?.(this.focused);
    this.emit();
    this.map.triggerRepaint();
  }

  private setAmbientLabels(): void {
    const data = this.data;
    if (!data) return;
    const points: AmbientLabel[] = [];
    for (const index of data.init.topStories.slice(0, 12)) {
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

  zoomIn(): void {
    this.map.zoomIn();
  }

  zoomOut(): void {
    this.map.zoomOut();
  }

  resetView(): void {
    this.camera.flyTo(DEFAULT_VIEW);
  }

  get focus(): FocusDetails | null {
    return this.focused;
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
    };
  }

  private emit(): void {
    this.options.onState?.(this.state);
  }

  destroy(): void {
    this.destroyed = true;
    window.removeEventListener("keydown", this.onKeyDown);
    this.map.remove();
  }
}

function toRippleUpdate(slot: RippleSlotState, category: number): StreamPatch["ripples"][number] {
  const toRad = Math.PI / 180;
  const x = (slot.lng + 180) / 360;
  const clamped = Math.max(-85.05112878, Math.min(85.05112878, slot.lat));
  const y =
    0.5 -
    Math.log((1 + Math.sin(clamped * toRad)) / (1 - Math.sin(clamped * toRad))) /
      (4 * Math.PI);
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
