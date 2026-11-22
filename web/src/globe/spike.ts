import { Map as MapLibreMap } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import "../styles/spike.css";
import { CameraController, type CameraTarget, type FlightStats } from "./camera";
import { loadGlobeConfig } from "./config";
import { CustomGlobeLayer } from "./custom/layer";
import type { DeckGlobeRenderer } from "./deck/renderer";
import {
  CATEGORY_COLORS,
  DEFAULT_STRENGTH_FLOOR,
  LINK_COLORS,
  categoryColor,
  categoryLabel,
  linkLabel,
} from "./encoding";
import { PatchHold } from "./gestures";
import { StoryLabels, type AmbientLabel } from "./labels";
import { FocusMirror } from "./focus-mirror";
import { PerfMeter } from "./perf";
import { buildLabelsOnlyStyle } from "./style/dor-labels";
import type {
  FakeInit,
  HudFocus,
  RippleUpdate,
  StreamPatch,
  WorkerCommand,
  WorkerResponse,
} from "../worker/fake/protocol";

const DEFAULT_VIEW: CameraTarget = { center: [6, 22], zoom: 1.55, bearing: 0, pitch: 0 };

type FocusInputKind = "key" | "click" | "api";

type FocusSample = {
  kind: FocusInputKind;
  requestId: number;
  /** The browser input event timestamp (includes any queueing before the handler). */
  eventAt: number;
  /** When the handler actually started. */
  handlerAt: number;
  appliedAt: number;
  renderAt: number;
  /** Event -> CPU render completion (not presentation). */
  latencyMs: number;
  /** Handler -> CPU render completion. */
  handlerLatencyMs: number;
  /** Render callbacks between the handler and completion; not presentation. */
  renderCallbacks: number;
  focusIndex: number | null;
  secondDegree: boolean;
  superseded: boolean;
  completed: boolean;
};

type PendingFocus = {
  requestId: number;
  eventAt: number;
  handlerAt: number;
  handlerFrame: number;
  kind: FocusInputKind;
  index: number | null;
  appliedAt: number;
};

type GestureWindow = { kind: string; start: number; end: number | null };

type SpikeApi = {
  ready: boolean;
  renderer: "custom" | "deck";
  still: boolean;
  map: MapLibreMap;
  focusSamples: FocusSample[];
  focusByIndex(index: number | null): void;
  focusNext(): void;
  focusPrev(): void;
  focusClear(): void;
  setFocusCursor(position: number): void;
  insertLink(a: number, b: number, type: number, strength: number): void;
  removeLink(index: number): void;
  insertStory(fields: {
    lng: number;
    lat: number;
    category: number;
    sources: number;
    heat: number;
    title: string;
    place: string;
  }): void;
  removeStory(index: number): void;
  setSecondDegree(on: boolean): void;
  setStrengthFloor(floor: number): void;
  screenPointOf(index: number): { x: number; y: number } | null;
  pickAt(x: number, y: number): number | null;
  degree(index: number): number;
  cameraCancel(): void;
  cameraStats(): FlightStats;
  rendererStats(): unknown;
  precisionCheck(): unknown;
  getState(): unknown;
  perf: {
    beginWindow(label: string): void;
    endWindow(): unknown;
    snapshot(): unknown;
    longTasksSince(start: number): unknown;
    windowSampleCounts(): { frames: number; js: number; active: boolean };
  };
  telemetry: {
    applyTimestamps: number[];
    gestureWindows: GestureWindow[];
    focusSwitches: number;
    focusRenderWaits: number[];
  };
};

function must<T extends HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) throw new Error(`missing #${id}`);
  return element as T;
}

function hexOf(rgb: readonly [number, number, number]): string {
  return `#${rgb.map((value) => value.toString(16).padStart(2, "0")).join("")}`;
}

function mergeStreamPatches(older: StreamPatch, newer: StreamPatch): StreamPatch {
  const rippleBySlot = new Map<number, RippleUpdate>();
  for (const ripple of older.ripples) rippleBySlot.set(ripple.slot, ripple);
  for (const ripple of newer.ripples) rippleBySlot.set(ripple.slot, ripple);
  const concat = <T extends Float32Array | Uint16Array | Uint8Array>(a: T, b: T): T => {
    const out = new (a.constructor as new (length: number) => T)(a.length + b.length);
    out.set(a, 0);
    out.set(b, a.length);
    return out;
  };
  return {
    type: "stream",
    now: newer.now,
    storyIndex: concat(older.storyIndex, newer.storyIndex),
    sources: concat(older.sources, newer.sources),
    heat: concat(older.heat, newer.heat),
    updatedAt: concat(older.updatedAt, newer.updatedAt),
    linkIndex: concat(older.linkIndex, newer.linkIndex),
    strength: concat(older.strength, newer.strength),
    width: concat(older.width, newer.width),
    linkColor: concat(older.linkColor, newer.linkColor),
    ripples: [...rippleBySlot.values()],
    ...(older.storyOps?.length || newer.storyOps?.length
      ? { storyOps: [...(older.storyOps ?? []), ...(newer.storyOps ?? [])] }
      : {}),
    ...(older.linkOps?.length || newer.linkOps?.length
      ? { linkOps: [...(older.linkOps ?? []), ...(newer.linkOps ?? [])] }
      : {}),
  };
}

async function boot(): Promise<void> {
  const params = new URLSearchParams(location.search);
  const rendererKind = params.get("renderer") === "deck" ? "deck" : "custom";
  const still = params.get("still") === "1";
  const debug = params.get("debug") === "1";
  const secondDegreeDefault = params.get("second") === "1";

  const meter = new PerfMeter();
  const config = loadGlobeConfig();
  const container = must<HTMLDivElement>("map");
  const errorBox = must<HTMLDivElement>("error");
  const debugBox = must<HTMLPreElement>("debug");
  const modeChip = must<HTMLSpanElement>("mode-chip");
  const rendererChip = must<HTMLSpanElement>("renderer-chip");
  const counts = must<HTMLSpanElement>("counts");
  const focusCard = must<HTMLElement>("focus-card");
  const focusKicker = must<HTMLSpanElement>("focus-kicker");
  const focusTitle = must<HTMLHeadingElement>("focus-title");
  const focusSources = must<HTMLElement>("focus-sources");
  const focusLinks = must<HTMLElement>("focus-links");
  const focusPlace = must<HTMLElement>("focus-place");
  const focusLinkList = must<HTMLDivElement>("focus-link-list");
  const categoryLegend = must<HTMLDivElement>("category-legend");
  const linkLegend = must<HTMLDivElement>("link-legend");
  const attribution = must<HTMLElement>("attribution");

  const showError = (error: unknown): void => {
    const message =
      error instanceof Error ? `${error.message}\n${error.stack ?? ""}` : String(error);
    errorBox.hidden = false;
    errorBox.textContent = message;
    console.error(error);
  };
  window.addEventListener("error", (event) => showError(event.error ?? event.message));
  window.addEventListener("unhandledrejection", (event) => showError(event.reason));

  attribution.textContent = `${config.attribution} · MapLibre GL JS v5`;
  rendererChip.textContent =
    rendererKind === "deck" ? "DECK.GL · INTERLEAVED" : "CUSTOM WEBGL LAYER";
  rendererChip.classList.add("focus");

  for (const category of [1, 3, 2, 5, 4, 6]) {
    const item = document.createElement("span");
    item.className = "legend-item";
    const rgb = CATEGORY_COLORS[category]!;
    item.innerHTML = `<span class="dot" style="--c:${hexOf(rgb)}"></span>${categoryLabel(category)}`;
    categoryLegend.appendChild(item);
  }
  for (const type of [0, 1, 2]) {
    const item = document.createElement("span");
    item.className = "legend-item";
    const rgb = LINK_COLORS[type]!;
    item.innerHTML = `<span class="ln ${type === 2 ? "dash" : ""}" style="--c:${hexOf(rgb)}"></span>${linkLabel(type)}`;
    linkLegend.appendChild(item);
  }

  const map = new MapLibreMap({
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
  const camera = new CameraController(map);

  let init: FakeInit | null = null;
  let customLayer: CustomGlobeLayer | null = null;
  let deckRenderer: DeckGlobeRenderer | null = null;
  let labels: StoryLabels | null = null;
  let worker: Worker | null = null;
  const hold = new PatchHold<StreamPatch>(mergeStreamPatches);

  let requestId = 0;
  let latestFocusRequestId = 0;
  let pendingFocus: PendingFocus | null = null;
  let focusNeedsFrame = false;
  let focusIndex: number | null = null;
  let secondDegree = secondDegreeDefault;
  let topStories: Uint16Array = new Uint16Array();
  let focusCursor = 0;
  let focusCursorExplicit = false;
  const dynamicStories = new Map<number, { lng: number; lat: number }>();
  let lastAmbientUpdate = 0;
  let lastDebugUpdate = 0;
  let lastInteractionAt = performance.now();
  let lastDragEndAt = 0;
  let idleRotating = false;
  let lastPick: number | null = null;
  let lastPickAt = 0;
  let lastPickPoint: { x: number; y: number } | null = null;
  let mirror: FocusMirror | null = null;
  let pendingHud: HudFocus | null = null;
  let pendingHudDirty = false;
  let frameCount = 0;
  let lastRenderAt = 0;
  const focusSamples: FocusSample[] = [];

  const api: SpikeApi = {
    ready: false,
    renderer: rendererKind,
    still,
    map,
    focusSamples: [],
    focusByIndex: () => undefined,
    focusNext: () => undefined,
    focusPrev: () => undefined,
    focusClear: () => undefined,
    setFocusCursor: () => undefined,
    insertLink: () => undefined,
    removeLink: () => undefined,
    insertStory: () => undefined,
    removeStory: () => undefined,
    setSecondDegree: () => undefined,
    setStrengthFloor: () => undefined,
    screenPointOf: () => null,
    pickAt: () => null,
    degree: () => 0,
    cameraCancel: () => undefined,
    cameraStats: () => camera.stats,
    rendererStats: () => ({}),
    precisionCheck: () => null,
    getState: () => ({}),
    perf: {
      beginWindow: (label) => meter.beginWindow(label),
      endWindow: () => meter.endWindow(),
      snapshot: () => meter.snapshot(),
      longTasksSince: (start) => meter.longTasksSince(start),
      windowSampleCounts: () => meter.windowSampleCounts,
    },
    telemetry: {
      applyTimestamps: [],
      gestureWindows: [],
      focusSwitches: 0,
      focusRenderWaits: [],
    },
  };
  (window as unknown as { __dorSpike: SpikeApi }).__dorSpike = api;

  const setAmbientLabels = (): void => {
    if (!init || !labels) return;
    const points: AmbientLabel[] = [];
    for (const story of init.hudTop.slice(0, 10)) {
      points.push({
        lng: init.stories.lng[story.index]!,
        lat: init.stories.lat[story.index]!,
        category: story.category,
        place: story.place,
      });
    }
    labels.setAmbient(points);
  };

  const focusFromInput = (index: number | null, eventAt: number, kind: FocusInputKind): void => {
    if (!init || !worker || !mirror) return;
    const handlerAt = performance.now();
    const t0 = Number.isFinite(eventAt) && eventAt > 0 ? eventAt : handlerAt;
    if (pendingFocus) {
      // This input never reached its first render; keep it in the sample set
      // as explicitly superseded rather than dropping it.
      focusSamples.push({
        kind: pendingFocus.kind,
        requestId: pendingFocus.requestId,
        eventAt: pendingFocus.eventAt,
        handlerAt: pendingFocus.handlerAt,
        appliedAt: pendingFocus.appliedAt,
        renderAt: handlerAt,
        latencyMs: handlerAt - pendingFocus.eventAt,
        handlerLatencyMs: handlerAt - pendingFocus.handlerAt,
        renderCallbacks: 0,
        focusIndex: pendingFocus.index,
        secondDegree,
        superseded: true,
        completed: false,
      });
      if (focusSamples.length > 300) focusSamples.shift();
      pendingFocus = null;
    }
    const nowSec = (Date.now() - init.epochMs) / 1000;
    const result = mirror.focus(index, secondDegree, nowSec);
    customLayer?.writeFocus(result.storyChanges, result.linkChanges, index !== null);
    deckRenderer?.writeFocus(result.storyChanges, result.linkChanges, index !== null);
    // Old overlay geometry must never survive a selection or Esc while the
    // worker reply is in flight.
    customLayer?.applyTethers([], []);
    // The globe state is already on the GPU; HUD and label text are deferred
    // to the end of the first focused frame so they cannot delay it.
    pendingHud = result.hud;
    pendingHudDirty = true;
    if (index !== null) {
      const position = topStories.indexOf(index);
      if (position >= 0) focusCursor = position;
      camera.flyTo(camera.fitThread(result.points));
    } else {
      camera.cancel();
    }
    focusIndex = index;
    const id = ++requestId;
    latestFocusRequestId = id;
    pendingFocus = {
      requestId: id,
      eventAt: t0,
      handlerAt,
      handlerFrame: frameCount,
      kind,
      index,
      appliedAt: performance.now(),
    };
    focusNeedsFrame = true;
    api.telemetry.focusSwitches += 1;
    map.triggerRepaint();
    worker.postMessage({
      type: "focus",
      index,
      secondDegree,
      requestId: id,
    } satisfies WorkerCommand);
  };

  const applyStreamPatch = (patch: StreamPatch): void => {
    if (!init) return;
    for (const op of patch.storyOps ?? []) {
      dynamicStories.set(op.index, { lng: op.lng, lat: op.lat });
      mirror?.applyStoryDelta(
        op.index,
        {
          lng: op.lng,
          lat: op.lat,
          category: op.category,
          title: op.title,
          place: op.place,
          sources: op.sources,
          heat: op.heat,
        },
        op.op === "add",
      );
      customLayer?.applyStoryOp(op);
    }
    for (const op of patch.linkOps ?? []) {
      mirror?.applyLinkDelta(op.index, op.a, op.b, op.op === "add", op.type, op.strength);
      customLayer?.applyLinkOp(op);
    }
    mirror?.applyStream(patch);
    customLayer?.stream(patch);
    if (deckRenderer) {
      deckRenderer.stream(patch);
      deckRenderer.flush();
    }
    api.telemetry.applyTimestamps.push(performance.now());
    if (api.telemetry.applyTimestamps.length > 500) api.telemetry.applyTimestamps.shift();
  };

  const updateFocusCard = (hud: HudFocus | null): void => {
    if (!hud) {
      focusCard.hidden = true;
      return;
    }
    focusCard.hidden = false;
    const rgb = categoryColor(hud.story.category);
    focusKicker.textContent = `● ${categoryLabel(hud.story.category).toUpperCase()} · STORY`;
    focusKicker.style.color = hexOf(rgb);
    focusTitle.textContent = hud.story.title;
    focusSources.textContent = String(hud.story.sources);
    focusLinks.textContent = String(hud.links.length);
    focusPlace.textContent = hud.story.place.toUpperCase();
    focusLinkList.innerHTML = "";
    for (const link of hud.links.slice(0, 6)) {
      const color = hexOf(LINK_COLORS[link.type] ?? LINK_COLORS[0]!);
      const row = document.createElement("div");
      row.className = "link";
      row.style.setProperty("--c", color);
      row.innerHTML = `
        <span class="k">${linkLabel(link.type)} · ${link.place.toUpperCase()}</span>
        <span class="sbar">${link.strength.toFixed(2)}<i style="width:${Math.round(link.strength * 100)}%"></i></span>
        <span class="t">${link.title}</span>
      `;
      focusLinkList.appendChild(row);
    }
  };

  const updateMode = (hud: HudFocus | null): void => {
    if (!hud) {
      modeChip.textContent = "AMBIENT · NO SELECTION";
      modeChip.classList.remove("focus");
      return;
    }
    modeChip.textContent = `FOCUSED · ${hud.story.place.toUpperCase()}${hud.secondDegree ? " · 2ND°" : ""}`;
    modeChip.classList.add("focus");
  };

  map.on("render", () => {
    lastRenderAt = performance.now();
    if (focusNeedsFrame && pendingFocus) {
      const renderAt = performance.now();
      api.telemetry.focusRenderWaits.push(renderAt - pendingFocus.appliedAt);
      const sample: FocusSample = {
        kind: pendingFocus.kind,
        requestId: pendingFocus.requestId,
        eventAt: pendingFocus.eventAt,
        handlerAt: pendingFocus.handlerAt,
        appliedAt: pendingFocus.appliedAt,
        renderAt,
        latencyMs: renderAt - pendingFocus.eventAt,
        handlerLatencyMs: renderAt - pendingFocus.handlerAt,
        renderCallbacks: Math.max(1, frameCount - pendingFocus.handlerFrame),
        focusIndex: pendingFocus.index,
        secondDegree,
        superseded: false,
        completed: true,
      };
      focusSamples.push(sample);
      if (focusSamples.length > 300) focusSamples.shift();
      if (api.telemetry.focusRenderWaits.length > 300) {
        api.telemetry.focusRenderWaits.shift();
      }
      focusNeedsFrame = false;
      pendingFocus = null;
    }
    if (pendingHudDirty) {
      labels?.setFocus(pendingHud);
      updateFocusCard(pendingHud);
      updateMode(pendingHud);
      pendingHudDirty = false;
    }
  });

  const gestureStack = new Set<string>();
  const setGesture = (kind: string, active: boolean): void => {
    if (active) {
      if (gestureStack.has(kind)) return;
      gestureStack.add(kind);
      if (gestureStack.size === 1) {
        hold.setGestureActive(true);
        meter.setPaused(true);
        api.telemetry.gestureWindows.push({ kind, start: performance.now(), end: null });
        if (api.telemetry.gestureWindows.length > 200) api.telemetry.gestureWindows.shift();
      }
      return;
    }
    if (!gestureStack.has(kind)) return;
    gestureStack.delete(kind);
    if (gestureStack.size === 0) {
      hold.setGestureActive(false);
      meter.setPaused(false);
      const openWindow = api.telemetry.gestureWindows
        .slice()
        .reverse()
        .find((entry) => entry.end === null);
      if (openWindow) openWindow.end = performance.now();
      const pending = hold.flush();
      if (pending) applyStreamPatch(pending);
    }
  };

  for (const [start, end] of [
    ["dragstart", "dragend"],
    ["zoomstart", "zoomend"],
    ["rotatestart", "rotateend"],
  ] as const) {
    map.on(start, (event) => {
      const original = (event as { originalEvent?: unknown }).originalEvent;
      if (!original) return;
      lastInteractionAt = performance.now();
      if (start === "dragstart" || start === "rotatestart") camera.cancel();
      lastDragEndAt = 0;
      setGesture(start, true);
    });
    map.on(end, (event) => {
      if (!(event as { originalEvent?: unknown }).originalEvent) return;
      lastDragEndAt = performance.now();
      setGesture(start, false);
    });
  }

  const selectByOffset = (delta: number, inputAt: number): void => {
    if (!init || topStories.length === 0) return;
    if (focusIndex === null) {
      if (!focusCursorExplicit) focusCursor = 0;
    } else {
      focusCursor = Math.max(0, Math.min(topStories.length - 1, focusCursor + delta));
    }
    focusCursorExplicit = false;
    const index = topStories[focusCursor];
    if (index === undefined) return;
    lastInteractionAt = performance.now();
    focusFromInput(index, inputAt, "key");
  };

  document.addEventListener("keydown", (event) => {
    if (event.key === "P" && event.shiftKey) {
      event.preventDefault();
      meter.toggleOverlay();
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      selectByOffset(focusIndex === null ? 0 : 1, event.timeStamp);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      selectByOffset(-1, event.timeStamp);
    } else if (event.key === "Escape") {
      event.preventDefault();
      lastInteractionAt = performance.now();
      focusFromInput(null, event.timeStamp, "key");
    } else if (event.key === "d" || event.key === "D") {
      secondDegree = !secondDegree;
      if (focusIndex !== null) focusFromInput(focusIndex, event.timeStamp, "key");
    }
  });

  document
    .getElementById("focus-close")
    ?.addEventListener("click", () => focusFromInput(null, performance.now(), "api"));

  api.focusSamples = focusSamples;
  api.focusByIndex = (index) => focusFromInput(index, performance.now(), "api");
  api.focusNext = () => selectByOffset(focusIndex === null ? 0 : 1, performance.now());
  api.focusPrev = () => selectByOffset(-1, performance.now());
  api.focusClear = () => focusFromInput(null, performance.now(), "api");
  api.setFocusCursor = (position) => {
    focusCursor = Math.max(0, Math.min(topStories.length - 1, Math.floor(position)));
    focusCursorExplicit = true;
  };
  api.insertLink = (a, b, type, strength) => {
    worker?.postMessage({
      type: "link",
      action: "add",
      a,
      b,
      linkType: type,
      strength,
    } satisfies WorkerCommand);
  };
  api.removeLink = (index) => {
    worker?.postMessage({ type: "link", action: "remove", index } satisfies WorkerCommand);
  };
  api.insertStory = (fields) => {
    worker?.postMessage({ type: "story", action: "add", ...fields } satisfies WorkerCommand);
  };
  api.removeStory = (index) => {
    worker?.postMessage({ type: "story", action: "remove", index } satisfies WorkerCommand);
  };
  api.pickAt = (x, y) => {
    const hit = customLayer?.pickNow(x, y);
    return hit?.kind === "story" ? hit.index : null;
  };
  api.degree = (index) => mirror?.degree(index) ?? 0;
  api.cameraCancel = () => {
    camera.cancel();
    map.stop();
  };
  api.screenPointOf = (index) => {
    if (!init || index < 0) return null;
    const dynamic = dynamicStories.get(index);
    if (!dynamic && index >= init.storyCount) return null;
    const lng = dynamic?.lng ?? init.stories.lng[index]!;
    const lat = dynamic?.lat ?? init.stories.lat[index]!;
    const point = map.project([lng, lat]);
    return { x: point.x, y: point.y };
  };
  api.setSecondDegree = (on) => {
    secondDegree = on;
    if (focusIndex !== null) focusFromInput(focusIndex, performance.now(), "api");
  };
  api.setStrengthFloor = (floor) => {
    customLayer?.setStrengthFloor(floor);
    deckRenderer?.setStrengthFloor(floor);
    map.triggerRepaint();
  };
  api.rendererStats = () =>
    customLayer ? customLayer.stats : { renderer: "deck", focus: focusIndex };
  api.precisionCheck = () => (customLayer ? customLayer.precisionCheck() : { supported: false });
  api.getState = () => ({
    renderer: rendererKind,
    still,
    idleRotation: idleRotating,
    focusIndex,
    secondDegree,
    focusSamples: focusSamples.length,
    camera: { ...camera.stats, active: camera.active, pending: camera.pending },
    rendererStats: api.rendererStats(),
    frames: frameCount,
    lastRenderAt,
    topStories: Array.from(topStories.slice(0, 60)),
    mirror: mirror
      ? {
          storyCapacity: (mirror as unknown as { storyCapacity: number }).storyCapacity,
          linkCapacity: (mirror as unknown as { linkCapacity: number }).linkCapacity,
        }
      : null,
  });

  await new Promise<void>((resolve) => {
    if (map.loaded()) resolve();
    else map.once("load", () => resolve());
  });

  if (params.get("mercator") === "1") map.setProjection({ type: "mercator" });

  const styleLayers = map.getStyle().layers ?? [];
  const firstSymbol = styleLayers.find((layer) => layer.type === "symbol")?.id;
  if (rendererKind === "custom") {
    customLayer = new CustomGlobeLayer();
    map.addLayer(customLayer, firstSymbol);
    customLayer.setStrengthFloor(DEFAULT_STRENGTH_FLOOR);
  } else {
    if (!firstSymbol) throw new Error("style has no symbol layer to anchor deck.gl layers");
    const module = await import("./deck/renderer");
    deckRenderer = new module.DeckGlobeRenderer(map, firstSymbol);
    deckRenderer.setStrengthFloor(DEFAULT_STRENGTH_FLOOR);
  }

  labels = new StoryLabels(map);
  labels.add();

  worker = new Worker(new URL("../worker/fake/index.ts", import.meta.url), {
    type: "module",
    name: "dor-fake-stream",
  });
  worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
    const message = event.data;
    if (message.type === "init") {
      init = message;
      mirror = new FocusMirror(message);
      topStories = message.topStories;
      counts.textContent = `${message.storyCount.toLocaleString()} stories · ${message.linkCount.toLocaleString()} links`;
      labels?.setFocus(null);
      setAmbientLabels();
      if (customLayer) {
        customLayer.setInit(message);
        customLayer.setStrengthFloor(message.arcRestFloor);
        if (still) customLayer.timeOverride = (Date.now() - message.epochMs) / 1000 + 10;
      }
      if (deckRenderer) {
        deckRenderer.init(message);
        deckRenderer.setStrengthFloor(message.arcRestFloor);
        if (still) deckRenderer.tick((Date.now() - message.epochMs) / 1000 + 10);
      }
      if (still) worker?.postMessage({ type: "stream", enabled: false } satisfies WorkerCommand);
      api.ready = true;
      map.triggerRepaint();
      return;
    }
    if (message.type === "stream") {
      hold.push(message);
      return;
    }
    if (message.type === "focus") {
      // The main thread already wrote the focus bytes synchronously. The
      // latest request stays valid after its first render, so a delayed tether
      // reply is accepted; superseded replies are ignored and can never move
      // focus bytes, HUD or the camera.
      if (message.requestId !== latestFocusRequestId) return;
      customLayer?.applyTethers(message.tethers, message.tetherRings);
      deckRenderer?.applyTethers();
      labels?.setFocus(message.hud);
      updateFocusCard(message.hud);
      updateMode(message.hud);
      // The authoritative HUD supersedes any local HUD waiting for the first
      // render; otherwise a fast reply would be overwritten by stale sources.
      pendingHud = null;
      pendingHudDirty = false;
    }
  };
  worker.onerror = (event) => showError(new Error(`data worker: ${event.message}`));

  const rememberPick = (event: {
    point: { x: number; y: number };
    originalEvent?: unknown;
  }): void => {
    if (!init || !customLayer || !event.originalEvent) return;
    const pick = customLayer.pickNow(event.point.x, event.point.y);
    lastPick = pick?.kind === "story" ? pick.index : null;
    lastPickAt = performance.now();
    lastPickPoint = { x: event.point.x, y: event.point.y };
    lastInteractionAt = lastPickAt;
  };
  map.on("mousedown", rememberPick);
  map.on("touchstart", rememberPick);

  map.on("click", (event) => {
    if (!init) return;
    if (performance.now() - lastDragEndAt < 200) return;
    const inputAt = (event.originalEvent as MouseEvent | undefined)?.timeStamp ?? performance.now();
    const cached =
      lastPick !== null &&
      lastPickPoint !== null &&
      performance.now() - lastPickAt < 1000 &&
      Math.hypot(lastPickPoint.x - event.point.x, lastPickPoint.y - event.point.y) < 8;
    const pick = cached ? null : customLayer?.pickNow(event.point.x, event.point.y);
    const picked = cached ? lastPick : pick?.kind === "story" ? pick.index : null;
    if (picked !== null && picked >= 0) {
      lastInteractionAt = performance.now();
      focusFromInput(picked, inputAt, "click");
    }
  });

  const frame = (now: number): void => {
    requestAnimationFrame(frame);
    frameCount += 1;
    meter.frame(now);
    if (!still) {
      const time = (Date.now() - (init?.epochMs ?? Date.now())) / 1000;
      deckRenderer?.tick(time);
      const patch = hold.take(now);
      if (patch) applyStreamPatch(patch);
      const shouldRotate =
        focusIndex === null &&
        !hold.isGestureActive() &&
        !map.isMoving() &&
        now - lastInteractionAt > 30_000;
      if (shouldRotate) {
        idleRotating = true;
        map.panBy([0.015, 0], { duration: 0 });
      } else if (idleRotating) {
        idleRotating = false;
      }
      map.triggerRepaint();
      worker?.postMessage({ type: "frame", now: Date.now() } satisfies WorkerCommand);
      if (focusIndex === null && !hold.isGestureActive() && now - lastAmbientUpdate > 4000) {
        lastAmbientUpdate = now;
        setAmbientLabels();
      }
    }
    if (debug && now - lastDebugUpdate > 500) {
      lastDebugUpdate = now;
      debugBox.textContent = JSON.stringify(api.getState(), null, 2);
    }
  };
  requestAnimationFrame(frame);
  debugBox.hidden = !debug;
}

boot().catch((error) => {
  const box = document.getElementById("error");
  if (box) {
    box.hidden = false;
    box.textContent =
      error instanceof Error ? `${error.message}\n${error.stack ?? ""}` : String(error);
  }
  console.error(error);
});
