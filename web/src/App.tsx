import { For, Show, createEffect, createMemo, createSignal, onCleanup, onMount } from "solid-js";
import { createVirtualizer } from "@tanstack/solid-virtual";
import type { Category, Precision, Role, WireRow } from "./gen/dor/v1/dor_pb";
import { afterShellPaint } from "./globe/load";
import { createFocusHandler } from "./globe/focus-events";
import { shouldIgnoreGlobeKeys, isTypingTarget } from "./keyboard";
import { nextSelectionIndex, selectionKeyForStory, keepSelectionForStory } from "./globe/selection";
import { TtlLruCache, LatestRequest } from "./text-cache";
import { LruCache } from "./worker/lru";
import type { LiveStory, StorySummary } from "./globe/live-data";
import { categoryColor, categoryLabel } from "./globe/encoding";
import { connectionLabel } from "./live/connection";
import { mergeWireRow } from "./live/wire";
import { nextTabStop } from "./ui/focus";
import { channelLabel } from "./ui/labels";
import { groupLinksByStrength } from "./ui/links";
import { sheetSwipeDirection } from "./ui/sheet-gesture";
import {
  rollingMinuteBars,
  rollingMinuteRate,
  sortWireItems,
  wireJoin,
  wireMetadataProvenance,
  wirePlace,
  wirePublishMs,
  wireSourceName,
  wireTime,
  type WireStoryContext,
} from "./live/wire-display";
import {
  AlertInbox,
  approximateLabel,
  historyIdentity,
  toHistory,
  type AlertToast,
  type AlertHitRecord,
  type IncomingAlert,
} from "./live/alerts";
import type { LiveGlobe, FocusDetails, GlobeState, LinkInfo } from "./globe/runtime";
import type {
  ConnectionState,
  SourceHealth,
  WireFilters,
  WorkerMessage,
  WorkerStatus,
} from "./worker/messages";

const CATEGORIES: Array<{ label: string; category: Category; color: string }> = [
  { label: "Conflict", category: 1, color: "#ff4f6e" },
  { label: "Economy", category: 3, color: "#ffc25c" },
  { label: "Politics", category: 2, color: "#b9a0ff" },
  { label: "Climate", category: 5, color: "#45f0a6" },
  { label: "Tech", category: 4, color: "#5ef2ff" },
  { label: "Disaster", category: 6, color: "#ff7a45" },
];

const EMPTY_GLOBE: GlobeState = {
  ready: false,
  storyCount: 0,
  linkCount: 0,
  focused: null,
  topStoryIds: [],
  topStories: [],
  edgeMarkers: [],
  secondDegree: false,
  quality: 0,
  tileFailed: false,
  scale: null,
  match: { active: false, matched: 0, total: 0 },
};

/** Briefs/details appear after the Editor writes them; retry soon, not never. */
const BRIEF_RETRY_MS = 5_000;

type WireItem = {
  row: WireRow;
  key: string;
  receivedAt: number;
};

type DisplayRow =
  | { kind: "article"; key: string; item: WireItem }
  | { kind: "burst"; key: string; count: number; source: string; at: number };

function clockText(ms: number): string {
  return new Date(ms).toISOString().slice(11, 19);
}

function hexColor(category: number): string {
  const rgb = categoryColor(category);
  return `#${rgb.map((value) => value.toString(16).padStart(2, "0")).join("")}`;
}

function articleKey(row: WireRow): string {
  return row.articleId || `${row.source}:${row.at}:${row.title}`;
}

function healthLabel(health: SourceHealth): string {
  if (health.ok === null) return "SOURCE HEALTH —";
  if (health.sourcesTotal > 0) {
    return `SOURCES ${health.sourcesOk}/${health.sourcesTotal}`;
  }
  return health.ok ? "SOURCE HEALTH OK" : "SOURCE HEALTH DEGRADED";
}

function healthClass(health: SourceHealth): string {
  if (health.ok === null) return "unknown";
  return health.ok ? "ok" : "bad";
}

function relativeTime(unixSeconds: number, nowMs: number): string {
  if (!unixSeconds) return "—";
  const delta = Math.max(0, Math.floor(nowMs / 1000 - unixSeconds));
  if (delta < 60) return `${delta}s ago`;
  if (delta < 3600) return `${Math.floor(delta / 60)}m ago`;
  if (delta < 86_400) return `${Math.floor(delta / 3600)}h ago`;
  return `${Math.floor(delta / 86_400)}d ago`;
}

function precisionLabel(precision: Precision): string {
  switch (precision) {
    case 0:
      return "COUNTRY";
    case 1:
      return "REGION";
    case 2:
      return "CITY";
    case 3:
      return "STREET";
    case 4:
      return "POINT";
  }
  return "UNKNOWN";
}

function roleLabel(role: Role): string {
  switch (role) {
    case 0:
      return "Venue";
    case 1:
      return "Origin";
    case 2:
      return "Party";
    case 3:
      return "Affected";
    case 4:
      return "Mentioned";
  }
  return "Mentioned";
}

function directionLabel(link: LinkInfo): string {
  if (link.direction === 0) return `${link.aPlace} → ${link.bPlace}`;
  if (link.direction === 1) return `${link.bPlace} → ${link.aPlace}`;
  return `${link.aPlace} · ${link.bPlace}`;
}

type PlaceRow = {
  key: string;
  name: string;
  fullName: string;
  role: Role;
  precision: Precision;
  count: number;
};

const ROLE_PRIORITY: Record<number, number> = { 0: 0, 1: 1, 2: 2, 3: 3, 4: 4 };

function shortPlaceName(name: string): string {
  const parts = name
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length === 0) return "Unknown place";
  const filtered: string[] = [parts[0]!];
  for (const part of parts.slice(1)) {
    if (part.toLowerCase() === filtered[0]!.toLowerCase()) continue;
    filtered.push(part);
  }
  if (filtered.length <= 2) return filtered.join(", ");
  return `${filtered[0]}, ${filtered[filtered.length - 1]}`;
}

/** Collapse the same place repeated by different source articles. */
function displayPlaces(locations: StorySummary["locations"]): PlaceRow[] {
  const rows = new Map<string, PlaceRow>();
  for (const location of locations) {
    const key =
      location.placeId ||
      `${shortPlaceName(location.name).toLowerCase()}|${location.lat.toFixed(2)},${location.lon.toFixed(2)}`;
    const existing = rows.get(key);
    if (existing) {
      if ((ROLE_PRIORITY[location.role] ?? 4) < (ROLE_PRIORITY[existing.role] ?? 4)) {
        existing.role = location.role;
      }
      if (location.precision > existing.precision) existing.precision = location.precision;
      existing.count++;
      continue;
    }
    rows.set(key, {
      key,
      name: shortPlaceName(location.name),
      fullName: location.name,
      role: location.role,
      precision: location.precision,
      count: 1,
    });
  }
  return [...rows.values()];
}

function linkOrder(link: LinkInfo): {
  first: { title: string; place: string };
  second: { title: string; place: string };
  arrow: string;
} {
  const a = { title: link.aTitle, place: link.aPlace };
  const b = { title: link.bTitle, place: link.bPlace };
  if (link.direction === 1) return { first: b, second: a, arrow: "→" };
  if (link.direction === 2) return { first: a, second: b, arrow: "↔" };
  return { first: a, second: b, arrow: "→" };
}

function distanceKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const toRad = Math.PI / 180;
  const dLat = (lat2 - lat1) * toRad;
  const dLon = (lon2 - lon1) * toRad;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * toRad) * Math.cos(lat2 * toRad) * Math.sin(dLon / 2) ** 2;
  return 2 * 6371 * Math.asin(Math.min(1, Math.sqrt(a)));
}

function bearingLabel(lat1: number, lon1: number, lat2: number, lon2: number): string {
  const toRad = Math.PI / 180;
  const y = Math.sin((lon2 - lon1) * toRad) * Math.cos(lat2 * toRad);
  const x =
    Math.cos(lat1 * toRad) * Math.sin(lat2 * toRad) -
    Math.sin(lat1 * toRad) * Math.cos(lat2 * toRad) * Math.cos((lon2 - lon1) * toRad);
  const bearing = ((Math.atan2(y, x) * 180) / Math.PI + 360) % 360;
  if (bearing >= 45 && bearing < 135) return "east";
  if (bearing >= 135 && bearing < 225) return "south";
  if (bearing >= 225 && bearing < 315) return "west";
  return "north";
}

function typeName(type: number): string {
  return type === 0 ? "Causal" : type === 1 ? "Reaction" : "Thematic";
}

type BriefProvenance = "generated" | "source_snippet" | "none";

type StoryText = {
  loading: boolean;
  available: boolean;
  title?: string;
  brief?: string;
  provenance?: BriefProvenance;
  error?: string;
};

type LinkWhy = {
  loading: boolean;
  available: boolean;
  why?: string;
  error?: string;
};

type StoryArticle = {
  article_id: string;
  source_id: string;
  source_name: string;
  url: string;
  title: string;
  published_at: number;
  published_at_iso?: string;
  lang?: string;
};

type StorySource = {
  id: string;
  name: string;
  url: string;
  article_count: number;
  health?: string;
};

type StoryDetails = {
  story: {
    id: string;
    title: string;
    brief?: string;
    brief_available?: boolean;
    brief_source?: BriefProvenance;
    title_source?: string;
    category: string;
    category_backend?: string;
    category_confidence?: number;
    degraded?: boolean;
    fallback?: boolean;
    place_backend?: string;
    place_degraded?: boolean;
    place_fallback?: boolean;
    source_count: number;
    first_seen: number;
    updated_at: number;
  };
  articles: StoryArticle[];
  sources: StorySource[];
  articles_truncated?: boolean;
};

type AlertRecord = {
  id: string;
  prompt: string;
  min_sources: number;
  region: string;
  strictness: number;
  channels: string[];
  enabled: boolean;
  ntfy_topic?: string;
  created_at?: string;
  updated_at?: string;
};

type BacktestResult = {
  would_fire: number;
  stories: Array<{ story_id: string; title: string; source_count: number; score: number }>;
  approx?: boolean;
  took_ms?: number;
  available?: boolean;
  error?: string;
};

type HistogramPayload = {
  start: number;
  end: number;
  hours: number;
  bins: number[];
  count: number;
  available: boolean;
  error?: string;
};

// Must match the gateway's maxClientPrompts/maxPromptRunes and the matcher's
// bounds so the chip the UI looks up scores by is the prompt the server
// actually evaluated.
const MAX_PROMPTS = 8;
const MAX_PROMPT_CHARS = 300;

const EMPTY_FILTERS: WireFilters = {
  categories: [],
  regions: [],
  minSources: 0,
  lang: [],
  sources: [],
  lat: 0,
  lon: 0,
  radiusKm: 0,
};

const API_BASE = (import.meta.env.VITE_API_URL || "").replace(/\/$/, "");

/** Establish the anonymous alert session before the WebSocket or alert reads. */
let sessionBootstrap: Promise<void> | null = null;
function ensureSession(): Promise<void> {
  if (!sessionBootstrap) {
    sessionBootstrap = fetch(`${API_BASE}/api/session`, {
      credentials: "include",
      cache: "no-store",
    })
      .then(() => undefined)
      .catch(() => {
        sessionBootstrap = null;
      });
  }
  return sessionBootstrap;
}

export function App() {
  let mapContainer!: HTMLDivElement;
  let scrollElement!: HTMLDivElement;
  let searchInput!: HTMLInputElement;
  let liveGlobe: LiveGlobe | null = null;
  let worker: Worker | null = null;
  let disposed = false;
  const loadAbort = new AbortController();
  let workerFrameHandle: number | null = null;
  let workerFrameReady = true;
  let workerBounds: unknown = null;
  let flushTimer: ReturnType<typeof setTimeout> | null = null;
  let freshTimer: ReturnType<typeof setTimeout> | null = null;
  let clockTimer: ReturnType<typeof setInterval> | null = null;
  let histogramTimer: ReturnType<typeof setInterval> | null = null;

  const [now, setNow] = createSignal(Date.now());
  const [connection, setConnection] = createSignal<ConnectionState>("connecting");
  const [lastSeq, setLastSeq] = createSignal(0);
  const [lastWireAt, setLastWireAt] = createSignal<number | null>(null);
  const [reconnectIn, setReconnectIn] = createSignal<number | null>(null);
  const [health, setHealth] = createSignal<SourceHealth>({
    ok: null,
    sourcesOk: 0,
    sourcesTotal: 0,
    detail: "health unavailable",
  });
  const [globe, setGlobe] = createSignal<GlobeState>(EMPTY_GLOBE);
  const [focus, setFocus] = createSignal<FocusDetails | null>(null);
  const [wireRows, setWireRows] = createSignal<WireItem[]>([]);
  const [pendingNew, setPendingNew] = createSignal(0);
  // Live story titles and source counts for wire join lines. A version signal
  // makes the map reactive without copying thousands of entries per patch.
  const storyContexts = new Map<string, WireStoryContext>();
  const [storyContextVersion, setStoryContextVersion] = createSignal(0);
  const setStoryContexts = (summaries: LiveStory[]): void => {
    storyContexts.clear();
    rememberStoryContexts(summaries);
  };
  const rememberStoryContexts = (summaries: LiveStory[]): void => {
    let changed = false;
    for (const entry of summaries) {
      const id = entry?.story?.id;
      if (!id) continue;
      const title = entry.story.title ?? "";
      const sourceCount = Number(entry.story.sourceCount) || 0;
      const place = entry.place || entry.story.locations?.[entry.story.primary]?.name || "";
      const previous = storyContexts.get(id);
      if (
        previous?.title === title &&
        previous.sourceCount === sourceCount &&
        previous.place === place
      ) {
        continue;
      }
      storyContexts.set(id, { title, sourceCount, place });
      changed = true;
      if (storyContexts.size > 6000) {
        const oldest = storyContexts.keys().next().value;
        if (oldest !== undefined) storyContexts.delete(oldest);
      }
    }
    if (changed) setStoryContextVersion((version) => version + 1);
  };
  const storyContext = (storyId: string): WireStoryContext | undefined => {
    storyContextVersion();
    return storyId ? storyContexts.get(storyId) : undefined;
  };
  const [freshKeys, setFreshKeys] = createSignal<Set<string>>(new Set<string>());
  const [pausedByHover, setPausedByHover] = createSignal(false);
  const [scrollTop, setScrollTop] = createSignal(0);
  const [expandedBatches, setExpandedBatches] = createSignal<Set<string>>(new Set());
  const [selectedArticle, setSelectedArticle] = createSignal<string | null>(null);
  const [layersOpen, setLayersOpen] = createSignal(false);
  const [searchFocused, setSearchFocused] = createSignal(false);
  const [threadOnly, setThreadOnly] = createSignal(false);
  const [storiesOnly, setStoriesOnly] = createSignal(false);
  const [selectedLink, setSelectedLink] = createSignal<LinkInfo | null>(null);
  const [linkWhy, setLinkWhy] = createSignal<LinkWhy | null>(null);
  const [linkWhyCached, setLinkWhyCached] = createSignal(false);
  const [storyText, setStoryText] = createSignal<StoryText | null>(null);
  const [storyDetails, setStoryDetails] = createSignal<StoryDetails | null>(null);
  const [searchQuery, setSearchQuery] = createSignal("");
  const [searchOpen, setSearchOpen] = createSignal(false);
  const [layerLines, setLayerLines] = createSignal(true);
  const [layerTethers, setLayerTethers] = createSignal(true);
  const [layerLabels, setLayerLabels] = createSignal(true);
  const [layerHeat, setLayerHeat] = createSignal(true);
  const [layerFloor, setLayerFloor] = createSignal(0.6);
  const [perfOverlay, setPerfOverlay] = createSignal(false);
  const [filters, setFilters] = createSignal<WireFilters>({ ...EMPTY_FILTERS });
  const [filterPanelOpen, setFilterPanelOpen] = createSignal(false);
  const [promptInput, setPromptInput] = createSignal("");
  const [prompts, setPrompts] = createSignal<string[]>([]);
  const [hideNonMatches, setHideNonMatches] = createSignal(false);
  const [filterPlaceQuery, setFilterPlaceQuery] = createSignal("");
  const [filterPlace, setFilterPlace] = createSignal<{
    name: string;
    lat: number;
    lon: number;
  } | null>(null);
  const [filterPlaceResults, setFilterPlaceResults] = createSignal<
    Array<{ id: string; name: string; lat: number; lon: number; country: string }>
  >([]);
  const [alertList, setAlertList] = createSignal<AlertRecord[]>([]);
  const [alertsOpen, setAlertsOpen] = createSignal(false);
  const [sidePanelOpen, setSidePanelOpen] = createSignal(false);
  const [ntfyCopied, setNtfyCopied] = createSignal<string | null>(null);
  const [alertDraftPrompt, setAlertDraftPrompt] = createSignal("");
  const [alertDraftMinSources, setAlertDraftMinSources] = createSignal(0);
  const [alertDraftRegion, setAlertDraftRegion] = createSignal("");
  const [alertDraftStrictness, setAlertDraftStrictness] = createSignal(0.8);
  const [alertDraftChannels, setAlertDraftChannels] = createSignal<string[]>(["inapp"]);
  const [alertToasts, setAlertToasts] = createSignal<AlertToast[]>([]);
  const [backtest, setBacktest] = createSignal<BacktestResult | null>(null);
  const [alertHits, setAlertHits] = createSignal<Record<string, AlertHitRecord[]>>({});
  const [linkReported, setLinkReported] = createSignal(false);
  const [pushState, setPushState] = createSignal<{
    loading: boolean;
    available: boolean | null;
    reason?: string;
  }>({ loading: false, available: null });
  const [mobileTab, setMobileTab] = createSignal<"wire" | "threads" | "near" | "alerts">("wire");
  const [histogram, setHistogram] = createSignal<HistogramPayload | null>(null);
  const [layerCoastline, setLayerCoastline] = createSignal(true);
  const [view, setView] = createSignal<{ center: [number, number]; zoom: number }>({
    center: [6, 22],
    zoom: 1.55,
  });
  const [cmdPlaces, setCmdPlaces] = createSignal<
    Array<{ id: string; name: string; lat: number; lon: number; country: string }>
  >([]);
  const [nearMe, setNearMe] = createSignal<Array<{ storyId: string; title: string; km: number }>>(
    [],
  );
  const [nearMeState, setNearMeState] = createSignal<"idle" | "loading" | "ready" | "denied">(
    "idle",
  );
  const pendingWire: WireItem[] = [];
  const alertInbox = new AlertInbox();
  const seen = new Set<string>();
  const briefCache = new TtlLruCache<string, StoryText>();
  const detailsCache = new TtlLruCache<string, StoryDetails>();
  const whyCache = new TtlLruCache<string, LinkWhy>();
  const prefetched = new LruCache<string, true>();
  const briefRequests = new LatestRequest();
  const whyRequests = new LatestRequest();
  const initialHash = new URLSearchParams(location.hash.slice(1));
  const initialQuery = new URLSearchParams(location.search);
  let hashTimer: ReturnType<typeof setTimeout> | null = null;
  let hoverTimer: ReturnType<typeof setTimeout> | null = null;
  let briefRetryTimer: ReturnType<typeof setTimeout> | null = null;
  let lastHashHref = "";
  let requestedSelection: string | null = null;
  let sheetTouchStartY = 0;
  let alertsDialog!: HTMLDivElement;
  let alertsReturnFocus: HTMLElement | null = null;
  let ntfyCopyTimer: ReturnType<typeof setTimeout> | null = null;
  let pendingInitialStory = initialHash.get("story") ?? initialQuery.get("story");

  const paused = createMemo(() => pausedByHover() || scrollTop() > 6);
  const ingestLag = createMemo(() => {
    const at = lastWireAt();
    if (!at) return null;
    return Math.max(0, Math.round(now() / 1000 - at));
  });
  const utc = createMemo(() => clockText(now()));
  const storyCount = createMemo(() => globe().storyCount);
  const linkCount = createMemo(() => globe().linkCount);
  const topStories = createMemo(() => globe().topStories.slice(0, 6));
  const threadIds = createMemo(() => new Set(focus()?.threadStoryIds ?? []));
  createEffect(() => {
    liveGlobe?.setLabelsDimmed(globe().match.active && globe().match.matched === 0);
  });
  const activeScore = (row: WireRow): number => {
    let score = 0;
    for (const prompt of prompts()) score = Math.max(score, row.promptScores[prompt] ?? 0);
    return score;
  };

  const isApproxMatch = (row: WireRow): boolean =>
    prompts().some((prompt) => row.approxPrompts.includes(prompt));

  const visibleWireRows = createMemo(() => {
    let rows = wireRows();
    const active = filters();
    if (active.categories.length > 0) {
      const wanted = new Set(active.categories);
      rows = rows.filter((item) => wanted.has(item.row.category));
    }
    if (active.sources.length > 0) {
      const wanted = new Set(active.sources.map((source) => source.toLowerCase()));
      rows = rows.filter((item) => wanted.has(item.row.source.toLowerCase()));
    }
    if (active.lang.length > 0) {
      const wanted = new Set(active.lang.map((lang) => lang.toLowerCase()));
      rows = rows.filter((item) => wanted.has(item.row.lang.toLowerCase()));
    }
    if (active.regions.length > 0) {
      rows = rows.filter((item) =>
        active.regions.some(
          (region) =>
            item.row.country.toLowerCase().includes(region.toLowerCase()) ||
            item.row.place.toLowerCase().includes(region.toLowerCase()),
        ),
      );
    }
    if (active.radiusKm > 0 && active.lat !== 0 && active.lon !== 0) {
      rows = rows.filter((item) => {
        if (!item.row.located) return false;
        const toRad = Math.PI / 180;
        const dLat = (item.row.lat - active.lat) * toRad;
        const dLon = (item.row.lon - active.lon) * toRad;
        const a =
          Math.sin(dLat / 2) ** 2 +
          Math.cos(active.lat * toRad) * Math.cos(item.row.lat * toRad) * Math.sin(dLon / 2) ** 2;
        const km = 2 * 6371 * Math.asin(Math.min(1, Math.sqrt(a)));
        return km <= active.radiusKm;
      });
    }
    if (prompts().length > 0 && hideNonMatches()) {
      rows = rows.filter((item) => activeScore(item.row) >= 0.5);
    }
    if (storiesOnly()) {
      const latestByStory = new Map<string, WireItem>();
      for (const item of rows) {
        const id = item.row.storyId;
        if (id && !latestByStory.has(id)) latestByStory.set(id, item);
      }
      rows = [...latestByStory.values()];
    }
    if (threadOnly() && focus()) {
      rows = rows.filter((item) => item.row.storyId && threadIds().has(item.row.storyId));
    }
    return rows;
  });

  const searchResults = createMemo(() => {
    globe();
    const query = searchQuery().trim();
    if (!query) return globe().topStories.slice(0, 8);
    return liveGlobe?.searchStories(query) ?? [];
  });

  const displayRows = createMemo<DisplayRow[]>(() => {
    const rows = visibleWireRows();
    const expanded = expandedBatches();
    const result: DisplayRow[] = [];
    let index = 0;
    while (index < rows.length) {
      const current = rows[index]!;
      const source = current.row.source.toUpperCase();
      const isBurst = source.includes("GDELT");
      if (isBurst) {
        let end = index + 1;
        while (
          end < rows.length &&
          rows[end]!.row.source.toUpperCase().includes("GDELT") &&
          Math.floor(Number(rows[end]!.row.at) / 60) === Math.floor(Number(current.row.at) / 60)
        ) {
          end++;
        }
        const count = end - index;
        const batchKey = `gdelt:${Number(current.row.at)}:${current.key}`;
        if (count > 1 && !expanded.has(batchKey)) {
          result.push({ kind: "article", key: current.key, item: current });
          result.push({
            kind: "burst",
            key: batchKey,
            count: count - 1,
            source: "GDELT",
            at: Number(current.row.at),
          });
          index = end;
          continue;
        }
      }
      result.push({ kind: "article", key: current.key, item: current });
      index++;
    }
    return result;
  });

  const virtualizer = createVirtualizer({
    get count() {
      return displayRows().length;
    },
    getScrollElement: () => scrollElement,
    estimateSize: () => 54,
    overscan: 10,
  });

  const rate = createMemo(() => rollingMinuteRate(wireRows(), now()));

  const rateBars = createMemo(() => rollingMinuteBars(wireRows(), now()));

  const timeline = createMemo(() => {
    const payload = histogram();
    if (payload?.available && payload.bins.length > 0) {
      const max = Math.max(1, ...payload.bins);
      return {
        real: true,
        total: payload.count,
        error: null as string | null,
        bars: payload.bins.map((value) =>
          Math.max(value > 0 ? 5 : 2, Math.round((value / max) * 100)),
        ),
      };
    }
    // Readable fallback while the history read is unavailable: last hour of
    // wire rows, never presented as the durable 24 h count.
    const rows = wireRows();
    const bins = new Array(36).fill(0);
    const end = now();
    const binMs = (24 * 60 * 60 * 1000) / 36;
    for (const item of rows) {
      const at = Number(item.row.at) * 1000 || item.receivedAt;
      const bin = Math.floor((at - (end - 24 * 60 * 60 * 1000)) / binMs);
      if (bin >= 0 && bin < 36) bins[bin] = (bins[bin] ?? 0) + 1;
    }
    const total = bins.reduce((sum, value) => sum + value, 0);
    const max = Math.max(1, ...bins);
    return {
      real: false,
      total,
      error: payload?.error ?? "Loading history…",
      bars: bins.map((value) => Math.max(value > 0 ? 5 : 2, Math.round((value / max) * 100))),
    };
  });

  const focusTimeline = createMemo(() => {
    const details = focus();
    if (!details) return [];
    const nowSeconds = Math.floor(now() / 1000);
    const stories = details.thread.length
      ? details.thread
      : [
          {
            storyId: details.storyId,
            title: details.story.title,
            place: details.live.place,
            category: details.live.category,
            firstSeen: Number(details.story.firstSeen),
            updatedAt: Number(details.story.updatedAt),
          },
        ];
    const firstSeen = stories.map((story) => story.firstSeen).filter((at) => at > 0);
    const start = Math.min(
      nowSeconds,
      firstSeen.length ? Math.min(...firstSeen) : nowSeconds - 3600,
    );
    const end = nowSeconds;
    const span = Math.max(60, end - start);
    const events = stories
      .map((story, index) => {
        const stamp =
          story.storyId === details.storyId ? story.firstSeen : story.updatedAt || story.firstSeen;
        const at = Math.min(end, Math.max(start, stamp || start));
        return {
          at,
          left: 4 + ((at - start) / span) * 92,
          label:
            story.storyId === details.storyId ? "first seen" : `→ ${shortPlaceName(story.place)}`,
          title: `${story.title} · ${clockText(at * 1000)} UTC`,
          color: hexColor(story.category),
          showLabel: index === 0,
        };
      })
      .sort((left, right) => left.at - right.at);
    // Keep only labels that fit; every event still has a dot and a title.
    let lastLabelAt = -100;
    for (const event of events) {
      event.showLabel = event.left - lastLabelAt >= 13;
      if (event.showLabel) lastLabelAt = event.left;
    }
    return events;
  });

  const places = createMemo(() => displayPlaces(focus()?.story.locations ?? []));
  const primaryPlaceKey = createMemo(() => {
    const details = focus();
    if (!details) return null;
    const primary = details.story.locations[details.story.primary] ?? details.story.locations[0];
    if (!primary) return null;
    return (
      primary.placeId ||
      `${shortPlaceName(primary.name).toLowerCase()}|${primary.lat.toFixed(2)},${primary.lon.toFixed(2)}`
    );
  });
  const partyCount = createMemo(
    () => (focus()?.story.locations ?? []).filter((place) => place.role === 2).length,
  );

  const enqueueWire = (rows: WireRow[]): void => {
    let added = 0;
    const updates: Array<{ key: string; row: WireRow }> = [];
    for (const row of rows) {
      const key = articleKey(row);
      const pendingIndex = pendingWire.findIndex((item) => item.key === key);
      if (pendingIndex >= 0) {
        const previous = pendingWire[pendingIndex]!;
        pendingWire[pendingIndex] = {
          row: mergeWireRow(previous.row, row),
          key,
          receivedAt: Date.now(),
        };
        continue;
      }
      if (seen.has(key)) {
        updates.push({ key, row });
        continue;
      }
      seen.add(key);
      pendingWire.push({ row, key, receivedAt: Date.now() });
      if (pendingWire.length > 1000) {
        const removed = pendingWire.shift();
        if (removed) seen.delete(removed.key);
      }
      added++;
    }
    if (updates.length > 0) {
      // The wire row was already visible; update metadata in place. This is
      // the article.clean -> article.placed transition and must not duplicate.
      // Publish times can change with the update, so re-sort after merging.
      setWireRows((previous) =>
        sortWireItems(
          previous.map((item) => {
            const update = updates.find((candidate) => candidate.key === item.key);
            return update ? { ...item, row: mergeWireRow(item.row, update.row) } : item;
          }),
        ),
      );
    }
    if (added === 0) return;
    if (paused()) {
      setPendingNew((count) => count + added);
      return;
    }
    scheduleFlush();
  };

  const flushWire = (): void => {
    if (paused() || pendingWire.length === 0) return;
    const batch = pendingWire.splice(0);
    setWireRows((previous) => {
      // Strict newest-first by effective publish time. Arrival order is not
      // publish order: a poll can deliver a batch oldest-last, and a source
      // dateline can carry a wrong timezone.
      const all = sortWireItems([...batch, ...previous]);
      const kept = all.slice(0, 1000);
      for (const item of all.slice(1000)) seen.delete(item.key);
      return kept;
    });
    const fresh = new Set(batch.map((item) => item.key));
    setFreshKeys(new Set<string>(fresh));
    if (freshTimer !== null) clearTimeout(freshTimer);
    freshTimer = setTimeout(() => {
      freshTimer = null;
      setFreshKeys(new Set<string>());
    }, 320);
    setPendingNew(0);
  };

  const scheduleFlush = (): void => {
    if (flushTimer !== null) return;
    flushTimer = setTimeout(() => {
      flushTimer = null;
      flushWire();
    }, 100);
  };

  const resumeAndFlush = (): void => {
    setPausedByHover(false);
    setScrollTop(0);
    scrollElement?.scrollTo({ top: 0, behavior: "smooth" });
    if (flushTimer !== null) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    flushWire();
  };

  createEffect(() => {
    if (!paused() && pendingNew() > 0) {
      if (flushTimer !== null) clearTimeout(flushTimer);
      flushTimer = null;
      flushWire();
    }
  });

  const updateFilters = (patch: Partial<WireFilters>): void => {
    const next = { ...filters(), ...patch };
    setFilters(next);
    worker?.postMessage({ type: "filters", filters: next });
  };

  const updatePrompts = (next: string[]): void => {
    const bounded = next
      .map((prompt) => Array.from(prompt.trim()).slice(0, MAX_PROMPT_CHARS).join("").trim())
      .filter(Boolean);
    const unique = [...new Set(bounded)].slice(0, MAX_PROMPTS);
    setPrompts(unique);
    worker?.postMessage({ type: "prompts", prompts: unique });
  };

  const addPrompt = (): void => {
    const prompt = promptInput().trim();
    if (!prompt) return;
    updatePrompts([...prompts(), prompt]);
    setPromptInput("");
  };

  const searchPlaces = async (query: string, forFilter = false): Promise<void> => {
    if (query.trim().length < 2) {
      if (forFilter) setFilterPlaceResults([]);
      else setCmdPlaces([]);
      return;
    }
    try {
      const payload = await apiJson<{
        places: Array<{ id: string; name: string; lat: number; lon: number; country: string }>;
      }>(`/api/places?query=${encodeURIComponent(query)}&limit=8`);
      if (forFilter) setFilterPlaceResults(payload.places ?? []);
      else setCmdPlaces(payload.places ?? []);
    } catch {
      if (forFilter) setFilterPlaceResults([]);
      else setCmdPlaces([]);
    }
  };

  const selectFilterPlace = (place: { name: string; lat: number; lon: number }): void => {
    setFilterPlace(place);
    setFilterPlaceQuery(place.name);
    setFilterPlaceResults([]);
    updateFilters({
      lat: place.lat,
      lon: place.lon,
      radiusKm: filters().radiusKm > 0 ? filters().radiusKm : 100,
    });
  };

  const requestNearMe = (): void => {
    if (!navigator.geolocation) {
      setNearMeState("denied");
      return;
    }
    setNearMeState("loading");
    navigator.geolocation.getCurrentPosition(
      (position) => {
        const nearby =
          liveGlobe?.nearbyStories(position.coords.latitude, position.coords.longitude, 12) ?? [];
        setNearMe(
          nearby.map(({ entry, km }) => ({
            storyId: entry.story.id,
            title: entry.story.title,
            km,
          })),
        );
        setNearMeState("ready");
      },
      () => setNearMeState("denied"),
      { timeout: 10000, maximumAge: 300000 },
    );
  };

  const zoomBand = createMemo(() => {
    const zoom = view().zoom;
    if (zoom < 3.5) return { key: "globe", label: "GLOBE", zoom: 1.55 };
    if (zoom < 8.5) return { key: "region", label: "REGION", zoom: 6 };
    if (zoom < 13.5) return { key: "city", label: "CITY", zoom: 11 };
    return { key: "street", label: "STREET", zoom: 15 };
  });

  const syncHash = (storyId: string | null = focus()?.storyId ?? null): void => {
    const params = new URLSearchParams();
    if (storyId) params.set("story", storyId);
    if (liveGlobe) {
      const view = liveGlobe.view;
      params.set(
        "view",
        `${view.center[0].toFixed(3)},${view.center[1].toFixed(3)},${view.zoom.toFixed(2)}`,
      );
    }
    if (threadOnly()) params.set("f", "thread");
    else if (globe().secondDegree) params.set("f", "second");
    const next = params.toString();
    const href = `${location.pathname}${location.search}${next ? `#${next}` : ""}`;
    // moveend fires for real gestures and flights; only rewrite the URL when
    // the shareable state actually changed.
    if (href === lastHashHref) return;
    lastHashHref = href;
    history.replaceState(null, "", href);
  };

  const scheduleHash = (): void => {
    if (hashTimer !== null) clearTimeout(hashTimer);
    hashTimer = setTimeout(() => {
      hashTimer = null;
      syncHash(focus()?.storyId ?? pendingInitialStory);
    }, 180);
  };

  const detailsMiss = new TtlLruCache<string, true>();
  const briefInflight = new Set<string>();
  const detailsInflight = new Set<string>();

  const scheduleBriefRetry = (storyId: string): void => {
    if (briefRetryTimer !== null) clearTimeout(briefRetryTimer);
    briefRetryTimer = setTimeout(() => {
      briefRetryTimer = null;
      if (focus()?.storyId === storyId) void loadStoryText(storyId);
    }, BRIEF_RETRY_MS);
  };

  const loadStoryText = async (storyId: string): Promise<void> => {
    const cached = briefCache.get(storyId);
    if (cached) {
      if (focus()?.storyId === storyId) setStoryText(cached);
      // A missing brief is cached briefly so a later Editor result is picked
      // up by a retry instead of being remembered as permanently unavailable.
      if (!cached.available) scheduleBriefRetry(storyId);
      return;
    }
    if (briefInflight.has(storyId)) return;
    briefInflight.add(storyId);
    const token = briefRequests.next();
    if (focus()?.storyId === storyId) setStoryText({ loading: true, available: false });
    let next: StoryText;
    try {
      const response = await fetch(`${API_BASE}/api/text?story_id=${encodeURIComponent(storyId)}`, {
        credentials: "include",
      });
      const payload = (await response.json()) as {
        available?: boolean;
        title?: string;
        brief?: string;
        provenance?: BriefProvenance;
        error?: string;
      };
      next = {
        loading: false,
        available: Boolean(payload.available) && Boolean(payload.brief),
        ...(payload.title ? { title: payload.title } : {}),
        ...(payload.brief ? { brief: payload.brief } : {}),
        provenance: payload.provenance ?? "none",
        ...(payload.error ? { error: payload.error } : {}),
      };
    } catch (error) {
      next = {
        loading: false,
        available: false,
        error: String(error),
      };
    } finally {
      briefInflight.delete(storyId);
    }
    if (next.available) briefCache.set(storyId, next);
    else briefCache.set(storyId, next, BRIEF_RETRY_MS);
    if (briefRequests.isCurrent(token) && focus()?.storyId === storyId) setStoryText(next);
    if (!next.available) scheduleBriefRetry(storyId);
  };

  const loadStoryDetails = async (storyId: string): Promise<void> => {
    const cached = detailsCache.get(storyId);
    if (cached) {
      if (focus()?.storyId === storyId) setStoryDetails(cached);
      return;
    }
    if (detailsMiss.get(storyId) || detailsInflight.has(storyId)) {
      if (focus()?.storyId === storyId) setStoryDetails(null);
      return;
    }
    detailsInflight.add(storyId);
    try {
      const response = await fetch(`${API_BASE}/api/stories/${encodeURIComponent(storyId)}`, {
        credentials: "include",
      });
      if (!response.ok) {
        detailsMiss.set(storyId, true, BRIEF_RETRY_MS);
        if (focus()?.storyId === storyId) setStoryDetails(null);
        return;
      }
      const payload = (await response.json()) as StoryDetails;
      detailsCache.set(storyId, payload);
      if (focus()?.storyId === storyId) setStoryDetails(payload);
    } catch {
      detailsMiss.set(storyId, true, BRIEF_RETRY_MS);
      if (focus()?.storyId === storyId) setStoryDetails(null);
    } finally {
      detailsInflight.delete(storyId);
    }
  };

  const apiJson = async <T,>(path: string, init?: RequestInit): Promise<T> => {
    const response = await fetch(`${API_BASE}${path}`, {
      credentials: "include",
      headers: { "content-type": "application/json", ...(init?.headers ?? {}) },
      ...init,
    });
    const payload = (await response.json()) as T & { error?: string; reason?: string };
    if (!response.ok) {
      throw new Error(payload.error || payload.reason || `HTTP ${response.status}`);
    }
    return payload;
  };

  const loadAlerts = async (): Promise<void> => {
    try {
      const payload = await apiJson<{ alerts: AlertRecord[] }>("/api/alerts");
      setAlertList(payload.alerts ?? []);
    } catch {
      setAlertList([]);
    }
  };

  const loadHistogram = async (): Promise<void> => {
    try {
      const payload = await apiJson<HistogramPayload>("/api/histogram?hours=24&bins=36");
      setHistogram(payload);
    } catch (error) {
      setHistogram({
        start: Math.floor(Date.now() / 1000) - 86_400,
        end: Math.floor(Date.now() / 1000),
        hours: 24,
        bins: [],
        count: 0,
        available: false,
        error: String(error),
      });
    }
  };

  const refreshAlertsAndHits = async (): Promise<void> => {
    await loadAlerts();
  };

  const loadAlertHits = async (alertId: string): Promise<void> => {
    try {
      const payload = await apiJson<{ hits: AlertHitRecord[] }>(
        `/api/alerts/hits?alert_id=${encodeURIComponent(alertId)}`,
      );
      setAlertHits((previous) => ({ ...previous, [alertId]: payload.hits ?? [] }));
    } catch {
      setAlertHits((previous) => ({ ...previous, [alertId]: [] }));
    }
  };

  const reportWrongLink = async (link: LinkInfo): Promise<void> => {
    try {
      await apiJson("/api/report-wrong-link", {
        method: "POST",
        body: JSON.stringify({ a: link.aId, b: link.bId, reason: "wrong relation" }),
      });
      setLinkReported(true);
    } catch {
      setLinkReported(true);
    }
  };

  const saveAlert = async (): Promise<void> => {
    const prompt = alertDraftPrompt().trim();
    if (!prompt) return;
    try {
      const payload = await apiJson<{ alert: AlertRecord }>("/api/alerts", {
        method: "POST",
        body: JSON.stringify({
          prompt,
          min_sources: alertDraftMinSources(),
          region: alertDraftRegion(),
          strictness: alertDraftStrictness(),
          channels: alertDraftChannels(),
          enabled: true,
        }),
      });
      setAlertList((previous) => [payload.alert, ...previous]);
      setAlertDraftPrompt("");
      setBacktest(null);
    } catch (error) {
      setBacktest({ would_fire: 0, stories: [], error: String(error) });
    }
  };

  const toggleAlert = async (alert: AlertRecord): Promise<void> => {
    try {
      const payload = await apiJson<{ alert: AlertRecord }>(`/api/alerts/${alert.id}`, {
        method: "PATCH",
        body: JSON.stringify({ enabled: !alert.enabled }),
      });
      setAlertList((previous) =>
        previous.map((item) => (item.id === alert.id ? payload.alert : item)),
      );
    } catch {
      /* keep current state */
    }
  };

  const deleteAlert = async (alert: AlertRecord): Promise<void> => {
    try {
      await apiJson(`/api/alerts/${alert.id}`, { method: "DELETE" });
      setAlertList((previous) => previous.filter((item) => item.id !== alert.id));
    } catch {
      /* keep current state */
    }
  };

  const runBacktest = async (): Promise<void> => {
    const prompt = alertDraftPrompt().trim();
    if (!prompt) return;
    try {
      const payload = await apiJson<BacktestResult>("/api/alerts/backtest", {
        method: "POST",
        body: JSON.stringify({
          prompt,
          min_sources: alertDraftMinSources(),
          region: alertDraftRegion(),
          strictness: alertDraftStrictness(),
        }),
      });
      setBacktest(payload);
    } catch (error) {
      setBacktest({ would_fire: 0, stories: [], error: String(error) });
    }
  };

  const dismissToast = (toast: AlertToast): void => {
    setAlertToasts((previous) => alertInbox.dismiss(previous, toast));
  };

  const handleAlertHit = (hit: IncomingAlert): void => {
    const alert = alertList().find((item) => item.id === hit.alertId);
    if (alert && !alert.channels.includes("inapp")) return;
    setAlertToasts((previous) => {
      const next = alertInbox.push(previous, hit);
      if (next !== previous) {
        const record = toHistory(next[0]!);
        setAlertHits((history) => {
          const existing = history[record.alert_id];
          if (!existing) return history;
          const key = historyIdentity(record);
          return {
            ...history,
            [record.alert_id]: [
              record,
              ...existing.filter((item) => historyIdentity(item) !== key),
            ].slice(0, 100),
          };
        });
      }
      return next;
    });
  };

  const muteAlertStory = async (toast: AlertToast): Promise<void> => {
    dismissToast(toast);
    try {
      await apiJson(`/api/alerts/${toast.alertId}/mute`, {
        method: "POST",
        body: JSON.stringify({ story_id: toast.storyId }),
      });
    } catch {
      /* toast already dismissed; mute is best-effort */
    }
  };

  const enablePush = async (): Promise<void> => {
    setPushState({ loading: true, available: null });
    try {
      if (!("serviceWorker" in navigator) || !("PushManager" in window)) {
        setPushState({ loading: false, available: false, reason: "Push is not supported here" });
        return;
      }
      const key = await apiJson<{ available: boolean; public_key?: string; reason?: string }>(
        "/api/push/key",
      );
      if (!key.available || !key.public_key) {
        setPushState({
          loading: false,
          available: false,
          reason: key.reason || "Push is not configured",
        });
        return;
      }
      const registration = await navigator.serviceWorker.register("/sw.js");
      const permission = await Notification.requestPermission();
      if (permission !== "granted") {
        setPushState({
          loading: false,
          available: false,
          reason: "Notification permission denied",
        });
        return;
      }
      const normalized = key.public_key.replace(/-/g, "+").replace(/_/g, "/").replace(/=+$/, "");
      const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
      const decoded = atob(padded);
      const applicationServerKey = Uint8Array.from(decoded, (char) => char.charCodeAt(0));
      const subscription = await registration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey,
      });
      await apiJson("/api/push/subscriptions", {
        method: "POST",
        body: JSON.stringify(subscription.toJSON()),
      });
      setPushState({ loading: false, available: true });
    } catch (error) {
      setPushState({ loading: false, available: false, reason: String(error) });
    }
  };

  const loadLinkWhy = async (link: LinkInfo): Promise<void> => {
    const key = `${link.aId}\u0000${link.bId}`;
    const isSelected = (): boolean => {
      const current = selectedLink();
      return current !== null && current.aId === link.aId && current.bId === link.bId;
    };
    const cached = whyCache.get(key);
    if (cached) {
      if (isSelected()) {
        setLinkWhyCached(true);
        setLinkWhy(cached);
      }
      return;
    }
    const token = whyRequests.next();
    if (isSelected()) {
      setLinkWhyCached(false);
      setLinkWhy({ loading: true, available: false });
    }
    let next: LinkWhy;
    try {
      const response = await fetch(
        `${API_BASE}/api/text?link_a=${encodeURIComponent(link.aId)}&link_b=${encodeURIComponent(link.bId)}`,
        { credentials: "include" },
      );
      const payload = (await response.json()) as {
        available?: boolean;
        why?: string;
        error?: string;
      };
      next = {
        loading: false,
        available: Boolean(payload.available) && Boolean(payload.why),
        ...(payload.why ? { why: payload.why } : {}),
        ...(payload.error ? { error: payload.error } : {}),
      };
    } catch (error) {
      next = { loading: false, available: false, error: String(error) };
    }
    if (next.available) whyCache.set(key, next);
    else whyCache.set(key, next, BRIEF_RETRY_MS);
    // A late answer for a closed or replaced popover must not overwrite the
    // current one (the review's "why" race).
    if (whyRequests.isCurrent(token) && isSelected()) {
      setLinkWhyCached(false);
      setLinkWhy(next);
    }
  };

  const openLink = (link: LinkInfo): void => {
    setSelectedLink(link);
    setLinkReported(false);
    setLinkWhyCached(false);
    void loadLinkWhy(link);
  };

  const prefetchStory = (storyId: string): void => {
    if (!storyId || prefetched.has(storyId)) return;
    prefetched.set(storyId, true);
    worker?.postMessage({ type: "focus", storyId });
  };

  const rowEnter = (storyId: string): void => {
    if (!storyId) return;
    if (hoverTimer !== null) clearTimeout(hoverTimer);
    hoverTimer = setTimeout(() => {
      hoverTimer = null;
      prefetchStory(storyId);
    }, 100);
  };

  const rowLeave = (): void => {
    if (hoverTimer !== null) clearTimeout(hoverTimer);
    hoverTimer = null;
  };

  const applyStatus = (status: WorkerStatus): void => {
    setConnection(status.connection);
    setLastSeq(status.lastSeq);
    setLastWireAt(status.lastWireAt);
    setReconnectIn(status.reconnectInMs);
    setHealth(status.health);
  };

  const handleMessage = (message: WorkerMessage): void => {
    switch (message.type) {
      case "status":
        applyStatus(message);
        break;
      case "init":
        seen.clear();
        pendingWire.length = 0;
        setWireRows([]);
        setPendingNew(0);
        // Keep the selected row: a reconnect snapshot preserves the focused
        // story, so the user's row must not jump back to the newest one.
        setLastSeq(message.seq);
        setStoryContexts(message.summaries);
        liveGlobe?.setInit(message);
        if (message.recentWire.length) enqueueWire(message.recentWire);
        break;
      case "patch":
        workerBounds = message.bounds;
        setLastSeq(message.seq);
        rememberStoryContexts(message.summaries);
        liveGlobe?.queuePatch(message);
        for (const hit of message.alerts) handleAlertHit(hit);
        enqueueWire(message.wire);
        break;
      case "frameDone":
        workerFrameReady = true;
        break;
      case "resync":
        seen.clear();
        pendingWire.length = 0;
        setWireRows([]);
        setPendingNew(0);
        setLastSeq(0);
        setStoryContexts([]);
        break;
    }
  };

  onMount(() => {
    if (initialQuery.has("diagnostics")) {
      (window as unknown as { __dorApp?: unknown }).__dorApp = {
        get bounds() {
          return {
            seen: seen.size,
            visible: wireRows().length,
            pending: pendingWire.length,
            brief: briefCache.size,
            details: detailsCache.size,
            why: whyCache.size,
            prefetched: prefetched.size,
            worker: workerBounds,
          };
        },
        get selection() {
          const key = selectedArticle();
          const item = key ? wireRows().find((row) => row.key === key) : undefined;
          return {
            key,
            storyId: item?.row.storyId ?? null,
            title: item?.row.title ?? null,
            focusedStoryId: focus()?.storyId ?? null,
            focusedTitle: focus()?.story.title ?? null,
            connection: connection(),
            lastSeq: lastSeq(),
          };
        },
        get rows() {
          return articleRows()
            .slice(0, 200)
            .map((row) => ({ key: row.key, storyId: row.storyId, displayIndex: row.displayIndex }));
        },
      };
    }
    void (async () => {
      await afterShellPaint(loadAbort.signal);
      if (disposed) return;
      // Saved alerts and their live hits are scoped to this anonymous
      // session. Establish it before reading alerts or opening the socket.
      await ensureSession();
      if (disposed) return;
      void loadAlerts();
      void loadHistogram();
      histogramTimer = setInterval(() => void loadHistogram(), 60_000);
      const { LiveGlobe } = await import("./globe/runtime");
      if (disposed) return;
      liveGlobe = new LiveGlobe(mapContainer, {
        onState: setGlobe,
        onFocus: createFocusHandler<FocusDetails>(setFocus, (details) => {
          if (details) {
            pendingInitialStory = null;
            const rows = wireRows().map((item) => ({
              key: item.key,
              storyId: item.row.storyId,
            }));
            // Only a story change may move the selected row; a metadata
            // refresh of the same story keeps the user's row.
            const nextSelection =
              keepSelectionForStory(selectedArticle(), rows, details.storyId) ??
              selectionKeyForStory(rows, details.storyId, requestedSelection);
            requestedSelection = null;
            setSelectedArticle(nextSelection);
            setStoryText(briefCache.get(details.storyId) ?? { loading: true, available: false });
            setStoryDetails(detailsCache.get(details.storyId) ?? null);
            void loadStoryText(details.storyId);
            void loadStoryDetails(details.storyId);
            worker?.postMessage({ type: "focus", storyId: details.storyId });
            syncHash(details.storyId);
          } else {
            requestedSelection = null;
            setSelectedArticle(null);
            setThreadOnly(false);
            setSelectedLink(null);
            setStoryText(null);
            setStoryDetails(null);
            syncHash(null);
          }
        }),
        onLinkClick: openLink,
        onView: (nextView) => {
          setView(nextView);
          scheduleHash();
        },
        onContextRestored: () => {
          // Custom GPU state cannot survive a context loss; ask the worker for
          // one authoritative snapshot (no camera move, focus is preserved).
          worker?.postMessage({ type: "refresh" });
        },
      });
      liveGlobe.setStrengthFloor(layerFloor());
      const viewParam = initialHash.get("view")?.split(",").map(Number);
      if (
        viewParam &&
        viewParam.length === 3 &&
        viewParam.every((value) => Number.isFinite(value))
      ) {
        liveGlobe.setView([viewParam[0]!, viewParam[1]!], viewParam[2]!);
      }
      if (initialQuery.get("alert")) setAlertsOpen(true);
      if (initialHash.get("f") === "thread") setThreadOnly(true);
      if (initialHash.get("f") === "second") liveGlobe.setSecondDegree(true);
      if (pendingInitialStory) liveGlobe.focusStoryId(pendingInitialStory);

      worker = new Worker(new URL("./worker/index.ts", import.meta.url), {
        type: "module",
        name: "dor-data-worker",
      });
      worker.onmessage = (event: MessageEvent<WorkerMessage>) => handleMessage(event.data);
      worker.onerror = (event) => {
        setConnection("offline");
        console.error(`data worker: ${event.message}`);
      };
      // Seed filters/prompts before start so the first socket greeting
      // carries them ahead of Hello and the resume snapshot is projected
      // with the app's real prompt set.
      worker.postMessage({ type: "filters", filters: filters() });
      worker.postMessage({ type: "prompts", prompts: prompts() });
      worker.postMessage({
        type: "start",
        mode:
          initialQuery.get("perf") === "1"
            ? "perf"
            : initialQuery.get("wirefixture") === "1"
              ? "wirefixture"
              : initialQuery.get("mock") === "1"
                ? "mock"
                : "live",
      });
      const frame = (): void => {
        if (disposed) return;
        if (workerFrameReady && liveGlobe?.isReady) {
          workerFrameReady = false;
          worker?.postMessage({ type: "frame" });
        }
        workerFrameHandle = requestAnimationFrame(frame);
      };
      workerFrameHandle = requestAnimationFrame(frame);
    })().catch((error) => {
      if (!disposed) {
        setConnection("offline");
        console.error(error);
      }
    });

    clockTimer = setInterval(() => setNow(Date.now()), 1000);

    const onKey = (event: KeyboardEvent): void => {
      // ⌘K works everywhere, including from an input.
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        searchInput?.focus();
        return;
      }
      if (event.key === "Escape") {
        // Never consume Esc while the user is typing.
        if (isTypingTarget(event.target)) return;
        // Esc closes the topmost open panel. Consuming the event also keeps
        // the runtime Esc handler from clearing globe focus behind it.
        if (alertsOpen()) {
          event.preventDefault();
          setAlertsOpen(false);
          return;
        }
        if (selectedLink()) {
          event.preventDefault();
          setSelectedLink(null);
          return;
        }
        if (filterPanelOpen()) {
          event.preventDefault();
          setFilterPanelOpen(false);
          return;
        }
        if (layersOpen()) {
          event.preventDefault();
          setLayersOpen(false);
          return;
        }
        if (searchOpen()) {
          event.preventDefault();
          setSearchOpen(false);
          return;
        }
      }
      // Typing, dialogs and open panels own the keyboard: q/Esc/arrows must
      // not change globe focus or walk the wire behind them.
      if (shouldIgnoreGlobeKeys(event, document)) return;
      if (event.key === "Escape") {
        setSearchOpen(false);
        setSelectedLink(null);
      }
      if (event.shiftKey && event.key.toLowerCase() === "p") {
        event.preventDefault();
        setPerfOverlay((value) => !value);
        return;
      }
      if (event.key === "ArrowDown" || event.key === "ArrowUp") {
        event.preventDefault();
        moveSelection(event.key === "ArrowDown" ? 1 : -1);
      }
    };
    window.addEventListener("keydown", onKey);
    onCleanup(() => {
      window.removeEventListener("keydown", onKey);
      disposed = true;
      loadAbort.abort();
      if (workerFrameHandle !== null) cancelAnimationFrame(workerFrameHandle);
      worker?.terminate();
      worker = null;
      liveGlobe?.destroy();
      liveGlobe = null;
      if (clockTimer !== null) clearInterval(clockTimer);
      if (histogramTimer !== null) clearInterval(histogramTimer);
      if (flushTimer !== null) clearTimeout(flushTimer);
      if (freshTimer !== null) clearTimeout(freshTimer);
      if (briefRetryTimer !== null) clearTimeout(briefRetryTimer);
      if (hashTimer !== null) clearTimeout(hashTimer);
      if (ntfyCopyTimer !== null) clearTimeout(ntfyCopyTimer);
    });
  });

  const articleRows = (): Array<{ key: string; storyId: string; displayIndex: number }> =>
    displayRows().flatMap((row, displayIndex) =>
      row.kind === "article" ? [{ key: row.key, storyId: row.item.row.storyId, displayIndex }] : [],
    );

  const focusStory = (storyId: string): void => {
    // Adjacent rows of the same story only move the selection; they must not
    // refocus the globe, refly the camera, or reload the panel.
    if (!storyId || focus()?.storyId === storyId) return;
    liveGlobe?.focusStoryId(storyId);
  };

  // Fill a missing selection for the focused story once rows exist (deep link
  // or a reconnect that no longer contains the old row). A valid selected row
  // is never replaced here.
  createEffect(() => {
    const details = focus();
    if (!details) return;
    const rows = wireRows().map((item) => ({ key: item.key, storyId: item.row.storyId }));
    const current = selectedArticle();
    if (current && rows.some((row) => row.key === current)) return;
    const key = selectionKeyForStory(rows, details.storyId);
    if (key) setSelectedArticle(key);
  });

  const moveSelection = (delta: number): void => {
    const rows = articleRows();
    const next = nextSelectionIndex(rows, selectedArticle(), delta);
    if (next === null) return;
    const item = rows[next]!;
    requestedSelection = item.key;
    setSelectedArticle(item.key);
    // Keyboard walk keeps the selected row centred: estimated row heights are
    // unreliable for wrapped headlines, and centring is always fully visible.
    virtualizer.scrollToIndex(item.displayIndex, { align: "center" });
    focusStory(item.storyId);
  };

  const selectArticle = (item: WireItem): void => {
    requestedSelection = item.key;
    setSelectedArticle(item.key);
    focusStory(item.row.storyId);
  };

  const clearFocus = (): void => {
    liveGlobe?.clearFocus();
    setFocus(null);
    setSelectedArticle(null);
    setSelectedLink(null);
    setSidePanelOpen(false);
  };

  const goLive = (): void => {
    setNow(Date.now());
    void loadHistogram();
    resumeAndFlush();
  };

  const partOfStory = createMemo(() => {
    const details = focus();
    if (!details || view().zoom < 13.5) return null;
    const primary = details.story.locations[details.story.primary] ?? details.story.locations[0];
    if (!primary) return null;
    const km = distanceKm(view().center[1], view().center[0], primary.lat, primary.lon);
    const direction =
      km < 0.05
        ? "here"
        : `${km.toFixed(km >= 10 ? 0 : 1)} km ${bearingLabel(
            view().center[1],
            view().center[0],
            primary.lat,
            primary.lon,
          )} ←`;
    return {
      title: details.story.title,
      precision: precisionLabel(primary.precision).toLowerCase(),
      direction,
    };
  });

  const focusTopStory = (story: StorySummary): void => {
    liveGlobe?.focusStoryId(story.id);
  };

  const expandBatch = (key: string): void => {
    setExpandedBatches((previous) => {
      const next = new Set(previous);
      next.add(key);
      return next;
    });
  };

  const liveLabel = createMemo(() => connectionLabel(connection()));

  /** Links grouped by strength, matching board M2's tiers. */
  const linkTiers = createMemo(() => groupLinksByStrength(focus()?.hud.links ?? []));

  const ntfyAlerts = createMemo(() =>
    alertList().filter((alert) => Boolean(alert.ntfy_topic) && alert.channels.includes("ntfy")),
  );

  const copyNtfyTopic = async (topic: string): Promise<void> => {
    try {
      await navigator.clipboard.writeText(topic);
    } catch {
      return;
    }
    setNtfyCopied(topic);
    if (ntfyCopyTimer !== null) clearTimeout(ntfyCopyTimer);
    ntfyCopyTimer = setTimeout(() => setNtfyCopied(null), 1600);
  };

  // At mid widths the story panel is a drawer, so a new focus opens it.
  let lastFocusedStory: string | null = null;
  createEffect(() => {
    const current = focus();
    if (!current) {
      lastFocusedStory = null;
      return;
    }
    if (current.storyId !== lastFocusedStory) {
      lastFocusedStory = current.storyId;
      setSidePanelOpen(true);
    }
  });

  // A modal dialog owns the page: background controls are inert, not just
  // visually behind the drawer. Runs before the focus-restore effect below.
  const clearInert = (): void => {
    for (const child of document.querySelectorAll<HTMLElement>(".app3 > *")) {
      child.removeAttribute("inert");
    }
  };
  createEffect(() => {
    const open = alertsOpen();
    const children = document.querySelectorAll<HTMLElement>(".app3 > *");
    for (const child of children) {
      if (child.classList.contains("alerts-drawer")) continue;
      if (open) child.setAttribute("inert", "");
      else child.removeAttribute("inert");
    }
  });
  onCleanup(clearInert);

  // Modal focus: remember the opener, move focus in, restore it on close.
  createEffect(() => {
    if (alertsOpen()) {
      const active = document.activeElement;
      alertsReturnFocus = active instanceof HTMLElement ? active : null;
      queueMicrotask(() => alertsDialog?.focus());
    } else if (alertsReturnFocus) {
      const target = alertsReturnFocus;
      alertsReturnFocus = null;
      if (target.isConnected) target.focus();
    }
  });

  const dialogFocusables = (): HTMLElement[] => {
    if (!alertsDialog) return [];
    const selector =
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
    return [...alertsDialog.querySelectorAll<HTMLElement>(selector)].filter(
      (element) => element.getAttribute("aria-hidden") !== "true",
    );
  };

  const onAlertsKeyDown = (event: KeyboardEvent): void => {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      setAlertsOpen(false);
      return;
    }
    if (event.key !== "Tab") return;
    const nodes = dialogFocusables();
    if (nodes.length === 0) {
      event.preventDefault();
      alertsDialog?.focus();
      return;
    }
    const active = document.activeElement as HTMLElement | null;
    const current = active === null ? -1 : nodes.indexOf(active);
    const next = nextTabStop(nodes.length, current, event.shiftKey);
    if (next === null) return;
    event.preventDefault();
    nodes[next]!.focus();
  };

  return (
    <div class="app3">
      <header class="topbar">
        <div class="brand">
          <span class="mark" />
          <span class="word">DOR</span>
        </div>
        <span
          class={`live ${connection() === "online" ? "" : "muted-live"} ${connection()}`}
          title={`Gateway ${connection()}`}
        >
          {liveLabel()}
        </span>
        <label class={`search ${searchFocused() ? "focused" : ""}`}>
          <span aria-hidden="true">⌕</span>
          <input
            ref={searchInput}
            type="text"
            value={searchQuery()}
            placeholder="Search stories, places, people"
            onFocus={() => {
              setSearchFocused(true);
              setSearchOpen(true);
            }}
            onBlur={() => {
              setSearchFocused(false);
              setTimeout(() => setSearchOpen(false), 150);
            }}
            onInput={(event) => {
              const query = event.currentTarget.value;
              setSearchQuery(query);
              setSearchOpen(true);
              void searchPlaces(query);
            }}
            onKeyDown={(event) => {
              if (event.key === "Escape") {
                event.stopPropagation();
                setSearchOpen(false);
                searchInput.blur();
              } else if (event.key === "Enter") {
                const first = searchResults()[0];
                if (first) {
                  liveGlobe?.focusStoryId(first.story.id);
                  setSearchQuery("");
                  setSearchOpen(false);
                  searchInput.blur();
                }
              }
            }}
            aria-label="Search stories, places and people"
          />
          <span class="kbd">⌘K</span>
        </label>
        <Show when={searchOpen()}>
          <div class="search-results glass" onMouseDown={(event) => event.preventDefault()}>
            <span class="cap">
              {searchQuery().trim() ? `${searchResults().length} stories` : "Top stories"}
            </span>
            <Show
              when={searchResults().length > 0}
              fallback={<div class="search-none">No story matches “{searchQuery()}”.</div>}
            >
              <For each={searchResults()}>
                {(result) => (
                  <button
                    class="search-result"
                    onMouseDown={(event) => {
                      event.preventDefault();
                      liveGlobe?.focusStoryId(result.story.id);
                      setSearchQuery("");
                      setSearchOpen(false);
                      searchInput.blur();
                    }}
                  >
                    <span class="dot" style={`--c:${hexColor(result.category)}`} />
                    <span class="h">
                      <b>{result.story.title}</b>
                      <small>
                        {categoryLabel(result.category)} · {result.place} ·{" "}
                        {result.story.sourceCount} sources
                      </small>
                    </span>
                  </button>
                )}
              </For>
            </Show>
            <Show when={cmdPlaces().length > 0}>
              <span class="cap search-section">Places</span>
              <For each={cmdPlaces()}>
                {(place) => (
                  <button
                    class="search-result"
                    onMouseDown={(event) => {
                      event.preventDefault();
                      liveGlobe?.flyTo([place.lon, place.lat], 10);
                      setSearchQuery("");
                      setCmdPlaces([]);
                      setSearchOpen(false);
                      searchInput.blur();
                    }}
                  >
                    <span class="dot" style="--c:#5ef2ff" />
                    <span class="h">
                      <b>{place.name}</b>
                      <small>{place.country || "Place"}</small>
                    </span>
                  </button>
                )}
              </For>
            </Show>
          </div>
        </Show>
        <div class="chips" role="group" aria-label="Categories">
          <button
            class="chip"
            aria-pressed={filters().categories.length === 0}
            onClick={() => updateFilters({ categories: [] })}
          >
            All
          </button>
          <For each={CATEGORIES}>
            {(category) => (
              <button
                class="chip"
                style={`--c:${category.color}`}
                aria-pressed={filters().categories.includes(category.category)}
                onClick={() => {
                  const active = filters().categories;
                  updateFilters({
                    categories: active.includes(category.category)
                      ? active.filter((item) => item !== category.category)
                      : [...active, category.category],
                  });
                }}
              >
                <span class="sw" />
                {category.label}
              </button>
            )}
          </For>
        </div>
        <div class="right">
          <Show when={connection() !== "online" && reconnectIn() !== null}>
            <span class="reconnect" title={health().detail}>
              retry {Math.ceil(reconnectIn()! / 1000)}s
            </span>
          </Show>
          <span class={`health ${healthClass(health())}`} title={health().detail}>
            {healthLabel(health())}
            <b>{ingestLag() === null ? "LAG —" : `LAG ${ingestLag()}s`}</b>
          </span>
          <span>
            UTC <b>{utc()}</b>
          </span>
          <button
            class="ibtn bell"
            title="Alerts"
            aria-label="Alerts"
            onClick={() => {
              setAlertsOpen(true);
              void refreshAlertsAndHits();
            }}
          >
            ◔
            <Show when={alertToasts().length > 0}>
              <span class="badge">{alertToasts().length}</span>
            </Show>
          </button>
          <a class="ops-link mono" href="/ops" target="_blank" rel="noopener noreferrer">
            OPS
          </a>
        </div>
      </header>

      <aside class={`wire mobile-tab-${mobileTab()}`} aria-label="Live wire">
        <div class="mobile-tabs" role="tablist" aria-label="Mobile sections">
          <For each={["wire", "threads", "near", "alerts"] as const}>
            {(tab) => (
              <button
                role="tab"
                aria-selected={mobileTab() === tab}
                onClick={() => {
                  setMobileTab(tab);
                  if (tab === "alerts") void loadAlerts();
                }}
              >
                {tab === "wire"
                  ? "Wire"
                  : tab === "threads"
                    ? "Threads"
                    : tab === "near"
                      ? "Near me"
                      : "Alerts"}
              </button>
            )}
          </For>
        </div>
        <div class="wire-head">
          <div class="r1">
            <h2>Live wire</h2>
            <span class="rate" title="Articles in the last minute">
              <i>
                <For each={rateBars()}>{(height) => <b style={`height:${height}%`} />}</For>
              </i>
              {rate() > 0 ? `${rate()} / min` : "— / min"}
            </span>
            <div class="seg" role="group" aria-label="Stream granularity">
              <button aria-pressed={!storiesOnly()} onClick={() => setStoriesOnly(false)}>
                Every article
              </button>
              <button
                aria-pressed={storiesOnly()}
                disabled={storyCount() === 0}
                title={storyCount() === 0 ? "Waiting for the first story" : "One row per story"}
                onClick={() => setStoriesOnly(true)}
              >
                Stories only
              </button>
            </div>
            <button
              class="ibtn filter-btn"
              aria-pressed={filterPanelOpen()}
              aria-label="Filters"
              title="Filters"
              onClick={() => setFilterPanelOpen((open) => !open)}
            >
              ⚙
            </button>
          </div>
          <Show when={filterPanelOpen()}>
            <div class="filter-drawer glass">
              <div class="filter-row">
                <span class="cap">Category</span>
                <span>
                  {filters().categories.length === 0
                    ? "All"
                    : `${filters().categories.length} active`}
                </span>
              </div>
              <label class="filter-field">
                <span>Sources</span>
                <input
                  value={filters().sources.join(", ")}
                  placeholder="bbc-world, reuters"
                  onInput={(event) =>
                    updateFilters({
                      sources: event.currentTarget.value
                        .split(",")
                        .map((source) => source.trim())
                        .filter(Boolean),
                    })
                  }
                />
              </label>
              <label class="filter-field">
                <span>Region / place text</span>
                <input
                  value={filters().regions.join(", ")}
                  placeholder="Europe, Indonesia"
                  onInput={(event) =>
                    updateFilters({
                      regions: event.currentTarget.value
                        .split(",")
                        .map((region) => region.trim())
                        .filter(Boolean),
                    })
                  }
                />
              </label>
              <div class="filter-grid">
                <label class="filter-field">
                  <span>Min sources</span>
                  <input
                    type="number"
                    min="0"
                    value={filters().minSources}
                    onInput={(event) =>
                      updateFilters({ minSources: Number(event.currentTarget.value) || 0 })
                    }
                  />
                </label>
                <label class="filter-field">
                  <span>Language</span>
                  <input
                    value={filters().lang.join(", ")}
                    placeholder="en"
                    onInput={(event) =>
                      updateFilters({
                        lang: event.currentTarget.value
                          .split(",")
                          .map((lang) => lang.trim())
                          .filter(Boolean),
                      })
                    }
                  />
                </label>
              </div>
              <label class="filter-field">
                <span>Within radius of</span>
                <input
                  value={filterPlaceQuery()}
                  placeholder="Search a city or country"
                  onInput={(event) => {
                    setFilterPlaceQuery(event.currentTarget.value);
                    void searchPlaces(event.currentTarget.value, true);
                  }}
                />
              </label>
              <Show when={filterPlaceResults().length > 0}>
                <div class="filter-place-results">
                  <For each={filterPlaceResults().slice(0, 5)}>
                    {(place) => (
                      <button onClick={() => selectFilterPlace(place)}>
                        {place.name} <span>{place.country}</span>
                      </button>
                    )}
                  </For>
                </div>
              </Show>
              <div class="filter-grid">
                <label class="filter-field">
                  <span>Radius km</span>
                  <input
                    type="number"
                    min="0"
                    value={filters().radiusKm}
                    onInput={(event) => {
                      const radiusKm = Number(event.currentTarget.value) || 0;
                      const place = filterPlace();
                      updateFilters({
                        radiusKm,
                        ...(place ? { lat: place.lat, lon: place.lon } : {}),
                      });
                    }}
                  />
                </label>
                <label class="filter-toggle">
                  <input
                    type="checkbox"
                    checked={hideNonMatches()}
                    onChange={(event) => setHideNonMatches(event.currentTarget.checked)}
                  />
                  Hide prompt non-matches
                </label>
              </div>
              <div class="filter-actions">
                <span class="cap">
                  {filterPlace()
                    ? `${filterPlace()!.name} · ${filters().radiusKm} km`
                    : "No radius centre"}
                </span>
                <button
                  class="text-btn"
                  onClick={() => {
                    setFilters({ ...EMPTY_FILTERS });
                    setFilterPlace(null);
                    setFilterPlaceQuery("");
                    setFilterPlaceResults([]);
                    updateFilters({ ...EMPTY_FILTERS });
                  }}
                >
                  Clear filters
                </button>
              </div>
            </div>
          </Show>
          <Show when={focus()}>
            <div class="pfs">
              <span class="pf on">
                <span class="k">THREAD</span>
                {focus()!.story.title.length > 34
                  ? `${focus()!.story.title.slice(0, 34)}…`
                  : focus()!.story.title}
                <button class="x" title="Clear focus" aria-label="Clear focus" onClick={clearFocus}>
                  ×
                </button>
              </span>
              <button
                class={`pf ${threadOnly() ? "on" : ""}`}
                aria-pressed={threadOnly()}
                onClick={() => {
                  setThreadOnly((value) => !value);
                  syncHash();
                }}
              >
                <span class="k">SHOW</span>
                {threadOnly() ? "Only this thread" : "All articles"}
              </button>
              <button
                class={`pf ${globe().secondDegree ? "on" : ""}`}
                aria-pressed={globe().secondDegree}
                onClick={() => {
                  liveGlobe?.setSecondDegree(!globe().secondDegree);
                  syncHash();
                }}
              >
                <span class="k">2ND°</span>
                {globe().secondDegree ? "on" : "off"}
              </button>
            </div>
          </Show>
          <label class="prompt">
            <span class="spark" aria-hidden="true">
              ✦
            </span>
            <input
              type="text"
              value={promptInput()}
              placeholder="Filter with a prompt, e.g. “protests that could disrupt shipping”"
              aria-label="Prompt filter"
              onInput={(event) => setPromptInput(event.currentTarget.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  addPrompt();
                }
              }}
            />
            <span class="kbd">↵</span>
          </label>
          <Show when={prompts().length > 0}>
            <div class="pfs">
              <For each={prompts()}>
                {(prompt) => (
                  <span class="pf on">
                    <span class="k">PROMPT</span>
                    {prompt}
                    <button
                      class="x"
                      title="Remove prompt"
                      aria-label={`Remove prompt ${prompt}`}
                      onClick={() => updatePrompts(prompts().filter((item) => item !== prompt))}
                    >
                      ×
                    </button>
                  </span>
                )}
              </For>
              <button
                class={`pf ${hideNonMatches() ? "on" : ""}`}
                onClick={() => setHideNonMatches((value) => !value)}
              >
                <span class="k">SHOW</span>
                {hideNonMatches() ? "Matches only" : "All rows dimmed"}
              </button>
            </div>
          </Show>
        </div>

        <div
          class="wrows"
          ref={scrollElement}
          onMouseMove={() => setPausedByHover(true)}
          onMouseLeave={() => setPausedByHover(false)}
          onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
          role="region"
          aria-label="Live article wire"
          data-paused={paused()}
          data-hover={pausedByHover()}
          data-scroll={scrollTop()}
          data-wire={wireRows().length}
          data-display={displayRows().length}
          data-pending={pendingNew()}
        >
          <Show
            when={displayRows().length > 0}
            fallback={
              <div class="wire-empty">
                <span class="cap">
                  {connection() === "online" ? "Listening" : "Gateway unavailable"}
                </span>
                <p>
                  {connection() === "online"
                    ? "Waiting for the first article. The wire fills as sources publish."
                    : "The data worker is reconnecting. Existing rows stay in place."}
                </p>
                <span class="wire-empty-meta mono">
                  seq {lastSeq()} · {connection()}
                </span>
              </div>
            }
          >
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
              <For each={virtualizer.getVirtualItems()}>
                {(virtualRow) => {
                  const display = () => displayRows()[virtualRow.index];
                  return (
                    <div
                      data-index={virtualRow.index}
                      ref={(element) => {
                        element.setAttribute("data-index", String(virtualRow.index));
                        virtualizer.measureElement(element);
                      }}
                      class={`wr ${
                        display()?.kind === "article" &&
                        selectedArticle() ===
                          (display() as { kind: "article"; item: WireItem }).item.key
                          ? "sel"
                          : ""
                      } ${
                        display()?.kind === "article" &&
                        freshKeys().has((display() as { kind: "article"; item: WireItem }).item.key)
                          ? "fresh"
                          : ""
                      } ${
                        display()?.kind === "article" && focus()
                          ? threadIds().has(
                              (display() as { kind: "article"; item: WireItem }).item.row.storyId,
                            )
                            ? "thread"
                            : "dim"
                          : ""
                      } ${
                        display()?.kind === "article" &&
                        prompts().length > 0 &&
                        activeScore((display() as { kind: "article"; item: WireItem }).item.row) <
                          0.5
                          ? "dim"
                          : ""
                      }`}
                      style={{
                        position: "absolute",
                        top: 0,
                        left: 0,
                        width: "100%",
                        transform: `translateY(${virtualRow.start}px)`,
                      }}
                    >
                      <Show
                        when={display()?.kind === "article"}
                        fallback={
                          <button
                            class="wburst"
                            onClick={() => {
                              const item = display();
                              if (item?.kind === "burst") expandBatch(item.key);
                            }}
                          >
                            +{" "}
                            {display()?.kind === "burst"
                              ? (display() as { count: number }).count
                              : 0}{" "}
                            more from the{" "}
                            {display()?.kind === "burst"
                              ? (display() as { source: string }).source
                              : "batch"}{" "}
                            batch
                            {" · "}
                            <span class="expand">expand</span>
                          </button>
                        }
                      >
                        {(() => {
                          const item = display();
                          if (item?.kind !== "article") return null;
                          const row = item.item.row;
                          const color = hexColor(row.category);
                          const story = storyContext(row.storyId);
                          const place = wirePlace(row, story?.place);
                          const provenance = wireMetadataProvenance(row);
                          const join = wireJoin(row, story);
                          const tooltip = [
                            row.title,
                            place.title !== place.text ? place.title : "",
                            provenance.approx ? provenance.title : "",
                          ]
                            .filter(Boolean)
                            .join(" · ");
                          return (
                            <button
                              class="wr-body"
                              style={`--c:${color}`}
                              onClick={() => selectArticle(item.item)}
                              onMouseEnter={() => rowEnter(row.storyId)}
                              onMouseLeave={rowLeave}
                              title={tooltip}
                              data-wire-at={wirePublishMs(row, item.item.receivedAt)}
                              data-wire-key={item.item.key}
                            >
                              <time>{wireTime(row, item.item.receivedAt)}</time>
                              <span class="dot" />
                              <span class="h">
                                <span class="hl">
                                  <span class="ws-line">
                                    <span class="ws">{wireSourceName(row)}</span>
                                    <Show when={provenance.approx}>
                                      <span class="approx" aria-hidden="true">
                                        ≈ approx
                                      </span>
                                    </Show>
                                  </span>
                                  <span class="title">{row.title || "Untitled article"}</span>
                                </span>
                                <Show when={join}>
                                  <span class="join">{join}</span>
                                </Show>
                              </span>
                              <span
                                class={`pl ${place.global ? "global" : ""}`}
                                data-place={place.global ? "global" : place.text}
                              >
                                <span class="pl-name">{place.text}</span>
                                <Show when={prompts().length > 0 && activeScore(row) > 0}>
                                  <span
                                    class="match-score"
                                    title={
                                      isApproxMatch(row) ? "Approximate match" : "Prompt match"
                                    }
                                  >
                                    {isApproxMatch(row) ? "≈" : "·"} {activeScore(row).toFixed(2)}
                                  </span>
                                </Show>
                              </span>
                            </button>
                          );
                        })()}
                      </Show>
                    </div>
                  );
                }}
              </For>
            </div>
          </Show>
          <Show when={paused() && pendingNew() > 0}>
            <button class="paused-pill" onClick={resumeAndFlush}>
              <span>Paused while you read</span>
              <b>↓ {pendingNew()} new</b>
            </button>
          </Show>
        </div>
        <div class="wfoot">
          <span class="cap desktop-only">Hover pauses · ↑ ↓ to select</span>
          <span class="cap">{lastSeq() > 0 ? `seq ${lastSeq()}` : "newest first"}</span>
        </div>
        <Show when={mobileTab() !== "wire"}>
          <div class="mobile-panel">
            <Show when={mobileTab() === "threads"}>
              <For each={topStories()}>
                {(story) => (
                  <button class="thread" onClick={() => focusTopStory(story.story)}>
                    <span class="cap">
                      {categoryLabel(story.category)} · {story.story.sourceCount} sources
                    </span>
                    <p class="ttl">{story.story.title}</p>
                    <div class="foot">
                      <span>{story.place}</span>
                      <span>strongest {story.strongestLink.toFixed(2)}</span>
                    </div>
                  </button>
                )}
              </For>
            </Show>
            <Show when={mobileTab() === "near"}>
              <button class="chip" onClick={requestNearMe} disabled={nearMeState() === "loading"}>
                {nearMeState() === "loading" ? "Locating…" : "Use my location"}
              </button>
              <Show when={nearMeState() === "denied"}>
                <p class="cap">Location permission was not granted. Near me stays off.</p>
              </Show>
              <For each={nearMe()}>
                {(item) => (
                  <button class="thread" onClick={() => liveGlobe?.focusStoryId(item.storyId)}>
                    <b>{item.title}</b>
                    <span class="foot">
                      <span>{item.km.toFixed(0)} km away</span>
                      <span>open story →</span>
                    </span>
                  </button>
                )}
              </For>
            </Show>
            <Show when={mobileTab() === "alerts"}>
              <button class="chip" onClick={() => setAlertsOpen(true)}>
                Manage alerts
              </button>
              <Show when={alertList().length > 0} fallback={<p class="cap">No alerts yet.</p>}>
                <For each={alertList()}>
                  {(alert) => (
                    <div class="mobile-alert-row">
                      <b>{alert.prompt}</b>
                      <span class="cap">{alert.enabled ? "on" : "off"}</span>
                    </div>
                  )}
                </For>
              </Show>
            </Show>
          </div>
        </Show>
      </aside>

      <main class="stage">
        <div
          ref={mapContainer}
          class="map-host"
          role="img"
          aria-label="Live globe of located stories"
        />
        <div class="hud-corners" />
        <div class="zoom-rail glass">
          <For
            each={
              [
                ["GLOBE", 1.55],
                ["REGION", 6],
                ["CITY", 11],
                ["STREET", 15],
              ] as const
            }
          >
            {([label, zoom]) => (
              <button
                aria-current={zoomBand().label === label}
                onClick={() => liveGlobe?.flyToBand(label, zoom)}
              >
                {label}
              </button>
            )}
          </For>
          <span class="cap">{view().zoom.toFixed(1)}</span>
        </div>
        <Show when={zoomBand().label !== "GLOBE"}>
          <div class="stage-crumbs cap">
            {zoomBand().label} · {focus()?.hud.story.place ?? "Global"}
          </div>
        </Show>
        <Show when={globe().match.active && globe().match.matched === 0}>
          <div class="no-matches glass mono">No stories match this filter · globe dimmed</div>
        </Show>
        <Show when={partOfStory()}>
          {(part) => (
            <div class="part-story glass">
              <span class="cap">● Part of story</span>
              <b>{part().title}</b>
              <span class="mono">
                {part().precision} · centre {part().direction}
              </span>
            </div>
          )}
        </Show>
        <div class="tl">
          <span class="cap">{focus() ? "FOCUSED THREAD" : "AMBIENT · NO SELECTION"}</span>
          <Show when={focus()}>
            <button class="back" onClick={clearFocus}>
              ← GLOBAL VIEW <span class="desktop-only">(Esc)</span>
            </button>
          </Show>
          <span class="big">{focus() ? focus()!.story.title : "Global · live"}</span>
          <Show when={focus()}>
            <span class="back">
              {focus()!.hud.story.sources} sources · {focus()!.hud.links.length} direct links
            </span>
          </Show>
        </div>
        <div class="tr">
          <button
            class="ibtn"
            aria-pressed={globe().secondDegree}
            aria-label="Second-degree links"
            title="Second-degree links"
            onClick={() => {
              liveGlobe?.setSecondDegree(!globe().secondDegree);
              syncHash();
            }}
          >
            ⋯
          </button>
          <button
            class="ibtn panel-toggle"
            aria-label="Story panel"
            aria-expanded={sidePanelOpen()}
            aria-controls="story-panel"
            title="Story panel"
            onClick={() => setSidePanelOpen((open) => !open)}
          >
            ▤
          </button>
          <button
            class="ibtn"
            aria-pressed={layersOpen()}
            aria-label="Layers"
            title="Layers"
            onClick={() => setLayersOpen((open) => !open)}
          >
            ≡
          </button>
        </div>
        <Show when={layersOpen()}>
          <div class="layers glass">
            <span class="cap">Layers</span>
            <div class="ln2">
              Relation lines
              <button
                class={`sw2 ${layerLines() ? "" : "off"}`}
                aria-pressed={layerLines()}
                aria-label="Toggle relation lines"
                onClick={() => {
                  const next = !layerLines();
                  setLayerLines(next);
                  liveGlobe?.setLinesVisible(next);
                }}
              />
            </div>
            <div class="ln2">Link strength floor</div>
            <div class="slider2" style={`--v:${Math.round(((layerFloor() - 0.3) / 0.65) * 100)}%`}>
              <span>all</span>
              <input
                type="range"
                min="0.3"
                max="0.95"
                step="0.05"
                value={layerFloor()}
                aria-label="Link strength floor"
                onInput={(event) => {
                  const value = Number(event.currentTarget.value);
                  setLayerFloor(value);
                  liveGlobe?.setStrengthFloor(value);
                }}
              />
              <span>strong</span>
              <b>{layerFloor().toFixed(2)}</b>
            </div>
            <div class="ln2">
              Second-degree links
              <button
                class={`sw2 ${globe().secondDegree ? "" : "off"}`}
                aria-pressed={globe().secondDegree}
                aria-label="Toggle second-degree links"
                onClick={() => {
                  liveGlobe?.setSecondDegree(!globe().secondDegree);
                  syncHash();
                }}
              />
            </div>
            <div class="ln2">
              Location tethers
              <button
                class={`sw2 ${layerTethers() ? "" : "off"}`}
                aria-pressed={layerTethers()}
                aria-label="Toggle location tethers"
                onClick={() => {
                  const next = !layerTethers();
                  setLayerTethers(next);
                  liveGlobe?.setTethersVisible(next);
                }}
              />
            </div>
            <div class="ln2">
              Labels
              <button
                class={`sw2 ${layerLabels() ? "" : "off"}`}
                aria-pressed={layerLabels()}
                aria-label="Toggle labels"
                onClick={() => {
                  const next = !layerLabels();
                  setLayerLabels(next);
                  liveGlobe?.setLabelsVisible(next);
                }}
              />
            </div>
            <div class="ln2">
              Heat (cluster glow)
              <button
                class={`sw2 ${layerHeat() ? "" : "off"}`}
                aria-pressed={layerHeat()}
                aria-label="Toggle heat"
                onClick={() => {
                  const next = !layerHeat();
                  setLayerHeat(next);
                  liveGlobe?.setHeatVisible(next);
                }}
              />
            </div>
            <div class="ln2">
              Coastline when zoomed in
              <button
                class={`sw2 ${layerCoastline() ? "" : "off"}`}
                aria-pressed={layerCoastline()}
                aria-label="Toggle coastline"
                onClick={() => {
                  const next = !layerCoastline();
                  setLayerCoastline(next);
                  liveGlobe?.setCoastlineVisible(next);
                }}
              />
            </div>
            <div class="ln2">
              Perf overlay
              <button
                class={`sw2 ${perfOverlay() ? "" : "off"}`}
                aria-pressed={perfOverlay()}
                aria-label="Toggle perf overlay"
                onClick={() => setPerfOverlay((value) => !value)}
              >
                <span class="kbd">⇧P</span>
              </button>
            </div>
          </div>
        </Show>
        <Show when={globe().storyCount === 0}>
          <div class="stage-empty glass">
            <span class="cap">
              {connection() === "online" ? "Waiting for live data" : "Reconnecting to the gateway"}
            </span>
            <h3>Articles first. Stories next.</h3>
            <p>
              The wire is live now. The globe fills when the classifier publishes its first located
              stories.
            </p>
            <span class="mono">
              {wireRows().length} wire rows · {storyCount()} stories
            </span>
          </div>
        </Show>
        <div class="bl glass">
          <div class="legend">
            <div class="row">
              <span class="ln" style="--c:#ffc25c" />
              Causal
            </div>
            <div class="row">
              <span class="ln" style="--c:#ff6fd8" />
              Reaction
            </div>
            <div class="row">
              <span class="ln dash" style="--c:#5ef2ff" />
              Thematic
            </div>
          </div>
          <div class="strength-key">
            <span>
              <i style="border-top-width:1px;opacity:.5" />
              loose
            </span>
            <span>
              <i style="border-top-width:2px;opacity:.8" />
              related
            </span>
            <span>
              <i style="border-top-width:3px" />
              strong
            </span>
          </div>
          <Show when={globe().scale}>
            {(scale) => (
              <div class="scalebar mono" title="Map scale">
                <i style={`width:${Math.round(scale().px)}px`} />
                <span>{scale().km >= 1 ? `${scale().km} km` : `${scale().km * 1000} m`}</span>
              </div>
            )}
          </Show>
        </div>
        <div class="br">
          <button
            class="ibtn"
            title="Zoom in"
            aria-label="Zoom in"
            onClick={() => liveGlobe?.zoomIn()}
          >
            +
          </button>
          <button
            class="ibtn"
            title="Zoom out"
            aria-label="Zoom out"
            onClick={() => liveGlobe?.zoomOut()}
          >
            −
          </button>
          <button
            class="ibtn"
            title="Reset view"
            aria-label="Reset view"
            onClick={() => liveGlobe?.resetView()}
          >
            ◎
          </button>
        </div>
        <div class="edge-markers">
          <For each={globe().edgeMarkers}>
            {(marker) => (
              <button
                class="rim"
                title={`Rotate to ${marker.label}`}
                aria-label={`Rotate to ${marker.label}`}
                onClick={() => liveGlobe?.flyTo(marker.center, marker.zoom)}
              >
                {marker.count > 1 ? `+${marker.count} ` : "◂ "}
                <b>{marker.label}</b> ▸
              </button>
            )}
          </For>
        </div>
        <Show when={selectedLink()}>
          {(link) => (
            <div class="arc-popover glass">
              <div class="arc-popover-head">
                <span
                  class="cap"
                  style={`color:${link().type === 0 ? "#ffc25c" : link().type === 1 ? "#ff6fd8" : "#5ef2ff"}`}
                >
                  {typeName(link().type)} · {directionLabel(link())}
                </span>
                <button
                  class="text-btn"
                  aria-label="Close link details"
                  onClick={() => setSelectedLink(null)}
                >
                  ×
                </button>
              </div>
              <h3>{linkOrder(link()).first.title}</h3>
              <h4>
                {linkOrder(link()).arrow} {linkOrder(link()).second.title}
              </h4>
              <div class="facts">
                <span>
                  <b>{link().strength.toFixed(2)}</b> strength
                </span>
                <span>
                  <b>{link().confidence.toFixed(2)}</b> confidence
                </span>
                <span>
                  <b>{link().distanceKm.toFixed(0)}</b> km
                </span>
                <Show when={link().unverified}>
                  <span class="unverified">unverified</span>
                </Show>
              </div>
              <p class="brief">
                {linkWhy()?.loading
                  ? "Loading why…"
                  : linkWhy()?.available
                    ? linkWhy()!.why
                    : "Why unavailable for this link yet."}
              </p>
              <div class="arc-popover-foot">
                <span class="agentk">WHY · agent{linkWhyCached() ? " · cached" : ""}</span>
                <button class="text-btn" onClick={() => void reportWrongLink(link())}>
                  {linkReported() ? "reported" : "report wrong link"}
                </button>
              </div>
            </div>
          )}
        </Show>
        <Show when={perfOverlay()}>
          <div class="perf-chip mono">
            FPS {globe().perf?.fps.toFixed(1) ?? "—"} · P95{" "}
            {globe().perf?.frameMs.p95.toFixed(1) ?? "—"} ms
            <br />
            LONG TASKS {globe().perf?.longTasks ?? 0} · JS/FRAME{" "}
            {globe().perf?.jsMs.mean.toFixed(2) ?? "—"} ms
            <br />
            FOCUS {globe().focusLatencyMs?.toFixed(1) ?? "—"} ms · INIT {globe().initCount ?? 0} ·
            PATCH {globe().patchCount ?? 0}
            <br />
            QUALITY {globe().quality} · DPR ≤ 2 · {globe().storyCount} stories
          </div>
        </Show>
        <Show when={globe().tileFailed}>
          <span class="tile-failed-chip mono">BASEMAP OFFLINE · live layers only</span>
        </Show>
        <span class="attrib mono">
          © OpenFreeMap · © OpenMapTiles · © OpenStreetMap contributors · GDELT
        </span>
      </main>

      <aside
        id="story-panel"
        class={`side ${sidePanelOpen() ? "side-open" : ""}`}
        aria-label="Story panel"
      >
        <div class="side-panel-bar">
          <span class="cap">Story panel</span>
          <button
            class="text-btn"
            aria-label="Hide story panel"
            onClick={() => setSidePanelOpen(false)}
          >
            ×
          </button>
        </div>
        <Show
          when={focus()}
          fallback={
            <>
              <div class="sec side-head">
                <span class="cap">Live threads</span>
                <span class="cap">by momentum</span>
              </div>
              <div class="sec side-threads">
                <Show
                  when={topStories().length > 0}
                  fallback={
                    <div class="side-empty">
                      <span class="cap">No stories yet</span>
                      <p>The side panel will show the strongest threads once stories land.</p>
                    </div>
                  }
                >
                  <For each={topStories()}>
                    {(story) => (
                      <button class="thread" onClick={() => focusTopStory(story.story)}>
                        <span class="cap">
                          {categoryLabel(story.category)} · {story.story.sourceCount} sources ·{" "}
                          {story.degree} links
                        </span>
                        <p class="ttl">{story.story.title}</p>
                        <div class="foot">
                          <span>{story.place}</span>
                          <span>
                            strongest{" "}
                            {story.strongestLink > 0 ? story.strongestLink.toFixed(2) : "—"}
                          </span>
                        </div>
                      </button>
                    )}
                  </For>
                </Show>
              </div>
            </>
          }
        >
          {(details) => (
            <>
              <div class="sec side-head">
                <span class="cap" style={`color:${hexColor(details().story.category)}`}>
                  {categoryLabel(details().story.category)} · Story
                  <Show when={storyDetails()?.story.category_backend === "rule"}>
                    <span class="approx"> · approx</span>
                  </Show>
                </span>
                <button
                  class="text-btn"
                  aria-label="Clear focus and show global view"
                  onClick={clearFocus}
                >
                  Global view ×
                </button>
              </div>
              <div class="sec">
                <h1>{storyText()?.title || details().story.title}</h1>
                <div class="facts">
                  <span>
                    first seen <b>{relativeTime(Number(details().story.firstSeen), now())}</b>
                  </span>
                  <span>
                    <b>{details().hud.story.sources}</b> sources
                  </span>
                  <span>
                    updated <b>{relativeTime(Number(details().story.updatedAt), now())}</b>
                  </span>
                </div>
              </div>
              <div class="sec">
                <Show when={storyText()?.available && storyText()?.provenance === "generated"}>
                  <span class="agentk">
                    AGENT BRIEF · from {details().hud.story.sources} sources
                  </span>
                </Show>
                <Show when={storyText()?.available && storyText()?.provenance === "source_snippet"}>
                  <span class="agentk">
                    SOURCE EXCERPT · from {details().hud.story.sources} sources
                  </span>
                </Show>
                <Show
                  when={storyText()?.available}
                  fallback={
                    <p class="brief muted">
                      {storyText()?.loading
                        ? "Generating brief…"
                        : "No agent brief for this story. The headline and sources stand on their own."}
                    </p>
                  }
                >
                  <p class="brief">{storyText()!.brief}</p>
                </Show>
              </div>
              <div class="sec">
                <span class="cap">
                  Places
                  <Show when={storyDetails()?.story.place_degraded}>
                    <span class="approx"> · approx</span>
                  </Show>
                </span>
                <For each={places().slice(0, 8)}>
                  {(place) => (
                    <div class="place">
                      <span
                        class={place.key === primaryPlaceKey() ? "fill" : "ring"}
                        style={`--c:${hexColor(details().story.category)}`}
                      />
                      <span class="role">{roleLabel(place.role)}</span>
                      <span class="place-name" title={place.fullName}>
                        {place.name}
                      </span>
                      <span class="prec">
                        {precisionLabel(place.precision)}
                        {place.count > 1 ? ` ×${place.count}` : ""}
                      </span>
                    </div>
                  )}
                </For>
                <Show when={places().length > 8}>
                  <span class="cap">+{places().length - 8} more places</span>
                </Show>
                <Show when={partyCount() > 6}>
                  <span class="cap">Multi-region · {partyCount()} parties</span>
                </Show>
              </div>
              <Show when={storyDetails()}>
                {(detailsData) => (
                  <div class="sec">
                    <div class="tierh">
                      <span class="cap">Sources</span>
                      <span class="cap">
                        {detailsData().sources.length} feeds · {detailsData().articles.length}{" "}
                        recent
                      </span>
                    </div>
                    <Show
                      when={detailsData().articles.length > 0}
                      fallback={<span class="cap">Source list not available yet</span>}
                    >
                      <For each={detailsData().articles.slice(0, 6)}>
                        {(article) => (
                          <a
                            class="source-article"
                            href={article.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            title={article.title}
                          >
                            <span class="src-name">{article.source_name || article.source_id}</span>
                            <span class="src-title">{article.title}</span>
                            <span class="src-time">
                              {relativeTime(article.published_at, now())}
                            </span>
                          </a>
                        )}
                      </For>
                      <Show when={detailsData().articles_truncated}>
                        <span class="cap">Newest 20 shown</span>
                      </Show>
                    </Show>
                  </div>
                )}
              </Show>
              <Show when={details().hud.links.length > 0}>
                <div class="sec side-links">
                  <For each={linkTiers()}>
                    {(tier) => (
                      <>
                        <div class="tierh">
                          <span class="cap">{tier.label}</span>
                          <span class="cap">{tier.links.length}</span>
                        </div>
                        <For each={tier.links}>
                          {(link) => (
                            <button
                              class="lk"
                              style={`--c:${link.type === 0 ? "#ffc25c" : link.type === 1 ? "#ff6fd8" : "#5ef2ff"}`}
                              onClick={() => {
                                const info = liveGlobe?.linkInfo(link.index);
                                if (info) openLink(info);
                              }}
                            >
                              <span class="k">
                                {typeName(link.type)}{" "}
                                {link.direction === 1 ? "←" : link.direction === 0 ? "→" : "↔"}{" "}
                                {link.unverified ? "unverified · " : ""}
                                {link.place.toUpperCase()}
                              </span>
                              <span class="sbar">
                                {link.strength.toFixed(2)}
                                <i style={`--v:${Math.round(link.strength * 100)}%`} />
                              </span>
                              <span class="t">{link.title}</span>
                            </button>
                          )}
                        </For>
                      </>
                    )}
                  </For>
                </div>
              </Show>
            </>
          )}
        </Show>
        <div class="sec side-stats">
          <div class="stats">
            <div class="stat">
              <span class="cap">Stories</span>
              <div class="v">{storyCount().toLocaleString()}</div>
            </div>
            <div class="stat">
              <span class="cap">Links</span>
              <div class="v">{linkCount().toLocaleString()}</div>
            </div>
            <div class="stat">
              <span class="cap">Sources</span>
              <div class="v">{health().sourcesTotal.toLocaleString()}</div>
            </div>
          </div>
        </div>
      </aside>

      <Show when={focus()}>
        <div class="mobile-story glass" role="region" aria-label="Focused story">
          {/* Only the handle switches stories; the body below scrolls. */}
          <div
            class="mobile-story-grab"
            aria-hidden="true"
            onTouchStart={(event) => {
              sheetTouchStartY = event.touches[0]?.clientY ?? 0;
            }}
            onTouchEnd={(event) => {
              const endY = event.changedTouches[0]?.clientY ?? sheetTouchStartY;
              const direction = sheetSwipeDirection(sheetTouchStartY, endY);
              if (direction !== 0) moveSelection(direction);
            }}
          />
          <div class="mobile-story-head">
            <button class="text-btn" aria-label="Back to the wire" onClick={clearFocus}>
              ← Wire
            </button>
            <span class="cap">Focused story</span>
            <button
              class="text-btn mobile-story-close"
              aria-label="Close story"
              onClick={clearFocus}
            >
              ×
            </button>
          </div>
          <div class="mobile-story-scroll">
            <h2>{storyText()?.title || focus()!.story.title}</h2>
            <div class="facts">
              <span>
                <b>{focus()!.hud.story.sources}</b> sources
              </span>
              <span>
                <b>{focus()!.hud.links.length}</b> links
              </span>
              <span>{focus()!.hud.story.place}</span>
            </div>
            <Show
              when={storyText()?.available}
              fallback={<p class="brief muted">Brief not available yet.</p>}
            >
              <p class="brief">{storyText()!.brief}</p>
            </Show>
            <Show when={focus()!.hud.links.length > 0}>
              <div class="mobile-links">
                <For each={linkTiers()}>
                  {(tier) => (
                    <>
                      <div class="tierh">
                        <span class="cap">{tier.label}</span>
                        <span class="cap">{tier.links.length}</span>
                      </div>
                      <For each={tier.links}>
                        {(link) => (
                          <button
                            class="mobile-link"
                            onClick={() => {
                              const info = liveGlobe?.linkInfo(link.index);
                              if (info) openLink(info);
                            }}
                          >
                            <span class="mobile-link-main">
                              <span class="mobile-link-k">
                                {typeName(link.type)}{" "}
                                {link.direction === 1 ? "←" : link.direction === 0 ? "→" : "↔"}{" "}
                                {link.place}
                              </span>
                              <span class="mobile-link-t">{link.title}</span>
                            </span>
                            <b>{link.strength.toFixed(2)}</b>
                          </button>
                        )}
                      </For>
                    </>
                  )}
                </For>
              </div>
            </Show>
          </div>
          <span class="mobile-story-swipe">DRAG HANDLE ↕ · NEXT / PREVIOUS</span>
        </div>
      </Show>

      <Show when={alertsOpen()}>
        <div
          ref={alertsDialog}
          class="alerts-drawer glass"
          role="dialog"
          aria-modal="true"
          aria-labelledby="alerts-title"
          aria-describedby="alerts-desc"
          tabindex="-1"
          onKeyDown={onAlertsKeyDown}
        >
          <div class="alerts-head">
            <span class="cap" id="alerts-title">
              Alerts
            </span>
            <button class="text-btn" aria-label="Close alerts" onClick={() => setAlertsOpen(false)}>
              ×
            </button>
          </div>
          <p class="alerts-desc" id="alerts-desc">
            Prompt alerts fire when a matching story appears. Create, mute or backtest one here.
          </p>
          <div class="alert-form">
            <label class="filter-field">
              <span>Prompt</span>
              <input
                value={alertDraftPrompt()}
                placeholder="e.g. ports and shipping disruption"
                onInput={(event) => setAlertDraftPrompt(event.currentTarget.value)}
              />
            </label>
            <div class="filter-grid">
              <label class="filter-field">
                <span>Min sources</span>
                <input
                  type="number"
                  min="0"
                  value={alertDraftMinSources()}
                  onInput={(event) =>
                    setAlertDraftMinSources(Number(event.currentTarget.value) || 0)
                  }
                />
              </label>
              <label class="filter-field">
                <span>Region</span>
                <input
                  value={alertDraftRegion()}
                  placeholder="Any"
                  onInput={(event) => setAlertDraftRegion(event.currentTarget.value)}
                />
              </label>
            </div>
            <label class="alert-strict">
              <span>
                Strictness <b>{alertDraftStrictness().toFixed(2)}</b>
              </span>
              <input
                type="range"
                min="0.5"
                max="1"
                step="0.01"
                value={alertDraftStrictness()}
                onInput={(event) => setAlertDraftStrictness(Number(event.currentTarget.value))}
              />
            </label>
            <div class="alert-channels">
              <span class="cap">Channels</span>
              <For each={["inapp", "ntfy", "push"]}>
                {(channel) => (
                  <label>
                    <input
                      type="checkbox"
                      checked={alertDraftChannels().includes(channel)}
                      onChange={(event) => {
                        const channels = event.currentTarget.checked
                          ? [...alertDraftChannels(), channel]
                          : alertDraftChannels().filter((item) => item !== channel);
                        setAlertDraftChannels([...new Set(channels)]);
                      }}
                    />
                    {channelLabel(channel)}
                  </label>
                )}
              </For>
            </div>
            <div class="alert-actions">
              <button class="chip" onClick={() => void runBacktest()}>
                Backtest 24h
              </button>
              <button class="chip primary" onClick={() => void saveAlert()}>
                Save alert
              </button>
            </div>
            <Show when={backtest()}>
              {(result) => (
                <div class="backtest">
                  <span class="cap">
                    {result().available === false
                      ? result().error || "Backtest unavailable"
                      : `${result().would_fire} ${result().would_fire === 1 ? "story" : "stories"} would have fired`}
                    {result().approx ? " · approx" : ""}
                  </span>
                  <For each={result().stories.slice(0, 4)}>
                    {(story) => (
                      <button
                        class="backtest-row"
                        onClick={() => liveGlobe?.focusStoryId(story.story_id)}
                      >
                        {story.title}
                        <b>{story.score.toFixed(2)}</b>
                      </button>
                    )}
                  </For>
                </div>
              )}
            </Show>
          </div>
          <div class="alert-list">
            <Show when={alertList().length > 0} fallback={<span class="cap">No alerts yet</span>}>
              <For each={alertList()}>
                {(alert) => (
                  <div class="alert-item">
                    <div class="alert-title">
                      <b>{alert.prompt}</b>
                      <span class="cap">
                        {alert.channels.map(channelLabel).join(" · ")} · strict{" "}
                        {alert.strictness.toFixed(2)}
                      </span>
                    </div>
                    <div class="alert-item-actions">
                      <button
                        class={`sw2 ${alert.enabled ? "" : "off"}`}
                        aria-pressed={alert.enabled}
                        aria-label={`${alert.enabled ? "Disable" : "Enable"} alert`}
                        onClick={() => void toggleAlert(alert)}
                      />
                      <button class="text-btn" onClick={() => void loadAlertHits(alert.id)}>
                        hits
                      </button>
                      <button class="text-btn" onClick={() => void deleteAlert(alert)}>
                        delete
                      </button>
                    </div>
                    <Show when={alertHits()[alert.id]}>
                      <div class="alert-hits">
                        <For each={alertHits()[alert.id]}>
                          {(hit) => (
                            <button onClick={() => liveGlobe?.focusStoryId(hit.story_id)}>
                              {hit.level === "escalated" ? "↑ " : ""}
                              {hit.title}
                              {approximateLabel(hit.approx)}
                            </button>
                          )}
                        </For>
                      </div>
                    </Show>
                  </div>
                )}
              </For>
            </Show>
          </div>
          <Show when={ntfyAlerts().length > 0}>
            <div class="ntfy-block">
              <span class="cap">Phone alerts · ntfy</span>
              <p>
                Install the ntfy app on your phone and subscribe to each topic below (use the same
                ntfy server Dor is configured with). Enable “phone (ntfy)” on an alert to send it
                there.
              </p>
              <For each={ntfyAlerts()}>
                {(alert) => (
                  <div class="ntfy-row">
                    <code>{alert.ntfy_topic}</code>
                    <button
                      class="chip"
                      aria-label={`Copy ntfy topic ${alert.ntfy_topic}`}
                      onClick={() => void copyNtfyTopic(alert.ntfy_topic!)}
                    >
                      {ntfyCopied() === alert.ntfy_topic ? "Copied" : "Copy"}
                    </button>
                    <small>{alert.prompt}</small>
                    <span class="sr-only" role="status" aria-live="polite">
                      {ntfyCopied() === alert.ntfy_topic ? "Topic copied" : ""}
                    </span>
                  </div>
                )}
              </For>
            </div>
          </Show>
          <div class="push-row">
            <div>
              <span class="cap">Browser push</span>
              <p class="cap">
                {pushState().loading
                  ? "Checking…"
                  : pushState().available === true
                    ? "Enabled for this browser"
                    : pushState().reason || "Enable only on user action"}
              </p>
            </div>
            <button
              class="chip"
              disabled={pushState().loading || pushState().available === true}
              onClick={() => void enablePush()}
            >
              {pushState().available === true ? "Enabled" : "Enable push"}
            </button>
          </div>
        </div>
      </Show>
      <Show when={alertToasts().length > 0}>
        <div class="toast-stack">
          <For each={alertToasts()}>
            {(toast) => (
              <div class="alert-toast glass">
                <span class="cap">
                  {toast.level === "escalated" ? "Escalated" : "Alert"} · {toast.prompt}
                  {approximateLabel(toast.approx)}
                </span>
                <b>{toast.title}</b>
                <div class="toast-actions">
                  <button
                    class="chip primary"
                    onClick={() => {
                      liveGlobe?.focusStoryId(toast.storyId);
                      dismissToast(toast);
                    }}
                  >
                    Open story
                  </button>
                  <button class="chip" onClick={() => void muteAlertStory(toast)}>
                    Mute this story
                  </button>
                  <button class="text-btn" onClick={() => dismissToast(toast)}>
                    dismiss
                  </button>
                </div>
              </div>
            )}
          </For>
        </div>
      </Show>

      <footer class="timeline">
        <Show
          when={focus()}
          fallback={
            <>
              <div class="tlrow">
                <span
                  class="cap"
                  style="width:78px"
                  title={
                    timeline().real
                      ? `${timeline().total.toLocaleString()} articles in the last 24 h (database)`
                      : (timeline().error ?? "History unavailable")
                  }
                >
                  Last 24h
                </span>
                <div class="bars">
                  <For each={timeline().bars}>{(height) => <i style={`height:${height}%`} />}</For>
                  <span class="win" />
                  <span class="now" />
                </div>
                <button
                  class="chip"
                  aria-pressed="true"
                  title="Jump to the newest articles"
                  onClick={goLive}
                >
                  ● Live
                </button>
              </div>
              <div class="ticks">
                <span>−24h</span>
                <span>−18h</span>
                <span>−12h</span>
                <span>−6h</span>
                <span>now</span>
              </div>
            </>
          }
        >
          <div class="tlrow">
            <span class="cap" style="width:78px">
              This thread
            </span>
            <div class="track">
              <For each={focusTimeline()}>
                {(event) => (
                  <button
                    class={`ev ${event.showLabel ? "" : "dotonly"}`}
                    style={`left:${event.left}%;--c:${event.color}`}
                    title={event.title}
                    aria-label={event.title}
                  >
                    {event.showLabel ? event.label : ""}
                  </button>
                )}
              </For>
            </div>
          </div>
          <div class="ticks">
            <span>first seen</span>
            <span>linked stories at their last update</span>
            <span>now</span>
          </div>
        </Show>
      </footer>
    </div>
  );
}
