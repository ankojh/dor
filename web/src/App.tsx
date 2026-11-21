import { For, Show, createEffect, createMemo, createSignal, onCleanup, onMount } from "solid-js";
import { createVirtualizer } from "@tanstack/solid-virtual";
import type { Category, Story, WireRow } from "./gen/dor/v1/dor_pb";
import { JoinKind, Precision, Role } from "./gen/dor/v1/dor_pb";
import { categoryColor, categoryLabel } from "./globe/encoding";
import { mergeWireRow } from "./live/wire";
import {
  LiveGlobe,
  type FocusDetails,
  type GlobeState,
  type LinkInfo,
} from "./globe/runtime";
import type {
  ConnectionState,
  SourceHealth,
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
};

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

function wireTime(row: WireRow, receivedAt: number): string {
  const seconds = Number(row.at) || Math.floor(receivedAt / 1000);
  return new Date(seconds * 1000).toISOString().slice(11, 19);
}

function hexColor(category: number): string {
  const rgb = categoryColor(category);
  return `#${rgb.map((value) => value.toString(16).padStart(2, "0")).join("")}`;
}

function articleKey(row: WireRow): string {
  return row.articleId || `${row.source}:${row.at}:${row.title}`;
}

function sourceName(source: string): string {
  return source || "WIRE";
}

function joinText(row: WireRow): string {
  if (row.join === JoinKind.JOINED) {
    return row.storyId ? `↳ joined story ${row.storyId.slice(0, 10)}` : "↳ joined a story";
  }
  if (row.storyId) return "↳ new story";
  return "";
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
    case Precision.COUNTRY:
      return "COUNTRY";
    case Precision.REGION:
      return "REGION";
    case Precision.CITY:
      return "CITY";
    case Precision.STREET:
      return "STREET";
    case Precision.POINT:
      return "POINT";
  }
}

function roleLabel(role: Role): string {
  switch (role) {
    case Role.VENUE:
      return "Venue";
    case Role.ORIGIN:
      return "Origin";
    case Role.PARTY:
      return "Party";
    case Role.AFFECTED:
      return "Affected";
    case Role.MENTIONED:
      return "Mentioned";
  }
}

function directionLabel(link: LinkInfo): string {
  if (link.direction === 0) return `${link.aPlace} → ${link.bPlace}`;
  if (link.direction === 1) return `${link.bPlace} → ${link.aPlace}`;
  return `${link.aPlace} · ${link.bPlace}`;
}

function typeName(type: number): string {
  return type === 0 ? "Causal" : type === 1 ? "Reaction" : "Thematic";
}

type StoryText = {
  loading: boolean;
  available: boolean;
  brief?: string;
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
    category: string;
    source_count: number;
    first_seen: number;
    updated_at: number;
  };
  articles: StoryArticle[];
  sources: StorySource[];
  articles_truncated?: boolean;
};

const API_BASE = (import.meta.env.VITE_API_URL || "").replace(/\/$/, "");

export function App() {
  let mapContainer!: HTMLDivElement;
  let scrollElement!: HTMLDivElement;
  let searchInput!: HTMLInputElement;
  let liveGlobe: LiveGlobe | null = null;
  let worker: Worker | null = null;
  let mockStop: (() => void) | null = null;
  let flushTimer: ReturnType<typeof setTimeout> | null = null;
  let freshTimer: ReturnType<typeof setTimeout> | null = null;
  let clockTimer: ReturnType<typeof setInterval> | null = null;

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
  const pendingWire: WireItem[] = [];
  const seen = new Set<string>();
  const briefCache = new Map<string, StoryText>();
  const detailsCache = new Map<string, StoryDetails>();
  const whyCache = new Map<string, LinkWhy>();
  const prefetched = new Set<string>();
  const initialHash = new URLSearchParams(location.hash.slice(1));
  let hashTimer: ReturnType<typeof setTimeout> | null = null;
  let hoverTimer: ReturnType<typeof setTimeout> | null = null;
  let pendingInitialStory = initialHash.get("story");

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
  const visibleWireRows = createMemo(() => {
    let rows = wireRows();
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

  const rate = createMemo(() => {
    const cutoff = now() - 60_000;
    return wireRows().filter((item) => item.receivedAt >= cutoff).length;
  });

  const rateBars = createMemo(() => {
    const buckets = [0, 0, 0, 0, 0];
    const cutoff = now() - 60_000;
    for (const item of wireRows()) {
      if (item.receivedAt < cutoff) continue;
      const bucket = Math.min(4, Math.floor((item.receivedAt - cutoff) / 12_000));
      buckets[bucket] = (buckets[bucket] ?? 0) + 1;
    }
    const max = Math.max(1, ...buckets);
    return buckets.map((value) => Math.max(12, Math.round((value / max) * 100)));
  });

  const timelineBars = createMemo(() => {
    const rows = wireRows();
    if (rows.length === 0) return new Array(36).fill(0);
    const end = now();
    const binMs = 24 * 60 * 60 * 1000 / 36;
    const bins = new Array(36).fill(0);
    for (const item of rows) {
      const at = Number(item.row.at) * 1000 || item.receivedAt;
      const bin = Math.floor((at - (end - 24 * 60 * 60 * 1000)) / binMs);
      if (bin >= 0 && bin < 36) bins[bin] = (bins[bin] ?? 0) + 1;
    }
    const max = Math.max(1, ...bins);
    return bins.map((value) => Math.max(2, Math.round((value / max) * 100)));
  });

  const focusTimeline = createMemo(() => {
    const details = focus();
    if (!details) return [];
    const links = details.hud.links.slice(0, 5);
    const events = [
      { left: 4, label: "first seen", color: "#ffc25c" },
      ...links.map((link, index) => ({
        left: 38 + (index * 46) / Math.max(1, links.length - 1),
        label: `→ ${link.place}`,
        color: link.type === 0 ? "#ffc25c" : link.type === 1 ? "#ff6fd8" : "#5ef2ff",
      })),
      { left: 96, label: "now", color: "#ff4f6e" },
    ];
    return events;
  });

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
      added++;
    }
    if (updates.length > 0) {
      // The wire row was already visible; update metadata in place. This is
      // the article.clean -> article.placed transition and must not duplicate.
      setWireRows((previous) =>
        previous.map((item) => {
          const update = updates.find((candidate) => candidate.key === item.key);
          return update ? { ...item, row: mergeWireRow(item.row, update.row) } : item;
        }),
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
    setWireRows((previous) => [...batch.reverse(), ...previous].slice(0, 1000));
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

  const syncHash = (storyId: string | null = focus()?.storyId ?? null): void => {
    const params = new URLSearchParams();
    if (storyId) params.set("story", storyId);
    if (liveGlobe) {
      const view = liveGlobe.view;
      params.set("view", `${view.center[0].toFixed(3)},${view.center[1].toFixed(3)},${view.zoom.toFixed(2)}`);
    }
    if (threadOnly()) params.set("f", "thread");
    else if (globe().secondDegree) params.set("f", "second");
    const next = params.toString();
    history.replaceState(null, "", `${location.pathname}${location.search}${next ? `#${next}` : ""}`);
  };

  const scheduleHash = (): void => {
    if (hashTimer !== null) clearTimeout(hashTimer);
    hashTimer = setTimeout(() => {
      hashTimer = null;
      syncHash(focus()?.storyId ?? pendingInitialStory);
    }, 180);
  };

  const loadStoryText = async (storyId: string): Promise<void> => {
    const cached = briefCache.get(storyId);
    if (cached) {
      setStoryText(cached);
      return;
    }
    setStoryText({ loading: true, available: false });
    try {
      const response = await fetch(`${API_BASE}/api/text?story_id=${encodeURIComponent(storyId)}`);
      const payload = (await response.json()) as {
        available?: boolean;
        brief?: string;
        error?: string;
      };
      const next: StoryText = {
        loading: false,
        available: Boolean(payload.available) && Boolean(payload.brief),
        ...(payload.brief ? { brief: payload.brief } : {}),
        ...(payload.error ? { error: payload.error } : {}),
      };
      briefCache.set(storyId, next);
      if (focus()?.storyId === storyId) setStoryText(next);
    } catch (error) {
      const next: StoryText = {
        loading: false,
        available: false,
        error: String(error),
      };
      briefCache.set(storyId, next);
      if (focus()?.storyId === storyId) setStoryText(next);
    }
  };

  const loadStoryDetails = async (storyId: string): Promise<void> => {
    const cached = detailsCache.get(storyId);
    if (cached) {
      setStoryDetails(cached);
      return;
    }
    try {
      const response = await fetch(`${API_BASE}/api/stories/${encodeURIComponent(storyId)}`);
      if (!response.ok) {
        setStoryDetails(null);
        return;
      }
      const payload = (await response.json()) as StoryDetails;
      detailsCache.set(storyId, payload);
      if (focus()?.storyId === storyId) setStoryDetails(payload);
    } catch {
      setStoryDetails(null);
    }
  };

  const loadLinkWhy = async (link: LinkInfo): Promise<void> => {
    const key = `${link.aId} ${link.bId}`;
    const cached = whyCache.get(key);
    if (cached) {
      setLinkWhy(cached);
      return;
    }
    setLinkWhy({ loading: true, available: false });
    try {
      const response = await fetch(
        `${API_BASE}/api/text?link_a=${encodeURIComponent(link.aId)}&link_b=${encodeURIComponent(link.bId)}`,
      );
      const payload = (await response.json()) as { available?: boolean; why?: string; error?: string };
      const next: LinkWhy = {
        loading: false,
        available: Boolean(payload.available) && Boolean(payload.why),
        ...(payload.why ? { why: payload.why } : {}),
        ...(payload.error ? { error: payload.error } : {}),
      };
      whyCache.set(key, next);
      setLinkWhy(next);
    } catch (error) {
      const next: LinkWhy = { loading: false, available: false, error: String(error) };
      whyCache.set(key, next);
      setLinkWhy(next);
    }
  };

  const openLink = (link: LinkInfo): void => {
    setSelectedLink(link);
    void loadLinkWhy(link);
  };

  const prefetchStory = (storyId: string): void => {
    if (!storyId || prefetched.has(storyId)) return;
    prefetched.add(storyId);
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
      case "snapshot":
        seen.clear();
        pendingWire.length = 0;
        setWireRows([]);
        setPendingNew(0);
        setSelectedArticle(null);
        setLastSeq(message.seq);
        liveGlobe?.setSnapshot(message.stories, message.links);
        if (message.recentWire?.length) enqueueWire(message.recentWire);
        break;
      case "deltas":
        setLastSeq(message.seq);
        liveGlobe?.applyDeltas(message.deltas);
        enqueueWire(
          message.deltas
            .filter((delta) => delta.body.case === "wire")
            .map((delta) => (delta.body.case === "wire" ? delta.body.value : null))
            .filter((row): row is WireRow => row !== null),
        );
        break;
      case "resync":
        seen.clear();
        pendingWire.length = 0;
        setWireRows([]);
        setPendingNew(0);
        setLastSeq(0);
        break;
    }
  };

  onMount(() => {
    liveGlobe = new LiveGlobe(mapContainer, {
      onState: setGlobe,
      onFocus: (details) => {
        setFocus(details);
        if (details) {
          pendingInitialStory = null;
          const matching = wireRows().find((item) => item.row.storyId === details.storyId);
          setSelectedArticle(matching?.key ?? null);
          setStoryText(briefCache.get(details.storyId) ?? { loading: true, available: false });
          setStoryDetails(detailsCache.get(details.storyId) ?? null);
          void loadStoryText(details.storyId);
          void loadStoryDetails(details.storyId);
          worker?.postMessage({ type: "focus", storyId: details.storyId });
          syncHash(details.storyId);
        } else {
          setSelectedArticle(null);
          setThreadOnly(false);
          setSelectedLink(null);
          setStoryText(null);
          setStoryDetails(null);
          syncHash(null);
        }
      },
      onLinkClick: openLink,
      onView: scheduleHash,
    });
    liveGlobe.setStrengthFloor(layerFloor());
    const viewParam = initialHash.get("view")?.split(",").map(Number);
    if (viewParam && viewParam.length === 3 && viewParam.every((value) => Number.isFinite(value))) {
      liveGlobe.setView([viewParam[0]!, viewParam[1]!], viewParam[2]!);
    }
    if (initialHash.get("f") === "thread") setThreadOnly(true);
    if (initialHash.get("f") === "second") liveGlobe.setSecondDegree(true);
    if (pendingInitialStory) liveGlobe.focusStoryId(pendingInitialStory);

    if (new URLSearchParams(location.search).get("mock") === "1") {
      void import("./live/mock").then(({ startMock }) => {
        mockStop = startMock(handleMessage).stop;
      });
    } else {
      worker = new Worker(new URL("./worker/index.ts", import.meta.url), {
        type: "module",
        name: "dor-data-worker",
      });
      worker.onmessage = (event: MessageEvent<WorkerMessage>) => handleMessage(event.data);
      worker.onerror = (event) => {
        setConnection("offline");
        console.error(`data worker: ${event.message}`);
      };
    }

    clockTimer = setInterval(() => setNow(Date.now()), 1000);

    const onKey = (event: KeyboardEvent): void => {
      const target = event.target as HTMLElement | null;
      const typing =
        target?.tagName === "INPUT" ||
        target?.tagName === "TEXTAREA" ||
        target?.isContentEditable === true;
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        searchInput?.focus();
        return;
      }
      if (event.key === "Escape") {
        setSearchOpen(false);
        setSelectedLink(null);
      }
      if (typing) return;
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
      worker?.terminate();
      worker = null;
      mockStop?.();
      mockStop = null;
      liveGlobe?.destroy();
      liveGlobe = null;
      if (clockTimer !== null) clearInterval(clockTimer);
      if (flushTimer !== null) clearTimeout(flushTimer);
      if (freshTimer !== null) clearTimeout(freshTimer);
    });
  });

  const moveSelection = (delta: number): void => {
    const rows = displayRows()
      .filter((row): row is { kind: "article"; key: string; item: WireItem } => row.kind === "article")
      .map((row) => row.item);
    if (rows.length === 0) return;
    const current = rows.findIndex((item) => item.key === selectedArticle());
    const next = Math.max(0, Math.min(rows.length - 1, current < 0 ? 0 : current + delta));
    const item = rows[next];
    if (!item) return;
    setSelectedArticle(item.key);
    if (item.row.storyId) liveGlobe?.focusStoryId(item.row.storyId);
  };

  const selectArticle = (item: WireItem): void => {
    setSelectedArticle(item.key);
    if (item.row.storyId) liveGlobe?.focusStoryId(item.row.storyId);
  };

  const clearFocus = (): void => {
    liveGlobe?.clearFocus();
    setFocus(null);
    setSelectedArticle(null);
  };

  const focusTopStory = (story: Story): void => {
    liveGlobe?.focusStoryId(story.id);
  };

  const expandBatch = (key: string): void => {
    setExpandedBatches((previous) => {
      const next = new Set(previous);
      next.add(key);
      return next;
    });
  };

  return (
    <div class="app3">
      <header class="topbar">
        <div class="brand">
          <span class="mark" />
          <span class="word">DOR</span>
        </div>
        <span class={`live ${connection() === "online" ? "" : "muted-live"}`}>
          {connection() === "online" ? "LIVE" : "LIVE"}
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
              setSearchQuery(event.currentTarget.value);
              setSearchOpen(true);
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
          </div>
        </Show>
        <div class="chips" role="group" aria-label="Categories">
          <button class="chip" aria-pressed="true">
            All
          </button>
          <For each={CATEGORIES}>
            {(category) => (
              <button class="chip" style={`--c:${category.color}`} title="Filters arrive in M4">
                <span class="sw" />
                {category.label}
              </button>
            )}
          </For>
        </div>
        <div class="right">
          <Show when={connection() !== "online"}>
            <span class="reconnect" title={health().detail}>
              {connection() === "connecting" ? "CONNECTING" : connection() === "reconnecting" ? "RECONNECTING" : "OFFLINE"}
              {reconnectIn() !== null ? ` · ${Math.ceil(reconnectIn()! / 1000)}s` : ""}
            </span>
          </Show>
          <span class={`health ${healthClass(health())}`} title={health().detail}>
            {healthLabel(health())}
            <b>{ingestLag() === null ? "LAG —" : `LAG ${ingestLag()}s`}</b>
          </span>
          <span>
            UTC <b>{utc()}</b>
          </span>
          <button class="ibtn bell" title="Alerts (placeholder)" aria-label="Alerts placeholder">
            ◔
          </button>
        </div>
      </header>

      <aside class="wire">
        <div class="wire-head">
          <div class="r1">
            <h2>Live wire</h2>
            <span class="rate" title="Articles received in the last minute">
              <i>
                <For each={rateBars()}>{(height) => <b style={`height:${height}%`} />}</For>
              </i>
              {rate() > 0 ? `${rate()} / min` : "— / min"}
            </span>
            <div class="seg" role="group" aria-label="Stream granularity">
              <button
                aria-pressed={!storiesOnly()}
                onClick={() => setStoriesOnly(false)}
              >
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
          </div>
          <Show when={focus()}>
            <div class="pfs">
              <span class="pf on">
                <span class="k">THREAD</span>
                {focus()!.story.title.length > 34
                  ? `${focus()!.story.title.slice(0, 34)}…`
                  : focus()!.story.title}
                <button class="x" title="Clear focus" onClick={clearFocus}>
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
            <span class="spark" aria-hidden="true">✦</span>
            <input
              type="text"
              placeholder="Filter with a prompt — prompt filters arrive in M4"
              aria-label="Prompt filter placeholder"
              disabled
            />
            <span class="kbd">↵</span>
          </label>
        </div>

        <div
          class="wrows"
          ref={scrollElement}
          onMouseMove={() => setPausedByHover(true)}
          onMouseLeave={() => setPausedByHover(false)}
          onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
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
                <span class="cap">{connection() === "online" ? "Listening" : "Gateway unavailable"}</span>
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
                      class={`wr ${
                        display()?.kind === "article" &&
                        selectedArticle() === (display() as { kind: "article"; item: WireItem }).item.key
                          ? "sel"
                          : ""
                      } ${
                        display()?.kind === "article" &&
                        freshKeys().has((display() as { kind: "article"; item: WireItem }).item.key)
                          ? "fresh"
                          : ""
                      } ${
                        display()?.kind === "article" && focus()
                          ? threadIds().has((display() as { kind: "article"; item: WireItem }).item.row.storyId)
                            ? "thread"
                            : "dim"
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
                            + {display()?.kind === "burst" ? (display() as { count: number }).count : 0} more from the{" "}
                            {display()?.kind === "burst" ? (display() as { source: string }).source : "batch"} batch
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
                          return (
                            <button
                              class="wr-body"
                              style={`--c:${color}`}
                              onClick={() => selectArticle(item.item)}
                              onMouseEnter={() => rowEnter(row.storyId)}
                              onMouseLeave={rowLeave}
                              title={row.title}
                            >
                              <time>{wireTime(row, item.item.receivedAt)}</time>
                              <span class="dot" />
                              <span class="h">
                                <span class="ws">{sourceName(row.source)}</span>
                                <span class="title">{row.title || "Untitled article"}</span>
                                <Show when={joinText(row)}>
                                  <span class="join">{joinText(row)}</span>
                                </Show>
                              </span>
                              <span class="pl">{row.place || "—"}</span>
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
          <span class="cap">Hover pauses · ↑ ↓ to select</span>
          <span class="cap">{lastSeq() > 0 ? `seq ${lastSeq()}` : "newest first"}</span>
        </div>
      </aside>

      <main class="stage">
        <div ref={mapContainer} class="map-host" aria-label="Live globe" />
        <div class="hud-corners" />
        <div class="tl">
          <span class="cap">
            {focus() ? `Focused · ${focus()!.hud.story.place.toUpperCase()}` : "Ambient · no selection"}
          </span>
          <Show when={focus()}>
            <button class="back" onClick={clearFocus}>
              ← GLOBAL VIEW (Esc)
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
            title="Second-degree links"
            onClick={() => {
              liveGlobe?.setSecondDegree(!globe().secondDegree);
              syncHash();
            }}
          >
            ⋯
          </button>
          <button
            class="ibtn"
            aria-pressed={layersOpen()}
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
              Coastline when zoomed in <span class="kbd">M5</span>
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
            <span class="cap">{connection() === "online" ? "Waiting for live data" : "Reconnecting to the gateway"}</span>
            <h3>Articles first. Stories next.</h3>
            <p>
              The wire is live now. The globe fills when the classifier publishes its first
              located stories.
            </p>
            <span class="mono">{wireRows().length} wire rows · {storyCount()} stories</span>
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
        </div>
        <div class="br">
          <button class="ibtn" title="Zoom in" onClick={() => liveGlobe?.zoomIn()}>
            +
          </button>
          <button class="ibtn" title="Zoom out" onClick={() => liveGlobe?.zoomOut()}>
            −
          </button>
          <button class="ibtn" title="Reset view" onClick={() => liveGlobe?.resetView()}>
            ◎
          </button>
        </div>
        <Show when={!focus()}>
          <div class="edge-markers">
            <For each={globe().edgeMarkers}>
            {(marker) => (
              <button
                class="rim"
                title={`Rotate to ${marker.label}`}
                onClick={() => liveGlobe?.flyTo(marker.center, marker.zoom)}
              >
                +{marker.count} <b>{marker.label}</b> ▸
              </button>
              )}
            </For>
          </div>
        </Show>
        <Show when={selectedLink()}>
          {(link) => (
            <div class="arc-popover glass">
              <div class="arc-popover-head">
                <span class="cap" style={`color:${link().type === 0 ? "#ffc25c" : link().type === 1 ? "#ff6fd8" : "#5ef2ff"}`}>
                  {typeName(link().type)} · {directionLabel(link())}
                </span>
                <button class="text-btn" onClick={() => setSelectedLink(null)}>
                  ×
                </button>
              </div>
              <h3>{link().aTitle}</h3>
              <h4>→ {link().bTitle}</h4>
              <div class="facts">
                <span><b>{link().strength.toFixed(2)}</b> strength</span>
                <span><b>{link().confidence.toFixed(2)}</b> confidence</span>
                <span><b>{link().distanceKm.toFixed(0)}</b> km</span>
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
              <span class="agentk">WHY · agent · cached</span>
            </div>
          )}
        </Show>
        <Show when={perfOverlay()}>
          <div class="perf-chip mono">
            QUALITY {globe().quality} · DPR ≤ 2 · {globe().storyCount} stories
          </div>
        </Show>
        <span class="attrib mono">
          © OpenFreeMap · © OpenMapTiles · © OpenStreetMap contributors · GDELT
        </span>
      </main>

      <aside class="side">
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
                            strongest {story.strongestLink > 0 ? story.strongestLink.toFixed(2) : "—"}
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
                </span>
                <button class="text-btn" onClick={clearFocus}>
                  Global view ×
                </button>
              </div>
              <div class="sec">
                <h1>{details().story.title}</h1>
                <div class="facts">
                  <span>
                    first seen{" "}
                    <b>{relativeTime(Number(details().story.firstSeen), now())}</b>
                  </span>
                  <span>
                    <b>{details().hud.story.sources}</b> sources
                  </span>
                  <span>
                    updated{" "}
                    <b>{relativeTime(Number(details().story.updatedAt), now())}</b>
                  </span>
                </div>
              </div>
              <div class="sec">
                <span class="agentk">
                  AGENT BRIEF · from {details().hud.story.sources} sources
                </span>
                <Show
                  when={storyText()?.available}
                  fallback={
                    <p class="brief muted">
                      {storyText()?.loading
                        ? "Generating brief…"
                        : "Brief not available yet. The story remains readable from its sources and places."}
                    </p>
                  }
                >
                  <p class="brief">{storyText()!.brief}</p>
                </Show>
              </div>
              <div class="sec">
                <span class="cap">Places</span>
                <For each={details().story.locations.slice(0, 6)}>
                  {(place, index) => (
                    <div class="place">
                      <span
                        class={index() === details().story.primary ? "fill" : "ring"}
                        style={`--c:${hexColor(details().story.category)}`}
                      />
                      <span class="role">{roleLabel(place.role)}</span>
                      <span class="place-name">{place.name || "Unknown place"}</span>
                      <span class="prec">{precisionLabel(place.precision)}</span>
                    </div>
                  )}
                </For>
                <Show when={details().story.locations.length > 6}>
                  <span class="cap">+{details().story.locations.length - 6} more places</span>
                </Show>
              </div>
              <Show when={storyDetails()}>
                {(detailsData) => (
                  <div class="sec">
                    <div class="tierh">
                      <span class="cap">Sources</span>
                      <span class="cap">
                        {detailsData().sources.length} feeds ·{" "}
                        {detailsData().articles.length} recent
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
                  <For
                    each={[
                      {
                        label: "Strongly linked",
                        links: details().hud.links.filter((link) => link.strength >= 0.75),
                      },
                      {
                        label: "Related",
                        links: details().hud.links.filter(
                          (link) => link.strength >= 0.5 && link.strength < 0.75,
                        ),
                      },
                      {
                        label: "Loosely linked",
                        links: details().hud.links.filter((link) => link.strength < 0.5),
                      },
                    ].filter((tier) => tier.links.length > 0)}
                  >
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
                                {typeName(link.type)} {link.direction === 1 ? "←" : link.direction === 0 ? "→" : "↔"}{" "}
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
        <div class="mobile-story glass">
          <div class="mobile-story-head">
            <span class="cap">Focused story</span>
            <button class="text-btn" onClick={clearFocus}>
              ×
            </button>
          </div>
          <h2>{focus()!.story.title}</h2>
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
              <For each={focus()!.hud.links.slice(0, 3)}>
                {(link) => (
                  <button
                    class="mobile-link"
                    onClick={() => {
                      const info = liveGlobe?.linkInfo(link.index);
                      if (info) openLink(info);
                    }}
                  >
                    <span>{typeName(link.type)} · {link.place}</span>
                    <b>{link.strength.toFixed(2)}</b>
                  </button>
                )}
              </For>
            </div>
          </Show>
        </div>
      </Show>

      <footer class="timeline">
        <Show
          when={focus()}
          fallback={
            <>
              <div class="tlrow">
                <span class="cap" style="width:78px">Last 24h</span>
                <div class="bars">
                  <For each={timelineBars()}>{(height) => <i style={`height:${height}%`} />}</For>
                  <span class="win" />
                  <span class="now" />
                </div>
                <button class="chip" aria-pressed="true">● Live</button>
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
            <span class="cap" style="width:78px">This thread</span>
            <div class="track">
              <For each={focusTimeline()}>
                {(event) => (
                  <button
                    class="ev"
                    style={`left:${event.left}%;--c:${event.color}`}
                    title={event.label}
                  >
                    {event.label}
                  </button>
                )}
              </For>
            </div>
          </div>
          <div class="ticks">
            <span>first seen</span>
            <span>linked stories</span>
            <span>now</span>
          </div>
        </Show>
      </footer>
    </div>
  );
}
