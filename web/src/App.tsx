import { For, Show, createEffect, createMemo, createSignal, onCleanup, onMount } from "solid-js";
import { createVirtualizer } from "@tanstack/solid-virtual";
import type { Category, Story, WireRow } from "./gen/dor/v1/dor_pb";
import { JoinKind } from "./gen/dor/v1/dor_pb";
import { categoryColor } from "./globe/encoding";
import { LiveGlobe, type FocusDetails, type GlobeState } from "./globe/runtime";
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
  const pendingWire: WireItem[] = [];
  const seen = new Set<string>();

  const paused = createMemo(() => pausedByHover() || scrollTop() > 6);
  const ingestLag = createMemo(() => {
    const at = lastWireAt();
    if (!at) return null;
    return Math.max(0, Math.round(now() / 1000 - at));
  });
  const utc = createMemo(() => clockText(now()));
  const storyCount = createMemo(() => globe().storyCount);
  const linkCount = createMemo(() => globe().linkCount);
  const topStories = createMemo(() => globe().topStories.slice(0, 3));

  const displayRows = createMemo<DisplayRow[]>(() => {
    const rows = wireRows();
    const expanded = expandedBatches();
    const result: DisplayRow[] = [];
    let index = 0;
    while (index < rows.length) {
      const current = rows[index]!;
      const source = current.row.source.toUpperCase();
      const isGdelt = source.includes("GDELT");
      if (isGdelt) {
        let end = index + 1;
        while (
          end < rows.length &&
          rows[end]!.row.source.toUpperCase().includes("GDELT") &&
          Math.floor(Number(rows[end]!.row.at) / 60) === Math.floor(Number(current.row.at) / 60)
        ) {
          end++;
        }
        const count = end - index;
        const batchKey = `gdelt:${Number(current.row.at)}:${index}`;
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

  const enqueueWire = (rows: WireRow[]): void => {
    let added = 0;
    for (const row of rows) {
      const key = articleKey(row);
      if (seen.has(key)) continue;
      seen.add(key);
      pendingWire.push({ row, key, receivedAt: Date.now() });
      added++;
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

  createEffect(() => {
    const count = displayRows().length;
    void count;
    virtualizer.measure();
  });

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
      },
    });

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
      if (typing) return;
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
    const rows = wireRows();
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
            placeholder="Search stories, places, people"
            onFocus={() => setSearchFocused(true)}
            onBlur={() => setSearchFocused(false)}
            aria-label="Search stories, places and people"
          />
          <span class="kbd">⌘K</span>
        </label>
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
              <button aria-pressed="true">Every article</button>
              <button disabled title="Stories-only stream arrives in M2">
                Stories only
              </button>
            </div>
          </div>
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
          onMouseEnter={() => setPausedByHover(true)}
          onMouseLeave={() => setPausedByHover(false)}
          onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
          aria-label="Live article wire"
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
                      class={`wr ${display()?.kind === "article" && selectedArticle() === (display() as { kind: "article"; item: WireItem }).item.key ? "sel" : ""} ${display()?.kind === "article" && freshKeys().has((display() as { kind: "article"; item: WireItem }).item.key) ? "fresh" : ""}`}
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
          <span class="big">{focus() ? focus()!.story.title : "Global · live"}</span>
          <Show when={focus()}>
            <span class="back">{focus()!.hud.story.sources} sources · {focus()!.hud.links.length} direct links</span>
          </Show>
        </div>
        <div class="tr">
          <button
            class="ibtn"
            aria-pressed={layersOpen()}
            title="Layers"
            onClick={() => setLayersOpen((open) => !open)}
          >
            ≡
          </button>
          <button class="ibtn" title="Autopilot arrives in M2" disabled>
            ▷
          </button>
        </div>
        <Show when={layersOpen()}>
          <div class="layers glass">
            <span class="cap">Layers</span>
            <div class="ln2">
              Relation lines <span class="sw2" />
            </div>
            <div class="ln2">Link strength floor</div>
            <div class="slider2" style="--v:60%">
              <span>all</span>
              <span class="track" />
              <span>strong</span>
              <b>0.60</b>
            </div>
            <div class="ln2">
              Second-degree links <span class="sw2 off" />
            </div>
            <div class="ln2">
              Location tethers <span class="sw2" />
            </div>
            <div class="ln2">
              Labels <span class="sw2" />
            </div>
            <div class="ln2">
              Heat (cluster glow) <span class="sw2 off" />
            </div>
            <div class="ln2">
              Perf overlay <span class="kbd">⇧P</span>
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
                          {CATEGORIES.find((entry) => entry.category === story.category)?.label ?? "Other"} ·{" "}
                          {story.story.sourceCount} sources
                        </span>
                        <p class="ttl">{story.story.title}</p>
                        <div class="foot">
                          <span>{story.place}</span>
                          <span>heat {story.story.heat.toFixed(2)}</span>
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
                <span class="cap">Focused story</span>
                <button class="text-btn" onClick={clearFocus}>
                  Global view ×
                </button>
              </div>
              <div class="sec">
                <span class="cap" style={`color:${hexColor(details().story.category)}`}>
                  {CATEGORIES.find((entry) => entry.category === details().story.category)?.label ?? "Story"}
                </span>
                <h1>{details().story.title}</h1>
                <div class="facts">
                  <span>
                    <b>{details().hud.story.sources}</b> sources
                  </span>
                  <span>
                    <b>{details().hud.links.length}</b> links
                  </span>
                  <span>
                    <b>{details().hud.story.heat.toFixed(2)}</b> heat
                  </span>
                  <span>{details().hud.story.place}</span>
                </div>
                <p class="brief">
                  Story brief and timeline arrive in M3. The globe is showing the real focus path:
                  the selected story, its connected stories and its direct links.
                </p>
              </div>
              <Show when={details().hud.links.length > 0}>
                <div class="sec">
                  <div class="tierh">
                    <span class="cap">Direct links</span>
                    <span class="cap">strength</span>
                  </div>
                  <For each={details().hud.links.slice(0, 6)}>
                    {(link) => (
                      <div class="lk" style={`--c:${link.type === 0 ? "#ffc25c" : link.type === 1 ? "#ff6fd8" : "#5ef2ff"}`}>
                        <span class="k">
                          {link.type === 0 ? "Causal" : link.type === 1 ? "Reaction" : "Thematic"} ·{" "}
                          {link.place.toUpperCase()}
                        </span>
                        <span class="sbar">
                          {link.strength.toFixed(2)}
                          <i style={`--v:${Math.round(link.strength * 100)}%`} />
                        </span>
                        <span class="t">{link.title}</span>
                      </div>
                    )}
                  </For>
                </div>
              </Show>
              <Show when={details().hud.parties.length > 0}>
                <div class="sec">
                  <span class="cap">Parties</span>
                  <For each={details().hud.parties}>
                    {(party) => (
                      <div class="place">
                        <span class="fill" style="--c:#ff6fd8" />
                        {party.label}
                      </div>
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
              <span class="cap">Rows</span>
              <div class="v">{wireRows().length.toLocaleString()}</div>
            </div>
          </div>
        </div>
      </aside>

      <footer class="timeline">
        <div class="tlrow">
          <span class="cap" style="width:78px">
            Last 24h
          </span>
          <div class="bars">
            <For each={timelineBars()}>{(height) => <i style={`height:${height}%`} />}</For>
            <span class="win" />
            <span class="now" />
          </div>
          <button class="chip" aria-pressed="true">
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
      </footer>
    </div>
  );
}
