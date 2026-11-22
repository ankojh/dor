/**
 * Deterministic wire fixture for UI verification (`?wirefixture=1`).
 *
 * Dev-only: never selected by the production default. It sends one snapshot
 * with 120 shuffled historical rows (plus four timezone-shifted future rows)
 * and then a steady four-second stream so ordering, place labels, source
 * names, join context and the rolling-minute rate can be checked against
 * isolated rendered UI.
 */
import type { Delta, Link, Story, WireRow } from "../gen/dor/v1/dor_pb";
import type { FeedMessage } from "../worker/messages";

const CITIES = [
  { name: "Brussels", country: "BE", category: 2 },
  { name: "Singapore", country: "SG", category: 3 },
  { name: "Nairobi", country: "KE", category: 5 },
  { name: "Berlin", country: "DE", category: 2 },
  { name: "Taipei", country: "TW", category: 4 },
  { name: "Kyiv", country: "UA", category: 1 },
  { name: "São Paulo", country: "BR", category: 5 },
  { name: "Washington", country: "US", category: 2 },
  { name: "Tokyo", country: "JP", category: 6 },
  { name: "Rotterdam", country: "NL", category: 3 },
  { name: "Suez", country: "EG", category: 3 },
  { name: "Geneva", country: "CH", category: 3 },
];

const SOURCES = [
  "economictimes",
  "dw-world",
  "thehill",
  "thehindu-world",
  "sky-world",
  "gdelt-gkg",
  "usgs-earthquakes",
  "lemonde-en",
  "al-monitor",
  "theafricareport",
  "japantimes",
  "iol",
];

const TITLES = [
  "EU mulls tax on Big Tech to boost budget",
  "Chipmakers weigh response to new export rules",
  "Aftershock recorded off the coast",
  "Coalition talks resume after weekend break",
  "Heatwave warnings across the east coast",
  "Overnight strikes hit regional energy grid",
  "Cloud providers warn of GPU shortages",
  "Port strike extends, container backlog grows",
  "Canal transits slow amid congestion",
  "US and China agree trade deal after Geneva talks",
  "Floods displace thousands after week of rain",
  "Rice exporters pause new contracts",
];

const STORIES: Story[] = CITIES.map(
  (city, index) =>
    ({
      id: `fixture-story-${index}`,
      title: `${TITLES[index % TITLES.length]} · ${city.name}`,
      category: city.category,
      locations: [
        {
          placeId: `fixture-place-${index}`,
          name: city.name,
          lat: 0,
          lon: 0,
          precision: 2,
          role: 0,
          weight: 1,
        },
      ],
      primary: 0,
      sourceCount: 3 + (index % 8),
      heat: 0.4 + (index % 6) * 0.09,
      firstSeen: BigInt(Math.floor(Date.now() / 1000) - 7200),
      updatedAt: BigInt(Math.floor(Date.now() / 1000) - index * 11),
    }) as unknown as Story,
);

const LINKS: Link[] = [];

function row(index: number, at: number, future = false): WireRow {
  const city = CITIES[index % CITIES.length]!;
  const source = SOURCES[index % SOURCES.length]!;
  const story = STORIES[index % STORIES.length]!;
  const unlocated = index % 6 === 0;
  const countryOnly = !unlocated && index % 11 === 5;
  const joined = index % 3 === 0;
  const rebuilt = {
    articleId: `fixture-article-${future ? "future-" : ""}${index}`,
    storyId: index % 3 === 2 ? "" : story.id,
    title: TITLES[index % TITLES.length]!,
    source,
    place: unlocated ? city.name : countryOnly ? "" : city.name,
    category: city.category,
    at: BigInt(at),
    join: joined ? 1 : 0,
    promptScores: {},
    country: countryOnly ? city.country : unlocated ? "" : city.country,
    lang: "en",
    lat: unlocated ? 0 : 1,
    lon: unlocated ? 0 : 1,
    located: !unlocated,
    approxPrompts: [],
  } as unknown as WireRow & {
    degraded?: boolean;
    categoryBackend?: string;
    placeBackend?: string;
    placeDegraded?: boolean;
    categoryConfidence?: number;
  };
  if (index % 9 === 0) {
    rebuilt.degraded = true;
    rebuilt.categoryBackend = "rule";
    rebuilt.categoryConfidence = 0.62;
    rebuilt.placeDegraded = true;
    rebuilt.placeBackend = "hints";
  }
  return rebuilt;
}

/** Seeded Fisher–Yates so every run shows the same shuffled arrival order. */
function shuffle<T>(values: T[], seed: number): T[] {
  const out = [...values];
  let state = seed >>> 0;
  for (let i = out.length - 1; i > 0; i--) {
    state = (state * 1664525 + 1013904223) >>> 0;
    const j = state % (i + 1);
    [out[i], out[j]] = [out[j]!, out[i]!];
  }
  return out;
}

export function startWireFixture(onMessage: (message: FeedMessage) => void): { stop(): void } {
  const nowSec = Math.floor(Date.now() / 1000);
  const historical: WireRow[] = [];
  for (let i = 0; i < 120; i++) historical.push(row(i, nowSec - 10 - i * 12));
  const shuffled = shuffle(historical, 20261122);
  // Source reported local time as UTC: eight hours ahead. The wire must clamp
  // these instead of showing a future clock.
  const future = [0, 1, 2, 3].map((i) => row(120 + i, nowSec + 8 * 3600 - i * 30, true));

  onMessage({
    type: "snapshot",
    seq: 1,
    builtAt: nowSec,
    stories: STORIES,
    links: LINKS,
    recentWire: [...shuffled, ...future],
  });
  onMessage({
    type: "status",
    connection: "online",
    lastSeq: 1,
    lastWireAt: nowSec,
    reconnectInMs: null,
    health: { ok: true, sourcesOk: 12, sourcesTotal: 12, detail: "wire fixture" },
  });

  let article = 200;
  const timer = setInterval(() => {
    const at = Math.floor(Date.now() / 1000);
    const next = row(article, at);
    article++;
    const delta = {
      $typeName: "dor.v1.Delta",
      seq: BigInt(article),
      body: { case: "wire", value: next },
    } as Delta;
    onMessage({ type: "deltas", seq: article, deltas: [delta] });
  }, 4_000);

  return { stop: () => clearInterval(timer) };
}
