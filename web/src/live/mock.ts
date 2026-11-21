/**
 * Explicit dev-only feed (`?mock=1`). Never used by the production default:
 * the real app only reads the gateway worker.
 */
import type { Delta, Link, Story, WireRow } from "../gen/dor/v1/dor_pb";
import type { WorkerMessage } from "../worker/messages";

const CITIES: Array<{ name: string; lon: number; lat: number; category: number }> = [
  { name: "Rotterdam", lon: 4.4777, lat: 51.9244, category: 3 },
  { name: "Singapore", lon: 103.8198, lat: 1.3521, category: 3 },
  { name: "Suez", lon: 32.5498, lat: 29.9668, category: 3 },
  { name: "Dubai", lon: 55.2708, lat: 25.2048, category: 3 },
  { name: "Geneva", lon: 6.1432, lat: 46.2044, category: 2 },
  { name: "New York", lon: -74.006, lat: 40.7128, category: 3 },
  { name: "Taipei", lon: 121.5654, lat: 25.033, category: 4 },
  { name: "Washington", lon: -77.0369, lat: 38.9072, category: 2 },
  { name: "Kyiv", lon: 30.5234, lat: 50.4501, category: 1 },
  { name: "Tokyo", lon: 139.6917, lat: 35.6895, category: 6 },
  { name: "São Paulo", lon: -46.6333, lat: -23.5505, category: 5 },
  { name: "Nairobi", lon: 36.8219, lat: -1.2921, category: 5 },
];

const TITLES = [
  "Asia–Europe freight rates climb for a second week",
  "Chipmakers weigh response to new export rules",
  "Aftershock recorded off the coast",
  "Coalition talks resume after weekend break",
  "Heatwave warnings across the east coast",
  "Overnight strikes hit regional energy grid",
  "Cloud providers warn of GPU shortages",
  "Port strike extends, container backlog grows",
  "Canal transits slow amid congestion",
  "US and China agree trade deal after Geneva talks",
];

function unix(): number {
  return Math.floor(Date.now() / 1000);
}

function buildStories(): Story[] {
  return CITIES.map((city, index) => ({
    id: `mock-story-${index}`,
    title: `${TITLES[index % TITLES.length]} · ${city.name}`,
    category: city.category,
    locations: [
      {
        placeId: `mock-place-${index}`,
        name: city.name,
        lat: city.lat,
        lon: city.lon,
        precision: 2,
        role: 0,
        weight: 1,
      },
      ...(index % 4 === 0
        ? [
            {
              placeId: `mock-party-${index}`,
              name: index % 8 === 0 ? "United States" : "China",
              lat: index % 8 === 0 ? 38.9 : 35.8,
              lon: index % 8 === 0 ? -77.0 : 104.1,
              precision: 0,
              role: 2,
              weight: 0.8,
            },
          ]
        : []),
    ],
    primary: 0,
    sourceCount: 4 + (index % 9) * 3,
    heat: 0.45 + (index % 7) * 0.08,
    firstSeen: BigInt(unix() - 3600),
    updatedAt: BigInt(unix() - index * 7),
  })) as unknown as Story[];
}

function buildLinks(): Link[] {
  const links: Link[] = [];
  for (let i = 0; i < CITIES.length; i++) {
    for (let j = i + 1; j < CITIES.length; j++) {
      if ((i * 7 + j * 3) % 4 !== 0) continue;
      links.push({
        a: `mock-story-${i}`,
        b: `mock-story-${j}`,
        type: (i + j) % 3,
        dir: 0,
        confidence: 0.7,
        strength: 0.45 + ((i + j) % 5) * 0.1,
        updatedAt: BigInt(unix()),
      } as unknown as Link);
    }
  }
  return links;
}

export function startMock(onMessage: (message: WorkerMessage) => void): { stop(): void } {
  const stories = buildStories();
  const links = buildLinks();
  let seq = 1;
  let article = 0;
  const timer = setInterval(() => {
    const city = CITIES[article % CITIES.length]!;
    const story = stories[article % stories.length]!;
    const isGdelt = article % 17 === 0;
    const rows: WireRow[] = [];
    const count = isGdelt ? 3 : 1;
    for (let n = 0; n < count; n++) {
      const row = {
        articleId: `mock-article-${article}-${n}`,
        storyId: story.id,
        title: `${TITLES[(article + n) % TITLES.length]}`,
        source: isGdelt ? "GDELT" : article % 2 === 0 ? "RSS" : "USGS",
        place: city.name,
        category: city.category,
        at: BigInt(unix()),
        join: article % 3 === 0 ? 0 : 1,
        promptScores: {},
      } as unknown as WireRow;
      rows.push(row);
    }
    const deltas: Delta[] = rows.map((row) => ({
      seq: BigInt(seq++),
      body: { case: "wire", value: row },
    })) as unknown as Delta[];
    onMessage({ type: "deltas", seq, deltas });
    onMessage({
      type: "status",
      connection: "online",
      lastSeq: seq,
      lastWireAt: unix(),
      reconnectInMs: null,
      health: { ok: true, sourcesOk: 6, sourcesTotal: 6, detail: "mock dev feed" },
    });
    article++;
  }, 40);
  onMessage({
    type: "snapshot",
    seq: 1,
    builtAt: unix(),
    stories,
    links,
  });
  onMessage({
    type: "status",
    connection: "online",
    lastSeq: 1,
    lastWireAt: null,
    reconnectInMs: null,
    health: { ok: true, sourcesOk: 6, sourcesTotal: 6, detail: "mock dev feed" },
  });
  return {
    stop: () => clearInterval(timer),
  };
}
