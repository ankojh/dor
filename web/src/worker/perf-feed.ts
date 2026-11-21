/** Opt-in production-path load fixture, seeded by the M0 fake world. */
import type { Delta, Link, Story, WireRow } from "../gen/dor/v1/dor_pb";
import type { FeedMessage } from "./messages";
import { FakeWorld } from "./fake/state";

export function startPerfFeed(post: (message: FeedMessage) => void): () => void {
  const now = Date.now();
  const world = new FakeWorld({ now });
  const init = world.initMessage(now);
  const stories = Array.from({ length: init.storyCount }, (_, i) => ({
    id: `perf-${i}`,
    title: init.stories.title[i]!,
    category: init.stories.category[i]!,
    locations: [
      {
        name: init.stories.place[i]!,
        lon: init.stories.lng[i]!,
        lat: init.stories.lat[i]!,
        precision: 2,
        role: 0,
      },
    ],
    primary: 0,
    sourceCount: init.stories.sources[i]!,
    heat: init.stories.heat[i]!,
    firstSeen: BigInt(Math.floor(now / 1000) - 3600),
    updatedAt: BigInt(Math.floor(now / 1000)),
  })) as unknown as Story[];
  const links = Array.from({ length: init.linkCount }, (_, i) => ({
    a: `perf-${init.links.a[i]}`,
    b: `perf-${init.links.b[i]}`,
    type: init.links.type[i]!,
    strength: init.links.strength[i]!,
    dir: 0,
    confidence: 0.8,
    updatedAt: BigInt(Math.floor(now / 1000)),
  })) as unknown as Link[];
  let seq = 1;
  post({ type: "snapshot", seq, builtAt: now, stories, links });
  post({
    type: "status",
    connection: "online",
    lastSeq: seq,
    lastWireAt: Math.floor(now / 1000),
    reconnectInMs: null,
    health: { ok: true, sourcesOk: 1, sourcesTotal: 1, detail: "50/s performance fixture" },
  });
  let last = now;
  let lastBurst = now;
  const timer = setInterval(() => {
    const at = Date.now();
    world.advance(at, at - last);
    last = at;
    const patch = world.flush(at);
    if (!patch) return;
    const deltas: Delta[] = [];
    for (let i = 0; i < patch.storyIndex.length; i++) {
      const index = patch.storyIndex[i]!,
        story = stories[index]!;
      story.sourceCount = patch.sources[i]!;
      story.heat = patch.heat[i]!;
      story.updatedAt = BigInt(Math.floor(at / 1000));
      deltas.push({ seq: BigInt(++seq), body: { case: "storyUpsert", value: story } } as Delta);
      const row = {
        articleId: `perf-article-${seq}`,
        storyId: story.id,
        at: BigInt(Math.floor(at / 1000)),
        source: "PERF",
        title: story.title,
        category: story.category,
        place: story.locations[0]!.name,
        country: "",
        lang: "en",
        located: true,
        lat: story.locations[0]!.lat,
        lon: story.locations[0]!.lon,
        promptScores: {},
        approxPrompts: [],
        join: 0,
      } as unknown as WireRow;
      deltas.push({ seq: BigInt(++seq), body: { case: "wire", value: row } } as Delta);
    }
    for (let i = 0; i < patch.linkIndex.length; i++) {
      const link = links[patch.linkIndex[i]!]!;
      link.strength = patch.strength[i]!;
      deltas.push({ seq: BigInt(++seq), body: { case: "linkUpsert", value: link } } as Delta);
    }
    // Exercise production slot reuse and incident arcs while the camera moves.
    if (at - lastBurst >= 10000) {
      lastBurst = at;
      for (const story of stories.slice(-5)) {
        deltas.push({
          seq: BigInt(++seq),
          body: { case: "storyExpire", value: story.id },
        } as Delta);
        deltas.push({ seq: BigInt(++seq), body: { case: "storyUpsert", value: story } } as Delta);
      }
      for (const link of links)
        if (stories.slice(-5).some((s) => s.id === link.a || s.id === link.b)) {
          deltas.push({ seq: BigInt(++seq), body: { case: "linkUpsert", value: link } } as Delta);
        }
      const link = links[0]!;
      deltas.push({
        seq: BigInt(++seq),
        body: { case: "linkRemove", value: { a: link.a, b: link.b } },
      } as Delta);
      deltas.push({ seq: BigInt(++seq), body: { case: "linkUpsert", value: link } } as Delta);
    }
    if (deltas.length) post({ type: "deltas", seq, deltas });
  }, 20);
  return () => clearInterval(timer);
}
