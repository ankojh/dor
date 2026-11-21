import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { fromBinary } from "@bufbuild/protobuf";
import type { Message } from "@bufbuild/protobuf";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";
import {
  AckSchema,
  AlertHit,
  AlertHitSchema,
  ArticleSchema,
  Category,
  ClientMessageSchema,
  DeltaBatchSchema,
  DeltaSchema,
  Direction,
  FocusSchema,
  GetTextSchema,
  HelloSchema,
  JoinKind,
  Link,
  LinkKey,
  LinkKeySchema,
  LinkSchema,
  LinkType,
  MatchScoreSchema,
  Precision,
  RawArticleSchema,
  RecordFrameSchema,
  ResyncSchema,
  Role,
  ServerMessageSchema,
  SetFiltersSchema,
  SetPromptsSchema,
  SnapshotSchema,
  SourceKind,
  Story,
  StorySchema,
  WireRow,
  WireRowSchema,
  type Delta,
} from "../gen/dor/v1/dor_pb";

function load<T extends Message>(schema: GenMessage<T>, name: string): T {
  const bytes = readFileSync(new URL(`../../../proto/testdata/${name}.bin`, import.meta.url));
  return fromBinary(schema, bytes);
}

function expectLocation(actual: Story["locations"][number]): void {
  expect(actual.placeId).toBe("place-fr");
  expect(actual.name).toBe("France");
  expect(actual.lat).toBe(46.5);
  expect(actual.lon).toBe(2.25);
  expect(actual.precision).toBe(Precision.COUNTRY);
  expect(actual.role).toBe(Role.PARTY);
  expect(actual.weight).toBe(0.75);
}

function expectStory(story: Story): void {
  expect(story.id).toBe("story-1");
  expect(story.title).toBe("Flooding in Paris");
  expect(story.category).toBe(Category.CLIMATE);
  expect(story.locations).toHaveLength(2);
  const first = story.locations[0];
  const second = story.locations[1];
  if (!first || !second) {
    throw new Error("expected two locations");
  }
  expectLocation(first);
  expect(second.placeId).toBe("place-paris");
  expect(second.name).toBe("Paris");
  expect(second.lat).toBe(48.5);
  expect(second.lon).toBe(2.25);
  expect(second.precision).toBe(Precision.CITY);
  expect(second.role).toBe(Role.VENUE);
  expect(second.weight).toBe(1);
  expect(story.primary).toBe(1);
  expect(story.sourceCount).toBe(3);
  expect(story.heat).toBe(0.875);
  expect(story.firstSeen).toBe(1_700_000_000n);
  expect(story.updatedAt).toBe(1_700_000_123n);
}

function expectLink(link: Link): void {
  expect(link.a).toBe("story-1");
  expect(link.b).toBe("story-2");
  expect(link.type).toBe(LinkType.REACTION);
  expect(link.dir).toBe(Direction.B_TO_A);
  expect(link.confidence).toBe(0.875);
  expect(link.strength).toBe(0.625);
  expect(link.updatedAt).toBe(1_700_000_200n);
  expect(link.unverified).toBe(true);
}

function expectLinkKey(key: LinkKey): void {
  expect(key.a).toBe("story-1");
  expect(key.b).toBe("story-2");
}

function expectWireRow(row: WireRow): void {
  expect(row.articleId).toBe("art-1");
  expect(row.storyId).toBe("story-1");
  expect(row.title).toBe("Flooding «Paris»");
  expect(row.source).toBe("BBC");
  expect(row.place).toBe("Paris, France");
  expect(row.category).toBe(Category.ECONOMY);
  expect(row.at).toBe(1_700_000_400n);
  expect(row.join).toBe(JoinKind.JOINED);
  expect(row.promptScores).toEqual({ "prompt-a": 0.5, "prompt-b": 0.25 });
}

function expectAlert(alert: AlertHit): void {
  expect(alert.alertId).toBe("alert-1");
  expect(alert.storyId).toBe("story-1");
  expect(alert.level).toBe("escalated");
  expect(alert.firedAt).toBe(1_700_000_300n);
  expect(alert.title).toBe("Flooding in Paris");
  expect(alert.prompt).toBe("Tell me about floods");
}

function deltaBody(delta: Delta, expected: string): Delta["body"] {
  expect(delta.body.case).toBe(expected);
  return delta.body;
}

describe("Go golden protobuf binaries decode in TypeScript", () => {
  it("Article", () => {
    const article = load(ArticleSchema, "article");
    expect(article.id).toBe("art-1");
    expect(article.sourceId).toBe("src-bbc");
    expect(article.url).toBe("https://example.com/a?utm_source=test");
    expect(article.title).toBe("Flooding «Paris»");
    expect(article.lede).toBe("Lede with emoji 🌍 and ünïcødé.");
    expect(article.publishedAt).toBe(1_700_000_000n);
    expect(article.lang).toBe("en");
    expect(article.dateline).toBe("PARIS, Nov 20");
    expect(article.storyId).toBe("story-1");
    expect(article.join).toBe(JoinKind.JOINED);
  });

  it("Story, including every location field", () => {
    expectStory(load(StorySchema, "story"));
  });

  it("RawArticle, including every hint field", () => {
    const raw = load(RawArticleSchema, "raw_article");
    expect(raw.id).toBe("raw:0123456789abcdef");
    expect(raw.sourceId).toBe("bbc-world");
    expect(raw.kind).toBe(SourceKind.RSS);
    expect(raw.url).toBe("https://example.com/b/world/flooding-in-paris");
    expect(raw.title).toBe("Flooding «Paris»");
    expect(raw.summary).toBe("Short snippet with emoji 🌍 and ünïcødé.");
    expect(raw.publishedAt).toBe(1_700_000_000n);
    expect(raw.fetchedAt).toBe(1_700_000_030n);
    expect(raw.lang).toBe("en");
    expect(raw.burst).toBe(true);
    const hints = raw.hints;
    if (!hints) throw new Error("missing hints");
    expect(hints.located).toBe(true);
    expect(hints.lat).toBe(48.5);
    expect(hints.lon).toBe(2.25);
    expect(hints.precision).toBe(Precision.CITY);
    expect(hints.themes).toEqual(["PROTEST", "ECON_ENERGY"]);
    expect(hints.country).toBe("FR");
    expect(hints.hazard).toBe("flood");
    expect(hints.magnitude).toBe(5.4);
    expect(hints.severity).toBe(0.75);
    expect(hints.eventId).toBe("us7000abcd");
    expect(hints.magnitudeUnit).toBe("Mw");
    expect(hints.place).toBe("Paris, France");
    expect(hints.revision).toBe("1700000030");
  });

  it("MatchScore", () => {
    const score = load(MatchScoreSchema, "match_score");
    expect(score.articleId).toBe("art-1");
    expect(Object.keys(score.scores)).toEqual(["floods"]);
    expect(score.scores.floods).toBeCloseTo(0.82, 5);
    expect(score.approx).toEqual(["energy"]);
    expect(score.updatedAt).toBe(1_700_000_500n);
  });

  it("RecordFrame", () => {
    const frame = load(RecordFrameSchema, "record_frame");
    expect(frame.subject).toBe("article.raw.priority");
    expect(frame.msgId).toBe("raw:0123456789abcdef");
    expect(frame.receivedAtMs).toBe(1_700_000_030_123n);
    expect(Array.from(frame.payload)).toEqual([1, 2, 3]);
  });

  it("Link", () => {
    expectLink(load(LinkSchema, "link"));
  });

  it("LinkKey", () => {
    expectLinkKey(load(LinkKeySchema, "link_key"));
  });

  it("AlertHit", () => {
    expectAlert(load(AlertHitSchema, "alert_hit"));
  });

  it("WireRow, including the prompt score map", () => {
    expectWireRow(load(WireRowSchema, "wire_row"));
  });

  it("Delta with a story upsert", () => {
    const delta = load(DeltaSchema, "delta_story");
    expect(delta.seq).toBe(42n);
    const body = deltaBody(delta, "storyUpsert");
    if (body.case !== "storyUpsert") throw new Error("wrong case");
    expectStory(body.value);
  });

  it("Delta with story_expire", () => {
    const delta = load(DeltaSchema, "delta_story_expire");
    expect(delta.seq).toBe(44n);
    const body = deltaBody(delta, "storyExpire");
    if (body.case !== "storyExpire") throw new Error("wrong case");
    expect(body.value).toBe("story-9");
  });

  it("Delta with a link upsert", () => {
    const delta = load(DeltaSchema, "delta_link");
    expect(delta.seq).toBe(45n);
    const body = deltaBody(delta, "linkUpsert");
    if (body.case !== "linkUpsert") throw new Error("wrong case");
    expectLink(body.value);
  });

  it("Delta with a link removal", () => {
    const delta = load(DeltaSchema, "delta_link_remove");
    expect(delta.seq).toBe(46n);
    const body = deltaBody(delta, "linkRemove");
    if (body.case !== "linkRemove") throw new Error("wrong case");
    expectLinkKey(body.value);
  });

  it("Delta with a wire row", () => {
    const delta = load(DeltaSchema, "delta_wire");
    expect(delta.seq).toBe(47n);
    const body = deltaBody(delta, "wire");
    if (body.case !== "wire") throw new Error("wrong case");
    expectWireRow(body.value);
  });

  it("Delta with an alert", () => {
    const delta = load(DeltaSchema, "delta_alert");
    expect(delta.seq).toBe(48n);
    const body = deltaBody(delta, "alert");
    if (body.case !== "alert") throw new Error("wrong case");
    expectAlert(body.value);
  });

  it("Snapshot", () => {
    const snapshot = load(SnapshotSchema, "snapshot");
    expect(snapshot.seq).toBe(42n);
    expect(snapshot.builtAt).toBe(1_700_000_400n);
    expect(snapshot.stories).toHaveLength(1);
    expect(snapshot.links).toHaveLength(1);
    expect(snapshot.recentWire).toHaveLength(1);
    const recent = snapshot.recentWire[0];
    if (!recent) throw new Error("missing recent wire row");
    expectWireRow(recent);
    const story = snapshot.stories[0];
    const link = snapshot.links[0];
    if (!story || !link) throw new Error("missing snapshot entities");
    expectStory(story);
    expectLink(link);
  });

  it("DeltaBatch", () => {
    const batch = load(DeltaBatchSchema, "delta_batch");
    expect(batch.deltas).toHaveLength(2);
    const first = batch.deltas[0];
    const second = batch.deltas[1];
    if (!first || !second) throw new Error("missing deltas");
    expect(first.seq).toBe(42n);
    expect(deltaBody(first, "storyUpsert").case).toBe("storyUpsert");
    expect(second.seq).toBe(43n);
    const body = deltaBody(second, "wire");
    if (body.case !== "wire") throw new Error("wrong case");
    expectWireRow(body.value);
  });

  it("Resync", () => {
    const resync = load(ResyncSchema, "resync");
    expect(resync.seq).toBe(43n);
    expect(resync.reason).toBe("client behind");
  });

  it("ServerMessage envelopes", () => {
    const snapshot = load(ServerMessageSchema, "server_snapshot");
    expect(snapshot.body.case).toBe("snapshot");
    if (snapshot.body.case !== "snapshot") throw new Error("wrong case");
    expect(snapshot.body.value.seq).toBe(42n);

    const deltas = load(ServerMessageSchema, "server_deltas");
    expect(deltas.body.case).toBe("deltas");
    if (deltas.body.case !== "deltas") throw new Error("wrong case");
    expect(deltas.body.value.deltas).toHaveLength(2);

    const resync = load(ServerMessageSchema, "server_resync");
    expect(resync.body.case).toBe("resync");
    if (resync.body.case !== "resync") throw new Error("wrong case");
    expect(resync.body.value.reason).toBe("client behind");
  });

  it("Hello", () => {
    const hello = load(HelloSchema, "hello");
    expect(hello.lastSeq).toBe(42n);
    expect(hello.clientVersion).toBe("web-0.1.0-test");
  });

  it("SetFilters", () => {
    const filters = load(SetFiltersSchema, "set_filters");
    expect(filters.categories).toEqual([Category.CLIMATE, Category.ECONOMY]);
    expect(filters.regions).toEqual(["eu-west", "fr"]);
    expect(filters.minSources).toBe(2);
    expect(filters.lang).toEqual(["en", "fr"]);
    expect(filters.sources).toEqual(["bbc-world", "npr-news"]);
    expect(filters.lat).toBe(48.5);
    expect(filters.lon).toBe(2.25);
    expect(filters.radiusKm).toBe(250);
  });

  it("SetPrompts", () => {
    expect(load(SetPromptsSchema, "set_prompts").prompts).toEqual(["floods", "energy"]);
  });

  it("Focus", () => {
    expect(load(FocusSchema, "focus").storyId).toBe("story-1");
  });

  it("GetText", () => {
    const getText = load(GetTextSchema, "get_text");
    expect(getText.storyIds).toEqual(["story-1", "story-2"]);
    expect(getText.linkKeys).toHaveLength(1);
    const key = getText.linkKeys[0];
    if (!key) throw new Error("missing link key");
    expectLinkKey(key);
  });

  it("Ack", () => {
    expect(load(AckSchema, "ack").seq).toBe(42n);
  });

  it("ClientMessage envelopes", () => {
    const hello = load(ClientMessageSchema, "client_hello");
    expect(hello.body.case).toBe("hello");
    if (hello.body.case !== "hello") throw new Error("wrong case");
    expect(hello.body.value.clientVersion).toBe("web-0.1.0-test");

    const filters = load(ClientMessageSchema, "client_set_filters");
    expect(filters.body.case).toBe("setFilters");
    if (filters.body.case !== "setFilters") throw new Error("wrong case");
    expect(filters.body.value.minSources).toBe(2);

    const prompts = load(ClientMessageSchema, "client_set_prompts");
    expect(prompts.body.case).toBe("setPrompts");
    if (prompts.body.case !== "setPrompts") throw new Error("wrong case");
    expect(prompts.body.value.prompts).toEqual(["floods", "energy"]);

    const focus = load(ClientMessageSchema, "client_focus");
    expect(focus.body.case).toBe("focus");
    if (focus.body.case !== "focus") throw new Error("wrong case");
    expect(focus.body.value.storyId).toBe("story-1");

    const getText = load(ClientMessageSchema, "client_get_text");
    expect(getText.body.case).toBe("getText");
    if (getText.body.case !== "getText") throw new Error("wrong case");
    expect(getText.body.value.storyIds).toEqual(["story-1", "story-2"]);

    const ack = load(ClientMessageSchema, "client_ack");
    expect(ack.body.case).toBe("ack");
    if (ack.body.case !== "ack") throw new Error("wrong case");
    expect(ack.body.value.seq).toBe(42n);
  });
});
