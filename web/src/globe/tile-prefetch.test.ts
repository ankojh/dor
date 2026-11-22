import { describe, expect, it } from "vitest";
import {
  glyphRangeUrls,
  resolveTileTemplate,
  TilePrefetcher,
  tileUrlsAround,
  tileX,
  tileY,
} from "./tile-prefetch";

function tiles(urls: string[], template: string): Array<{ z: number; x: number; y: number }> {
  // Template uses the same placeholders as tileUrlsAround; parse by removing
  // the fixed prefix and suffix around "{z}/{x}/{y}".
  const marker = template.indexOf("{z}");
  const prefix = template.slice(0, marker);
  return urls.map((url) => {
    const rest = url.slice(prefix.length);
    const match = /^(\d+)\/(\d+)\/(\d+)/.exec(rest);
    if (!match) throw new Error(`unexpected tile URL: ${url}`);
    return { z: Number(match[1]), x: Number(match[2]), y: Number(match[3]) };
  });
}

describe("tileX / tileY", () => {
  it("maps lon 0, lat 0 to the centre tile at z1", () => {
    expect(tileX(0, 1)).toBe(1);
    expect(tileY(0, 1)).toBe(1);
  });

  it("matches the known z10 tile for London", () => {
    expect(tileX(-0.1276, 10)).toBe(511);
    expect(tileY(51.5072, 10)).toBe(340);
  });

  it("stays finite at extreme latitudes", () => {
    for (const lat of [-90, -89, 0, 89, 90]) {
      expect(Number.isFinite(tileY(lat, 3))).toBe(true);
    }
    for (const lat of [-85, -45, 0, 45, 85]) {
      const y = tileY(lat, 3);
      expect(y).toBeGreaterThanOrEqual(0);
      expect(y).toBeLessThanOrEqual(7);
    }
    // The exact ±90 edge can land on -1/max+1; tileUrlsAround is what
    // guarantees the emitted URLs stay in range (covered below).
  });
});

describe("tileUrlsAround", () => {
  it("returns at most 9 URLs", () => {
    const urls = tileUrlsAround("https://tiles/{z}/{x}/{y}.pbf", 13.405, 52.52, 5, 1);
    expect(urls.length).toBeLessThanOrEqual(9);
    expect(urls.length).toBe(9);
  });

  it("never produces out-of-range tiles at the antimeridian", () => {
    const template = "https://tiles/{z}/{x}/{y}.pbf";
    for (const lon of [-180, -179.999, 179.999, 180]) {
      const urls = tileUrlsAround(template, lon, 0, 2, 1);
      expect(urls.length).toBeGreaterThan(0);
      expect(urls.length).toBeLessThanOrEqual(9);
      for (const tile of tiles(urls, template)) {
        expect(tile.z).toBe(2);
        expect(tile.x).toBeGreaterThanOrEqual(0);
        expect(tile.x).toBeLessThanOrEqual(3);
        expect(tile.y).toBeGreaterThanOrEqual(0);
        expect(tile.y).toBeLessThanOrEqual(3);
      }
    }
  });

  it("clamps at the poles too", () => {
    const template = "https://tiles/{z}/{x}/{y}.pbf";
    for (const lat of [-89, 89]) {
      for (const tile of tiles(tileUrlsAround(template, 0, lat, 1, 1), template)) {
        expect(tile.y).toBeGreaterThanOrEqual(0);
        expect(tile.y).toBeLessThanOrEqual(1);
      }
    }
  });

  it("substitutes z/x/y and removes {ratio}/{r}", () => {
    const template = "https://tiles.example/{z}/{x}/{y}{ratio}{r}.pbf";
    const urls = tileUrlsAround(template, 0, 0, 1, 0);
    expect(urls).toEqual(["https://tiles.example/1/1/1.pbf"]);

    const around = tileUrlsAround(template, 0, 0, 1, 1);
    for (const url of around) {
      expect(url).not.toContain("{");
      expect(url).toMatch(/^https:\/\/tiles\.example\/1\/[01]\/[01]\.pbf$/);
    }
  });
});

describe("glyphRangeUrls", () => {
  it("encodes the fontstack and substitutes both ranges", () => {
    const urls = glyphRangeUrls(
      "https://glyphs/{fontstack}/{range}.pbf",
      ["Noto Sans Regular"],
      ["0-255", "256-511"],
    );
    expect(urls).toEqual([
      "https://glyphs/Noto%20Sans%20Regular/0-255.pbf",
      "https://glyphs/Noto%20Sans%20Regular/256-511.pbf",
    ]);
  });

  it("crosses every font with every range", () => {
    const urls = glyphRangeUrls("https://glyphs/{fontstack}/{range}.pbf", ["A B", "C"], ["0-255"]);
    expect(urls).toEqual(["https://glyphs/A%20B/0-255.pbf", "https://glyphs/C/0-255.pbf"]);
  });
});

describe("resolveTileTemplate", () => {
  it("keeps {z}/{x}/{y} literal after resolving a relative TileJSON template", () => {
    expect(resolveTileTemplate("planet/{z}/{x}/{y}.pbf", "https://tiles.example/tiles.json")).toBe(
      "https://tiles.example/planet/{z}/{x}/{y}.pbf",
    );
    expect(
      resolveTileTemplate("https://cdn.example/{z}/{x}/{y}.pbf", "https://tiles.example/"),
    ).toBe("https://cdn.example/{z}/{x}/{y}.pbf");
  });

  it("restores tokens that URL resolution percent-encoded", () => {
    expect(
      resolveTileTemplate("https://cdn.example/%7Bz%7D/%7Bx%7D/%7By%7D.pbf", "https://x/"),
    ).toBe("https://cdn.example/{z}/{x}/{y}.pbf");
  });
});

describe("TilePrefetcher full path", () => {
  it("resolves TileJSON and fetches numeric tile URLs, not encoded placeholders", async () => {
    const calls: string[] = [];
    const original = globalThis.fetch;
    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
      calls.push(url);
      if (url.endsWith("tiles.json")) {
        return new Response(
          JSON.stringify({
            tiles: ["https://cdn.example/planet/{z}/{x}/{y}.pbf"],
            minzoom: 0,
            maxzoom: 14,
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }
      return new Response(new Uint8Array([1]), { status: 200 });
    }) as typeof fetch;
    try {
      const prefetcher = new TilePrefetcher("https://tiles.example/tiles.json");
      prefetcher.prefetch([13.4, 52.5], 11);
      await new Promise((resolve) => setTimeout(resolve, 250));
      expect(calls[0]).toBe("https://tiles.example/tiles.json");
      const tileCalls = calls.filter((url) => url.includes("/planet/"));
      expect(tileCalls.length).toBeGreaterThan(0);
      expect(tileCalls.every((url) => /\/(?:11|12)\/\d+\/\d+\.pbf$/.test(url))).toBe(true);
      expect(tileCalls.some((url) => url.includes("%7B") || url.includes("{"))).toBe(false);
    } finally {
      globalThis.fetch = original;
    }
  });
});
