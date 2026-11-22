/**
 * Tile and glyph warming for the zoom rail. MapLibre already streams what it
 * renders; this fetches the next band's neighbouring vector tiles and the
 * glyph ranges our labels use, so names appear immediately on arrival.
 *
 * Everything here is best-effort: a failed prefetch must never affect the
 * map, and responses land in the browser's HTTP cache rather than our own.
 */

export type TileTemplate = {
  tiles: string[];
  minzoom?: number;
  maxzoom?: number;
};

export function tileX(lon: number, zoom: number): number {
  const n = 2 ** Math.floor(zoom);
  const wrapped = ((((lon + 180) % 360) + 360) % 360) - 180;
  return Math.max(0, Math.min(n - 1, Math.floor(((wrapped + 180) / 360) * n)));
}

export function tileY(lat: number, zoom: number): number {
  const n = 2 ** Math.floor(zoom);
  const radians = (Math.max(-85.05112878, Math.min(85.05112878, lat)) * Math.PI) / 180;
  const mercator = Math.log(Math.tan(radians) + 1 / Math.cos(radians));
  return Math.max(0, Math.min(n - 1, Math.floor(((1 - mercator / Math.PI) / 2) * n)));
}

/** Neighbouring tile URLs at one integer zoom, clamped at the world edges. */
export function tileUrlsAround(
  template: string,
  lon: number,
  lat: number,
  zoom: number,
  radius = 1,
): string[] {
  const z = Math.max(0, Math.floor(zoom));
  const max = 2 ** z - 1;
  const x = tileX(lon, z);
  const y = tileY(lat, z);
  const urls: string[] = [];
  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const tx = x + dx;
      const ty = y + dy;
      if (tx < 0 || ty < 0 || tx > max || ty > max) continue;
      urls.push(
        template
          .replace("{z}", String(z))
          .replace("{x}", String(tx))
          .replace("{y}", String(ty))
          .replace("{ratio}", "")
          .replace("{r}", ""),
      );
    }
  }
  return urls;
}

/** Resolve a TileJSON template without losing its {z}/{x}/{y} placeholders. */
export function resolveTileTemplate(template: string, base: string): string {
  return new URL(template, base)
    .toString()
    .replace(/%7B(z|x|y|ratio|r)%7D/gi, (_, token: string) => `{${token.toLowerCase()}}`);
}

export function glyphRangeUrls(template: string, fonts: string[], ranges: string[]): string[] {
  const urls: string[] = [];
  for (const font of fonts) {
    for (const range of ranges) {
      urls.push(
        template.replace("{fontstack}", encodeURIComponent(font)).replace("{range}", range),
      );
    }
  }
  return urls;
}

const warmed = new Set<string>();

async function warm(urls: string[]): Promise<void> {
  for (const url of urls) {
    if (warmed.has(url)) continue;
    warmed.add(url);
    try {
      await fetch(url, { cache: "force-cache", credentials: "omit" });
    } catch {
      /* the map's own loader remains the source of truth */
    }
  }
}

export class TilePrefetcher {
  private template: string | null = null;
  private maxZoom = 14;
  private loading: Promise<void> | null = null;
  private lastCenter: [number, number] | null = null;
  private lastZoom = 0;

  constructor(private readonly tilejsonUrl: string) {}

  /** Warm glyph ranges used by our labels. Safe to call before the map loads. */
  preloadGlyphs(glyphsTemplate: string): void {
    void warm(glyphRangeUrls(glyphsTemplate, ["Noto Sans Regular"], ["0-255", "256-511"]));
  }

  /** Resolve the TileJSON template once, then prefetch around a view. */
  prefetch(center: [number, number], zoom: number, radius = 1): void {
    if (this.loading) return;
    this.lastCenter = center;
    this.lastZoom = zoom;
    this.loading = this.resolveTemplate()
      .then(() => this.warmAround(center, zoom, radius))
      .catch(() => undefined)
      .finally(() => {
        this.loading = null;
        const latestCenter = this.lastCenter;
        const latestZoom = this.lastZoom;
        if (latestCenter && (latestZoom !== zoom || latestCenter !== center)) {
          this.prefetch(latestCenter, latestZoom, radius);
        }
      });
  }

  private async resolveTemplate(): Promise<void> {
    if (this.template) return;
    const response = await fetch(this.tilejsonUrl, { cache: "force-cache", credentials: "omit" });
    if (!response.ok) return;
    const body = (await response.json()) as TileTemplate;
    const template = body.tiles?.[0];
    if (!template) return;
    this.template = resolveTileTemplate(template, this.tilejsonUrl);
    this.maxZoom = body.maxzoom ?? 14;
  }

  private async warmAround(center: [number, number], zoom: number, radius: number): Promise<void> {
    if (!this.template) return;
    const targets = [Math.floor(zoom), Math.min(this.maxZoom, Math.floor(zoom) + 1)];
    const urls = new Set<string>();
    for (const target of new Set(targets)) {
      for (const url of tileUrlsAround(this.template, center[0], center[1], target, radius)) {
        urls.add(url);
      }
    }
    await warm([...urls]);
  }
}
