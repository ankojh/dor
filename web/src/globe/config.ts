/**
 * Tile provider configuration. OpenFreeMap is keyless; keep every provider
 * detail behind Vite env vars so a keyed provider can be swapped in through a
 * server-side proxy without touching browser code (plan §13).
 */
export type GlobeConfig = {
  /** Full style URL override. When set, the built labels-only style is skipped. */
  styleUrl: string;
  /** Vector tile TileJSON URL (OpenMapTiles schema). */
  tilesUrl: string;
  /** Glyph PBF template, must contain {fontstack} and {range}. */
  glyphsUrl: string;
  attribution: string;
};

export const DOR_ATTRIBUTION =
  "© OpenFreeMap · © OpenMapTiles · © OpenStreetMap contributors · GDELT";

export function loadGlobeConfig(): GlobeConfig {
  const env = import.meta.env as Record<string, string | undefined>;
  return {
    styleUrl: env.VITE_TILE_STYLE_URL ?? "",
    tilesUrl: env.VITE_TILES_URL ?? "https://tiles.openfreemap.org/planet",
    glyphsUrl: env.VITE_GLYPHS_URL ?? "https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf",
    attribution: DOR_ATTRIBUTION,
  };
}
