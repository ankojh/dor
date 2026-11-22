import type { GeoJSONSource, Map as MapLibreMap } from "maplibre-gl";
import { categoryColor, categoryLabel } from "./encoding";
import type { HudFocus } from "../worker/fake/protocol";

export const STORY_LABEL_SOURCE = "dor-story-labels";
export const STORY_LABEL_LAYER = "dor-story-labels";

type LabelFeature = {
  type: "Feature";
  geometry: { type: "Point"; coordinates: [number, number] };
  properties: { label: string; color: string };
};

function hex(rgb: readonly [number, number, number]): string {
  return `#${rgb.map((value) => value.toString(16).padStart(2, "0")).join("")}`;
}

export function storyLabel(category: number, place: string): string {
  const compact = place.split(",")[0]?.trim() || place;
  return `${categoryLabel(category).toUpperCase()} · ${compact.toUpperCase()}`;
}

export type AmbientLabel = { lng: number; lat: number; category: number; place: string };

/**
 * Story names drawn with MapLibre's own glyphs and collision placement, so
 * they follow the globe and stay readable at street zoom. Only the focused
 * thread (or a dozen hot ambient stories) is in the source, so updates are a
 * handful of features and never a per-frame DOM change.
 */
export class StoryLabels {
  private readonly map: MapLibreMap;
  private ready = false;

  constructor(map: MapLibreMap) {
    this.map = map;
  }

  add(): void {
    if (this.ready) return;
    if (!this.map.getSource(STORY_LABEL_SOURCE)) {
      this.map.addSource(STORY_LABEL_SOURCE, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
    }
    if (!this.map.getLayer(STORY_LABEL_LAYER)) {
      this.map.addLayer({
        id: STORY_LABEL_LAYER,
        type: "symbol",
        source: STORY_LABEL_SOURCE,
        layout: {
          "text-field": ["get", "label"],
          "text-font": ["Noto Sans Regular"],
          "text-size": ["interpolate", ["linear"], ["zoom"], 0, 10, 8, 11, 16, 12.5],
          "text-letter-spacing": 0.12,
          "text-transform": "uppercase",
          "text-allow-overlap": false,
          "text-optional": true,
          "text-padding": 4,
          "text-variable-anchor": ["top", "bottom", "left", "right"],
          "text-radial-offset": 1.1,
          "text-max-width": 14,
        },
        paint: {
          "text-color": ["get", "color"],
          "text-halo-color": "rgba(3,5,11,0.92)",
          "text-halo-width": 1.6,
        },
      });
    }
    this.ready = true;
  }

  setVisible(visible: boolean): void {
    if (!this.ready) return;
    this.map.setLayoutProperty(STORY_LABEL_LAYER, "visibility", visible ? "visible" : "none");
  }

  setFocus(focus: HudFocus | null): void {
    if (!this.ready) return;
    if (!focus) {
      this.setFeatures([]);
      return;
    }
    const seen = new Set<string>();
    const features: LabelFeature[] = [];
    for (const label of focus.labels) {
      const key = label.place;
      if (seen.has(key)) continue;
      seen.add(key);
      features.push({
        type: "Feature",
        geometry: { type: "Point", coordinates: [label.lng, label.lat] },
        properties: {
          label: storyLabel(label.category, label.place),
          color: hex(categoryColor(label.category)),
        },
      });
    }
    this.setFeatures(features);
  }

  setAmbient(points: AmbientLabel[]): void {
    if (!this.ready) return;
    this.setFeatures(
      points.map((point) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [point.lng, point.lat] },
        properties: {
          label: storyLabel(point.category, point.place),
          color: hex(categoryColor(point.category)),
        },
      })),
    );
  }

  private setFeatures(features: LabelFeature[]): void {
    const source = this.map.getSource(STORY_LABEL_SOURCE) as GeoJSONSource | undefined;
    if (!source) return;
    source.setData({ type: "FeatureCollection", features });
  }
}
