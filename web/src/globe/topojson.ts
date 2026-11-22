/**
 * Minimal TopoJSON decoder for the bundled Natural Earth 1:110m countries.
 *
 * The app needs two things offline: a faint low-zoom land fallback when vector
 * tiles fail, and the country polygon under a focused story's party places.
 * TopoJSON keeps both in ~105 KB instead of a few hundred KB of GeoJSON.
 */

export type Topology = {
  type: "Topology";
  transform?: { scale: [number, number]; translate: [number, number] };
  arcs: number[][][];
  objects: Record<
    string,
    {
      type: "GeometryCollection";
      geometries: Array<{
        type: "Polygon" | "MultiPolygon";
        arcs: number[][] | number[][][];
        id?: string;
        properties?: { name?: string };
      }>;
    }
  >;
};

export type WorldGeometry =
  | { type: "Polygon"; coordinates: number[][][] }
  | { type: "MultiPolygon"; coordinates: number[][][][] };

export type WorldFeature = {
  type: "Feature";
  id?: string;
  properties: { name: string };
  geometry: WorldGeometry;
};

export type WorldFeatureCollection = {
  type: "FeatureCollection";
  features: WorldFeature[];
};

type Position = [number, number];

function decodeArc(arc: number[][], topology: Topology): Position[] {
  const transform = topology.transform;
  const scaleX = transform?.scale[0] ?? 1;
  const scaleY = transform?.scale[1] ?? 1;
  const translateX = transform?.translate[0] ?? 0;
  const translateY = transform?.translate[1] ?? 0;
  let x = 0;
  let y = 0;
  const out: Position[] = [];
  for (const point of arc) {
    x += point[0] ?? 0;
    y += point[1] ?? 0;
    out.push([x * scaleX + translateX, y * scaleY + translateY]);
  }
  return out;
}

function ringFromArcs(indices: number[], arcs: Position[][]): Position[] {
  const ring: Position[] = [];
  for (const raw of indices) {
    const reversed = raw < 0;
    const arc = arcs[reversed ? ~raw : raw];
    if (!arc) continue;
    const points = reversed ? [...arc].reverse() : arc;
    for (const point of points) {
      const last = ring[ring.length - 1];
      if (last && last[0] === point[0] && last[1] === point[1]) continue;
      ring.push([point[0], point[1]]);
    }
  }
  if (ring.length > 0) {
    const first = ring[0]!;
    const last = ring[ring.length - 1]!;
    if (first[0] !== last[0] || first[1] !== last[1]) ring.push([first[0], first[1]]);
  }
  return ring;
}

/** Convert a TopoJSON topology into the flat GeoJSON structure MapLibre uses. */
export function decodeTopology(topology: Topology): WorldFeatureCollection {
  const decodedArcs = topology.arcs.map((arc) => decodeArc(arc, topology));
  const features: WorldFeature[] = [];
  for (const object of Object.values(topology.objects)) {
    if (object.type !== "GeometryCollection") continue;
    for (const geometry of object.geometries) {
      if (geometry.type === "Polygon") {
        const rings = (geometry.arcs as number[][]).map((ring) => ringFromArcs(ring, decodedArcs));
        features.push({
          type: "Feature",
          ...(geometry.id ? { id: geometry.id } : {}),
          properties: { name: geometry.properties?.name ?? "" },
          geometry: { type: "Polygon", coordinates: rings },
        });
      } else if (geometry.type === "MultiPolygon") {
        const polygons = (geometry.arcs as number[][][]).map((polygon) =>
          polygon.map((ring) => ringFromArcs(ring, decodedArcs)),
        );
        features.push({
          type: "Feature",
          ...(geometry.id ? { id: geometry.id } : {}),
          properties: { name: geometry.properties?.name ?? "" },
          geometry: { type: "MultiPolygon", coordinates: polygons },
        });
      }
    }
  }
  return { type: "FeatureCollection", features };
}

type Ring = number[][];

function pointInRing(lon: number, lat: number, ring: Ring): boolean {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i]?.[0];
    const yi = ring[i]?.[1];
    const xj = ring[j]?.[0];
    const yj = ring[j]?.[1];
    if (xi === undefined || yi === undefined || xj === undefined || yj === undefined) continue;
    const intersects = yi > lat !== yj > lat && lon < ((xj - xi) * (lat - yi)) / (yj - yi) + xi;
    if (intersects) inside = !inside;
  }
  return inside;
}

function pointInPolygon(lon: number, lat: number, rings: Ring[]): boolean {
  const outer = rings[0];
  if (!outer || !pointInRing(lon, lat, outer)) return false;
  // Holes: an even-odd test over every ring is enough for Natural Earth.
  let inside = true;
  for (let index = 1; index < rings.length; index++) {
    if (pointInRing(lon, lat, rings[index]!)) inside = false;
  }
  return inside;
}

export function featureContains(feature: WorldFeature, lon: number, lat: number): boolean {
  if (feature.geometry.type === "Polygon") {
    return pointInPolygon(lon, lat, feature.geometry.coordinates as Ring[]);
  }
  return (feature.geometry.coordinates as Ring[][]).some((polygon) =>
    pointInPolygon(lon, lat, polygon),
  );
}

/** Country polygon containing a coordinate, or null for water/unmatched. */
export function countryAt(features: WorldFeature[], lon: number, lat: number): WorldFeature | null {
  for (const feature of features) {
    if (featureContains(feature, lon, lat)) return feature;
  }
  return null;
}
