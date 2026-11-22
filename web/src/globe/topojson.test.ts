import { describe, expect, it } from "vitest";
import { countryAt, decodeTopology, type Topology, type WorldFeature } from "./topojson";
import bundled from "./world-110m.json";

/**
 * A 2-arc square: arc 0 runs along the left and top edge, arc 1 closes the
 * shape along the right and bottom. The transform is non-trivial so the test
 * catches quantized deltas leaking through unscaled.
 *
 * Quantized corners: (0,0), (0,2), (2,2), (2,0)
 * Decoded with scale (2,3) and translate (10,20).
 */
const squareTopology: Topology = {
  type: "Topology",
  transform: { scale: [2, 3], translate: [10, 20] },
  arcs: [
    [
      [0, 0],
      [0, 2],
      [2, 0],
    ],
    [
      [2, 2],
      [0, -2],
      [-2, 0],
    ],
  ],
  objects: {
    countries: {
      type: "GeometryCollection",
      geometries: [
        { type: "Polygon", arcs: [[0, 1]], id: "square", properties: { name: "Square" } },
      ],
    },
  },
};

describe("decodeTopology", () => {
  it("decodes a 2-arc polygon into one closed, transformed ring", () => {
    const world = decodeTopology(squareTopology);

    expect(world.type).toBe("FeatureCollection");
    expect(world.features).toHaveLength(1);
    const feature = world.features[0]!;
    expect(feature.id).toBe("square");
    expect(feature.properties.name).toBe("Square");
    expect(feature.geometry.type).toBe("Polygon");

    const rings = (feature.geometry as { coordinates: number[][][] }).coordinates;
    expect(rings).toHaveLength(1);
    const ring = rings[0]!;
    expect(ring).toHaveLength(5);
    expect(ring).toEqual([
      [10, 20],
      [10, 26],
      [14, 26],
      [14, 20],
      [10, 20],
    ]);
    expect(ring[0]).toEqual(ring[ring.length - 1]);
  });

  it("keeps MultiPolygon shapes as multipolygons", () => {
    const world = decodeTopology({
      ...squareTopology,
      objects: {
        countries: {
          type: "GeometryCollection",
          geometries: [{ type: "MultiPolygon", arcs: [[[0, 1]]], properties: { name: "Isles" } }],
        },
      },
    });

    expect(world.features).toHaveLength(1);
    expect(world.features[0]!.geometry.type).toBe("MultiPolygon");
    const polygons = (world.features[0]!.geometry as { coordinates: number[][][][] }).coordinates;
    expect(polygons).toHaveLength(1);
    expect(polygons[0]!).toHaveLength(1);
  });
});

function feature(name: string, coordinates: number[][][]): WorldFeature {
  return { type: "Feature", properties: { name }, geometry: { type: "Polygon", coordinates } };
}

const square = feature("Square", [
  [
    [0, 0],
    [4, 0],
    [4, 4],
    [0, 4],
    [0, 0],
  ],
]);
const donut = feature("Donut", [
  [
    [0, 0],
    [10, 0],
    [10, 10],
    [0, 10],
    [0, 0],
  ],
  [
    [4, 4],
    [6, 4],
    [6, 6],
    [4, 6],
    [4, 4],
  ],
]);

describe("countryAt", () => {
  it("finds the polygon containing a point", () => {
    expect(countryAt([square], 2, 2)?.properties.name).toBe("Square");
    expect(countryAt([square], 0.5, 0.5)?.properties.name).toBe("Square");
    expect(countryAt([square], 3.99, 3.99)?.properties.name).toBe("Square");
  });

  it("returns null outside every polygon", () => {
    expect(countryAt([square], 5, 5)).toBeNull();
    expect(countryAt([square], -1, 2)).toBeNull();
    expect(countryAt([], 2, 2)).toBeNull();
  });

  it("excludes holes", () => {
    expect(countryAt([donut], 1, 1)?.properties.name).toBe("Donut");
    expect(countryAt([donut], 5, 5)).toBeNull();
  });

  it("skips earlier non-matching features", () => {
    const farSquare = feature("Far Square", [
      [
        [20, 20],
        [24, 20],
        [24, 24],
        [20, 24],
        [20, 20],
      ],
    ]);
    expect(countryAt([donut, farSquare], 22, 22)?.properties.name).toBe("Far Square");
    expect(countryAt([donut, farSquare], 5, 1)?.properties.name).toBe("Donut");
  });
});

describe("bundled world-110m countries", () => {
  const world = decodeTopology(bundled as unknown as Topology);

  it("decodes more than 150 countries", () => {
    expect(world.features.length).toBeGreaterThan(150);
  });

  it("places Berlin in Germany", () => {
    const berlin = countryAt(world.features, 13.405, 52.52);
    expect(berlin).not.toBeNull();
    expect(berlin?.properties.name).toBe("Germany");
  });

  it("places Riyadh in Saudi Arabia", () => {
    const riyadh = countryAt(world.features, 46.675, 24.713);
    expect(riyadh).not.toBeNull();
    expect(riyadh?.properties.name).toBe("Saudi Arabia");
  });
});
