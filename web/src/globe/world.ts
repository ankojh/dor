import type { Topology } from "./topojson";
import { decodeTopology } from "./topojson";
import topology from "./world-110m.json";

/** Natural Earth 1:110m countries, decoded once. Public domain. */
export const WORLD = decodeTopology(topology as unknown as Topology);
