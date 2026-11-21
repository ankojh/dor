import {
  CATEGORY_CLIMATE,
  CATEGORY_CONFLICT,
  CATEGORY_DISASTER,
  CATEGORY_ECONOMY,
  CATEGORY_POLITICS,
  CATEGORY_TECH,
} from "../../globe/encoding";

/**
 * Plausible headlines for the fake stream. They exist so the HUD, labels and
 * screenshots read like the product; the data is synthetic.
 */
const TEMPLATES: Readonly<Record<number, readonly string[]>> = {
  [CATEGORY_CONFLICT]: [
    "Overnight strikes hit {place} energy grid",
    "Ceasefire talks scheduled for {place}",
    "Border patrols reinforced near {place}",
    "Aid convoy blocked outside {place}",
    "Arms talks resume in {place}",
    "Monitoring mission reports calm around {place}",
  ],
  [CATEGORY_POLITICS]: [
    "Coalition talks resume in {place}",
    "Election recount ordered in {place}",
    "{place} signals support for new trade pact",
    "Ministers meet in {place} on security budget",
    "Senate passes chip export bill",
    "Referendum campaign opens in {place}",
  ],
  [CATEGORY_ECONOMY]: [
    "Freight rates climb for a second week",
    "Rice exporters pause new contracts",
    "Frost hits coffee belt, futures jump",
    "{place} inflation slows for a third month",
    "Central bank holds rates, flags supply risks",
    "Retailers prepare for holiday spending",
    "Shippers reroute cargo around {place}",
  ],
  [CATEGORY_TECH]: [
    "Chipmakers weigh response to export rules",
    "Cloud providers warn of GPU shortages",
    "Memory chip prices tick up on tight supply",
    "{place} opens a second AI research campus",
    "Satellite startup raises a new round in {place}",
    "Data centre build-out slows near {place}",
  ],
  [CATEGORY_CLIMATE]: [
    "Heatwave warnings across the east coast",
    "Drought cuts river shipping near {place}",
    "Climate talks circulate draft text in {place}",
    "Record rainfall floods roads in {place}",
    "Glacier monitoring network expands near {place}",
    "Turbine supply chain tightens in {place}",
  ],
  [CATEGORY_DISASTER]: [
    "Strong offshore quake; tsunami advisory lifted",
    "Volcanic ash grounds regional flights",
    "Orange alert: tropical storm forming",
    "Wildfire crews contain blaze outside {place}",
    "Flood defence breach reported near {place}",
    "Aftershock recorded off the coast",
  ],
};

export function fakeTitle(category: number, place: string, rng: () => number): string {
  const templates = TEMPLATES[category] ?? TEMPLATES[CATEGORY_ECONOMY]!;
  const template = templates[Math.floor(rng() * templates.length)] ?? templates[0]!;
  return template.replaceAll("{place}", place);
}
