import { describe, expect, it } from "vitest";
import { groupLinksByStrength } from "./links";

type Link = { id: string; strength: number };

function link(id: string, strength: number): Link {
  return { id, strength };
}

describe("groupLinksByStrength", () => {
  it("places links on the 0.75 / 0.5 boundaries in the correct tier", () => {
    const tiers = groupLinksByStrength([
      link("strong", 0.75),
      link("related-high", 0.749),
      link("related-low", 0.5),
      link("loose", 0.499),
    ]);
    expect(tiers.map((tier) => tier.label)).toEqual([
      "Strongly linked",
      "Related",
      "Loosely linked",
    ]);
    expect(tiers[0]!.links.map((entry) => entry.id)).toEqual(["strong"]);
    expect(tiers[1]!.links.map((entry) => entry.id)).toEqual(["related-high", "related-low"]);
    expect(tiers[2]!.links.map((entry) => entry.id)).toEqual(["loose"]);
  });

  it("drops tiers that have no links", () => {
    const strongAndRelated = groupLinksByStrength([link("a", 0.9), link("b", 0.6)]);
    expect(strongAndRelated.map((tier) => tier.label)).toEqual(["Strongly linked", "Related"]);

    const onlyLoose = groupLinksByStrength([link("c", 0.1)]);
    expect(onlyLoose.map((tier) => tier.label)).toEqual(["Loosely linked"]);
  });

  it("preserves input order inside a tier", () => {
    const tiers = groupLinksByStrength([
      link("a", 0.8),
      link("b", 0.95),
      link("c", 0.75),
      link("d", 0.6),
      link("e", 0.5),
    ]);
    expect(tiers[0]!.links.map((entry) => entry.id)).toEqual(["a", "b", "c"]);
    expect(tiers[1]!.links.map((entry) => entry.id)).toEqual(["d", "e"]);
  });

  it("returns no tiers for an empty input", () => {
    expect(groupLinksByStrength([])).toEqual([]);
  });
});
