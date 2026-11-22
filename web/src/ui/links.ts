/** Strength tiers shared by the story panel and the mobile sheet (board M2). */
export type LinkTier<T> = {
  label: "Strongly linked" | "Related" | "Loosely linked";
  links: T[];
};

export function groupLinksByStrength<T extends { strength: number }>(
  links: readonly T[],
): Array<LinkTier<T>> {
  const tiers: Array<LinkTier<T>> = [
    { label: "Strongly linked", links: links.filter((link) => link.strength >= 0.75) },
    {
      label: "Related",
      links: links.filter((link) => link.strength >= 0.5 && link.strength < 0.75),
    },
    { label: "Loosely linked", links: links.filter((link) => link.strength < 0.5) },
  ];
  return tiers.filter((tier) => tier.links.length > 0);
}
