/** Move the active option in the search listbox by `delta`; clamp at the ends. */
export function nextSearchIndex(count: number, current: number, delta: number): number | null {
  if (count <= 0 || delta === 0) return null;
  if (current < 0) return delta > 0 ? 0 : count - 1;
  return Math.max(0, Math.min(count - 1, current + delta));
}
