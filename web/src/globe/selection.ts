/**
 * Wire selection rules.
 *
 * A selection is a specific wire row, not a story: several adjacent rows can
 * belong to the same story and ↑/↓ must walk them in order without refocusing
 * the globe. Only a story change may replace the selected row.
 */
export type SelectableRow = { key: string; storyId: string };

/**
 * Index of the row reached by `delta`. Clamps at both ends and never wraps, so
 * holding ↓ stops on the oldest row instead of jumping back to the newest.
 */
export function nextSelectionIndex(
  rows: readonly SelectableRow[],
  currentKey: string | null,
  delta: number,
): number | null {
  if (rows.length === 0) return null;
  const current = currentKey === null ? -1 : rows.findIndex((row) => row.key === currentKey);
  // No current row: ↓ starts at the newest row, ↑ at the oldest.
  if (current < 0) return delta < 0 ? rows.length - 1 : 0;
  return Math.max(0, Math.min(rows.length - 1, current + delta));
}

/** Keep the row the user clicked; otherwise use the story's newest row. */
export function selectionKeyForStory(
  rows: readonly SelectableRow[],
  storyId: string,
  preferredKey: string | null = null,
): string | null {
  if (preferredKey !== null) {
    const preferred = rows.find((row) => row.key === preferredKey);
    if (preferred && preferred.storyId === storyId) return preferredKey;
  }
  return rows.find((row) => row.storyId === storyId)?.key ?? null;
}

/** Never move the selected row off the focused story during a refresh. */
export function keepSelectionForStory(
  currentKey: string | null,
  rows: readonly SelectableRow[],
  storyId: string,
): string | null {
  if (currentKey === null) return null;
  const current = rows.find((row) => row.key === currentKey);
  return current && current.storyId === storyId ? currentKey : null;
}
