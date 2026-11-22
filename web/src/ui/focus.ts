/**
 * Next focus index inside a modal dialog.
 *
 * `current` is -1 when focus starts on the dialog container or outside it.
 * Tab wraps forward, Shift+Tab wraps backward; an empty dialog returns null.
 */
export function nextTabStop(count: number, current: number, shift: boolean): number | null {
  if (count <= 0) return null;
  if (current < 0 || current >= count) return shift ? count - 1 : 0;
  return shift ? (current - 1 + count) % count : (current + 1) % count;
}
