/**
 * Vertical swipe on the mobile sheet handle.
 *
 * Returns +1 for a swipe up (next story), -1 for down (previous), 0 for a
 * movement below the threshold. Only the handle uses this; the sheet body
 * keeps native pan-y scrolling.
 */
export function sheetSwipeDirection(startY: number, endY: number, threshold = 40): -1 | 0 | 1 {
  const delta = endY - startY;
  if (Math.abs(delta) < threshold) return 0;
  return delta < 0 ? 1 : -1;
}
