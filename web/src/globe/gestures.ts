/**
 * Gesture-mode data hold.
 *
 * While the map is being dragged, zoomed or rotated the app applies incoming
 * data at most four times a second and never rebuilds layout. Patches that
 * arrive in between are merged (not dropped) and applied on the next slot or
 * immediately when the gesture ends.
 */

export const GESTURE_APPLY_INTERVAL_MS = 250; // ≤ 4 applies / s

export type MergeFn<T> = (older: T, newer: T) => T;

export class PatchHold<T> {
  private pending: T | null = null;
  private gestureActive = false;
  private lastApplyAt = Number.NEGATIVE_INFINITY;
  private readonly merge: MergeFn<T>;
  private readonly intervalMs: number;

  constructor(merge: MergeFn<T>, intervalMs: number = GESTURE_APPLY_INTERVAL_MS) {
    this.merge = merge;
    this.intervalMs = intervalMs;
  }

  setGestureActive(active: boolean): void {
    this.gestureActive = active;
  }

  isGestureActive(): boolean {
    return this.gestureActive;
  }

  push(patch: T): void {
    this.pending = this.pending === null ? patch : this.merge(this.pending, patch);
  }

  hasPending(): boolean {
    return this.pending !== null;
  }

  /**
   * Returns the patch to apply now, or null if none is due. `nowMs` is a
   * monotonic clock (performance.now()).
   */
  take(nowMs: number): T | null {
    if (this.pending === null) return null;
    if (this.gestureActive && nowMs - this.lastApplyAt < this.intervalMs) return null;
    const patch = this.pending;
    this.pending = null;
    this.lastApplyAt = nowMs;
    return patch;
  }

  /** Forces a flush, used on gesture end and on stop. */
  flush(): T | null {
    const patch = this.pending;
    this.pending = null;
    this.lastApplyAt = performance.now();
    return patch;
  }

  /** Milliseconds until the next allowed apply while a gesture is active. */
  dueIn(nowMs: number): number {
    if (!this.gestureActive) return 0;
    return Math.max(0, this.intervalMs - (nowMs - this.lastApplyAt));
  }
}
