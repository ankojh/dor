/**
 * Fixed-capacity GPU ripple ring buffer.
 *
 * Twenty slots at most. New ripples overwrite the oldest slot; beyond the cap
 * a ripple that lands in the same coarse geocell as an active one merges into
 * that ripple (restarts its animation and raises its intensity) instead of
 * taking a slot. The shader animates each slot from its spawn time, so no
 * per-frame JavaScript loop over articles is needed.
 */

export const RIPPLE_CAPACITY = 20;
export const RIPPLE_LIFETIME_SECONDS = 1.4;
export const RIPPLE_FLASH_SECONDS = 0.35;
/** Off-thread ripples stay visible but are heavily muted while focused. */
export const RIPPLE_OFF_THREAD_DIM = 0.12;
/** Peak alpha of the white spawn flash. */
export const RIPPLE_FLASH_ALPHA = 0.9;
/** Coarse stand-in for an H3 cell: about 150 km at the equator. */
export const RIPPLE_MERGE_CELL_DEGREES = 1.5;

export type RippleSpawn = {
  lng: number;
  lat: number;
  category: number;
  /** Wall-clock seconds (Date.now() / 1000), shared with the shader. */
  spawnAt: number;
  /** 0..1; merged ripples are brighter. */
  intensity?: number;
  storyIndex?: number;
};

export type RippleSlotState = {
  slot: number;
  lng: number;
  lat: number;
  category: number;
  spawnAt: number;
  intensity: number;
  storyIndex: number;
};

function cellKey(lng: number, lat: number): string {
  return `${Math.floor(lng / RIPPLE_MERGE_CELL_DEGREES)}:${Math.floor(
    lat / RIPPLE_MERGE_CELL_DEGREES,
  )}`;
}

export class RippleRingBuffer {
  readonly capacity: number;
  private readonly slots: RippleSlotState[];

  constructor(capacity: number = RIPPLE_CAPACITY) {
    this.capacity = Math.max(1, capacity);
    this.slots = Array.from({ length: this.capacity }, (_, slot) => ({
      slot,
      lng: 0,
      lat: 0,
      category: 0,
      spawnAt: Number.NEGATIVE_INFINITY,
      intensity: 0,
      storyIndex: -1,
    }));
  }

  /** Slots that changed for this spawn (usually exactly one). */
  spawn(ripple: RippleSpawn): RippleSlotState[] {
    const now = ripple.spawnAt;
    const live = this.slots.filter((s) => now - s.spawnAt < RIPPLE_LIFETIME_SECONDS);
    const intensity = Math.min(1, Math.max(0.25, ripple.intensity ?? 0.5));

    if (live.length >= this.capacity) {
      const key = cellKey(ripple.lng, ripple.lat);
      const merge = live.find((s) => cellKey(s.lng, s.lat) === key);
      if (merge) {
        merge.spawnAt = now;
        merge.intensity = Math.min(1, merge.intensity + 0.25);
        merge.category = ripple.category;
        merge.storyIndex = ripple.storyIndex ?? merge.storyIndex;
        return [merge];
      }
      // Never truncate a live ripple. A full buffer drops the new pulse; the
      // worker rate-limits spawns so this is rare.
      return [];
    }

    const free = this.slots.find((s) => now - s.spawnAt >= RIPPLE_LIFETIME_SECONDS);
    const target = free;
    if (!target) return [];
    target.lng = ripple.lng;
    target.lat = ripple.lat;
    target.category = ripple.category;
    target.spawnAt = now;
    target.intensity = intensity;
    target.storyIndex = ripple.storyIndex ?? -1;
    return [target];
  }

  /** Snapshot for tests and for the initial GPU upload. */
  snapshot(): RippleSlotState[] {
    return this.slots.map((slot) => ({ ...slot }));
  }

  active(now: number): RippleSlotState[] {
    return this.slots.filter((s) => now - s.spawnAt < RIPPLE_LIFETIME_SECONDS);
  }

  clear(): void {
    for (const slot of this.slots) {
      slot.spawnAt = Number.NEGATIVE_INFINITY;
      slot.intensity = 0;
      slot.storyIndex = -1;
    }
  }
}
