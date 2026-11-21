import { describe, expect, it } from "vitest";
import { RIPPLE_CAPACITY, RIPPLE_LIFETIME_SECONDS, RippleRingBuffer } from "./ripples";

/** Distinct coarse cell per index (cells are 1.5° wide). */
const cellLng = (i: number): number => i * 3;

function fill(buffer: RippleRingBuffer, count: number, start = 100): void {
  for (let i = 0; i < count; i++) {
    buffer.spawn({
      lng: cellLng(i),
      lat: 0,
      category: 1,
      spawnAt: start + i * 0.01,
      intensity: 0.5,
      storyIndex: i,
    });
  }
}

describe("RippleRingBuffer", () => {
  it("has the plan's 20-slot capacity", () => {
    expect(RIPPLE_CAPACITY).toBe(20);
    expect(new RippleRingBuffer().capacity).toBe(20);
    expect(new RippleRingBuffer().snapshot()).toHaveLength(20);
  });

  it("never truncates a live ripple: a full buffer drops a distinct-cell pulse", () => {
    const buffer = new RippleRingBuffer();
    fill(buffer, 20);
    const before = buffer.snapshot();
    const dropped = buffer.spawn({
      lng: cellLng(20),
      lat: 0,
      category: 1,
      spawnAt: 100.3,
      storyIndex: 20,
    });
    expect(dropped).toEqual([]);
    expect(buffer.active(100.3)).toHaveLength(20);
    expect(buffer.snapshot()).toEqual(before);
  });

  it("counts only ripples younger than the lifetime as active", () => {
    const buffer = new RippleRingBuffer();
    buffer.spawn({ lng: 0, lat: 0, category: 0, spawnAt: 10 });
    expect(buffer.active(10 + RIPPLE_LIFETIME_SECONDS - 0.01)).toHaveLength(1);
    expect(buffer.active(10 + RIPPLE_LIFETIME_SECONDS)).toHaveLength(0);
  });

  it("merges into a live ripple in the same cell once full", () => {
    const buffer = new RippleRingBuffer();
    fill(buffer, 20);
    const before = buffer.snapshot()[5]!;
    expect(before.intensity).toBe(0.5);

    const [merged] = buffer.spawn({
      lng: cellLng(5) + 0.1,
      lat: 0.1,
      category: 3,
      spawnAt: 100.5,
      storyIndex: 99,
    });
    expect(merged!.slot).toBe(5);
    const after = buffer.snapshot()[5]!;
    expect(after.spawnAt).toBe(100.5);
    expect(after.intensity).toBeCloseTo(0.75, 9);
    expect(after.category).toBe(3);
    expect(buffer.active(100.6)).toHaveLength(20);

    buffer.spawn({ lng: cellLng(5), lat: 0, category: 3, spawnAt: 100.6 });
    buffer.spawn({ lng: cellLng(5), lat: 0, category: 3, spawnAt: 100.7 });
    expect(buffer.snapshot()[5]!.intensity).toBe(1);
  });

  it("reuses expired slots before overwriting live ones", () => {
    const buffer = new RippleRingBuffer();
    for (let i = 0; i < 19; i++) {
      buffer.spawn({ lng: cellLng(i), lat: 0, category: 1, spawnAt: 100, storyIndex: i });
    }
    buffer.spawn({ lng: cellLng(19), lat: 0, category: 1, spawnAt: 101, storyIndex: 19 });

    const now = 100 + RIPPLE_LIFETIME_SECONDS + 0.1;
    expect(buffer.active(now)).toHaveLength(1);
    const [slot] = buffer.spawn({ lng: 50, lat: 50, category: 2, spawnAt: now, storyIndex: 77 });
    expect(slot!.slot).toBe(0);
    const snapshot = buffer.snapshot();
    expect(snapshot[19]!.storyIndex).toBe(19);
    expect(snapshot[0]!.storyIndex).toBe(77);
    expect(buffer.active(now)).toHaveLength(2);
  });

  it("returns snapshot copies", () => {
    const buffer = new RippleRingBuffer();
    buffer.spawn({ lng: 1, lat: 2, category: 3, spawnAt: 5, intensity: 0.5 });
    const snapshot = buffer.snapshot();
    snapshot[0]!.lng = 999;
    snapshot[0]!.intensity = 0;
    expect(buffer.snapshot()[0]!.lng).toBe(1);
    expect(buffer.snapshot()[0]!.intensity).toBe(0.5);
  });
});
