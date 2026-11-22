/**
 * Small, pure GPU layout helpers: 2D texture rows and a capacity free-list.
 * Kept separate from WebGL so the maths is unit-testable.
 */

/** Texture width for per-instance data. Height grows with capacity. */
export const TEXTURE_WIDTH = 2048;

export function textureRows(count: number, width: number = TEXTURE_WIDTH): number {
  return Math.max(1, Math.ceil(Math.max(0, count) / width));
}

export function texelOf(index: number, width: number = TEXTURE_WIDTH): { x: number; y: number } {
  return { x: index % width, y: Math.floor(index / width) };
}

/**
 * Capacity pool with a free list. `reserve(count)` marks the first `count`
 * slots used; the remainder is spare capacity. `allocate` returns the lowest
 * free index or -1 when full, `free` returns a slot to the list.
 */
export class CapacityPool {
  capacity: number;
  private used: Uint8Array;
  private readonly freeList: number[];

  constructor(capacity: number, reserve = 0) {
    this.capacity = Math.max(0, capacity);
    this.used = new Uint8Array(this.capacity);
    this.freeList = [];
    const reserved = Math.min(this.capacity, Math.max(0, reserve));
    for (let i = 0; i < reserved; i++) this.used[i] = 1;
    for (let i = this.capacity - 1; i >= reserved; i--) this.freeList.push(i);
  }

  grow(capacity: number): void {
    if (capacity <= this.capacity) return;
    const used = new Uint8Array(capacity);
    used.set(this.used);
    this.used = used;
    for (let i = capacity - 1; i >= this.capacity; i--) this.freeList.unshift(i);
    this.capacity = capacity;
  }

  allocate(): number {
    const index = this.freeList.pop();
    if (index === undefined) return -1;
    this.used[index] = 1;
    return index;
  }

  /** Claim a specific worker-assigned slot; false when it is already used. */
  claim(index: number): boolean {
    if (index < 0 || index >= this.capacity || this.used[index] === 1) return false;
    const at = this.freeList.indexOf(index);
    if (at >= 0) this.freeList.splice(at, 1);
    this.used[index] = 1;
    return true;
  }

  free(index: number): boolean {
    if (index < 0 || index >= this.capacity) return false;
    if (this.used[index] === 0) return false;
    this.used[index] = 0;
    this.freeList.push(index);
    return true;
  }

  isUsed(index: number): boolean {
    return index >= 0 && index < this.capacity && this.used[index] === 1;
  }

  get usedCount(): number {
    let count = 0;
    for (let i = 0; i < this.capacity; i++) count += this.used[i]!;
    return count;
  }

  get freeCount(): number {
    return this.freeList.length;
  }

  get spareCapacity(): number {
    return this.capacity - this.usedCount;
  }
}
