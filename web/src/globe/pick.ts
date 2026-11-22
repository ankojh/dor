/**
 * GPU id-pass helpers shared by the layer and its tests.
 *
 * Story ids and link ids live in separate passes so a dot always wins over an
 * arc under the same pixel (the review's P0: a straight-chord arc test ran
 * before the dot pick and opened the wrong popover).
 */
export type PickHit = { kind: "story" | "link"; index: number };

export function choosePick(story: number | null, link: number | null): PickHit | null {
  if (story !== null && story >= 0) return { kind: "story", index: story };
  if (link !== null && link >= 0) return { kind: "link", index: link };
  return null;
}

/** 24-bit little-endian id used by the pick shaders (red + green + blue). */
export function decodePickId(red: number, green: number, blue: number): number {
  return red + green * 256 + blue * 65536;
}

export function encodePickId(id: number): [number, number, number] {
  return [id % 256, Math.floor(id / 256) % 256, Math.floor(id / 65536)];
}
