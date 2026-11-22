export type Numeric =
  Float32Array | Float64Array | Uint8Array | Uint16Array | Uint32Array | Int32Array;
export function growArray<T extends Numeric>(array: T, length: number): T {
  if (array.length >= length) return array;
  const Constructor = array.constructor as { new (length: number): T };
  const next = new Constructor(length);
  next.set(array);
  return next;
}
