/**
 * Coarse region buckets for off-screen edge markers. These are deliberately
 * rectangular and conservative: the marker only says which direction to fly,
 * while the geographic data keeps the accurate point. Western Asia is checked
 * before Europe/Africa so Levant and Mesopotamian capitals are not counted as
 * Africa; the Red Sea African coast and Egypt stay in Africa.
 */
export function regionFor(lng: number, lat: number): string {
  if (lng < -30) return "americas";
  if (lng >= 110 && lat < -8) return "oceania";
  if (lng >= 34.5 && lng < 45 && lat >= 28 && lat < 38) return "asia";
  if (lng >= 40 && lng < 63 && lat >= 12 && lat < 42) return "asia";
  if (lng >= -25 && lng < 45 && lat >= 34) return "europe";
  if (lng >= -20 && lng < 55 && lat >= -36 && lat < 34) return "africa";
  if (lng >= 45) return "asia";
  return "europe";
}
