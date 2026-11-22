/**
 * Resolve the gateway WebSocket URL.
 *
 * The app is served next to the gateway (Vite proxies `/ws` in dev, the
 * gateway serves the built web app in production), so the default is always
 * the page's own origin. Only an explicit VITE_WS_URL overrides it.
 */
export type LocationLike = { protocol: string; host: string };

export function resolveSocketUrl(
  configured: string | undefined | null,
  location: LocationLike,
): string {
  if (configured && configured.length > 0) return configured;
  if (!location.host) return "ws://127.0.0.1:8080/ws";
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  return `${scheme}://${location.host}/ws`;
}
