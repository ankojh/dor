import type { ConnectionState } from "../worker/messages";

/** Short status copy shared by the topbar chip and the reconnect pill. */
export function connectionLabel(state: ConnectionState): string {
  switch (state) {
    case "online":
      return "LIVE";
    case "connecting":
      return "CONNECTING";
    case "reconnecting":
      return "RECONNECTING";
    default:
      return "OFFLINE";
  }
}
