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

export type ConnectionStatusCopy = {
  title: string;
  body: string;
};

/** Empty-wire and empty-stage copy derived from the same connection label. */
export function connectionStatusCopy(state: ConnectionState): ConnectionStatusCopy {
  const label = connectionLabel(state).toLowerCase();
  switch (state) {
    case "online":
      return {
        title: "Listening",
        body: "Waiting for the first article. The wire fills as sources publish.",
      };
    case "connecting":
      return {
        title: `Gateway ${label}`,
        body: "Connecting to the gateway. The wire fills when the first article arrives.",
      };
    case "reconnecting":
      return {
        title: `Gateway ${label}`,
        body: "The data worker is reconnecting. Existing rows stay in place.",
      };
    default:
      return {
        title: `Gateway ${label}`,
        body: "The data worker is offline. Existing rows stay in place.",
      };
  }
}
