import { describe, expect, it } from "vitest";
import type { ConnectionState } from "../worker/messages";
import { connectionLabel, connectionStatusCopy } from "./connection";

describe("connectionLabel", () => {
  it("maps each known connection state to its short status copy", () => {
    expect(connectionLabel("online")).toBe("LIVE");
    expect(connectionLabel("connecting")).toBe("CONNECTING");
    expect(connectionLabel("reconnecting")).toBe("RECONNECTING");
    expect(connectionLabel("offline")).toBe("OFFLINE");
  });

  it("falls back to OFFLINE for an unknown state at runtime", () => {
    expect(connectionLabel("unknown" as unknown as ConnectionState)).toBe("OFFLINE");
  });
});

describe("connectionStatusCopy", () => {
  it("shows listening copy while online", () => {
    expect(connectionStatusCopy("online")).toEqual({
      title: "Listening",
      body: "Waiting for the first article. The wire fills as sources publish.",
    });
  });

  it("derives the gateway title from the connection label", () => {
    const states: ConnectionState[] = ["connecting", "reconnecting", "offline"];
    for (const state of states) {
      expect(connectionStatusCopy(state).title).toBe(
        `Gateway ${connectionLabel(state).toLowerCase()}`,
      );
    }
    expect(connectionStatusCopy("connecting").title).toBe("Gateway connecting");
  });

  it("explains each non-online state in its body", () => {
    expect(connectionStatusCopy("connecting").body).toBe(
      "Connecting to the gateway. The wire fills when the first article arrives.",
    );
    expect(connectionStatusCopy("reconnecting").body).toBe(
      "The data worker is reconnecting. Existing rows stay in place.",
    );
    expect(connectionStatusCopy("offline").body).toBe(
      "The data worker is offline. Existing rows stay in place.",
    );
  });

  it("falls back to offline copy for an unknown state at runtime", () => {
    expect(connectionStatusCopy("unknown" as unknown as ConnectionState)).toEqual(
      connectionStatusCopy("offline"),
    );
  });
});
