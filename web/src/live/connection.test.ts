import { describe, expect, it } from "vitest";
import type { ConnectionState } from "../worker/messages";
import { connectionLabel } from "./connection";

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
