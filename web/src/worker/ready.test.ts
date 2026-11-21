import { describe, expect, it } from "vitest";
import { buildReadyMessage } from "./ready";

describe("data worker ready message", () => {
  it("round-trips a generated Story through the protobuf runtime", () => {
    const ready = buildReadyMessage();
    expect(ready.type).toBe("ready");
    expect(ready.protoStoryId).toBe("worker-probe");
    expect(ready.category).toBe("OTHER");
    expect(ready.storyBytes).toBeGreaterThan(0);
  });
});
