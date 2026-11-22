import { describe, expect, it } from "vitest";
import { channelLabel } from "./labels";

describe("channelLabel", () => {
  it("maps the known delivery channels to their human-readable names", () => {
    expect(channelLabel("inapp")).toBe("in-app");
    expect(channelLabel("ntfy")).toBe("phone (ntfy)");
    expect(channelLabel("push")).toBe("browser push");
  });

  it("passes an unknown channel through unchanged", () => {
    expect(channelLabel("sms")).toBe("sms");
    expect(channelLabel("")).toBe("");
  });
});
