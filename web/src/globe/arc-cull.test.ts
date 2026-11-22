import { describe, expect, it } from "vitest";
import { ARC_VERT, PICK_ARC_VERT } from "./custom/shaders";

describe("arc visibility and ID passes", () => {
  it("share the zoom/length and street-strength cull", () => {
    for (const source of [ARC_VERT, PICK_ARC_VERT]) {
      expect(source).toContain("dorArcZoomVisibility(aArcLength)");
      expect(source).toContain("dorArcStreetVisibility(strength)");
    }
  });
});
