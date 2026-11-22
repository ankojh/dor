import { describe, expect, it } from "vitest";
import { regionFor } from "./regions";

describe("off-screen region buckets", () => {
  it("keeps western Asian capitals out of Africa", () => {
    expect(regionFor(44.366, 33.315)).toBe("asia"); // Baghdad
    expect(regionFor(35.214, 31.768)).toBe("asia"); // Jerusalem
    expect(regionFor(35.502, 33.894)).toBe("asia"); // Beirut
    expect(regionFor(36.277, 33.514)).toBe("asia"); // Damascus
    expect(regionFor(46.675, 24.713)).toBe("asia"); // Riyadh
    expect(regionFor(51.389, 35.689)).toBe("asia"); // Tehran
    expect(regionFor(55.27, 25.2)).toBe("asia"); // Dubai
  });

  it("keeps Egypt and the African Red Sea coast in Africa", () => {
    expect(regionFor(31.235, 30.044)).toBe("africa"); // Cairo
    expect(regionFor(32.559, 15.5)).toBe("africa"); // Khartoum
    expect(regionFor(38.925, 15.339)).toBe("africa"); // Asmara
    expect(regionFor(38.757, 9.03)).toBe("africa"); // Addis Ababa
    expect(regionFor(43.145, 11.595)).toBe("africa"); // Djibouti
    expect(regionFor(45.318, 2.046)).toBe("africa"); // Mogadishu
    expect(regionFor(7.49, 9.06)).toBe("africa"); // Abuja
  });

  it("keeps European, American and Oceanian anchors stable", () => {
    expect(regionFor(13.405, 52.52)).toBe("europe"); // Berlin
    expect(regionFor(37.617, 55.756)).toBe("europe"); // Moscow
    expect(regionFor(-74.006, 40.713)).toBe("americas"); // New York
    expect(regionFor(151.209, -33.869)).toBe("oceania"); // Sydney
    expect(regionFor(174.763, -36.848)).toBe("oceania"); // Auckland
  });
});
