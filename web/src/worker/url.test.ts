/**
 * URL resolution for the gateway socket: same-origin by default, with an
 * explicit VITE_WS_URL as the only override.
 */
import { describe, expect, it } from "vitest";
import { resolveSocketUrl } from "./url";

describe("resolveSocketUrl", () => {
  it("prefers a configured VITE_WS_URL", () => {
    expect(
      resolveSocketUrl("ws://gateway.internal:9000/ws", { protocol: "https:", host: "app" }),
    ).toBe("ws://gateway.internal:9000/ws");
  });

  it("uses wss on an https page", () => {
    expect(resolveSocketUrl(undefined, { protocol: "https:", host: "dor.example" })).toBe(
      "wss://dor.example/ws",
    );
  });

  it("uses ws on an http page", () => {
    expect(resolveSocketUrl(undefined, { protocol: "http:", host: "dor.example" })).toBe(
      "ws://dor.example/ws",
    );
  });

  it("preserves the page port", () => {
    expect(resolveSocketUrl(undefined, { protocol: "http:", host: "127.0.0.1:5173" })).toBe(
      "ws://127.0.0.1:5173/ws",
    );
    expect(resolveSocketUrl(undefined, { protocol: "https:", host: "dor.example:8443" })).toBe(
      "wss://dor.example:8443/ws",
    );
  });

  it("ignores an empty configured value", () => {
    expect(resolveSocketUrl("", { protocol: "https:", host: "dor.example" })).toBe(
      "wss://dor.example/ws",
    );
    expect(resolveSocketUrl(null, { protocol: "http:", host: "dor.example" })).toBe(
      "ws://dor.example/ws",
    );
  });

  it("falls back to the local gateway when the page has no host", () => {
    expect(resolveSocketUrl(undefined, { protocol: "http:", host: "" })).toBe(
      "ws://127.0.0.1:8080/ws",
    );
    expect(resolveSocketUrl(undefined, { protocol: "https:", host: "" })).toBe(
      "ws://127.0.0.1:8080/ws",
    );
  });

  it("treats a non-https protocol as plain ws", () => {
    expect(resolveSocketUrl(undefined, { protocol: "file:", host: "dor.example" })).toBe(
      "ws://dor.example/ws",
    );
  });
});
