/**
 * Globe keyboard guards: shortcuts must never fire while the user is typing
 * or while a dialog/popover owns the keyboard.
 */
import { describe, expect, it } from "vitest";
import { KEYBOARD_BLOCKING_SELECTOR, isTypingTarget, shouldIgnoreGlobeKeys } from "./keyboard";

type FakeElement = {
  tagName?: string;
  isContentEditable?: boolean;
  closest?: (selector: string) => unknown;
};

function insideBlocker(selectorToMatch: string): FakeElement {
  return {
    tagName: "SPAN",
    closest: (selector) => (selector === selectorToMatch ? {} : null),
  };
}

describe("isTypingTarget", () => {
  it("recognises the text-entry tags", () => {
    expect(isTypingTarget({ tagName: "INPUT" })).toBe(true);
    expect(isTypingTarget({ tagName: "TEXTAREA" })).toBe(true);
    expect(isTypingTarget({ tagName: "SELECT" })).toBe(true);
    // Tags are compared case-insensitively.
    expect(isTypingTarget({ tagName: "input" })).toBe(true);
  });

  it("recognises explicitly editable elements", () => {
    expect(isTypingTarget({ tagName: "DIV", isContentEditable: true })).toBe(true);
  });

  it("recognises an element nested inside a contenteditable region", () => {
    expect(
      isTypingTarget({
        tagName: "SPAN",
        isContentEditable: false,
        closest: (selector: string) => (selector === '[contenteditable="true"]' ? {} : null),
      }),
    ).toBe(true);
  });

  it("does not treat ordinary controls or empty targets as typing", () => {
    expect(isTypingTarget({ tagName: "BUTTON" })).toBe(false);
    expect(isTypingTarget({ tagName: "DIV", isContentEditable: false })).toBe(false);
    expect(isTypingTarget({ tagName: "DIV", closest: () => null })).toBe(false);
    expect(isTypingTarget(null)).toBe(false);
    expect(isTypingTarget(undefined)).toBe(false);
  });
});

describe("shouldIgnoreGlobeKeys", () => {
  it("blocks when the event target is a typing control", () => {
    expect(shouldIgnoreGlobeKeys({ target: { tagName: "INPUT" } }, null)).toBe(true);
    expect(shouldIgnoreGlobeKeys({ target: { tagName: "TEXTAREA" } }, null)).toBe(true);
  });

  it("ignores q while the user is typing", () => {
    expect(shouldIgnoreGlobeKeys({ key: "q", target: { tagName: "INPUT" } }, null)).toBe(true);
    expect(shouldIgnoreGlobeKeys({ key: "q", target: { tagName: "TEXTAREA" } }, null)).toBe(true);
  });

  it("blocks when the target sits inside a dialog, popover or panel", () => {
    expect(shouldIgnoreGlobeKeys({ target: insideBlocker(KEYBOARD_BLOCKING_SELECTOR) }, null)).toBe(
      true,
    );
  });

  it("blocks when an open dialog owns the document even if the target is the body", () => {
    const scope = {
      querySelector: (selector: string) =>
        selector.includes('[role="dialog"]') ? { id: "dialog" } : null,
    };
    expect(shouldIgnoreGlobeKeys({ target: { tagName: "BODY", closest: () => null } }, scope)).toBe(
      true,
    );
  });

  it("blocks when the alerts drawer is open in the document", () => {
    const scope = {
      querySelector: (selector: string) =>
        selector.split(",").some((part) => part.trim() === ".alerts-drawer") ? {} : null,
    };
    expect(shouldIgnoreGlobeKeys({ target: { tagName: "BODY" } }, scope)).toBe(true);
  });

  it("blocks a default-prevented event regardless of target", () => {
    expect(
      shouldIgnoreGlobeKeys({ defaultPrevented: true, target: { tagName: "BODY" } }, null),
    ).toBe(true);
    expect(shouldIgnoreGlobeKeys({ defaultPrevented: true }, null)).toBe(true);
  });

  it("does not block a plain body/button event", () => {
    expect(shouldIgnoreGlobeKeys({ target: { tagName: "BODY", closest: () => null } }, null)).toBe(
      false,
    );
    expect(
      shouldIgnoreGlobeKeys({ target: { tagName: "BUTTON", closest: () => null } }, null),
    ).toBe(false);
    expect(shouldIgnoreGlobeKeys({}, null)).toBe(false);
    expect(shouldIgnoreGlobeKeys({}, { querySelector: () => null })).toBe(false);
  });
});
