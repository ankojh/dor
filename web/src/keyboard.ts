/**
 * Globe keyboard guards.
 *
 * Esc and ↑/↓ belong to the globe only when the user is not typing and no
 * dialog or popover owns the keyboard. Otherwise pressing "q" or Esc in the
 * prompt box must never clear the focused story.
 */
export type KeyEventLike = {
  key?: string;
  target?: unknown;
  defaultPrevented?: boolean;
};

type ElementLike = {
  tagName?: string;
  isContentEditable?: boolean;
  closest?: (selector: string) => unknown;
};

type ScopeLike = {
  querySelector?: (selector: string) => unknown;
};

const TEXT_TAGS = new Set(["INPUT", "TEXTAREA", "SELECT"]);

/** Open panels that own the keyboard while they are visible. */
export const KEYBOARD_BLOCKING_SELECTOR =
  '[role="dialog"], .alerts-drawer, .filter-drawer, .layers, .arc-popover, .search-results';

export function isTypingTarget(target: unknown): boolean {
  const element = target as ElementLike | null | undefined;
  if (!element) return false;
  const tag = (element.tagName ?? "").toUpperCase();
  if (TEXT_TAGS.has(tag) || element.isContentEditable === true) return true;
  return Boolean(element.closest?.('[contenteditable="true"]'));
}

/**
 * True when a globe shortcut must be ignored. `scope` defaults to the
 * document in the browser and is injectable for tests.
 */
export function shouldIgnoreGlobeKeys(
  event: KeyEventLike,
  scope: ScopeLike | null = typeof document === "undefined" ? null : document,
): boolean {
  if (event.defaultPrevented) return true;
  if (isTypingTarget(event.target)) return true;
  const element = event.target as ElementLike | null | undefined;
  if (element?.closest?.(KEYBOARD_BLOCKING_SELECTOR)) return true;
  return Boolean(scope?.querySelector?.(KEYBOARD_BLOCKING_SELECTOR));
}
