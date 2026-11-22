/**
 * Selection rules: a selection is a wire row, not a story. Several rows can
 * share a story and arrow keys must walk them in order; a story change is the
 * only thing allowed to replace the selected row.
 */
import { describe, expect, it } from "vitest";
import {
  keepSelectionForStory,
  nextSelectionIndex,
  selectionKeyForStory,
  type SelectableRow,
} from "./selection";

// Newest first, with two adjacent rows for story "s1".
const rows: SelectableRow[] = [
  { key: "s1-new", storyId: "s1" },
  { key: "s1-old", storyId: "s1" },
  { key: "s2-new", storyId: "s2" },
  { key: "s3-new", storyId: "s3" },
];

describe("nextSelectionIndex", () => {
  it("walks adjacent rows of the same story without skipping to the next story", () => {
    expect(nextSelectionIndex(rows, "s1-new", 1)).toBe(1);
    expect(rows[1]!.storyId).toBe("s1");
    expect(nextSelectionIndex(rows, "s1-old", 1)).toBe(2);
    expect(rows[2]!.storyId).toBe("s2");
  });

  it("walks back through the same story in order", () => {
    expect(nextSelectionIndex(rows, "s2-new", -1)).toBe(1);
    expect(rows[1]!.storyId).toBe("s1");
    expect(nextSelectionIndex(rows, "s1-old", -1)).toBe(0);
  });

  it("clamps at the first row instead of wrapping", () => {
    expect(nextSelectionIndex(rows, "s1-new", -1)).toBe(0);
    expect(nextSelectionIndex(rows, "s1-new", -5)).toBe(0);
  });

  it("clamps at the last row instead of wrapping", () => {
    expect(nextSelectionIndex(rows, "s3-new", 1)).toBe(3);
    expect(nextSelectionIndex(rows, "s3-new", 5)).toBe(3);
  });

  it("selects the first row on ↓ and the last row on ↑ when nothing is selected", () => {
    expect(nextSelectionIndex(rows, null, 1)).toBe(0);
    expect(nextSelectionIndex(rows, null, -1)).toBe(3);
  });

  it("treats a key that is no longer in the list like no selection", () => {
    expect(nextSelectionIndex(rows, "gone", 1)).toBe(0);
    expect(nextSelectionIndex(rows, "gone", -1)).toBe(3);
  });

  it("returns null for an empty list", () => {
    expect(nextSelectionIndex([], null, 1)).toBeNull();
    expect(nextSelectionIndex([], "s1-new", -1)).toBeNull();
  });
});

describe("selectionKeyForStory", () => {
  const storyRows: SelectableRow[] = [
    { key: "s1-new", storyId: "s1" },
    { key: "s2-new", storyId: "s2" },
    { key: "s1-old", storyId: "s1" },
    { key: "s3-new", storyId: "s3" },
  ];

  it("keeps a clicked older row that belongs to the same story", () => {
    expect(selectionKeyForStory(storyRows, "s1", "s1-old")).toBe("s1-old");
  });

  it("ignores a preferred row from a different story", () => {
    expect(selectionKeyForStory(storyRows, "s1", "s2-new")).toBe("s1-new");
  });

  it("falls back to the story's newest row when no preference is given", () => {
    expect(selectionKeyForStory(storyRows, "s1")).toBe("s1-new");
    expect(selectionKeyForStory(storyRows, "s1", null)).toBe("s1-new");
    expect(selectionKeyForStory(storyRows, "s2")).toBe("s2-new");
  });

  it("falls back when the preferred key is not in the list", () => {
    expect(selectionKeyForStory(storyRows, "s1", "s1-gone")).toBe("s1-new");
  });

  it("returns null when the story is absent", () => {
    expect(selectionKeyForStory(storyRows, "s4")).toBeNull();
    expect(selectionKeyForStory([], "s1")).toBeNull();
  });
});

describe("keepSelectionForStory", () => {
  it("keeps the current row while the focused story is unchanged", () => {
    expect(keepSelectionForStory("s1-old", rows, "s1")).toBe("s1-old");
  });

  it("rejects a row that belongs to a different story", () => {
    expect(keepSelectionForStory("s2-new", rows, "s1")).toBeNull();
  });

  it("rejects a row that is no longer present", () => {
    expect(keepSelectionForStory("s1-gone", rows, "s1")).toBeNull();
  });

  it("returns null when there is no current selection", () => {
    expect(keepSelectionForStory(null, rows, "s1")).toBeNull();
    expect(keepSelectionForStory("s1-new", [], "s1")).toBeNull();
  });
});
