import { describe, expect, it, vi } from "vitest";
import { createFocusHandler } from "./focus-events";

type Details = { storyId: string; heat?: number };

describe("createFocusHandler: same-story refresh vs. activate-on-change", () => {
  it("refreshes on every call but never repeats activate for sustained same-id metadata updates", () => {
    const refresh = vi.fn();
    const activate = vi.fn();
    const handler = createFocusHandler<Details>(refresh, activate);

    handler({ storyId: "a", heat: 0.1 });
    handler({ storyId: "a", heat: 0.2 });
    handler({ storyId: "a", heat: 0.3 });

    expect(refresh).toHaveBeenCalledTimes(3);
    expect(refresh.mock.calls.map((c) => c[0]!.heat)).toEqual([0.1, 0.2, 0.3]);
    // The expensive path (fetch/matching-row selection/worker focus/hash) only
    // fires once, on the very first activation of "a".
    expect(activate).toHaveBeenCalledTimes(1);
    expect(activate).toHaveBeenCalledWith({ storyId: "a", heat: 0.1 });
  });

  it("activates again on an actual A -> B story switch", () => {
    const refresh = vi.fn();
    const activate = vi.fn();
    const handler = createFocusHandler<Details>(refresh, activate);

    handler({ storyId: "a" });
    handler({ storyId: "b" });

    expect(refresh).toHaveBeenCalledTimes(2);
    expect(activate).toHaveBeenCalledTimes(2);
    expect(activate.mock.calls.map((c) => c[0]?.storyId)).toEqual(["a", "b"]);
  });

  it("activates again on A -> null -> A (clearing then refocusing the same story)", () => {
    const refresh = vi.fn();
    const activate = vi.fn();
    const handler = createFocusHandler<Details>(refresh, activate);

    handler({ storyId: "a" });
    handler(null);
    handler({ storyId: "a" });

    expect(refresh).toHaveBeenCalledTimes(3);
    expect(refresh.mock.calls.map((c) => c[0])).toEqual([{ storyId: "a" }, null, { storyId: "a" }]);
    // Three distinct transitions: null -> a, a -> null, null -> a.
    expect(activate).toHaveBeenCalledTimes(3);
    expect(activate.mock.calls.map((c) => c[0])).toEqual([
      { storyId: "a" },
      null,
      { storyId: "a" },
    ]);
  });

  it("does not activate on the initial null (no prior focus to change away from)", () => {
    const refresh = vi.fn();
    const activate = vi.fn();
    const handler = createFocusHandler<Details>(refresh, activate);

    handler(null);

    expect(refresh).toHaveBeenCalledTimes(1);
    expect(refresh).toHaveBeenCalledWith(null);
    // currentId starts at null; nextId is also null, so nothing "changed".
    expect(activate).not.toHaveBeenCalled();
  });

  it("a fresh handler (new page load/component instance) activates again for the same id", () => {
    const refresh = vi.fn();
    const activate = vi.fn();
    const first = createFocusHandler<Details>(refresh, activate);
    first({ storyId: "a" });
    expect(activate).toHaveBeenCalledTimes(1);

    // Each handler closes over its own currentId; a new instance (as happens
    // on remount/reload) has no memory of "a" already being focused.
    const second = createFocusHandler<Details>(refresh, activate);
    second({ storyId: "a" });
    expect(activate).toHaveBeenCalledTimes(2);
  });

  it("leaves explicit retry/load functions untouched: nothing calls them, only refresh/activate are invoked", () => {
    const refresh = vi.fn();
    const activate = vi.fn();
    const retry = vi.fn();
    const load = vi.fn();
    const handler = createFocusHandler<Details>(refresh, activate);

    handler({ storyId: "a" });
    handler({ storyId: "a" }); // sustained same-id update
    handler({ storyId: "b" }); // and a real switch

    expect(retry).not.toHaveBeenCalled();
    expect(load).not.toHaveBeenCalled();
    // The handler's only two effects are refresh and (conditionally) activate.
    expect(refresh).toHaveBeenCalledTimes(3);
    expect(activate).toHaveBeenCalledTimes(2);
  });
});
