import { describe, expect, it } from "vitest";
import {
  AlertInbox,
  alertIdentity,
  approximateLabel,
  generationOf,
  historyIdentity,
  toHistory,
  type AlertHitRecord,
  type AlertToast,
  type IncomingAlert,
} from "./alerts";

function incoming(overrides: Partial<IncomingAlert> & { alertId: string }): IncomingAlert {
  return {
    alertId: overrides.alertId,
    storyId: overrides.storyId ?? "story-1",
    level: overrides.level ?? "new",
    title: overrides.title ?? "Title",
    prompt: overrides.prompt ?? "energy",
    firedAt: overrides.firedAt ?? 1_700_000_000n,
    generation: overrides.generation,
    approx: overrides.approx,
    sourceCount: overrides.sourceCount,
  };
}

describe("AlertInbox lifecycle", () => {
  it("treats generations 0, 1 and 2 of the same alert/story as distinct toasts", () => {
    const inbox = new AlertInbox();
    let toasts: AlertToast[] = [];
    toasts = inbox.push(toasts, incoming({ alertId: "alert-1", generation: 0n }));
    toasts = inbox.push(toasts, incoming({ alertId: "alert-1", generation: 1n }));
    toasts = inbox.push(toasts, incoming({ alertId: "alert-1", generation: 2n }));
    expect(toasts).toHaveLength(3);
    expect(toasts.map((t) => t.generation)).toEqual([2n, 1n, 0n]); // newest first
  });

  it("dedupes an exact replay of the same generation", () => {
    const inbox = new AlertInbox();
    let toasts: AlertToast[] = [];
    toasts = inbox.push(toasts, incoming({ alertId: "alert-1", generation: 1n }));
    toasts = inbox.push(toasts, incoming({ alertId: "alert-1", generation: 1n }));
    expect(toasts).toHaveLength(1);
  });

  it("dismisses one generation independently of the others", () => {
    const inbox = new AlertInbox();
    let toasts: AlertToast[] = [];
    toasts = inbox.push(toasts, incoming({ alertId: "alert-1", generation: 0n }));
    toasts = inbox.push(toasts, incoming({ alertId: "alert-1", generation: 1n }));
    toasts = inbox.dismiss(
      toasts,
      toasts.find((t) => t.generation === 0n)!,
    );
    expect(toasts).toHaveLength(1);
    expect(toasts[0]!.generation).toBe(1n);
  });

  it("does not resurrect a dismissed alert on replay (the delivered cache outlives dismissal)", () => {
    const inbox = new AlertInbox();
    let toasts: AlertToast[] = [];
    toasts = inbox.push(toasts, incoming({ alertId: "alert-1", generation: 0n }));
    toasts = inbox.dismiss(toasts, toasts[0]!);
    expect(toasts).toHaveLength(0);
    toasts = inbox.push(toasts, incoming({ alertId: "alert-1", generation: 0n })); // same replay
    expect(toasts).toHaveLength(0);
  });

  it("keeps at most 4 visible toasts, dropping the oldest first", () => {
    const inbox = new AlertInbox();
    let toasts: AlertToast[] = [];
    for (let g = 0; g < 6; g++) {
      toasts = inbox.push(toasts, incoming({ alertId: "alert-1", generation: BigInt(g) }));
    }
    expect(toasts).toHaveLength(4);
    expect(toasts.map((t) => t.generation)).toEqual([5n, 4n, 3n, 2n]);
  });

  it("defaults generation/approx/sourceCount when an old server omits them", () => {
    const inbox = new AlertInbox();
    const toasts = inbox.push([], incoming({ alertId: "alert-1" }));
    expect(toasts[0]!.generation).toBe(0n);
    expect(toasts[0]!.approx).toBe(false);
    expect(toasts[0]!.sourceCount).toBe(0);
  });
});

describe("Alert generation/identity across the uint64 boundary", () => {
  it("parses a decimal generation string above 2^53 exactly, with an exact identity match", () => {
    const big = "9007199254740993"; // 2^53 + 1, unsafe as a JS number
    const generation = generationOf(big);
    expect(generation).toBe(9007199254740993n);
    const a = alertIdentity("alert-1", "story-1", generation);
    const b = alertIdentity("alert-1", "story-1", BigInt(big));
    expect(a).toBe(b);
  });

  it("rejects an imprecise JS number that cannot represent the generation safely", () => {
    expect(() => generationOf(Number.MAX_SAFE_INTEGER + 2)).toThrow();
  });

  it("defaults to generation 0 for old servers that omit the field entirely", () => {
    expect(generationOf(undefined)).toBe(0n);
  });

  it("accepts an already-safe number or bigint generation without throwing", () => {
    expect(generationOf(3)).toBe(3n);
    expect(generationOf(3n)).toBe(3n);
  });

  it("round-trips historyIdentity through an AlertHitRecord with a generation beyond uint53", () => {
    const record: AlertHitRecord = {
      alert_id: "alert-1",
      story_id: "story-1",
      level: "new",
      fired_at: 1_700_000_000,
      title: "Title",
      prompt: "energy",
      generation: "18446744073709551615", // 2^64 - 1
    };
    expect(historyIdentity(record)).toBe(
      alertIdentity("alert-1", "story-1", 18446744073709551615n),
    );
  });
});

describe("toHistory / approximateLabel round-trip", () => {
  it("retains approx and sourceCount from the live toast into the history record", () => {
    const toast: AlertToast = {
      alertId: "alert-1",
      storyId: "story-1",
      level: "new",
      title: "Title",
      prompt: "energy",
      generation: 7n,
      approx: true,
      sourceCount: 12,
      firedAt: 1_700_000_000,
    };
    const record = toHistory(toast);
    expect(record.approx).toBe(true);
    expect(record.source_count).toBe(12);
    expect(record.generation).toBe("7"); // decimal string, safe across the wire
    expect(approximateLabel(record.approx)).toBe(" · approx");
  });

  it("produces no approx label for a non-approximate alert, or when approx is unset (old-server default)", () => {
    expect(approximateLabel(false)).toBe("");
    expect(approximateLabel(undefined)).toBe("");
  });
});
