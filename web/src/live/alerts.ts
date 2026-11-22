import { LruCache } from "../worker/lru";
import type { AlertHit } from "../gen/dor/v1/dor_pb";

export type AlertToast = Pick<
  AlertHit,
  "alertId" | "storyId" | "level" | "title" | "prompt" | "generation" | "approx" | "sourceCount"
> & { firedAt: number };
export type AlertHitRecord = {
  alert_id: string;
  story_id: string;
  level: string;
  fired_at: number;
  title: string;
  prompt: string;
  /** New HTTP responses use decimal strings; old responses may omit it or use safe numbers. */
  generation?: string | bigint | number;
  approx?: boolean;
  source_count?: number;
};
export type IncomingAlert = Pick<
  AlertHit,
  "alertId" | "storyId" | "level" | "title" | "prompt" | "firedAt"
> &
  Partial<Pick<AlertHit, "generation" | "approx" | "sourceCount">>;
export function generationOf(value: AlertHitRecord["generation"]): bigint {
  if (value === undefined) return 0n;
  if (typeof value === "number" && !Number.isSafeInteger(value))
    throw new Error("imprecise alert generation");
  return BigInt(value);
}
export function alertIdentity(alertId: string, storyId: string, generation: bigint): string {
  return JSON.stringify([alertId, storyId, generation.toString()]);
}
export function approximateLabel(approx: boolean | undefined): string {
  return approx ? " · approx" : "";
}
export function historyIdentity(hit: AlertHitRecord): string {
  return alertIdentity(hit.alert_id, hit.story_id, generationOf(hit.generation));
}
export function toHistory(hit: AlertToast): AlertHitRecord {
  return {
    alert_id: hit.alertId,
    story_id: hit.storyId,
    level: hit.level,
    title: hit.title,
    prompt: hit.prompt,
    fired_at: hit.firedAt,
    generation: hit.generation.toString(),
    approx: hit.approx,
    source_count: hit.sourceCount,
  };
}

/** Delivery replay is bounded independently of the four visible toasts. */
export class AlertInbox {
  private readonly delivered = new LruCache<string, true>(1000);
  push(current: AlertToast[], incoming: IncomingAlert): AlertToast[] {
    const toast: AlertToast = {
      ...incoming,
      firedAt: Number(incoming.firedAt),
      generation: incoming.generation ?? 0n,
      approx: incoming.approx ?? false,
      sourceCount: incoming.sourceCount ?? 0,
    };
    const key = alertIdentity(toast.alertId, toast.storyId, toast.generation);
    if (this.delivered.get(key)) return current;
    this.delivered.set(key, true);
    return [toast, ...current].slice(0, 4);
  }
  dismiss(current: AlertToast[], toast: AlertToast): AlertToast[] {
    const key = alertIdentity(toast.alertId, toast.storyId, toast.generation);
    return current.filter(
      (candidate) =>
        alertIdentity(candidate.alertId, candidate.storyId, candidate.generation) !== key,
    );
  }
}
