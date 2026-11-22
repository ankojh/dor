import { TtlLruCache } from "./text-cache";

export type BriefProvenance = "generated" | "source_snippet" | "none";

export type StoryText = {
  loading: boolean;
  available: boolean;
  title?: string;
  brief?: string;
  provenance?: BriefProvenance;
  error?: string;
};

/**
 * Lazy story briefs: one request per story, a short negative cache for a
 * missing Editor brief, and a retry that belongs to the focused story only.
 *
 * Completion is applied by story identity rather than a global request token.
 * Returning to a story whose request is still in flight must not strand it at
 * "Generating brief", and an inactive story's late miss must not replace the
 * focused story's retry timer.
 */
export class BriefLoader {
  private readonly cache = new TtlLruCache<string, StoryText>();
  private readonly inflight = new Set<string>();
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private disposed = false;

  constructor(
    private readonly fetchBrief: (storyId: string) => Promise<StoryText>,
    private readonly focusedStoryId: () => string | null,
    private readonly show: (storyId: string, text: StoryText) => void,
    private readonly retryMs: number,
  ) {}

  get size(): number {
    return this.cache.size;
  }

  cached(storyId: string): StoryText | undefined {
    return this.cache.get(storyId);
  }

  /** Put the cached value (or a loading placeholder) into the panel now. */
  showCached(storyId: string): void {
    this.show(storyId, this.cache.get(storyId) ?? { loading: true, available: false });
  }

  async load(storyId: string): Promise<void> {
    if (this.disposed) return;
    const cached = this.cache.get(storyId);
    if (cached) {
      if (this.focusedStoryId() === storyId) this.show(storyId, cached);
      // A missing brief is cached briefly so a later Editor result is picked
      // up by a retry instead of being remembered as permanently unavailable.
      if (!cached.available) this.scheduleRetry(storyId);
      return;
    }
    if (this.inflight.has(storyId)) return;
    this.inflight.add(storyId);
    if (this.focusedStoryId() === storyId) this.show(storyId, { loading: true, available: false });
    let next: StoryText;
    try {
      next = await this.fetchBrief(storyId);
    } catch (error) {
      next = {
        loading: false,
        available: false,
        error: String(error),
      };
    } finally {
      this.inflight.delete(storyId);
    }
    if (this.disposed) return;
    if (next.available) this.cache.set(storyId, next);
    else this.cache.set(storyId, next, this.retryMs);
    if (this.focusedStoryId() === storyId) this.show(storyId, next);
    if (!next.available) this.scheduleRetry(storyId);
  }

  private scheduleRetry(storyId: string): void {
    // A response for an inactive story must not replace the focused story's
    // retry; the focus handler reloads this story (from cache) on return.
    if (this.focusedStoryId() !== storyId) return;
    if (this.retryTimer !== null) clearTimeout(this.retryTimer);
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      if (this.focusedStoryId() === storyId) void this.load(storyId);
    }, this.retryMs);
  }

  dispose(): void {
    this.disposed = true;
    if (this.retryTimer !== null) clearTimeout(this.retryTimer);
    this.retryTimer = null;
  }
}
