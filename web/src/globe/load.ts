/** Request the map chunk only after the shell has made a contentful paint. */
export async function afterShellPaint(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return;
  if (
    typeof PerformanceObserver !== "undefined" &&
    PerformanceObserver.supportedEntryTypes.includes("paint")
  ) {
    if (performance.getEntriesByName("first-contentful-paint").length) return;
    await new Promise<void>((resolve) => {
      const done = (): void => {
        clearTimeout(fallback);
        observer.disconnect();
        signal.removeEventListener("abort", done);
        resolve();
      };
      const observer = new PerformanceObserver((list) => {
        if (list.getEntries().some((entry) => entry.name === "first-contentful-paint")) done();
      });
      // Some embedded browsers advertise Paint Timing without delivering an
      // entry. Keep that missing signal from blocking the map and data socket.
      const fallback = setTimeout(done, 1_000);
      signal.addEventListener("abort", done, { once: true });
      observer.observe({ type: "paint", buffered: true });
    });
  } else {
    // Paint Timing is unavailable in some engines. Yield through a paint and
    // then a task; no engine import occurs in the initial render task.
    await new Promise<void>((resolve) =>
      requestAnimationFrame(() => requestAnimationFrame(() => setTimeout(resolve, 0))),
    );
  }
}
