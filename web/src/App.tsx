import { createSignal, onCleanup, onMount } from "solid-js";
import type { WorkerReady } from "./worker/messages";

export function App() {
  const [status, setStatus] = createSignal("starting data worker…");
  const [detail, setDetail] = createSignal("");

  onMount(() => {
    const worker = new Worker(new URL("./worker/index.ts", import.meta.url), {
      type: "module",
      name: "dor-data-worker",
    });
    worker.onmessage = (event: MessageEvent<WorkerReady>) => {
      const message = event.data;
      if (message.type === "ready") {
        setStatus("worker ready");
        setDetail(
          `proto story ${message.protoStoryId} · ${message.storyBytes} bytes · category ${message.category}`,
        );
      }
    };
    worker.onerror = (event) => {
      setStatus("worker failed");
      setDetail(event.message);
    };
    onCleanup(() => worker.terminate());
  });

  return (
    <main>
      <header>
        <span class="eyebrow">dor · shell</span>
        <h1>A live map of the world's stories</h1>
      </header>
      <section class="panel">
        <div class="row">
          <span class="label">data worker</span>
          <span class="value" data-testid="worker-status">
            {status()}
          </span>
        </div>
        <div class="row">
          <span class="label">detail</span>
          <span class="value muted" data-testid="worker-detail">
            {detail()}
          </span>
        </div>
      </section>
      <p class="foot">
        The globe and wire land in later tickets. This shell exists to prove the worker, the
        generated protocol types and the build pipeline.
      </p>
    </main>
  );
}
