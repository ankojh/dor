import { buildReadyMessage } from "./ready";
import type { WorkerMessage } from "./messages";

// This file is the data worker entry point (loaded as a module worker by
// App.tsx). The socket, decode loop and live state land in later tickets.
const scope = globalThis as unknown as { postMessage(message: WorkerMessage): void };
scope.postMessage(buildReadyMessage());
