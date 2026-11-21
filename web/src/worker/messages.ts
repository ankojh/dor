/** Messages posted by the data worker to the main thread. */
export type WorkerReady = {
  type: "ready";
  protoStoryId: string;
  storyBytes: number;
  category: string;
};

export type WorkerMessage = WorkerReady;
