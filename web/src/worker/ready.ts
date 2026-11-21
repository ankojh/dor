import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { Category, StorySchema } from "../gen/dor/v1/dor_pb";
import type { WorkerReady } from "./messages";

/**
 * Builds the worker's ready message. It round-trips a generated protobuf type
 * so a broken codegen or runtime fails at worker startup, not at first frame.
 */
export function buildReadyMessage(): WorkerReady {
  const story = create(StorySchema, {
    id: "worker-probe",
    title: "worker probe",
    category: Category.OTHER,
    sourceCount: 1,
    heat: 0.5,
    firstSeen: 1_700_000_000n,
    updatedAt: 1_700_000_001n,
  });
  const bytes = toBinary(StorySchema, story);
  const decoded = fromBinary(StorySchema, bytes);
  return {
    type: "ready",
    protoStoryId: decoded.id,
    storyBytes: bytes.length,
    category: Category[decoded.category] ?? "UNKNOWN",
  };
}
