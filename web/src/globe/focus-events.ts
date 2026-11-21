/** Keep streaming metadata separate from activating a different selection. */
export function createFocusHandler<T extends { storyId: string }>(
  refresh: (details: T | null) => void,
  activate: (details: T | null) => void,
): (details: T | null) => void {
  let currentId: string | null = null;
  return (details) => {
    const nextId = details?.storyId ?? null;
    const changed = nextId !== currentId;
    currentId = nextId;
    refresh(details);
    if (changed) activate(details);
  };
}
