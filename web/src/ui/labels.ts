/** Human-readable delivery channel names used in alert copy. */
export function channelLabel(channel: string): string {
  switch (channel) {
    case "inapp":
      return "in-app";
    case "ntfy":
      return "phone (ntfy)";
    case "push":
      return "browser push";
    default:
      return channel;
  }
}
