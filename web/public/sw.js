/* Dor alert push service worker. Registered only after the user clicks
 * "Enable push"; in-app alerts work without it. */
self.addEventListener("push", (event) => {
  let payload = {};
  try {
    payload = event.data ? event.data.json() : {};
  } catch {
    payload = {};
  }
  const title = payload.title || "Dor alert";
  const options = {
    body: payload.body || payload.prompt || "",
    tag: payload.tag || `dor-alert:${payload.alert_id || "unknown"}:${payload.story_id || "unknown"}`,
    data: {
      url:
        payload.url ||
        `/?story=${encodeURIComponent(payload.story_id || "")}&alert=${encodeURIComponent(payload.alert_id || "")}`,
      alert_id: payload.alert_id,
      story_id: payload.story_id,
      prompt: payload.prompt,
      level: payload.level,
      fired_at: payload.fired_at,
    },
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const url = event.notification.data?.url || "/";
  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((windows) => {
      for (const client of windows) {
        if ("focus" in client) {
          client.navigate(url);
          return client.focus();
        }
      }
      return clients.openWindow(url);
    }),
  );
});
