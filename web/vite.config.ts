import { resolve } from "node:path";
import { defineConfig } from "vitest/config";
import solid from "vite-plugin-solid";

export default defineConfig({
  plugins: [solid()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/ws": {
        target: process.env.VITE_WS_PROXY_TARGET ?? "ws://127.0.0.1:8080",
        ws: true,
        changeOrigin: true,
      },
      "/api": {
        target: process.env.VITE_API_PROXY_TARGET ?? "http://127.0.0.1:8080",
        changeOrigin: true,
      },
      "/ops": {
        target: process.env.VITE_API_PROXY_TARGET ?? "http://127.0.0.1:8080",
        changeOrigin: true,
      },
    },
  },
  worker: {
    format: "es",
  },
  build: {
    target: "es2022",
    rollupOptions: {
      input: {
        main: resolve(__dirname, "index.html"),
      },
    },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
  },
});
