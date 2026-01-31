import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const base = process.env.VITE_BASE || "/";

export default defineConfig({
  base,
  plugins: [react()],
  esbuild: {
    loader: "jsx",
    include: /src\/.*\.jsx?$/,
  },
  server: {
    port: 3000,
    proxy: {
      "/api": {
        target: "http://localhost:5000",
        changeOrigin: true,
      },
    },
  },
});
