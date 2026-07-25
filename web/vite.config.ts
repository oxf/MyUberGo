import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Proxy the API through Kong (the API gateway) so the browser stays
// same-origin (the Go services have no CORS handling, by design — see
// CLAUDE.md) and so requests get JWT verification + rate limiting just like
// in the containerized build. Kong does its own path-prefix stripping — see
// gateway/kong.yml — so no rewrite is needed here.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': { target: 'http://localhost:8090', changeOrigin: true },
    },
  },
})
