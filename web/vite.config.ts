import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Proxy the service APIs so the browser stays same-origin (the Go services
// have no CORS handling, by design — see CLAUDE.md).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api/auth': { target: 'http://localhost:8000', changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/auth/, '') },
      '/api/ride': { target: 'http://localhost:8001', changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/ride/, '') },
      '/api/driver': { target: 'http://localhost:8003', changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/driver/, '') },
    },
  },
})
