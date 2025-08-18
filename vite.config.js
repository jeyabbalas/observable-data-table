import { defineConfig } from 'vite';
import { resolve } from 'path';

export default defineConfig({
  root: 'demo',
  server: {
    port: 3000,
    open: true
  },
  build: {
    outDir: 'dist',
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'demo/index.html')
      }
    }
  },
  optimizeDeps: {
    exclude: [
      '@duckdb/duckdb-wasm'
    ]
  },
  worker: {
    format: 'es'
  },
  resolve: {
    alias: {
      '@datatable/core': resolve(__dirname, 'src/index.js')
    }
  }
});