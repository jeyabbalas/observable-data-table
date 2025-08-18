import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./tests/setup.js']
  },
  resolve: {
    alias: {
      '@datatable/core': resolve(__dirname, 'src/index.js')
    }
  }
});