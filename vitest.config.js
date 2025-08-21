import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./tests/setup.js'],
    testTimeout: 10000, // 10 second timeout to prevent hanging
    pool: 'forks', // Prevent test interference
    isolate: true // Ensure test isolation
  },
  resolve: {
    alias: {
      '@datatable/core': resolve(__dirname, 'src/index.js')
    }
  }
});