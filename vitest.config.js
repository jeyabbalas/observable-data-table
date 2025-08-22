import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./tests/setup.js'],
    testTimeout: 10000, // 10 second timeout to prevent hanging
    hookTimeout: 30000, // 30 second timeout for beforeEach/afterEach
    pool: 'forks', // Prevent test interference
    isolate: true, // Ensure test isolation
    sequence: {
      concurrent: false // Run integration tests sequentially
    }
  },
  resolve: {
    alias: {
      '@datatable/core': resolve(__dirname, 'src/index.js')
    }
  }
});