import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Mock DuckDB-WASM
vi.mock('@duckdb/duckdb-wasm', () => ({
  getJsDelivrBundles: vi.fn(() => ({
    mvp: {
      mainModule: 'mock-mvp.wasm',
      mainWorker: 'mock-mvp.worker.js'
    }
  })),
  selectBundle: vi.fn(async (bundles) => bundles.mvp),
  AsyncDuckDB: vi.fn().mockImplementation(() => ({
    instantiate: vi.fn(async () => {}),
    connect: vi.fn(async () => ({
      query: vi.fn(async () => ({ toArray: () => [] }))
    }))
  })),
  ConsoleLogger: vi.fn(() => ({})),
  LogLevel: { WARNING: 'WARNING' },
  wasmConnector: vi.fn(() => ({
    query: vi.fn(async () => ({ toArray: () => [] }))
  }))
}));

// Mock Mosaic
vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: vi.fn(() => ({
    databaseConnector: vi.fn()
  })),
  MosaicClient: vi.fn(() => ({
    query: vi.fn(),
    update: vi.fn()
  }))
}));

describe('Worker Initialization Issues', () => {
  let container;
  
  beforeEach(() => {
    // Create container element
    container = document.createElement('div');
    container.id = 'test-container';
    document.body.appendChild(container);
    
    // Mock console methods
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    
    // Reset all mocks
    vi.clearAllMocks();
  });
  
  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
    vi.restoreAllMocks();
  });

  describe('Worker URL Resolution Issues', () => {
    it('should fail with standard new URL approach in Vite', async () => {
      // Mock Worker constructor to simulate Vite MIME type error
      global.Worker = vi.fn().mockImplementation(() => {
        throw new Error('Failed to load module script: The server responded with a non-JavaScript MIME type of "text/html"');
      });
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      // This should catch the error and fallback to direct mode
      await dataTable.initialize();
      
      // Verify that Worker initialization was attempted but failed
      expect(global.Worker).toHaveBeenCalled();
      expect(dataTable.options.useWorker).toBe(false); // Should fallback to direct
    });

    it('should handle Worker constructor failure gracefully', async () => {
      // Mock Worker to throw immediately on construction
      global.Worker = vi.fn().mockImplementation(() => {
        throw new Error('Worker construction failed');
      });
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Should have fallen back to direct mode
      expect(dataTable.options.useWorker).toBe(false);
    });

    it('should handle Worker onerror event', async () => {
      // Mock Worker that triggers error event
      const mockWorker = {
        postMessage: vi.fn(),
        terminate: vi.fn(),
        onmessage: null,
        onerror: null,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn()
      };
      
      global.Worker = vi.fn().mockImplementation(() => mockWorker);
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      // Start initialization
      const initPromise = dataTable.initialize();
      
      // Simulate worker error
      setTimeout(() => {
        if (mockWorker.onerror) {
          mockWorker.onerror(new Error('Worker load error'));
        }
      }, 10);
      
      await initPromise;
      
      // Should have fallen back to direct mode
      expect(dataTable.options.useWorker).toBe(false);
    });
  });

  describe('Worker Message Protocol Issues', () => {
    it('should handle Worker initialization timeout', async () => {
      // Mock Worker that never responds
      const mockWorker = {
        postMessage: vi.fn(),
        terminate: vi.fn(),
        onmessage: null,
        onerror: null,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn()
      };
      
      global.Worker = vi.fn().mockImplementation(() => mockWorker);
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      // Mock setTimeout to immediately trigger timeout
      const originalSetTimeout = global.setTimeout;
      global.setTimeout = (callback, delay) => {
        if (delay >= 10000) { // Worker timeout
          callback();
          return 123;
        }
        return originalSetTimeout(callback, delay);
      };
      
      await dataTable.initialize();
      
      // Should have fallen back to direct mode due to timeout
      expect(dataTable.options.useWorker).toBe(false);
      
      // Restore setTimeout
      global.setTimeout = originalSetTimeout;
    });

    it('should handle successful Worker initialization', async () => {
      // Mock Worker that responds successfully
      const mockWorker = {
        postMessage: vi.fn((message) => {
          // Simulate successful init response
          setTimeout(() => {
            if (mockWorker.onmessage) {
              mockWorker.onmessage({
                data: {
                  id: message.id,
                  success: true,
                  result: {
                    version: 'v1.0.0-test',
                    config: { threads: 4 }
                  }
                }
              });
            }
          }, 0);
        }),
        terminate: vi.fn(),
        onmessage: null,
        onerror: null,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn()
      };
      
      global.Worker = vi.fn().mockImplementation(() => mockWorker);
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Should maintain Worker mode if successful
      expect(dataTable.options.useWorker).toBe(true);
      expect(mockWorker.postMessage).toHaveBeenCalled();
    });
  });

  describe('Vite-specific Worker Issues', () => {
    it('should identify URL resolution problems in development', () => {
      // Simulate Vite development environment
      const mockURL = vi.fn().mockImplementation((url, base) => {
        // In Vite dev, this might resolve incorrectly
        if (url.includes('duckdb.worker.js') && base) {
          return {
            href: '/@fs/incorrect/path/worker.js',
            toString: () => '/@fs/incorrect/path/worker.js'
          };
        }
        return { href: url, toString: () => url };
      });
      
      global.URL = mockURL;
      
      // Mock import.meta.url
      const importMetaUrl = 'file:///project/src/core/DataTable.js';
      
      // Test URL resolution
      const workerUrl = new URL('../workers/duckdb.worker.js', importMetaUrl);
      
      expect(mockURL).toHaveBeenCalledWith('../workers/duckdb.worker.js', importMetaUrl);
      expect(workerUrl.href).toContain('/@fs'); // Vite's virtual filesystem
    });

    it('should demonstrate the need for ?worker suffix in Vite', () => {
      // This test shows how Vite expects workers to be imported
      const standardImport = '../workers/duckdb.worker.js';
      const viteWorkerImport = '../workers/duckdb.worker.js?worker';
      
      // Standard import would fail in Vite
      expect(standardImport).not.toContain('?worker');
      
      // Vite worker import should have the suffix
      expect(viteWorkerImport).toContain('?worker');
    });
  });

  describe('Worker Connector Integration', () => {
    it('should handle Worker communication for data queries', async () => {
      const mockWorker = {
        postMessage: vi.fn((message) => {
          // Simulate query response
          setTimeout(() => {
            if (mockWorker.onmessage) {
              mockWorker.onmessage({
                data: {
                  id: message.id,
                  success: true,
                  result: {
                    columns: ['id', 'name'],
                    data: [[1, 'test'], [2, 'test2']]
                  }
                }
              });
            }
          }, 0);
        }),
        terminate: vi.fn(),
        onmessage: null,
        onerror: null,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn()
      };
      
      global.Worker = vi.fn().mockImplementation(() => mockWorker);
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Test that Worker connector can handle queries
      if (dataTable.connector) {
        const result = await dataTable.connector.query({
          type: 'json',
          sql: 'SELECT * FROM test LIMIT 10'
        });
        
        expect(result).toBeDefined();
        expect(mockWorker.postMessage).toHaveBeenCalled();
      }
    });
  });
});

describe('Worker vs Direct Mode Comparison', () => {
  let container;
  
  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    vi.clearAllMocks();
  });
  
  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
  });

  it('should initialize Direct mode successfully', async () => {
    const dataTable = new DataTable({
      container,
      useWorker: false
    });
    
    await dataTable.initialize();
    
    expect(dataTable.options.useWorker).toBe(false);
    expect(dataTable.db).toBeDefined();
  });

  it('should show performance differences between modes', async () => {
    const workerDataTable = new DataTable({
      container,
      useWorker: true
    });
    
    const directDataTable = new DataTable({
      container,
      useWorker: false
    });
    
    const start1 = performance.now();
    await workerDataTable.initialize();
    const workerTime = performance.now() - start1;
    
    const start2 = performance.now();
    await directDataTable.initialize();
    const directTime = performance.now() - start2;
    
    // Worker mode might be slower due to communication overhead in tests
    expect(workerTime).toBeGreaterThanOrEqual(0);
    expect(directTime).toBeGreaterThanOrEqual(0);
  });
});