import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Mock the Worker constructor to test our fix
let mockWorkerCreated = false;
let workerCreateError = null;

// Mock Mosaic and DuckDB
vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: vi.fn(() => ({
    databaseConnector: vi.fn()
  })),
  MosaicClient: vi.fn(() => ({
    query: vi.fn(),
    update: vi.fn()
  }))
}));

vi.mock('@duckdb/duckdb-wasm', () => ({
  getJsDelivrBundles: vi.fn(() => ({})),
  selectBundle: vi.fn(async () => ({ mainModule: 'mock.wasm' })),
  AsyncDuckDB: vi.fn(() => ({
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

describe('Worker Mode Integration - MIME Type Fix', () => {
  let container;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    
    // Reset tracking variables
    mockWorkerCreated = false;
    workerCreateError = null;
    
    vi.clearAllMocks();
  });

  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
  });

  it('should demonstrate the original MIME type error was fixed', async () => {
    // Mock the original failing scenario
    global.Worker = vi.fn().mockImplementation((url, options) => {
      if (url.href && url.href.includes('duckdb.worker.js')) {
        // This was the original error
        throw new Error('Failed to load module script: The server responded with a non-JavaScript MIME type of "text/html"');
      }
      return {
        postMessage: vi.fn(),
        terminate: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn()
      };
    });

    const dataTable = new DataTable({
      container,
      useWorker: true
    });

    // The initialize should handle the Worker error gracefully and fallback
    await dataTable.initialize();

    // Verify that Worker creation was attempted
    expect(global.Worker).toHaveBeenCalled();
    
    // Verify that it fell back to direct mode
    expect(dataTable.options.useWorker).toBe(false);
    expect(dataTable.performance.mode).toContain('fallback');
  });

  it('should use createWorker method instead of direct Worker construction', () => {
    // Mock successful Worker creation
    const mockWorker = {
      postMessage: vi.fn(),
      terminate: vi.fn(),
      onmessage: null,
      onerror: null,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn()
    };

    global.Worker = vi.fn().mockImplementation(() => {
      mockWorkerCreated = true;
      return mockWorker;
    });

    const dataTable = new DataTable({
      container,
      useWorker: true
    });

    // Test the createWorker method directly
    const worker = dataTable.createWorker();
    
    expect(mockWorkerCreated).toBe(true);
    expect(worker).toBeDefined();
    expect(global.Worker).toHaveBeenCalledWith(
      expect.objectContaining({
        href: expect.stringContaining('duckdb.worker.js')
      }),
      expect.objectContaining({ type: 'module' })
    );
  });

  it('should handle Vite dev server URL transformation', () => {
    // Mock Vite development environment
    Object.defineProperty(window, 'location', {
      value: {
        origin: 'http://localhost:5174',
        port: '5174'
      },
      writable: true
    });

    const mockWorker = {
      postMessage: vi.fn(),
      terminate: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn()
    };

    global.Worker = vi.fn().mockImplementation((url) => {
      // Verify the URL transformation happened
      expect(url).toContain('localhost:5174/src/workers/duckdb.worker.js');
      mockWorkerCreated = true;
      return mockWorker;
    });

    const dataTable = new DataTable({
      container,
      useWorker: true
    });

    // Create worker using our fixed method
    const worker = dataTable.createWorker();

    expect(mockWorkerCreated).toBe(true);
    expect(global.Worker).toHaveBeenCalled();
  });

  it('should fallback to standard Worker creation in production', () => {
    // Mock production environment (no port or non-dev port)
    Object.defineProperty(window, 'location', {
      value: {
        origin: 'https://example.com',
        port: '80'
      },
      writable: true
    });

    const mockWorker = {
      postMessage: vi.fn(),
      terminate: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn()
    };

    global.Worker = vi.fn().mockImplementation((url) => {
      // Should use the standard URL approach
      expect(url.href).toMatch(/.*\/src\/workers\/duckdb\.worker\.js$/);
      mockWorkerCreated = true;
      return mockWorker;
    });

    const dataTable = new DataTable({
      container,
      useWorker: true
    });

    const worker = dataTable.createWorker();

    expect(mockWorkerCreated).toBe(true);
    expect(global.Worker).toHaveBeenCalled();
  });

  it('should provide better error messages with WorkerConnector improvements', async () => {
    const mockWorker = {
      postMessage: vi.fn(),
      terminate: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      onmessage: null,
      onerror: null
    };

    global.Worker = vi.fn().mockImplementation(() => mockWorker);

    const dataTable = new DataTable({
      container,
      useWorker: true
    });

    // Mock successful initialization
    await dataTable.initialize();

    if (dataTable.connector) {
      // Test error handling improvements
      dataTable.worker = null; // Simulate missing worker
      
      try {
        await dataTable.connector.query({ type: 'json', sql: 'SELECT 1' });
        expect.fail('Should have thrown error');
      } catch (error) {
        expect(error.message).toContain('Worker not initialized');
        expect(error.message).toContain('cannot execute query');
      }
    }
  });
});

describe('Worker Mode Success Scenarios', () => {
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

  it('should successfully initialize in Worker mode when Worker loads properly', async () => {
    const mockWorker = {
      postMessage: vi.fn((message) => {
        // Simulate successful init response
        setTimeout(() => {
          if (mockWorker.onmessage && message.type === 'init') {
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
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      onmessage: null,
      onerror: null
    };

    global.Worker = vi.fn().mockImplementation(() => mockWorker);

    const dataTable = new DataTable({
      container,
      useWorker: true
    });

    await dataTable.initialize();

    // Should remain in Worker mode if successful
    expect(dataTable.options.useWorker).toBe(true);
    expect(dataTable.worker).toBeDefined();
    expect(mockWorker.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'init',
        payload: expect.objectContaining({
          config: expect.any(Object)
        })
      })
    );
  });
});