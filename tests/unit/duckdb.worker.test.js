import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// Mock DuckDB-WASM for testing
vi.mock('@duckdb/duckdb-wasm', () => ({
  getJsDelivrBundles: vi.fn(() => ({
    mvp: {
      mainModule: 'mock-mvp.wasm',
      mainWorker: 'mock-mvp.worker.js'
    },
    eh: {
      mainModule: 'mock-eh.wasm', 
      mainWorker: 'mock-eh.worker.js'
    }
  })),
  selectBundle: vi.fn(async (bundles) => bundles.mvp),
  AsyncDuckDB: vi.fn().mockImplementation(() => ({
    instantiate: vi.fn(async () => {}),
    connect: vi.fn(async () => ({
      query: vi.fn(async (sql) => {
        if (sql.includes('version()')) {
          return { toArray: () => [{ version: 'v1.0.0-mock' }] };
        }
        if (sql.includes('DESCRIBE')) {
          return { 
            toArray: () => [
              { column_name: 'id', column_type: 'INTEGER', null: 'NO' },
              { column_name: 'name', column_type: 'VARCHAR', null: 'YES' }
            ] 
          };
        }
        if (sql.includes('COUNT(*)')) {
          return { toArray: () => [{ count: 100 }] };
        }
        return { toArray: () => [] };
      })
    })),
    registerFileText: vi.fn(async () => {}),
    registerFileBuffer: vi.fn(async () => {})
  })),
  ConsoleLogger: vi.fn().mockImplementation(() => ({})),
  LogLevel: {
    WARNING: 'WARNING'
  }
}));

describe('DuckDB Worker', () => {
  let worker;
  let mockSelf;

  beforeEach(async () => {
    // Reset modules to ensure clean state
    vi.resetModules();
    
    // Mock worker environment
    mockSelf = {
      postMessage: vi.fn(),
      onmessage: null
    };
    global.self = mockSelf;
    
    // Mock URL constructor for worker context
    global.URL = vi.fn().mockImplementation((url) => ({
      toString: () => url
    }));
    
    // Mock import.meta.url
    global.import = { meta: { url: 'file:///mock/worker.js' } };
  });

  afterEach(() => {
    if (worker) {
      worker.terminate?.();
    }
    vi.clearAllMocks();
  });

  it('should handle worker initialization message', async () => {
    // Import the worker module to set up handlers
    await import('../../src/workers/duckdb.worker.js');
    
    // Get the message handler that was set up
    const messageHandler = mockSelf.onmessage;
    expect(messageHandler).toBeDefined();
    
    // Create a mock message event
    const mockEvent = {
      data: {
        id: 'test-1',
        type: 'init',
        payload: {
          config: {
            'max_memory': '256MB'
          }
        }
      }
    };
    
    // Call the message handler
    await messageHandler(mockEvent);
    
    // Verify the response was sent
    expect(mockSelf.postMessage).toHaveBeenCalledWith({
      id: 'test-1',
      success: true,
      result: expect.objectContaining({
        status: 'initialized',
        version: 'v1.0.0-mock',
        config: expect.objectContaining({
          'max_memory': '256MB'
        })
      })
    });
  });

  it('should handle query execution message', async () => {
    // Import worker and get message handler
    await import('../../src/workers/duckdb.worker.js');
    const messageHandler = mockSelf.onmessage;
    
    // First initialize
    await messageHandler({
      data: { id: 'init-1', type: 'init', payload: {} }
    });
    
    // Clear previous calls
    mockSelf.postMessage.mockClear();
    
    // Then execute query
    const queryEvent = {
      data: {
        id: 'query-1',
        type: 'exec',
        payload: {
          sql: 'SELECT * FROM test_table'
        }
      }
    };
    
    await messageHandler(queryEvent);
    
    // Verify query execution response
    expect(mockSelf.postMessage).toHaveBeenCalledWith({
      id: 'query-1',
      success: true,
      result: []
    });
  });

  it('should handle CSV data loading message', async () => {
    // Import worker and get message handler
    await import('../../src/workers/duckdb.worker.js');
    const messageHandler = mockSelf.onmessage;
    
    // Initialize first
    await messageHandler({
      data: { id: 'init-1', type: 'init', payload: {} }
    });
    
    // Clear previous calls
    mockSelf.postMessage.mockClear();
    
    // Load CSV data
    const loadEvent = {
      data: {
        id: 'load-1',
        type: 'load',
        payload: {
          format: 'csv',
          data: 'name,age\\nAlice,30\\nBob,25',
          tableName: 'users',
          options: { delimiter: ',' }
        }
      }
    };
    
    await messageHandler(loadEvent);
    
    // Verify loading response
    expect(mockSelf.postMessage).toHaveBeenCalledWith({
      id: 'load-1',
      success: true,
      result: expect.objectContaining({
        status: 'loaded',
        tableName: 'users',
        schema: expect.any(Object),
        rowCount: 100,
        format: 'csv'
      })
    });
  });

  it('should handle unknown message types with error', async () => {
    // Import worker and get message handler
    await import('../../src/workers/duckdb.worker.js');
    const messageHandler = mockSelf.onmessage;
    
    const unknownEvent = {
      data: {
        id: 'unknown-1',
        type: 'unknown_type',
        payload: {}
      }
    };
    
    await messageHandler(unknownEvent);
    
    // Verify error response
    expect(mockSelf.postMessage).toHaveBeenCalledWith({
      id: 'unknown-1',
      success: false,
      error: 'Unknown message type: unknown_type',
      stack: expect.any(String)
    });
  });

  it('should handle multiple initialization calls gracefully', async () => {
    // Import worker and get message handler
    await import('../../src/workers/duckdb.worker.js');
    const messageHandler = mockSelf.onmessage;
    
    // First initialization
    await messageHandler({
      data: { id: 'init-1', type: 'init', payload: {} }
    });
    
    // Second initialization (should return already_initialized)
    await messageHandler({
      data: { id: 'init-2', type: 'init', payload: {} }
    });
    
    // Verify second init returns already_initialized
    const secondCall = mockSelf.postMessage.mock.calls.find(
      call => call[0].id === 'init-2'
    );
    
    expect(secondCall[0]).toMatchObject({
      id: 'init-2',
      success: true,
      result: {
        status: 'already_initialized'
      }
    });
  });

  it('should require initialization before other operations', async () => {
    // Import worker and get message handler
    await import('../../src/workers/duckdb.worker.js');
    const messageHandler = mockSelf.onmessage;
    
    // Try to execute query without initialization
    const queryEvent = {
      data: {
        id: 'query-1',
        type: 'exec',
        payload: { sql: 'SELECT 1' }
      }
    };
    
    await messageHandler(queryEvent);
    
    // Should return error
    expect(mockSelf.postMessage).toHaveBeenCalledWith({
      id: 'query-1',
      success: false,
      error: 'DuckDB not initialized. Call init first.',
      stack: expect.any(String)
    });
  });
});