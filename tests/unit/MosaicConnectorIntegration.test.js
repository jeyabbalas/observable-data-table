import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// Mock DuckDB-WASM and Mosaic before importing
vi.mock('@duckdb/duckdb-wasm', () => ({
  AsyncDuckDB: vi.fn().mockImplementation(() => ({
    instantiate: vi.fn().mockResolvedValue(),
    connect: vi.fn().mockResolvedValue({
      query: vi.fn().mockResolvedValue({
        toArray: vi.fn().mockReturnValue([])
      })
    })
  })),
  ConsoleLogger: vi.fn(),
  LogLevel: { WARNING: 'WARNING' },
  selectBundle: vi.fn().mockResolvedValue({
    mainModule: 'mock-module',
    mainWorker: 'mock-worker',
    pthreadWorker: 'mock-pthread'
  }),
  getJsDelivrBundles: vi.fn().mockReturnValue({})
}));

vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: vi.fn().mockImplementation(() => ({
    databaseConnector: vi.fn(),
    query: vi.fn(),
    requestQuery: vi.fn(),
    connect: vi.fn()
  })),
  wasmConnector: vi.fn().mockImplementation((options) => ({
    options,
    query: vi.fn().mockResolvedValue([])
  })),
  MosaicClient: class MockMosaicClient {
    constructor() {}
  },
  Selection: {
    crossfilter: vi.fn(() => ({}))
  }
}));

vi.mock('@uwdata/mosaic-sql', () => ({
  Query: {
    from: vi.fn(() => ({
      select: vi.fn(() => ({
        where: vi.fn(() => ({
          orderby: vi.fn(() => ({
            limit: vi.fn(() => ({
              offset: vi.fn(() => ({}))
            }))
          }))
        }))
      }))
    }))
  }
}));

vi.mock('@preact/signals-core', () => ({
  signal: vi.fn((initial) => ({
    value: initial,
    subscribe: vi.fn(),
    peek: vi.fn(() => initial)
  }))
}));

import { DataTable } from '../../src/core/DataTable.js';

describe('Mosaic Connector Integration - Direct Mode', () => {
  let dataTable;
  let mockContainer;

  beforeEach(async () => {
    mockContainer = document.createElement('div');
    document.body.appendChild(mockContainer);
    
    // Reset all mocks and restore default implementations
    vi.clearAllMocks();
    
    // Restore the wasmConnector mock to default working state
    const { wasmConnector } = await import('@uwdata/mosaic-core');
    wasmConnector.mockImplementation((options) => ({
      options,
      query: vi.fn().mockResolvedValue([])
    }));
    
    global.Worker = vi.fn().mockImplementation(() => ({
      postMessage: vi.fn(),
      terminate: vi.fn(),
      addEventListener: vi.fn(),
      onerror: null
    }));
    
    global.URL = {
      createObjectURL: vi.fn().mockReturnValue('mock-blob-url'),
      revokeObjectURL: vi.fn()
    };
    
    global.Blob = vi.fn().mockImplementation(() => ({}));
  });

  afterEach(async () => {
    if (dataTable) {
      await dataTable.destroy();
    }
    if (mockContainer.parentNode) {
      mockContainer.parentNode.removeChild(mockContainer);
    }
    vi.clearAllMocks();
  });

  describe('Direct Mode Initialization', () => {
    it('should initialize with wasmConnector in direct mode', async () => {
      const { wasmConnector } = await import('@uwdata/mosaic-core');
      
      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false // Force direct mode
      });

      await dataTable.initialize();

      // Verify wasmConnector was called with both duckdb and connection
      expect(wasmConnector).toHaveBeenCalledWith({
        duckdb: expect.anything(),
        connection: expect.anything()
      });

      // Verify coordinator received the connector
      expect(dataTable.coordinator.databaseConnector).toHaveBeenCalledWith(
        expect.objectContaining({
          query: expect.any(Function)
        })
      );
    });

    it('should pass DuckDB instance and connection to wasmConnector', async () => {
      const { wasmConnector } = await import('@uwdata/mosaic-core');
      
      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      const connectorCall = wasmConnector.mock.calls[0][0];
      
      expect(connectorCall).toHaveProperty('duckdb');
      expect(connectorCall).toHaveProperty('connection');
      expect(connectorCall.duckdb).toBeDefined();
      expect(connectorCall.connection).toBeDefined();
    });

    it('should handle wasmConnector initialization errors', async () => {
      const { wasmConnector } = await import('@uwdata/mosaic-core');
      wasmConnector.mockImplementation(() => {
        throw new Error('Connector initialization failed');
      });

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await expect(dataTable.initialize()).rejects.toThrow();
    });
  });

  describe('Connector Query Interface', () => {
    beforeEach(async () => {
      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });
      await dataTable.initialize();
    });

    it('should create connector with query method', () => {
      expect(dataTable.connector).toBeDefined();
      expect(typeof dataTable.connector.query).toBe('function');
    });

    it('should handle query requests in correct format', async () => {
      const mockQuery = {
        type: 'json',
        sql: 'SELECT * FROM test_table LIMIT 10'
      };

      // Call the connector's query method directly
      await dataTable.connector.query(mockQuery);

      // Verify the connector received the query in the correct format
      expect(dataTable.connector.query).toHaveBeenCalledWith(mockQuery);
    });

    it('should handle exec type queries', async () => {
      const mockExecQuery = {
        type: 'exec',
        sql: 'CREATE TABLE test AS SELECT 1'
      };

      const result = await dataTable.connector.query(mockExecQuery);

      // exec type queries should return undefined
      expect(result).toBeUndefined();
    });

    it('should handle json type queries', async () => {
      const mockJsonQuery = {
        type: 'json',
        sql: 'SELECT * FROM test_table'
      };

      const result = await dataTable.connector.query(mockJsonQuery);

      // json type queries should return array
      expect(Array.isArray(result)).toBe(true);
    });
  });

  describe('Coordinator Integration', () => {
    beforeEach(async () => {
      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });
      await dataTable.initialize();
    });

    it('should set connector on coordinator', () => {
      expect(dataTable.coordinator.databaseConnector).toHaveBeenCalledWith(
        dataTable.connector
      );
    });

    it('should handle coordinator query requests', async () => {
      const mockSQL = 'SELECT COUNT(*) FROM data';
      
      // Mock the coordinator's query method to call the connector
      dataTable.coordinator.query.mockImplementation(async (query, options) => {
        const { type = 'json' } = options || {};
        return await dataTable.connector.query({
          type,
          sql: query.toString()
        });
      });

      const result = await dataTable.coordinator.query(mockSQL);

      expect(dataTable.coordinator.query).toHaveBeenCalledWith(mockSQL);
      expect(Array.isArray(result)).toBe(true);
    });
  });

  describe('Error Handling', () => {
    it('should handle DuckDB connection errors', async () => {
      const { AsyncDuckDB } = await import('@duckdb/duckdb-wasm');
      
      AsyncDuckDB.mockImplementation(() => ({
        instantiate: vi.fn().mockRejectedValue(new Error('DuckDB initialization failed')),
        connect: vi.fn()
      }));

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await expect(dataTable.initialize()).rejects.toThrow('Direct DuckDB initialization failed');
    });

    it('should handle connector query errors', async () => {
      const { wasmConnector } = await import('@uwdata/mosaic-core');
      
      wasmConnector.mockImplementation(() => ({
        query: vi.fn().mockRejectedValue(new Error('Query execution failed'))
      }));

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      await expect(dataTable.connector.query({
        type: 'json',
        sql: 'SELECT * FROM nonexistent'
      })).rejects.toThrow('Query execution failed');
    });
  });

  describe('Bundle Selection', () => {
    it('should select appropriate DuckDB bundle', async () => {
      const { selectBundle } = await import('@duckdb/duckdb-wasm');
      
      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      expect(selectBundle).toHaveBeenCalled();
      
      // Should capture bundle type for performance tracking
      expect(dataTable.performance.bundleType).toBeDefined();
    });

    it('should handle bundle selection errors gracefully', async () => {
      const { selectBundle } = await import('@duckdb/duckdb-wasm');
      selectBundle.mockRejectedValue(new Error('Bundle selection failed'));

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await expect(dataTable.initialize()).rejects.toThrow();
    });
  });

  describe('Performance Tracking', () => {
    it('should track DuckDB version', async () => {
      const mockConnection = {
        query: vi.fn().mockResolvedValue({
          toArray: vi.fn().mockReturnValue([{ version: 'v1.1.1' }])
        })
      };

      const { AsyncDuckDB } = await import('@duckdb/duckdb-wasm');
      AsyncDuckDB.mockImplementation(() => ({
        instantiate: vi.fn().mockResolvedValue(),
        connect: vi.fn().mockResolvedValue(mockConnection)
      }));

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      expect(dataTable.performance.duckdbVersion).toBe('v1.1.1');
    });

    it('should track memory usage if available', async () => {
      global.performance = {
        memory: {
          usedJSHeapSize: 77 * 1024 * 1024,
          totalJSHeapSize: 116 * 1024 * 1024
        }
      };

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      expect(dataTable.performance.memoryUsage).toEqual({
        used: '77MB',
        total: '116MB'
      });
    });
  });

  describe('Connector Lifecycle', () => {
    it('should properly initialize connector after DuckDB setup', async () => {
      const initOrder = [];
      
      const { AsyncDuckDB } = await import('@duckdb/duckdb-wasm');
      const { wasmConnector } = await import('@uwdata/mosaic-core');
      
      AsyncDuckDB.mockImplementation(() => ({
        instantiate: vi.fn().mockImplementation(async () => {
          initOrder.push('duckdb-instantiate');
        }),
        connect: vi.fn().mockImplementation(async () => {
          initOrder.push('duckdb-connect');
          return {
            query: vi.fn().mockResolvedValue({
              toArray: vi.fn().mockReturnValue([])
            })
          };
        })
      }));

      wasmConnector.mockImplementation((options) => {
        initOrder.push('wasm-connector');
        return { query: vi.fn() };
      });

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      // Verify correct initialization order
      expect(initOrder).toEqual([
        'duckdb-instantiate',
        'duckdb-connect',
        'wasm-connector'
      ]);
    });
  });
});