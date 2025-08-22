import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Mock DuckDB-WASM for connection lifecycle testing
let mockConnection;
let mockDb;

vi.mock('@duckdb/duckdb-wasm', () => ({
  getJsDelivrBundles: vi.fn(() => ({
    mvp: {
      mainModule: 'mock-mvp.wasm',
      mainWorker: 'mock-mvp.worker.js'
    }
  })),
  selectBundle: vi.fn(async (bundles) => bundles.mvp),
  AsyncDuckDB: vi.fn().mockImplementation(() => {
    return mockDb || {
      instantiate: vi.fn(async () => {}),
      connect: vi.fn(async () => ({})),
      terminate: vi.fn(async () => {}),
      registerFileText: vi.fn(async () => {}),
      registerFileBuffer: vi.fn(async () => {})
    };
  }),
  ConsoleLogger: vi.fn(() => ({})),
  LogLevel: { WARNING: 'WARNING' }
}));

vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: vi.fn(() => ({
    databaseConnector: vi.fn(),
    connect: vi.fn(),
    query: vi.fn(() => Promise.resolve([])),
    requestQuery: vi.fn(),
    cache: {
      clear: vi.fn()
    }
  })),
  wasmConnector: vi.fn(() => ({
    query: vi.fn(async () => ({ toArray: () => [] }))
  })),
  Selection: {
    crossfilter: vi.fn(() => ({}))
  },
  MosaicClient: class MockMosaicClient {
    constructor() {}
    initialize() {}
  }
}));

describe('DuckDB Connection Lifecycle Management', () => {
  let container;
  
  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    
    // Create fresh mocks for each test
    mockConnection = {
      query: vi.fn(async (sql) => {
        if (sql.includes('version()')) {
          return { toArray: () => [{ version: 'v1.0.0-mock' }] };
        }
        if (sql.includes('SET')) {
          return undefined; // Configuration queries return undefined
        }
        if (sql.includes('CREATE OR REPLACE TABLE')) {
          return { toArray: () => [] }; // Table creation returns empty result
        }
        if (sql.includes('PRAGMA table_info') || sql.includes('DESCRIBE')) {
          return { 
            toArray: () => [
              { column_name: 'id', column_type: 'INTEGER', null: 'NO' },
              { column_name: 'name', column_type: 'VARCHAR', null: 'NO' }
            ]
          };
        }
        if (sql.includes('SELECT COUNT(*)')) {
          return { toArray: () => [{ count: 2 }] };
        }
        if (sql.includes('SELECT')) {
          return { toArray: () => [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }] };
        }
        return { toArray: () => [] };
      }),
      close: vi.fn(async () => {}),
      send: vi.fn(async () => ({ toArray: () => [] }))
    };

    mockDb = {
      instantiate: vi.fn(async () => {}),
      connect: vi.fn(async () => mockConnection),
      terminate: vi.fn(async () => {}),
      registerFileText: vi.fn(async () => {}),
      registerFileBuffer: vi.fn(async () => {})
    };
    
    // Mock Worker environment
    global.Worker = vi.fn().mockImplementation(() => ({
      terminate: vi.fn(),
      postMessage: vi.fn(),
      onmessage: null,
      onerror: null
    }));
    
    global.URL = {
      createObjectURL: vi.fn(() => 'blob:mock-worker-url'),
      revokeObjectURL: vi.fn()
    };
    global.Blob = vi.fn();
    
    vi.clearAllMocks();
  });
  
  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
  });

  describe('Connection Creation and Configuration', () => {
    it('should create and configure DuckDB connection properly', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Verify connection was created
      expect(mockDb.connect).toHaveBeenCalled();
      expect(dataTable.conn).toBe(mockConnection);
      
      // Verify configuration was applied
      expect(mockConnection.query).toHaveBeenCalledWith("SET max_memory='512MB'");
      expect(mockConnection.query).toHaveBeenCalledWith("SET enable_object_cache='true'");
    });

    it('should handle connection creation failure gracefully', async () => {
      // Mock connect to fail
      mockDb.connect.mockRejectedValueOnce(new Error('Connection failed'));
      
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await expect(dataTable.initialize()).rejects.toThrow();
      
      // Reset mock
      mockDb.connect.mockImplementation(async () => mockConnection);
    });

    it('should skip failed configuration settings without breaking initialization', async () => {
      // Mock some config settings to fail
      mockConnection.query.mockImplementation(async (sql) => {
        if (sql.includes('max_memory')) {
          throw new Error('Setting not supported');
        }
        if (sql.includes('version()')) {
          return { toArray: () => [{ version: 'v1.0.0-mock' }] };
        }
        return undefined;
      });
      
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      // Should still initialize successfully
      await expect(dataTable.initialize()).resolves.toBeDefined();
      
      // Reset mock
      mockConnection.query.mockImplementation(async (sql) => {
        if (sql.includes('version()')) {
          return { toArray: () => [{ version: 'v1.0.0-mock' }] };
        }
        return undefined;
      });
    });
  });

  describe('Connection Reuse and Pooling', () => {
    it('should reuse existing connection for multiple queries', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      const initialConnection = dataTable.conn;
      
      // Execute multiple queries
      await dataTable.executeSQL('SELECT 1');
      await dataTable.executeSQL('SELECT 2');
      await dataTable.executeSQL('SELECT 3');
      
      // Should use same connection
      expect(dataTable.conn).toBe(initialConnection);
      expect(mockConnection.query).toHaveBeenCalledTimes(16); // 12 config + version + 3 user queries
    });

    it('should handle connection errors without losing connection reference', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      const initialConnection = dataTable.conn;
      
      // Mock query to fail once
      mockConnection.query.mockRejectedValueOnce(new Error('Query failed'));
      
      await expect(dataTable.executeSQL('INVALID QUERY')).rejects.toThrow();
      
      // Connection should still be the same
      expect(dataTable.conn).toBe(initialConnection);
      
      // Next query should work and return the mock result
      const result = await dataTable.executeSQL('SELECT 1');
      expect(result).toBeDefined();
      expect(result.toArray).toBeDefined();
    });
  });

  describe('Connection Cleanup', () => {
    it('should properly close connection when clearData is called', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Load some data first
      const csvData = 'id,name\n1,Alice\n2,Bob';
      const file = new File([csvData], 'test.csv', { type: 'text/csv' });
      await dataTable.loadData(file);
      
      await dataTable.clearData();
      
      // Connection should still exist (not closed on clearData)
      expect(dataTable.conn).toBe(mockConnection);
      expect(mockConnection.close).not.toHaveBeenCalled();
    });

    it('should close connection when DataTable is destroyed', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      await dataTable.destroy();
      
      // Connection should be closed and reference cleared
      expect(mockConnection.close).toHaveBeenCalled();
      expect(dataTable.conn).toBeNull();
    });

    it('should handle connection close errors gracefully', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Mock close to fail
      mockConnection.close.mockRejectedValueOnce(new Error('Close failed'));
      
      // Should not throw
      await expect(dataTable.destroy()).resolves.not.toThrow();
      
      expect(dataTable.conn).toBeNull();
    });
  });

  describe('Worker Mode Connection Management', () => {
    it('should manage worker and connection together in Worker mode', async () => {
      const mockWorker = {
        terminate: vi.fn(),
        postMessage: vi.fn(),
        onmessage: null,
        onerror: null
      };
      
      global.Worker = vi.fn().mockImplementation(() => mockWorker);
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      expect(dataTable.worker).toBe(mockWorker);
      expect(dataTable.conn).toBe(mockConnection);
      
      await dataTable.destroy();
      
      // Both should be cleaned up
      expect(mockWorker.terminate).toHaveBeenCalled();
      expect(mockConnection.close).toHaveBeenCalled();
      expect(dataTable.worker).toBeNull();
      expect(dataTable.conn).toBeNull();
    });

    it('should handle worker termination before connection close', async () => {
      const mockWorker = {
        terminate: vi.fn(),
        postMessage: vi.fn(),
        onmessage: null,
        onerror: null
      };
      
      global.Worker = vi.fn().mockImplementation(() => mockWorker);
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Manually terminate worker first
      dataTable.worker.terminate();
      dataTable.worker = null;
      
      // Destroy should still handle connection cleanup
      await expect(dataTable.destroy()).resolves.not.toThrow();
      
      expect(mockConnection.close).toHaveBeenCalled();
    });
  });

  describe('Connection State Validation', () => {
    it('should validate connection before executing queries', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Manually clear connection
      dataTable.conn = null;
      
      await expect(dataTable.executeSQL('SELECT 1')).rejects.toThrow('DuckDB connection not available');
    });

    it('should handle database termination before connection close', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Mock database to fail termination
      mockDb.terminate.mockRejectedValueOnce(new Error('Termination failed'));
      
      // Should still attempt connection cleanup
      await expect(dataTable.destroy()).resolves.not.toThrow();
      
      expect(mockConnection.close).toHaveBeenCalled();
      expect(dataTable.db).toBeNull();
    });
  });

  describe('Memory and Resource Management', () => {
    it('should track connection lifecycle in performance metrics', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      expect(dataTable.performance.initStartTime).toBeDefined();
      expect(dataTable.performance.initEndTime).toBeDefined();
      expect(dataTable.performance.mode).toBe('Direct');
    });

    it('should clean up all resources in correct order', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      const cleanupOrder = [];
      
      // Track cleanup order
      mockConnection.close.mockImplementation(async () => {
        cleanupOrder.push('connection');
      });
      
      mockDb.terminate.mockImplementation(async () => {
        cleanupOrder.push('database');
      });
      
      const originalTerminate = dataTable.worker.terminate;
      dataTable.worker.terminate = vi.fn(() => {
        cleanupOrder.push('worker');
        originalTerminate.call(dataTable.worker);
      });
      
      await dataTable.destroy();
      
      // Should cleanup in correct order: UI first, then connection, then database, then worker
      expect(cleanupOrder).toEqual(['connection', 'database', 'worker']);
    });
  });
});