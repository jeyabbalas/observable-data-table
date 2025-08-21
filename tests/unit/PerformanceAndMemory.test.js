import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Mock DuckDB-WASM for testing
const mockDb = {
  instantiate: vi.fn(async () => {}),
  connect: vi.fn(async () => ({
    query: vi.fn(async (sql) => {
      if (sql.includes('version()')) {
        return { toArray: () => [{ version: 'v1.0.0-mock' }] };
      }
      if (sql.includes('COUNT(*)')) {
        return { toArray: () => [{ count: 100000 }] }; // Large dataset
      }
      return { toArray: () => [] };
    }),
    close: vi.fn(async () => {}),
    send: vi.fn(async () => ({ toArray: () => [] }))
  })),
  terminate: vi.fn(async () => {}),
  registerFileText: vi.fn(async () => {})
};

vi.mock('@duckdb/duckdb-wasm', () => ({
  getJsDelivrBundles: vi.fn(() => ({
    mvp: {
      mainModule: 'mock-mvp.wasm',
      mainWorker: 'mock-mvp.worker.js'
    }
  })),
  selectBundle: vi.fn(async (bundles) => bundles.mvp),
  AsyncDuckDB: vi.fn().mockImplementation(() => mockDb),
  ConsoleLogger: vi.fn(() => ({})),
  LogLevel: { WARNING: 'WARNING' }
}));

vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: vi.fn(() => ({
    databaseConnector: vi.fn(),
    connect: vi.fn(),
    cache: new Map()
  })),
  MosaicClient: vi.fn().mockImplementation(function() {
    this.requestQuery = vi.fn(async () => ({ toArray: () => [] }));
    this.coordinator = null;
    this.createTable = vi.fn();
    this.filterBy = vi.fn();
    this.initialize = vi.fn();
    return this;
  }),
  Selection: {
    crossfilter: vi.fn(() => ({})),
    column: vi.fn(() => ({}))
  },
  wasmConnector: vi.fn(() => ({
    query: vi.fn(async () => ({ toArray: () => [] }))
  }))
}));

describe('Performance and Memory Management', () => {
  let container;
  
  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    
    // Mock Worker and URL/Blob for worker creation
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
    
    // Setup performance.memory mock
    global.performance = {
      memory: {
        totalJSHeapSize: 1024 * 1024 * 1024, // 1GB
        usedJSHeapSize: 256 * 1024 * 1024,   // 256MB
        jsHeapSizeLimit: 2048 * 1024 * 1024  // 2GB
      },
      now: vi.fn(() => Date.now())
    };
    
    vi.clearAllMocks();
  });
  
  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
  });

  describe('Memory Configuration', () => {
    it('should use adaptive memory configuration when performance.memory is available', async () => {
      // Mock performance.memory - 2GB total, 512MB used = 1536MB available, 25% = 384MB
      global.performance = {
        memory: {
          totalJSHeapSize: 2 * 1024 * 1024 * 1024, // 2GB
          usedJSHeapSize: 512 * 1024 * 1024,      // 512MB
        },
        now: vi.fn(() => Date.now())
      };
      
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      const config = dataTable.getOptimalDuckDBConfig();
      
      // Should calculate 25% of available memory (1536MB * 0.25 = 384MB)
      // Temp directory should be 25% of max memory (384MB * 0.25 = 96MB)
      expect(config.max_memory).toBe('384MB');
      expect(config.enable_object_cache).toBe('true');
      expect(config.max_temp_directory_size).toBe('96MB');
      
      // Cleanup
      global.performance = undefined;
    });

    it('should use default memory configuration when performance.memory is not available', async () => {
      // Remove performance.memory mock to test default behavior
      global.performance = { now: vi.fn(() => Date.now()) };
      
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      const config = dataTable.getOptimalDuckDBConfig();
      
      expect(config.max_memory).toBe('512MB'); // Default
      expect(config.enable_object_cache).toBe('true');
    });
  });

  describe('Connection Lifecycle', () => {
    it('should properly cleanup connections on destroy', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      expect(dataTable.conn).toBeDefined();
      expect(dataTable.db).toBeDefined();
      
      const conn = dataTable.conn;
      const db = dataTable.db;
      
      await dataTable.destroy();
      
      expect(conn.close).toHaveBeenCalled();
      expect(db.terminate).toHaveBeenCalled();
      expect(dataTable.conn).toBeNull();
      expect(dataTable.db).toBeNull();
    });

    it('should handle connection cleanup errors gracefully', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Mock connection close to throw error
      dataTable.conn.close.mockRejectedValue(new Error('Connection close failed'));
      
      // Should not throw
      await expect(dataTable.destroy()).resolves.not.toThrow();
    });
  });

  describe('Query Performance Tracking', () => {
    it('should track query execution performance', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      await dataTable.executeSQL('SELECT COUNT(*) FROM test_table');
      
      const history = dataTable.getQueryHistory();
      
      expect(history).toHaveLength(1);
      expect(history[0]).toMatchObject({
        sql: 'SELECT COUNT(*) FROM test_table',
        timestamp: expect.any(Number),
        resultSize: expect.any(Number),
        streaming: false
      });
    });

    it('should support streaming for large queries', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Mock connection.send for streaming
      dataTable.conn.send = vi.fn(async () => ({ 
        toArray: () => new Array(100000).fill({ id: 1, name: 'test' })
      }));
      
      await dataTable.executeSQL('SELECT * FROM large_table', { streaming: true });
      
      expect(dataTable.conn.send).toHaveBeenCalled();
      
      const history = dataTable.getQueryHistory();
      expect(history[0].streaming).toBe(true);
    });
  });

  describe('Large Dataset Handling', () => {
    it('should handle large datasets without memory issues', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Mock large dataset
      const largeDataset = new Array(100000).fill().map((_, i) => ({
        id: i,
        name: `user_${i}`,
        value: Math.random() * 1000
      }));
      
      // Mock CSV data
      const csvData = 'id,name,value\n' + largeDataset.slice(0, 1000).map(
        row => `${row.id},${row.name},${row.value}`
      ).join('\n');
      
      const file = new File([csvData], 'large_data.csv', { type: 'text/csv' });
      
      // Should handle loading without errors
      await expect(dataTable.loadData(file)).resolves.toBeDefined();
    });
  });

  describe('Memory Pressure Scenarios', () => {
    it('should handle low memory scenarios gracefully', async () => {
      // Mock low memory scenario
      global.performance = {
        memory: {
          totalJSHeapSize: 100 * 1024 * 1024,  // 100MB total
          usedJSHeapSize: 80 * 1024 * 1024,    // 80MB used
        }
      };
      
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      const config = dataTable.getOptimalDuckDBConfig();
      
      // Should use minimum memory (128MB minimum)
      expect(config.max_memory).toBe('128MB');
      
      global.performance = undefined;
    });

    it('should handle high memory scenarios efficiently', async () => {
      // Mock high memory scenario
      global.performance = {
        memory: {
          totalJSHeapSize: 8 * 1024 * 1024 * 1024,  // 8GB total
          usedJSHeapSize: 1 * 1024 * 1024 * 1024,   // 1GB used
        }
      };
      
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      const config = dataTable.getOptimalDuckDBConfig();
      
      // Should cap at maximum (1024MB maximum)
      expect(config.max_memory).toBe('1024MB');
      
      global.performance = undefined;
    });
  });

  describe('Concurrent Operations', () => {
    it('should handle multiple simultaneous queries', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Execute multiple queries concurrently
      const queries = [
        'SELECT COUNT(*) FROM table1',
        'SELECT COUNT(*) FROM table2',
        'SELECT COUNT(*) FROM table3'
      ];
      
      const results = await Promise.all(
        queries.map(sql => dataTable.executeSQL(sql))
      );
      
      expect(results).toHaveLength(3);
      expect(dataTable.getQueryHistory()).toHaveLength(3);
    });
  });

  describe('Error Boundaries', () => {
    it('should handle DuckDB errors without crashing', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Mock query to throw error
      dataTable.conn.query.mockRejectedValueOnce(new Error('SQL syntax error'));
      
      await expect(dataTable.executeSQL('INVALID SQL')).rejects.toThrow('SQL syntax error');
      
      // DataTable should still be functional
      expect(dataTable.conn).toBeDefined();
    });

    it('should handle worker termination gracefully', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Mock worker to fail
      if (dataTable.worker) {
        dataTable.worker.terminate();
      }
      
      // Should handle gracefully
      await expect(dataTable.destroy()).resolves.not.toThrow();
    });
  });
});