import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Mock DuckDB-WASM for performance testing
const createMockDb = (queryDelay = 10) => ({
  instantiate: vi.fn(async () => {}),
  connect: vi.fn(async () => ({
    query: vi.fn(async (sql) => {
      // Simulate query execution time
      await new Promise(resolve => setTimeout(resolve, queryDelay));
      
      if (sql.includes('version()')) {
        return { toArray: () => [{ version: 'v1.0.0-mock' }] };
      }
      if (sql.includes('SET')) {
        return undefined;
      }
      if (sql.includes('CREATE TABLE')) {
        return undefined;
      }
      
      // Simulate different result sizes for performance testing
      const rowCount = sql.includes('LIMIT') ? 
        parseInt(sql.match(/LIMIT (\d+)/)?.[1] || '100') : 100;
      
      const mockData = Array.from({ length: rowCount }, (_, i) => ({
        id: i + 1,
        name: `Test User ${i + 1}`,
        value: Math.random() * 1000,
        timestamp: new Date().toISOString()
      }));
      
      return { 
        toArray: () => mockData,
        numRows: rowCount
      };
    }),
    close: vi.fn(async () => {})
  })),
  terminate: vi.fn(async () => {}),
  registerFileText: vi.fn(async () => {})
});

vi.mock('@duckdb/duckdb-wasm', () => ({
  getJsDelivrBundles: vi.fn(() => ({
    mvp: {
      mainModule: 'mock-mvp.wasm',
      mainWorker: 'mock-mvp.worker.js'
    }
  })),
  selectBundle: vi.fn(async (bundles) => bundles.mvp),
  AsyncDuckDB: vi.fn(() => createMockDb()),
  ConsoleLogger: vi.fn(() => ({})),
  LogLevel: { WARNING: 'WARNING' }
}));

vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: vi.fn(() => ({
    databaseConnector: vi.fn(),
    connect: vi.fn(),
    cache: new Map()
  })),
  MosaicClient: vi.fn().mockImplementation(() => ({
    initialize: vi.fn(),
    query: vi.fn(),
    update: vi.fn(),
    prepare: vi.fn(),
    queryResult: vi.fn(),
    queryPending: vi.fn(),
    queryError: vi.fn()
  })),
  wasmConnector: vi.fn(() => ({
    query: vi.fn(async () => ({ toArray: () => [] }))
  })),
  Selection: {
    crossfilter: vi.fn(() => ({}))
  }
}));

describe('Worker Performance Benchmarks', () => {
  let container;
  
  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    
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
    
    // Mock performance.memory for memory tests
    global.performance = {
      memory: {
        usedJSHeapSize: 50 * 1024 * 1024, // 50MB
        totalJSHeapSize: 100 * 1024 * 1024, // 100MB
        jsHeapSizeLimit: 100 * 1024 * 1024 // 100MB (make limit same as total for testing)
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

  describe('Initialization Performance', () => {
    it('should initialize Worker mode within 2 seconds', async () => {
      const start = Date.now();
      
      const dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error' // Reduce logging overhead
      });
      
      await dataTable.initialize();
      
      const initTime = Date.now() - start;
      expect(initTime).toBeLessThan(2000);
      expect(dataTable.performance.mode).toBe('Worker');
      
      await dataTable.destroy();
    });

    it('should track initialization performance metrics', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      expect(dataTable.performance.initStartTime).toBeDefined();
      expect(dataTable.performance.initEndTime).toBeDefined();
      expect(dataTable.performance.initEndTime).toBeGreaterThan(dataTable.performance.initStartTime);
      
      const initDuration = dataTable.performance.initEndTime - dataTable.performance.initStartTime;
      expect(initDuration).toBeGreaterThan(0);
      expect(initDuration).toBeLessThan(5000); // Should be under 5 seconds
      
      await dataTable.destroy();
    });
  });

  describe('Query Performance', () => {
    let dataTable;
    
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: true,
        enableCache: true
      });
      await dataTable.initialize();
    });
    
    afterEach(async () => {
      if (dataTable) {
        await dataTable.destroy();
      }
    });

    it('should execute simple queries quickly', async () => {
      const queries = [
        'SELECT 1',
        'SELECT COUNT(*) FROM test_table',
        'SELECT * FROM test_table LIMIT 10',
        'SELECT column1, column2 FROM test_table WHERE id > 100'
      ];
      
      for (const sql of queries) {
        const start = Date.now();
        await dataTable.executeSQL(sql);
        const queryTime = Date.now() - start;
        
        expect(queryTime).toBeLessThan(500); // Should complete within 500ms
      }
    });

    it('should benefit from query caching', async () => {
      const sql = 'SELECT * FROM test_table LIMIT 100';
      
      // First execution (cache miss)
      const start1 = Date.now();
      await dataTable.executeSQL(sql);
      const firstTime = Date.now() - start1;
      
      // Second execution (cache hit)
      const start2 = Date.now();
      await dataTable.executeSQL(sql);
      const secondTime = Date.now() - start2;
      
      // Cached query should be significantly faster
      expect(secondTime).toBeLessThan(firstTime * 0.5);
      
      const cacheStats = dataTable.getCacheStats();
      expect(cacheStats.hits).toBeGreaterThan(0);
    });

    it('should handle concurrent queries efficiently', async () => {
      const queries = Array.from({ length: 10 }, (_, i) => 
        `SELECT * FROM test_table WHERE id = ${i + 1}`
      );
      
      const start = Date.now();
      const results = await Promise.all(
        queries.map(sql => dataTable.executeSQL(sql))
      );
      const totalTime = Date.now() - start;
      
      expect(results).toHaveLength(10);
      expect(totalTime).toBeLessThan(2000); // All queries within 2 seconds
      
      // Check that all queries succeeded
      results.forEach(result => {
        expect(result).toBeDefined();
      });
    });
  });

  describe('Memory Management', () => {
    let dataTable;
    
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: true,
        enableCache: true
      });
      await dataTable.initialize();
    });
    
    afterEach(async () => {
      if (dataTable) {
        await dataTable.destroy();
      }
    });

    it('should monitor memory usage', () => {
      const memoryStatus = dataTable.getMemoryStatus();
      
      if (memoryStatus) {
        expect(memoryStatus).toHaveProperty('used');
        expect(memoryStatus).toHaveProperty('total');
        expect(memoryStatus).toHaveProperty('limit');
        expect(memoryStatus).toHaveProperty('usageRatio');
        expect(memoryStatus).toHaveProperty('level');
        
        expect(memoryStatus.usageRatio).toBeGreaterThanOrEqual(0);
        expect(memoryStatus.usageRatio).toBeLessThanOrEqual(1);
        expect(['normal', 'warning', 'critical']).toContain(memoryStatus.level);
      }
    });

    it('should handle memory pressure gracefully', async () => {
      // Simulate high memory usage (90% of heap limit)
      global.performance.memory.usedJSHeapSize = 90 * 1024 * 1024; // 90MB (90% of 100MB limit)
      
      const memoryStatus = dataTable.checkMemoryPressure();
      expect(memoryStatus.level).toBe('critical');
      
      // Cache should be disabled under critical pressure
      expect(dataTable.queryCache.enabled).toBe(false);
    });

    it('should adapt cache size based on memory pressure', async () => {
      const originalMaxSize = dataTable.queryCache.maxSize;
      
      // Simulate warning level memory pressure (80% of heap limit)
      global.performance.memory.usedJSHeapSize = 80 * 1024 * 1024; // 80MB (80% of 100MB limit)
      
      dataTable.checkMemoryPressure();
      
      // Cache size should be reduced
      expect(dataTable.queryCache.maxSize).toBeLessThan(originalMaxSize);
    });
  });

  describe('Stress Tests', () => {
    let dataTable;
    
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: true,
        enableCache: true,
        logLevel: 'error' // Reduce logging overhead for stress tests
      });
      await dataTable.initialize();
    });
    
    afterEach(async () => {
      if (dataTable) {
        await dataTable.destroy();
      }
    });

    it('should handle large datasets (10k rows)', async () => {
      // Mock large dataset response
      const mockDb = createMockDb(50); // 50ms query delay
      const { AsyncDuckDB } = await import('@duckdb/duckdb-wasm');
      AsyncDuckDB.mockImplementation(() => mockDb);
      
      const start = Date.now();
      const result = await dataTable.executeSQL('SELECT * FROM large_table LIMIT 10000');
      const queryTime = Date.now() - start;
      
      expect(result).toBeDefined();
      expect(queryTime).toBeLessThan(5000); // Should handle within 5 seconds
    });

    it('should handle burst of queries without memory leaks', async () => {
      const initialMemory = global.performance.memory.usedJSHeapSize;
      
      // Execute 100 queries rapidly
      const queries = Array.from({ length: 100 }, (_, i) => 
        dataTable.executeSQL(`SELECT ${i} as test_value`)
      );
      
      await Promise.all(queries);
      
      // Check that memory hasn't grown excessively
      const finalMemory = global.performance.memory.usedJSHeapSize;
      const memoryGrowth = finalMemory - initialMemory;
      
      // Memory growth should be reasonable (less than 100MB for 100 queries)
      expect(memoryGrowth).toBeLessThan(100 * 1024 * 1024);
    });

    it('should maintain performance with many cached queries', async () => {
      // Fill cache with many queries
      const cacheSize = 50;
      const queries = Array.from({ length: cacheSize }, (_, i) => 
        `SELECT ${i} as value`
      );
      
      // Execute all queries once to populate cache
      await Promise.all(queries.map(sql => dataTable.executeSQL(sql)));
      
      // Execute queries again and measure cache performance
      const start = Date.now();
      await Promise.all(queries.map(sql => dataTable.executeSQL(sql)));
      const cacheTime = Date.now() - start;
      
      // All cached queries should complete very quickly
      expect(cacheTime).toBeLessThan(100);
      
      const cacheStats = dataTable.getCacheStats();
      expect(parseInt(cacheStats.hitRate)).toBeGreaterThan(40); // >40% hit rate (more realistic)
    });

    it('should recover from connection failures', async () => {
      // Simulate connection failure
      const originalQuery = dataTable.conn.query;
      dataTable.conn.query = vi.fn().mockRejectedValueOnce(new Error('Connection lost'));
      
      // Should throw error for first query
      await expect(dataTable.executeSQL('SELECT 1')).rejects.toThrow('Connection lost');
      
      // Restore connection
      dataTable.conn.query = originalQuery;
      
      // Should work again
      const result = await dataTable.executeSQL('SELECT 1');
      expect(result).toBeDefined();
    });
  });

  describe('Cache Performance', () => {
    let dataTable;
    
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: true,
        enableCache: true,
        cacheTTL: 30000, // 30 second TTL
        cacheMaxSize: 50
      });
      await dataTable.initialize();
    });
    
    afterEach(async () => {
      if (dataTable) {
        await dataTable.destroy();
      }
    });

    it('should have efficient cache key generation', () => {
      const queries = [
        'SELECT * FROM table',
        'select * from table',
        'SELECT   *   FROM   table',
        'SELECT * FROM table LIMIT 100',
        'SELECT * FROM table LIMIT 100 OFFSET 0'
      ];
      
      const keys = queries.map(sql => 
        dataTable.queryCache.generateKey(sql, { limit: 100 })
      );
      
      // Similar queries should have similar keys (normalization)
      expect(keys[0]).toBe(keys[1]); // Case insensitive
      expect(keys[0]).toBe(keys[2]); // Whitespace normalization
      expect(keys[3]).not.toBe(keys[0]); // Different limits
    });

    it('should respect cache size limits', async () => {
      const maxSize = dataTable.queryCache.maxSize;
      
      // Execute more queries than cache can hold
      const queries = Array.from({ length: maxSize + 10 }, (_, i) => 
        `SELECT ${i} as unique_value`
      );
      
      await Promise.all(queries.map(sql => dataTable.executeSQL(sql)));
      
      // Cache should not exceed max size
      expect(dataTable.queryCache.cache.size).toBeLessThanOrEqual(maxSize);
      
      const stats = dataTable.getCacheStats();
      expect(stats.evictions).toBeGreaterThan(0);
    });

    it('should handle cache cleanup efficiently', () => {
      const cleanupStart = Date.now();
      dataTable.queryCache.cleanup();
      const cleanupTime = Date.now() - cleanupStart;
      
      // Cleanup should be very fast
      expect(cleanupTime).toBeLessThan(10);
    });
  });
});