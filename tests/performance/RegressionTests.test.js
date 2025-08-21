import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Performance thresholds (in milliseconds)
const PERFORMANCE_THRESHOLDS = {
  initialization: 3000,      // 3 seconds max for initialization
  simpleQuery: 200,         // 200ms max for simple queries
  dataLoad: 5000,           // 5 seconds max for small data loads
  cacheHit: 50,             // 50ms max for cached queries
  memoryCheck: 10,          // 10ms max for memory pressure checks
  batchExecution: 1000      // 1 second max for batch execution
};

// Mock DuckDB with controlled timing
const createTimedMockDb = (baseDelay = 10) => ({
  instantiate: vi.fn(async () => {
    await new Promise(resolve => setTimeout(resolve, baseDelay));
  }),
  connect: vi.fn(async () => ({
    query: vi.fn(async (sql) => {
      // Simulate realistic query execution times
      let delay = baseDelay;
      
      if (sql.includes('CREATE TABLE') || sql.includes('INSERT')) {
        delay = baseDelay * 2; // DDL operations take longer
      } else if (sql.includes('SELECT COUNT(*)')) {
        delay = baseDelay * 1.5; // Aggregations take a bit longer
      }
      
      await new Promise(resolve => setTimeout(resolve, delay));
      
      if (sql.includes('version()')) {
        return { toArray: () => [{ version: 'v1.0.0-mock' }] };
      }
      if (sql.includes('SET')) {
        return undefined;
      }
      
      // Return realistic mock data
      const mockData = Array.from({ length: 10 }, (_, i) => ({
        id: i + 1,
        name: `Row ${i + 1}`,
        value: Math.random() * 100
      }));
      
      return { 
        toArray: () => mockData,
        numRows: mockData.length
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
  AsyncDuckDB: vi.fn(() => createTimedMockDb()),
  ConsoleLogger: vi.fn(() => ({})),
  LogLevel: { WARNING: 'WARNING' }
}));

vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: vi.fn(() => ({
    databaseConnector: vi.fn(),
    connect: vi.fn(),
    cache: new Map()
  })),
  wasmConnector: vi.fn(() => ({
    query: vi.fn(async () => ({ toArray: () => [] }))
  }))
}));

describe('Performance Regression Tests', () => {
  let container;
  
  beforeAll(() => {
    // Setup performance monitoring
    global.performance = {
      memory: {
        usedJSHeapSize: 50 * 1024 * 1024,
        totalJSHeapSize: 100 * 1024 * 1024,
        jsHeapSizeLimit: 2048 * 1024 * 1024
      },
      now: vi.fn(() => Date.now())
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
  });
  
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

  describe('Initialization Performance', () => {
    it('should initialize within performance threshold', async () => {
      const start = performance.now();
      
      const dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      const duration = performance.now() - start;
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.initialization);
      
      await dataTable.destroy();
    });

    it('should maintain initialization performance with all features enabled', async () => {
      const start = performance.now();
      
      const dataTable = new DataTable({
        container,
        useWorker: true,
        enableCache: true,
        enableQueryBatching: true,
        persistSession: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      const duration = performance.now() - start;
      
      // Should not be significantly slower with all features
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.initialization * 1.5);
      
      await dataTable.destroy();
    });
  });

  describe('Query Performance', () => {
    let dataTable;
    
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: true,
        enableCache: true,
        logLevel: 'error'
      });
      await dataTable.initialize();
    });
    
    afterEach(async () => {
      if (dataTable) {
        await dataTable.destroy();
      }
    });

    it('should execute simple queries within threshold', async () => {
      const queries = [
        'SELECT 1',
        'SELECT COUNT(*) FROM test',
        'SELECT * FROM test LIMIT 10'
      ];
      
      for (const sql of queries) {
        const start = performance.now();
        await dataTable.executeSQL(sql);
        const duration = performance.now() - start;
        
        expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.simpleQuery);
      }
    });

    it('should serve cached queries within threshold', async () => {
      const sql = 'SELECT * FROM test LIMIT 5';
      
      // First execution (populates cache)
      await dataTable.executeSQL(sql);
      
      // Second execution (from cache)
      const start = performance.now();
      await dataTable.executeSQL(sql);
      const duration = performance.now() - start;
      
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.cacheHit);
      
      const cacheStats = dataTable.getCacheStats();
      expect(cacheStats.hits).toBeGreaterThan(0);
    });

    it('should handle concurrent queries efficiently', async () => {
      const queries = Array.from({ length: 5 }, (_, i) => 
        `SELECT ${i} as value`
      );
      
      const start = performance.now();
      await Promise.all(queries.map(sql => dataTable.executeSQL(sql)));
      const duration = performance.now() - start;
      
      // Should complete all queries reasonably quickly
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.simpleQuery * 2);
    });
  });

  describe('Memory Management Performance', () => {
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

    it('should check memory pressure within threshold', () => {
      const start = performance.now();
      dataTable.checkMemoryPressure();
      const duration = performance.now() - start;
      
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.memoryCheck);
    });

    it('should handle memory pressure response quickly', () => {
      // Simulate high memory usage
      global.performance.memory.usedJSHeapSize = 1600 * 1024 * 1024; // 1.6GB
      
      const start = performance.now();
      dataTable.checkMemoryPressure();
      const duration = performance.now() - start;
      
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.memoryCheck * 2);
    });
  });

  describe('Caching Performance', () => {
    let dataTable;
    
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: true,
        enableCache: true,
        cacheMaxSize: 100
      });
      await dataTable.initialize();
    });
    
    afterEach(async () => {
      if (dataTable) {
        await dataTable.destroy();
      }
    });

    it('should maintain cache lookup performance', () => {
      const sql = 'SELECT * FROM test';
      
      const start = performance.now();
      const result = dataTable.queryCache.get(sql);
      const duration = performance.now() - start;
      
      expect(duration).toBeLessThan(1); // Cache lookup should be sub-millisecond
    });

    it('should maintain cache insertion performance', async () => {
      const sql = 'SELECT * FROM test';
      const result = { toArray: () => [{ id: 1 }] };
      
      const start = performance.now();
      dataTable.queryCache.set(sql, result);
      const duration = performance.now() - start;
      
      expect(duration).toBeLessThan(5); // Cache insertion should be very fast
    });

    it('should handle cache cleanup efficiently', () => {
      // Fill cache
      for (let i = 0; i < 50; i++) {
        dataTable.queryCache.set(`SELECT ${i}`, { toArray: () => [] });
      }
      
      const start = performance.now();
      dataTable.queryCache.cleanup();
      const duration = performance.now() - start;
      
      expect(duration).toBeLessThan(10);
    });
  });

  describe('Query Batching Performance', () => {
    let dataTable;
    
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: true,
        enableQueryBatching: true,
        batchWindow: 5,
        maxBatchSize: 5
      });
      await dataTable.initialize();
    });
    
    afterEach(async () => {
      if (dataTable) {
        await dataTable.destroy();
      }
    });

    it('should execute batched queries within threshold', async () => {
      const queries = Array.from({ length: 5 }, (_, i) => 
        `SELECT ${i} as batch_value`
      );
      
      const start = performance.now();
      await Promise.all(queries.map(sql => dataTable.executeSQL(sql)));
      const duration = performance.now() - start;
      
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.batchExecution);
      
      const batchStats = dataTable.getBatchingStats();
      if (batchStats) {
        expect(batchStats.batchesExecuted).toBeGreaterThan(0);
      }
    });
  });

  describe('Data Loading Performance', () => {
    let dataTable;
    
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      await dataTable.initialize();
    });
    
    afterEach(async () => {
      if (dataTable) {
        await dataTable.destroy();
      }
    });

    it('should load small CSV data within threshold', async () => {
      const csvData = 'id,name,value\n1,Test,100\n2,Test2,200';
      const file = new File([csvData], 'test.csv', { type: 'text/csv' });
      
      const start = performance.now();
      await dataTable.loadData(file);
      const duration = performance.now() - start;
      
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.dataLoad);
    });

    it('should handle data loading with progress callbacks efficiently', async () => {
      const csvData = 'id,name,value\n1,Test,100\n2,Test2,200';
      const file = new File([csvData], 'test.csv', { type: 'text/csv' });
      
      let progressCallCount = 0;
      const progressCallback = () => { progressCallCount++; };
      
      dataTable.onProgress(progressCallback);
      
      const start = performance.now();
      await dataTable.loadData(file);
      const duration = performance.now() - start;
      
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.dataLoad);
      // Progress callbacks shouldn't significantly impact performance
    });
  });

  describe('Configuration Impact', () => {
    it('should not significantly impact performance with all optimizations enabled', async () => {
      const start = performance.now();
      
      const dataTable = new DataTable({
        container,
        useWorker: true,
        enableCache: true,
        enableQueryBatching: true,
        persistSession: true,
        cacheTTL: 30000,
        cacheMaxSize: 50,
        batchWindow: 10,
        maxBatchSize: 10,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      // Execute a few operations
      await dataTable.executeSQL('SELECT 1');
      await dataTable.executeSQL('SELECT 2');
      
      const duration = performance.now() - start;
      
      // Total time should still be reasonable
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.initialization * 2);
      
      await dataTable.destroy();
    });

    it('should maintain performance with disabled optimizations', async () => {
      const start = performance.now();
      
      const dataTable = new DataTable({
        container,
        useWorker: true,
        enableCache: false,
        enableQueryBatching: false,
        persistSession: false,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      // Execute operations
      await dataTable.executeSQL('SELECT 1');
      
      const duration = performance.now() - start;
      
      expect(duration).toBeLessThan(PERFORMANCE_THRESHOLDS.initialization);
      
      await dataTable.destroy();
    });
  });

  describe('Baseline Performance Comparison', () => {
    it('should provide performance statistics', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true,
        enableCache: true,
        enableQueryBatching: true
      });
      
      await dataTable.initialize();
      
      // Execute some queries to generate stats
      await dataTable.executeSQL('SELECT 1');
      await dataTable.executeSQL('SELECT 1'); // Should hit cache
      
      const cacheStats = dataTable.getCacheStats();
      const batchStats = dataTable.getBatchingStats();
      const memoryStatus = dataTable.getMemoryStatus();
      
      // Verify stats are available
      expect(cacheStats).toBeDefined();
      expect(batchStats).toBeDefined();
      if (memoryStatus) {
        expect(memoryStatus.usageRatio).toBeGreaterThanOrEqual(0);
      }
      
      await dataTable.destroy();
    });
  });
});