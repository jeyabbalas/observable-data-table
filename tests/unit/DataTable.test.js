import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Mock DuckDB-WASM
const mockConnection = {
  query: vi.fn(async (sql) => {
    if (sql.includes('version()')) {
      return { toArray: () => [{ version: 'v1.0.0-mock' }] };
    }
    if (sql.includes('SELECT * FROM')) {
      return { toArray: () => [{ name: 'Alice', age: 30 }, { name: 'Bob', age: 25 }] };
    }
    return { toArray: () => [] };
  }),
  close: vi.fn(async () => {})
};

// Create the mock class that behaves like AsyncDuckDB
class MockAsyncDuckDB {
  constructor() {
    this.instantiate = vi.fn(async () => {});
    this.connect = vi.fn(async () => mockConnection);
    this.terminate = vi.fn(async () => {});
  }
}

vi.mock('@duckdb/duckdb-wasm', () => ({
  getJsDelivrBundles: vi.fn(() => ({
    mvp: {
      mainModule: 'mock-mvp.wasm',
      mainWorker: 'mock-mvp.worker.js',
      pthreadWorker: 'mock-pthread.worker.js'
    },
    eh: {
      mainModule: 'mock-eh.wasm', 
      mainWorker: 'mock-eh.worker.js',
      pthreadWorker: 'mock-pthread.worker.js'
    }
  })),
  selectBundle: vi.fn(async (bundles) => bundles.mvp),
  AsyncDuckDB: MockAsyncDuckDB,
  ConsoleLogger: vi.fn(() => ({})),
  LogLevel: { ERROR: 'ERROR', WARNING: 'WARNING' }
}));

// Mock Mosaic components
const mockCoordinator = {
  databaseConnector: vi.fn(),
  connect: vi.fn(),
  query: vi.fn(() => Promise.resolve([])),
  requestQuery: vi.fn(),
  cache: {
    clear: vi.fn()
  }
};

vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: vi.fn(() => mockCoordinator),
  wasmConnector: vi.fn(() => ({
    query: vi.fn(async () => ({ toArray: () => [] }))
  })),
  Selection: {
    crossfilter: vi.fn(() => ({}))
  },
  MosaicClient: class MockMosaicClient {
    constructor() {}
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
    })),
    sql: vi.fn(() => ({}))
  }
}));

vi.mock('@preact/signals-core', () => ({
  signal: vi.fn((initial) => ({
    value: initial,
    subscribe: vi.fn(),
    peek: vi.fn(() => initial)
  }))
}));

describe('DataTable', () => {
  let container;
  let dataTable;

  beforeEach(() => {
    // Create a mock container
    container = document.createElement('div');
    document.body.appendChild(container);
    
    // Mock Worker constructor
    global.Worker = vi.fn().mockImplementation(() => ({
      terminate: vi.fn(),
      postMessage: vi.fn(),
      onmessage: null,
      onerror: null
    }));
    
    // Mock URL and Blob for worker creation
    global.URL = {
      createObjectURL: vi.fn(() => 'blob:mock-worker-url'),
      revokeObjectURL: vi.fn()
    };
    global.Blob = vi.fn();
    
    // Mock performance.memory for memory monitoring
    global.performance = {
      memory: {
        usedJSHeapSize: 50 * 1024 * 1024, // 50MB
        totalJSHeapSize: 100 * 1024 * 1024, // 100MB  
        jsHeapSizeLimit: 200 * 1024 * 1024 // 200MB
      }
    };
    
    // Reset all mocks
    vi.clearAllMocks();
  });

  afterEach(async () => {
    // Cleanup
    if (dataTable) {
      await dataTable.destroy();
      dataTable = null;
    }
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
    
    // Restore all mocks
    vi.restoreAllMocks();
  });

  describe('Constructor', () => {
    it('should create DataTable with default options', () => {
      dataTable = new DataTable({ container });
      
      expect(dataTable.options.container).toBe(container);
      expect(dataTable.options.height).toBe(500);
      expect(dataTable.options.persistSession).toBe(false);
      expect(dataTable.options.useWorker).toBe(true);
      expect(dataTable.options.logLevel).toBe('info');
    });

    it('should create DataTable with custom options', () => {
      const options = {
        container,
        height: 600,
        persistSession: true,
        useWorker: false,
        logLevel: 'debug'
      };
      
      dataTable = new DataTable(options);
      
      expect(dataTable.options.height).toBe(600);
      expect(dataTable.options.persistSession).toBe(true);
      expect(dataTable.options.useWorker).toBe(false);
      expect(dataTable.options.logLevel).toBe('debug');
    });

    it('should setup logging with correct levels', () => {
      dataTable = new DataTable({ container, logLevel: 'error' });
      
      expect(dataTable.log).toBeDefined();
      expect(typeof dataTable.log.error).toBe('function');
      expect(typeof dataTable.log.warn).toBe('function');
      expect(typeof dataTable.log.info).toBe('function');
      expect(typeof dataTable.log.debug).toBe('function');
    });
  });

  describe('Initialization', () => {
    it('should initialize successfully with worker', async () => {
      dataTable = new DataTable({ container, useWorker: true });
      
      await expect(dataTable.initialize()).resolves.toBe(dataTable);
      
      expect(dataTable.coordinator).toBeDefined();
      expect(dataTable.versionControl).toBeDefined();
      expect(dataTable.container).toBeDefined();
      expect(dataTable.db).toBeDefined();
      expect(dataTable.conn).toBeDefined();
      expect(dataTable.worker).toBeDefined();
      expect(dataTable.performance.mode).toBe('Worker');
    });

    it('should initialize successfully without worker', async () => {
      dataTable = new DataTable({ container, useWorker: false });
      
      await expect(dataTable.initialize()).resolves.toBe(dataTable);
      
      expect(dataTable.coordinator).toBeDefined();
      expect(dataTable.connector).toBeDefined();
      expect(dataTable.db).toBeDefined();
      expect(dataTable.conn).toBeDefined();
      expect(dataTable.performance.mode).toBe('Direct');
    });

    it('should create container in DOM', async () => {
      dataTable = new DataTable({ container });
      await dataTable.initialize();
      
      const createdContainer = container.querySelector('.datatable-container');
      expect(createdContainer).toBeTruthy();
      expect(createdContainer.style.height).toBe('500px');
    });

    it('should setup query cache and version control', async () => {
      dataTable = new DataTable({ container });
      await dataTable.initialize();
      
      expect(dataTable.queryCache).toBeDefined();
      expect(dataTable.versionControl).toBeDefined();
      expect(dataTable.dataLoader).toBeDefined();
    });
  });


  describe('Data Loading', () => {
    beforeEach(async () => {
      dataTable = new DataTable({ container });
      await dataTable.initialize();
    });

    it('should load CSV file successfully', async () => {
      const csvContent = 'name,age\nAlice,30\nBob,25';
      const file = new File([csvContent], 'test.csv', { type: 'text/csv' });
      
      // Mock dataLoader.load to return success
      dataTable.dataLoader.load = vi.fn(() => Promise.resolve({
        tableName: 'test_csv',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        rowCount: 2
      }));
      
      const result = await dataTable.loadData(file);
      
      expect(result.tableName).toBe('test_csv');
      expect(dataTable.tableName.value).toBe('test_csv');
      expect(dataTable.schema.value).toEqual({ name: { type: 'string' }, age: { type: 'number' } });
      expect(dataTable.dataLoader.load).toHaveBeenCalledWith(file, expect.objectContaining({
        onProgress: expect.any(Function)
      }));
    });

    it('should handle data loading errors', async () => {
      const file = new File(['invalid'], 'test.csv', { type: 'text/csv' });
      
      // Mock dataLoader.load to throw error
      dataTable.dataLoader.load = vi.fn(() => Promise.reject(new Error('Invalid format')));
      
      await expect(dataTable.loadData(file)).rejects.toThrow('Invalid format');
    });

    it('should create table renderer after loading data', async () => {
      const csvContent = 'name,age\nAlice,30\nBob,25';
      const file = new File([csvContent], 'test.csv', { type: 'text/csv' });
      
      // Mock dataLoader.load to return success
      dataTable.dataLoader.load = vi.fn(() => Promise.resolve({
        tableName: 'test_csv',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        rowCount: 2
      }));
      
      await dataTable.loadData(file);
      
      expect(dataTable.tableRenderer).toBeDefined();
      // The container should exist after loadData creates it
      expect(dataTable.container).toBeTruthy();
      expect(dataTable.container.className).toBe('datatable-container');
    });

    it('should emit progress events during loading', async () => {
      const csvContent = 'name,age\nAlice,30\nBob,25';
      const file = new File([csvContent], 'test.csv', { type: 'text/csv' });
      
      const progressCallback = vi.fn();
      dataTable.onProgress(progressCallback);
      
      // Mock dataLoader.load to call progress callback
      dataTable.dataLoader.load = vi.fn((source, options) => {
        // Simulate progress
        if (options.onProgress) {
          options.onProgress({ stage: 'loading', percent: 50 });
          options.onProgress({ stage: 'loading', percent: 100 });
        }
        return Promise.resolve({
          tableName: 'test_csv',
          schema: { name: { type: 'string' }, age: { type: 'number' } }
        });
      });
      
      await dataTable.loadData(file);
      
      expect(progressCallback).toHaveBeenCalledWith(expect.objectContaining({
        stage: 'loading',
        fileName: 'test.csv'
      }));
    });
  });

  describe('SQL Execution', () => {
    beforeEach(async () => {
      dataTable = new DataTable({ container, useWorker: false });
      await dataTable.initialize();
    });

    it('should execute SQL queries', async () => {
      const sql = 'SELECT * FROM test';
      
      const result = await dataTable.executeSQL(sql);
      
      expect(dataTable.currentSQL.value).toBe(sql);
      expect(dataTable.queryHistory).toHaveLength(1);
      expect(dataTable.queryHistory[0].sql).toBe(sql);
      expect(dataTable.queryHistory[0].timestamp).toBeDefined();
      expect(dataTable.queryHistory[0].cached).toBe(false);
    });

    it('should record non-SELECT commands in version control', async () => {
      const sql = 'UPDATE test SET name = "Alice" WHERE id = 1';
      dataTable.versionControl.recordCommand = vi.fn();
      
      await dataTable.executeSQL(sql);
      
      expect(dataTable.versionControl.recordCommand).toHaveBeenCalledWith(sql, {
        tableName: null
      });
    });

    it('should not record SELECT commands in version control', async () => {
      const sql = 'SELECT * FROM test';
      dataTable.versionControl.recordCommand = vi.fn();
      
      await dataTable.executeSQL(sql);
      
      expect(dataTable.versionControl.recordCommand).not.toHaveBeenCalled();
    });

    it('should use query caching for SELECT queries', async () => {
      const sql = 'SELECT * FROM test';
      dataTable.queryCache.get = vi.fn(() => null);
      dataTable.queryCache.set = vi.fn();
      
      await dataTable.executeSQL(sql);
      
      expect(dataTable.queryCache.get).toHaveBeenCalledWith(sql, {});
      expect(dataTable.queryCache.set).toHaveBeenCalledWith(sql, expect.anything(), {});
    });

    it('should handle SQL execution errors', async () => {
      // Mock connection to throw error
      dataTable.conn.query = vi.fn().mockRejectedValue(new Error('SQL syntax error'));
      
      const sql = 'INVALID SQL';
      
      await expect(dataTable.executeSQL(sql)).rejects.toThrow('SQL syntax error');
    });

    it('should handle memory pressure during query execution', async () => {
      // Mock critical memory pressure
      dataTable.checkMemoryPressure = vi.fn(() => ({ level: 'critical' }));
      
      const sql = 'SELECT * FROM test';
      
      await expect(dataTable.executeSQL(sql)).rejects.toThrow('Query cancelled due to memory pressure');
    });
  });

  describe('Data Management', () => {
    beforeEach(async () => {
      dataTable = new DataTable({ container });
      await dataTable.initialize();
      
      // Mock loaded state
      dataTable.tableName.value = 'test';
      dataTable.schema.value = { name: { type: 'string' } };
    });

    it('should clear data successfully', async () => {
      // Mock table renderer
      dataTable.tableRenderer = {
        destroy: vi.fn()
      };
      
      // Mock persistence manager
      dataTable.persistenceManager = {
        clearTable: vi.fn(() => Promise.resolve())
      };
      
      // Mock version control with recordCommand method
      dataTable.versionControl = {
        clear: vi.fn(() => Promise.resolve()),
        recordCommand: vi.fn(() => Promise.resolve())
      };
      
      await dataTable.clearData();
      
      expect(dataTable.tableName.value).toBe(null);
      expect(dataTable.schema.value).toEqual({});
      expect(dataTable.currentSQL.value).toBe('');
      expect(dataTable.queryHistory).toHaveLength(0);
      expect(dataTable.tableRenderer).toBe(null);
    });

    it('should return correct schema information', () => {
      const schema = dataTable.getSchema();
      
      expect(schema.tables).toEqual(['test']);
      expect(schema.columns).toEqual(['name']);
    });

    it('should return query history', () => {
      dataTable.queryHistory = [
        { sql: 'SELECT * FROM test', timestamp: Date.now() }
      ];
      
      const history = dataTable.getQueryHistory();
      
      expect(history).toHaveLength(1);
      expect(history[0].sql).toBe('SELECT * FROM test');
    });

    it('should provide cache statistics', () => {
      dataTable.queryCache.getStats = vi.fn(() => ({
        size: 5,
        hits: 10,
        misses: 15
      }));
      
      const stats = dataTable.getCacheStats();
      
      expect(stats.size).toBe(5);
      expect(stats.hits).toBe(10);
      expect(stats.misses).toBe(15);
    });

    it('should enable/disable caching', () => {
      dataTable.queryCache.setEnabled = vi.fn();
      
      dataTable.setCacheEnabled(false);
      
      expect(dataTable.queryCache.setEnabled).toHaveBeenCalledWith(false);
    });

    it('should clear cache manually', () => {
      dataTable.queryCache.clear = vi.fn();
      
      dataTable.clearCache();
      
      expect(dataTable.queryCache.clear).toHaveBeenCalled();
    });

    it('should check memory status', () => {
      const result = dataTable.getMemoryStatus();
      
      expect(result).toBeDefined();
      // Memory status depends on the mock we set up
    });
  });

  describe('Cleanup', () => {
    it('should destroy DataTable properly', async () => {
      dataTable = new DataTable({ container });
      await dataTable.initialize();
      
      const mockWorker = dataTable.worker;
      const mockPersistenceManager = {
        close: vi.fn()
      };
      dataTable.persistenceManager = mockPersistenceManager;
      
      await dataTable.destroy();
      
      if (mockWorker) {
        expect(mockWorker.terminate).toHaveBeenCalled();
      }
      expect(mockPersistenceManager.close).toHaveBeenCalled();
      expect(dataTable.coordinator).toBe(null);
      expect(dataTable.connector).toBe(null);
      expect(dataTable.db).toBe(null);
      expect(dataTable.worker).toBe(null);
    });

    it('should cleanup all resources properly', async () => {
      dataTable = new DataTable({ container });
      await dataTable.initialize();
      
      // Mock various components
      const mockTableRenderer = { destroy: vi.fn() };
      const mockQueryCache = { destroy: vi.fn() };
      const mockConnector = { destroy: vi.fn() };
      const mockPersistenceManager = { close: vi.fn() };
      
      dataTable.tableRenderer = mockTableRenderer;
      dataTable.queryCache = mockQueryCache;
      dataTable.connector = mockConnector;
      dataTable.persistenceManager = mockPersistenceManager;
      
      await dataTable.destroy();
      
      expect(mockTableRenderer.destroy).toHaveBeenCalled();
      expect(mockQueryCache.destroy).toHaveBeenCalled();
      expect(mockConnector.destroy).toHaveBeenCalled();
      expect(mockPersistenceManager.close).toHaveBeenCalled();
    });

    it('should handle destroy errors gracefully', async () => {
      dataTable = new DataTable({ container });
      await dataTable.initialize();
      
      // Mock db.terminate to throw error
      dataTable.db.terminate = vi.fn().mockRejectedValue(new Error('Terminate error'));
      
      // Should not throw despite the error
      await expect(dataTable.destroy()).resolves.toBeUndefined();
      
      // Database should be properly cleaned up even when terminate fails
      expect(dataTable.db).toBeNull();
    });
  });
});