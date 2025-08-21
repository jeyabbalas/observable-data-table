import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Mock DuckDB-WASM for integration testing
const mockDb = {
  instantiate: vi.fn(async () => {}),
  connect: vi.fn(async () => ({
    query: vi.fn(async (sql) => {
      if (sql.includes('version()')) {
        return { toArray: () => [{ version: 'v1.0.0-mock' }] };
      }
      if (sql.includes('SET')) {
        return undefined;
      }
      return { toArray: () => [] };
    }),
    close: vi.fn(async () => {})
  })),
  terminate: vi.fn(async () => {})
};

vi.mock('@duckdb/duckdb-wasm', () => ({
  getJsDelivrBundles: vi.fn(() => ({
    mvp: {
      mainModule: 'mock-mvp.wasm',
      mainWorker: 'mock-mvp.worker.js'
    }
  })),
  selectBundle: vi.fn(async (bundles) => bundles.mvp),
  AsyncDuckDB: vi.fn(() => mockDb),
  ConsoleLogger: vi.fn(() => ({})),
  LogLevel: { WARNING: 'WARNING' }
}));

vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: vi.fn(() => ({
    databaseConnector: vi.fn()
  })),
  MosaicClient: vi.fn(() => ({
    query: vi.fn(),
    update: vi.fn()
  })),
  wasmConnector: vi.fn(() => ({
    query: vi.fn(async () => ({ toArray: () => [] }))
  }))
}));

describe('Worker Mode Integration Tests', () => {
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
    
    vi.clearAllMocks();
  });
  
  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
  });

  describe('Worker Mode Success Cases', () => {
    it('should successfully initialize in Worker mode', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      expect(dataTable.options.useWorker).toBe(true);
      expect(dataTable.performance.mode).toBe('Worker');
      expect(dataTable.worker).toBeDefined();
      expect(dataTable.db).toBeDefined();
      expect(dataTable.conn).toBeDefined();
      
      await dataTable.destroy();
    });

    it('should handle data loading in Worker mode', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Mock CSV data
      const csvData = 'id,name,value\n1,Alice,100\n2,Bob,200';
      const file = new File([csvData], 'test.csv', { type: 'text/csv' });
      
      // Should be able to load data successfully
      await expect(dataTable.loadData(file)).resolves.toBeDefined();
      
      await dataTable.destroy();
    });
  });

  describe('Worker Mode Fallback Cases', () => {
    it('should fallback to Direct mode when Worker creation fails', async () => {
      // Mock Worker constructor to fail
      global.Worker = vi.fn().mockImplementation(() => {
        throw new Error('Worker creation failed');
      });
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Should fallback to Direct mode
      expect(dataTable.options.useWorker).toBe(false);
      expect(dataTable.performance.mode).toBe('Direct (fallback)');
      expect(dataTable.db).toBeDefined();
      expect(dataTable.conn).toBeDefined();
      
      await dataTable.destroy();
    });

    it('should handle AsyncDuckDB instantiation failure gracefully', async () => {
      // Mock instantiate to fail
      mockDb.instantiate.mockRejectedValueOnce(new Error('Instantiation failed'));
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Should fallback to Direct mode
      expect(dataTable.options.useWorker).toBe(false);
      expect(dataTable.performance.mode).toBe('Direct (fallback)');
      
      // Reset mock
      mockDb.instantiate.mockResolvedValue(undefined);
      
      await dataTable.destroy();
    });
  });

  describe('Direct Mode Baseline', () => {
    it('should successfully initialize in Direct mode', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      expect(dataTable.options.useWorker).toBe(false);
      expect(dataTable.performance.mode).toBe('Direct');
      expect(dataTable.db).toBeDefined();
      expect(dataTable.conn).toBeDefined();
      
      await dataTable.destroy();
    });

    it('should handle data operations in Direct mode', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await dataTable.initialize();
      
      // Should be able to execute SQL
      await expect(dataTable.executeSQL('SELECT 1')).resolves.toBeDefined();
      
      await dataTable.destroy();
    });
  });

  describe('Mode Comparison', () => {
    it('should track performance metrics for both modes', async () => {
      const workerDataTable = new DataTable({
        container,
        useWorker: true
      });
      
      const directDataTable = new DataTable({
        container,
        useWorker: false
      });
      
      await workerDataTable.initialize();
      await directDataTable.initialize();
      
      // Both should have performance tracking
      expect(workerDataTable.performance.initStartTime).toBeDefined();
      expect(workerDataTable.performance.initEndTime).toBeDefined();
      expect(directDataTable.performance.initStartTime).toBeDefined();
      expect(directDataTable.performance.initEndTime).toBeDefined();
      
      expect(workerDataTable.performance.mode).toBe('Worker');
      expect(directDataTable.performance.mode).toBe('Direct');
      
      await workerDataTable.destroy();
      await directDataTable.destroy();
    });
  });
});