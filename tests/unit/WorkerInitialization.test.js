import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Mock DuckDB-WASM
const mockDb = {
  instantiate: vi.fn(async () => {}),
  connect: vi.fn(async () => ({
    query: vi.fn(async (sql) => {
      if (sql.includes('version()')) {
        return { toArray: () => [{ version: 'v1.0.0-mock' }] };
      }
      return { toArray: () => [] };
    })
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
  AsyncDuckDB: vi.fn().mockImplementation(() => mockDb),
  ConsoleLogger: vi.fn(() => ({})),
  LogLevel: { WARNING: 'WARNING' }
}));

// Mock Mosaic
vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: vi.fn(() => ({
    databaseConnector: vi.fn()
  })),
  wasmConnector: vi.fn(() => ({
    query: vi.fn(async () => ({ toArray: () => [] }))
  }))
}));

describe('AsyncDuckDB Worker Mode Tests', () => {
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
  });
  
  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
    vi.restoreAllMocks();
  });

  describe('AsyncDuckDB Worker Mode', () => {
    it('should initialize successfully in Worker mode', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Should maintain Worker mode if successful
      expect(dataTable.options.useWorker).toBe(true);
      expect(dataTable.db).toBeDefined();
      expect(dataTable.conn).toBeDefined();
      expect(global.Worker).toHaveBeenCalled();
    });

    it('should handle Worker creation failure and fallback to Direct mode', async () => {
      // Mock Worker to throw on construction
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
      expect(dataTable.performance.mode).toBe('Direct (fallback)');
    });

    it('should properly terminate worker on cleanup', async () => {
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
      dataTable.destroy();
      
      expect(mockWorker.terminate).toHaveBeenCalled();
    });
  });

  describe('DuckDB Configuration', () => {
    it('should apply optimal DuckDB settings in Worker mode', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Verify that DuckDB configuration was applied
      const mockConn = await mockDb.connect();
      expect(mockConn.query).toHaveBeenCalledWith("SET max_memory='512MB'");
      expect(mockConn.query).toHaveBeenCalledWith("SET enable_object_cache='true'");
    });

    it('should handle DuckDB instantiation errors', async () => {
      // Mock instantiate to fail
      mockDb.instantiate.mockRejectedValueOnce(new Error('Instantiation failed'));
      
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Should fallback to direct mode
      expect(dataTable.options.useWorker).toBe(false);
      expect(dataTable.performance.mode).toBe('Direct (fallback)');
    });
  });

  describe('Bundle Selection and Worker Creation', () => {
    it('should properly select DuckDB bundle', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Verify bundle selection was called
      const { selectBundle } = await import('@duckdb/duckdb-wasm');
      expect(selectBundle).toHaveBeenCalled();
    });

    it('should handle blob worker creation for CORS avoidance', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      // Verify blob creation was attempted for worker
      expect(global.Blob).toHaveBeenCalled();
      expect(global.URL.createObjectURL).toHaveBeenCalled();
    });
  });

  describe('Mosaic Connector Integration', () => {
    it('should create wasmConnector with DuckDB instance', async () => {
      const dataTable = new DataTable({
        container,
        useWorker: true
      });
      
      await dataTable.initialize();
      
      expect(dataTable.connector).toBeDefined();
      expect(dataTable.coordinator).toBeDefined();
      
      // Verify wasmConnector was created
      const { wasmConnector } = await import('@uwdata/mosaic-core');
      expect(wasmConnector).toHaveBeenCalledWith({
        duckdb: dataTable.db,
        connection: dataTable.conn
      });
    });
  });
});

describe('Worker vs Direct Mode Comparison', () => {
  let container;
  
  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    vi.clearAllMocks();
    
    // Mock Worker for these tests
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
    expect(dataTable.performance.mode).toBe('Direct');
    expect(dataTable.db).toBeDefined();
  });

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
  });
});