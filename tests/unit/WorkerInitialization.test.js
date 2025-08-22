import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Simple mock setup for DuckDB-WASM
vi.mock('@duckdb/duckdb-wasm', () => ({
  getJsDelivrBundles: vi.fn(() => ({
    mvp: {
      mainModule: 'mock-mvp.wasm',
      mainWorker: 'mock-mvp.worker.js'
    }
  })),
  selectBundle: vi.fn(async (bundles) => bundles.mvp),
  AsyncDuckDB: vi.fn().mockImplementation(() => ({
    instantiate: vi.fn().mockResolvedValue(),
    connect: vi.fn().mockResolvedValue({
      query: vi.fn().mockImplementation(async (sql) => {
        if (sql.includes('version()')) {
          return { toArray: () => [{ version: 'v1.0.0-mock' }] };
        }
        if (sql.includes('SET')) {
          return undefined; // Configuration queries return undefined
        }
        return { toArray: () => [] };
      }),
      close: vi.fn().mockResolvedValue(),
      send: vi.fn().mockResolvedValue({ toArray: () => [] })
    }),
    terminate: vi.fn().mockResolvedValue(),
    registerFileText: vi.fn().mockResolvedValue(),
    registerFileBuffer: vi.fn().mockResolvedValue()
  })),
  ConsoleLogger: vi.fn(() => ({})),
  LogLevel: { WARNING: 'WARNING', ERROR: 'ERROR' }
}));

// Simple mock setup for Mosaic
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

describe('Worker Initialization', () => {
  let container;
  
  beforeEach(() => {
    // Create container element
    container = document.createElement('div');
    container.id = 'test-container';
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
  });
  
  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
    vi.restoreAllMocks();
  });

  it('should initialize DataTable in Worker mode successfully', async () => {
    const dataTable = new DataTable({
      container,
      useWorker: true
    });
    
    await dataTable.initialize();
    
    // Verify successful Worker mode initialization
    expect(dataTable.options.useWorker).toBe(true);
    expect(dataTable.performance.mode).toBe('Worker');
    expect(dataTable.db).toBeDefined();
    expect(dataTable.conn).toBeDefined();
    expect(global.Worker).toHaveBeenCalled();
  });
});