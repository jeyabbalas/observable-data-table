// Test setup and mocking utilities for integration tests
import { vi } from 'vitest';

// Mock DuckDB-WASM module
export const createMockDuckDB = () => {
  const mockConnection = {
    query: vi.fn().mockResolvedValue({
      toArray: () => [{ version: 'test-version' }],
      numRows: 0,
      columns: []
    }),
    close: vi.fn().mockResolvedValue(undefined),
    send: vi.fn().mockResolvedValue(undefined)
  };

  const mockDB = {
    connect: vi.fn().mockResolvedValue(mockConnection),
    instantiate: vi.fn().mockResolvedValue(undefined),
    close: vi.fn().mockResolvedValue(undefined),
    terminate: vi.fn().mockResolvedValue(undefined)
  };

  return {
    AsyncDuckDB: vi.fn().mockImplementation(() => mockDB),
    ConsoleLogger: vi.fn().mockImplementation(() => ({})),
    LogLevel: { ERROR: 0, WARN: 1, INFO: 2, DEBUG: 3 },
    getJsDelivrBundles: vi.fn().mockReturnValue({
      mvp: {
        mainModule: 'test://duckdb-mvp.wasm',
        mainWorker: 'test://duckdb-browser-mvp.worker.js'
      },
      eh: {
        mainModule: 'test://duckdb-eh.wasm', 
        mainWorker: 'test://duckdb-browser-eh.worker.js'
      }
    }),
    selectBundle: vi.fn().mockResolvedValue({
      mainModule: 'test://duckdb-mvp.wasm',
      mainWorker: 'test://duckdb-browser-mvp.worker.js'
    }),
    mockConnection,
    mockDB
  };
};

// Mock Mosaic modules
export const createMockMosaic = () => {
  const mockCoordinator = {
    databaseConnector: vi.fn(),
    connect: vi.fn().mockResolvedValue(undefined),
    clear: vi.fn(),
    on: vi.fn(),
    off: vi.fn(),
    requestQuery: vi.fn()
  };

  const mockWasmConnector = vi.fn().mockReturnValue({
    query: vi.fn().mockResolvedValue({
      toArray: () => [],
      numRows: 0,
      columns: []
    }),
    close: vi.fn().mockResolvedValue(undefined)
  });

  const mockSelection = {
    crossfilter: vi.fn().mockReturnValue({})
  };

  const mockQuery = {
    from: vi.fn().mockReturnThis(),
    select: vi.fn().mockReturnThis(),
    where: vi.fn().mockReturnThis(),
    orderby: vi.fn().mockReturnThis(),
    limit: vi.fn().mockReturnThis(),
    offset: vi.fn().mockReturnThis(),
    toString: vi.fn().mockReturnValue('SELECT * FROM test')
  };

  // Mock MosaicClient class
  const mockMosaicClient = vi.fn().mockImplementation(function() {
    this.initialize = vi.fn();
    this.prepare = vi.fn();
    this.queryResult = vi.fn();
    this.requestQuery = vi.fn();
    return this;
  });

  return {
    Coordinator: vi.fn().mockImplementation(() => mockCoordinator),
    MosaicClient: mockMosaicClient,
    wasmConnector: mockWasmConnector,
    Selection: mockSelection,
    Query: mockQuery,
    asc: vi.fn((field) => `${field} ASC`),
    desc: vi.fn((field) => `${field} DESC`),
    mockCoordinator,
    mockWasmConnector,
    mockSelection,
    mockQuery,
    mockMosaicClient
  };
};

// Mock Web APIs that aren't available in Node.js
export const setupWebAPIMocks = () => {
  // Mock Worker
  global.Worker = vi.fn().mockImplementation((url) => ({
    postMessage: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    terminate: vi.fn(),
    onerror: null,
    onmessage: null
  }));

  // Mock Blob and URL
  global.Blob = vi.fn().mockImplementation((parts, options) => ({
    size: parts.reduce((acc, part) => acc + (part.length || 0), 0),
    type: options?.type || ''
  }));

  global.URL = {
    createObjectURL: vi.fn().mockReturnValue('blob:test-url'),
    revokeObjectURL: vi.fn()
  };

  // Mock File API
  global.File = class File {
    constructor(data, name, options = {}) {
      this.name = name;
      this.size = data.reduce ? data.reduce((acc, item) => acc + (item.length || 0), 0) : data.length || 0;
      this.type = options.type || '';
      this.lastModified = Date.now();
      this._data = data;
    }
    
    async arrayBuffer() {
      if (this._data && this._data.length > 0) {
        const content = this._data[0];
        if (typeof content === 'string') {
          return new TextEncoder().encode(content).buffer;
        }
      }
      return new ArrayBuffer(100);
    }
    
    async text() {
      if (this._data && this._data.length > 0) {
        return this._data[0];
      }
      return 'test,data\n1,value';
    }
  };

  // Mock performance.memory
  if (!global.performance) {
    global.performance = {};
  }
  global.performance.memory = {
    usedJSHeapSize: 1024 * 1024,
    totalJSHeapSize: 2 * 1024 * 1024,
    jsHeapSizeLimit: 4 * 1024 * 1024
  };
};

// Test data generators
export const createTestCSVFile = () => {
  const csvContent = `id,name,value,date
1,Alice,100,2023-01-01
2,Bob,200,2023-01-02
3,Charlie,300,2023-01-03`;
  
  return new File([csvContent], 'test.csv', { type: 'text/csv' });
};

export const createTestJSONFile = () => {
  const jsonContent = JSON.stringify([
    { id: 1, name: 'Alice', value: 100, date: '2023-01-01' },
    { id: 2, name: 'Bob', value: 200, date: '2023-01-02' },
    { id: 3, name: 'Charlie', value: 300, date: '2023-01-03' }
  ]);
  
  return new File([jsonContent], 'test.json', { type: 'application/json' });
};

// Mock schema data
export const createMockSchema = () => ({
  id: { type: 'INTEGER', nullable: false },
  name: { type: 'VARCHAR', nullable: true },
  value: { type: 'INTEGER', nullable: true },
  date: { type: 'DATE', nullable: true }
});

// Setup all mocks for a test
export const setupIntegrationTest = () => {
  // Setup Web API mocks
  setupWebAPIMocks();

  // Mock DuckDB
  const mockDuckDB = createMockDuckDB();
  vi.doMock('@duckdb/duckdb-wasm', () => mockDuckDB);

  // Mock Mosaic
  const mockMosaic = createMockMosaic();
  vi.doMock('@uwdata/mosaic-core', () => mockMosaic);
  vi.doMock('@uwdata/mosaic-sql', () => mockMosaic);

  // Mock Preact signals
  vi.doMock('@preact/signals-core', () => ({
    signal: vi.fn((initialValue) => ({
      value: initialValue,
      subscribe: vi.fn(),
      unsubscribe: vi.fn()
    }))
  }));

  return {
    mockDuckDB,
    mockMosaic
  };
};

// Cleanup function
export const cleanupIntegrationTest = () => {
  vi.clearAllMocks();
  vi.resetModules();
  
  // Clean up global mocks
  delete global.Worker;
  delete global.Blob;
  delete global.URL;
  delete global.File;
};