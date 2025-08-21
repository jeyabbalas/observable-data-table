import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';

// Mock Mosaic components
vi.mock('@uwdata/mosaic-core', () => ({
  Coordinator: class MockCoordinator {
    constructor() {
      this.databaseConnector = vi.fn();
      this.connect = vi.fn();
      this.query = vi.fn(() => Promise.resolve([]));
      this.requestQuery = vi.fn();
    }
  },
  wasmConnector: vi.fn(() => ({
    getDuckDB: vi.fn(() => Promise.resolve({
      registerFileText: vi.fn(),
      registerFileBuffer: vi.fn(),
      query: vi.fn(() => Promise.resolve([]))
    }))
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
    
    // Reset all mocks
    vi.clearAllMocks();
  });

  afterEach(async () => {
    // Cleanup
    if (dataTable) {
      await dataTable.destroy();
    }
    if (container.parentNode) {
      container.parentNode.removeChild(container);
    }
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
    });

    it('should initialize successfully without worker', async () => {
      dataTable = new DataTable({ container, useWorker: false });
      
      await expect(dataTable.initialize()).resolves.toBe(dataTable);
      
      expect(dataTable.coordinator).toBeDefined();
      expect(dataTable.connector).toBeDefined();
    });

    it('should create container in DOM', async () => {
      dataTable = new DataTable({ container });
      await dataTable.initialize();
      
      const createdContainer = container.querySelector('.datatable-container');
      expect(createdContainer).toBeTruthy();
      expect(createdContainer.style.height).toBe('500px');
    });

    it('should handle initialization errors', async () => {
      // Mock coordinator to throw an error
      const { Coordinator } = await import('@uwdata/mosaic-core');
      Coordinator.mockImplementation(() => {
        throw new Error('Coordinator initialization failed');
      });
      
      dataTable = new DataTable({ container });
      
      await expect(dataTable.initialize()).rejects.toThrow('Coordinator initialization failed');
    });
  });

  describe('Worker Communication', () => {
    beforeEach(async () => {
      dataTable = new DataTable({ container, useWorker: true });
      await dataTable.initialize();
    });

    it('should send messages to worker', async () => {
      const mockWorker = dataTable.worker;
      const promise = dataTable.sendToWorker('test', { data: 'test' });
      
      expect(mockWorker.postMessage).toHaveBeenCalled();
      
      // Simulate worker response
      const messageCall = mockWorker.postMessage.mock.calls[0][0];
      const mockEvent = {
        data: {
          id: messageCall.id,
          success: true,
          result: { test: true }
        }
      };
      
      dataTable.handleWorkerMessage(mockEvent);
      
      await expect(promise).resolves.toEqual({ test: true });
    });

    it('should handle worker errors', async () => {
      const mockWorker = dataTable.worker;
      const promise = dataTable.sendToWorker('test', { data: 'test' });
      
      // Simulate worker error response
      const messageCall = mockWorker.postMessage.mock.calls[0][0];
      const mockEvent = {
        data: {
          id: messageCall.id,
          success: false,
          error: 'Worker error'
        }
      };
      
      dataTable.handleWorkerMessage(mockEvent);
      
      await expect(promise).rejects.toThrow('Worker error');
    });
  });

  describe('Data Loading', () => {
    beforeEach(async () => {
      dataTable = new DataTable({ container });
      await dataTable.initialize();
    });

    it('should load CSV file successfully', async () => {
      const csvContent = 'name,age\\nAlice,30\\nBob,25';
      const file = new File([csvContent], 'test.csv', { type: 'text/csv' });
      
      // Mock dataLoader.load to return success
      dataTable.dataLoader.load = vi.fn(() => Promise.resolve({
        tableName: 'test',
        schema: { name: { type: 'string' }, age: { type: 'number' } }
      }));
      
      const result = await dataTable.loadData(file);
      
      expect(result.tableName).toBe('test');
      expect(dataTable.tableName.value).toBe('test');
      expect(dataTable.dataLoader.load).toHaveBeenCalledWith(file, {});
    });

    it('should handle data loading errors', async () => {
      const file = new File(['invalid'], 'test.csv', { type: 'text/csv' });
      
      // Mock dataLoader.load to throw error
      dataTable.dataLoader.load = vi.fn(() => Promise.reject(new Error('Invalid format')));
      
      await expect(dataTable.loadData(file)).rejects.toThrow('Invalid format');
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
      dataTable.executeSQL = vi.fn(() => Promise.resolve());
      dataTable.persistenceManager = {
        clearTable: vi.fn(() => Promise.resolve())
      };
      dataTable.versionControl = {
        clear: vi.fn(() => Promise.resolve())
      };
      
      await dataTable.clearData();
      
      expect(dataTable.tableName.value).toBe(null);
      expect(dataTable.schema.value).toEqual({});
      expect(dataTable.currentSQL.value).toBe('');
      expect(dataTable.queryHistory).toHaveLength(0);
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
    });
  });
});