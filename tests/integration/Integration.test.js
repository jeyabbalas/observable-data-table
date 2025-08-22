import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { 
  setupIntegrationTest, 
  cleanupIntegrationTest,
  createTestCSVFile,
  createTestJSONFile,
  createMockSchema
} from './setup.js';

describe('Integration Tests', () => {
  let container;
  let dataTable;
  let mocks;

  beforeEach(() => {
    // Setup mocks before importing DataTable
    mocks = setupIntegrationTest();
    
    container = document.createElement('div');
    container.style.height = '400px';
    document.body.appendChild(container);
  });

  afterEach(async () => {
    if (dataTable) {
      try {
        await dataTable.destroy();
      } catch (error) {
        // Ignore cleanup errors in tests
      }
      dataTable = null;
    }
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
    cleanupIntegrationTest();
  });

  describe('DataTable Initialization', () => {
    it('should initialize successfully in direct mode', async () => {
      // Import DataTable after mocks are set up
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });

      await dataTable.initialize();

      expect(dataTable.performance.mode).toBe('Direct');
      expect(dataTable.coordinator).toBeDefined();
      expect(mocks.mockMosaic.Coordinator).toHaveBeenCalled();
      expect(mocks.mockDuckDB.AsyncDuckDB).toHaveBeenCalled();
    });

    it('should initialize successfully in worker mode', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });

      await dataTable.initialize();

      expect(dataTable.performance.mode).toBe('Worker');
      expect(dataTable.coordinator).toBeDefined();
      expect(mocks.mockMosaic.Coordinator).toHaveBeenCalled();
      expect(mocks.mockDuckDB.AsyncDuckDB).toHaveBeenCalled();
    });

    it('should set up mosaic connector correctly', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });

      await dataTable.initialize();

      expect(mocks.mockMosaic.wasmConnector).toHaveBeenCalledWith({
        duckdb: expect.any(Object),
        connection: expect.any(Object)
      });
      expect(mocks.mockMosaic.mockCoordinator.databaseConnector).toHaveBeenCalled();
    });

    it('should handle container creation', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        height: 600,
        logLevel: 'error'
      });

      await dataTable.initialize();

      expect(dataTable.options.height).toBe(600);
      expect(dataTable.container).toBeDefined();
    });
  });

  describe('Data Loading Integration', () => {
    beforeEach(async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      await dataTable.initialize();
      
      // Mock the DataLoader's load method directly
      dataTable.dataLoader.load = vi.fn();
    });

    it('should load CSV data successfully', async () => {
      const csvFile = createTestCSVFile();
      const mockResult = {
        tableName: 'test_123_abc',
        format: 'csv',
        schema: createMockSchema(),
        rowCount: 3
      };
      
      dataTable.dataLoader.load.mockResolvedValue(mockResult);

      const result = await dataTable.loadData(csvFile);

      expect(result).toBeDefined();
      expect(result.tableName).toBe('test_123_abc');
      expect(result.format).toBe('csv');
      expect(dataTable.tableName.value).toBe('test_123_abc');
      expect(dataTable.dataLoader.load).toHaveBeenCalledWith(csvFile, expect.any(Object));
    });

    it('should load JSON data successfully', async () => {
      const jsonFile = createTestJSONFile();
      const mockResult = {
        tableName: 'test_456_def',
        format: 'json',
        schema: createMockSchema(),
        rowCount: 3
      };
      
      dataTable.dataLoader.load.mockResolvedValue(mockResult);

      const result = await dataTable.loadData(jsonFile);

      expect(result).toBeDefined();
      expect(result.tableName).toBe('test_456_def');
      expect(result.format).toBe('json');
      expect(dataTable.tableName.value).toBe('test_456_def');
      expect(dataTable.dataLoader.load).toHaveBeenCalledWith(jsonFile, expect.any(Object));
    });

    it('should generate unique table names', async () => {
      const csvFile1 = createTestCSVFile();
      const csvFile2 = createTestCSVFile();
      
      const mockResult1 = {
        tableName: 'test_111_aaa',
        format: 'csv',
        schema: createMockSchema(),
        rowCount: 3
      };
      
      const mockResult2 = {
        tableName: 'test_222_bbb',
        format: 'csv',
        schema: createMockSchema(),
        rowCount: 3
      };
      
      dataTable.dataLoader.load
        .mockResolvedValueOnce(mockResult1)
        .mockResolvedValueOnce(mockResult2);

      const result1 = await dataTable.loadData(csvFile1);
      const result2 = await dataTable.loadData(csvFile2);

      expect(result1.tableName).not.toBe(result2.tableName);
      expect(result1.tableName).toBe('test_111_aaa');
      expect(result2.tableName).toBe('test_222_bbb');
    });

    it('should handle file format detection', async () => {
      const csvFile = createTestCSVFile();
      const jsonFile = createTestJSONFile();
      
      const csvResult = { tableName: 'test_csv', format: 'csv', schema: createMockSchema() };
      const jsonResult = { tableName: 'test_json', format: 'json', schema: createMockSchema() };
      
      dataTable.dataLoader.load
        .mockResolvedValueOnce(csvResult)
        .mockResolvedValueOnce(jsonResult);

      const csvResponse = await dataTable.loadData(csvFile);
      const jsonResponse = await dataTable.loadData(jsonFile);

      expect(csvResponse.format).toBe('csv');
      expect(jsonResponse.format).toBe('json');
    });

    it('should handle progress callbacks', async () => {
      const csvFile = createTestCSVFile();
      const mockResult = {
        tableName: 'test_progress',
        format: 'csv',
        schema: createMockSchema()
      };
      
      const progressCallback = vi.fn();
      dataTable.dataLoader.load.mockResolvedValue(mockResult);

      await dataTable.loadData(csvFile, { onProgress: progressCallback });

      expect(dataTable.dataLoader.load).toHaveBeenCalledWith(
        csvFile, 
        expect.objectContaining({
          onProgress: expect.any(Function)
        })
      );
    });
  });

  describe('Mosaic Integration', () => {
    beforeEach(async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      await dataTable.initialize();

      // Mock data loading and load test data
      dataTable.dataLoader.load = vi.fn();
      const csvFile = createTestCSVFile();
      const mockResult = {
        tableName: 'test_mosaic_123',
        format: 'csv',
        schema: createMockSchema(),
        rowCount: 3
      };
      
      dataTable.dataLoader.load.mockResolvedValue(mockResult);
      await dataTable.loadData(csvFile);
    });

    it('should create table renderer with Mosaic integration', () => {
      expect(dataTable.tableRenderer).toBeDefined();
      expect(dataTable.tableRenderer.coordinator).toBeDefined();
      expect(dataTable.tableRenderer.table).toBe('test_mosaic_123');
      // TableRenderer creates its own container structure within the provided container
      expect(dataTable.tableRenderer.container).toBeDefined();
    });

    it('should handle table renderer queries', () => {
      const renderer = dataTable.tableRenderer;
      const query = renderer.query();
      
      expect(query).toBeDefined();
      expect(renderer.orderBy.value).toEqual([]);
      expect(renderer.filters.value).toEqual([]);
      expect(mocks.mockMosaic.Query.from).toHaveBeenCalled();
    });

    it('should support sorting operations', () => {
      const renderer = dataTable.tableRenderer;
      
      // Simulate adding sort
      renderer.orderBy.value = [{ field: 'name', order: 'ASC' }];
      const query = renderer.query();
      
      expect(query).toBeDefined();
      expect(mocks.mockMosaic.asc).toHaveBeenCalledWith('name');
    });

    it('should support filter operations', () => {
      const renderer = dataTable.tableRenderer;
      
      // Simulate adding filter
      renderer.filters.value = ['id > 0'];
      const query = renderer.query();
      
      expect(query).toBeDefined();
      expect(mocks.mockMosaic.Query.where).toHaveBeenCalled();
    });

    it('should clean up resources properly', async () => {
      const renderer = dataTable.tableRenderer;
      expect(renderer).toBeDefined();

      await dataTable.destroy();

      expect(dataTable.tableRenderer).toBeNull();
      expect(mocks.mockDuckDB.mockConnection.close).toHaveBeenCalled();
    });
  });

  describe('Error Handling', () => {
    beforeEach(async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      await dataTable.initialize();
    });

    it('should handle invalid file gracefully', async () => {
      const invalidFile = new File(['invalid data'], 'invalid.txt', {
        type: 'text/plain'
      });

      // Mock successful response (DataLoader handles format detection)
      dataTable.dataLoader.load = vi.fn().mockResolvedValue({
        tableName: 'invalid_table',
        format: 'csv',
        schema: createMockSchema(),
        rowCount: 0
      });

      const result = await dataTable.loadData(invalidFile);
      expect(result).toBeDefined();
      expect(result.format).toBe('csv'); // Defaults to CSV
    });

    it('should handle missing container gracefully', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      expect(() => {
        new DataTable({ container: null });
      }).not.toThrow();
      
      // Test with undefined container instead (more likely real-world scenario)
      const dataTableWithUndefinedContainer = new DataTable({});
      expect(dataTableWithUndefinedContainer.options.container).toBe(document.body);
    });

    it('should handle database connection errors', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      // Mock connection failure
      mocks.mockDuckDB.AsyncDuckDB.mockImplementation(() => {
        throw new Error('Connection failed');
      });

      const failingDataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });

      await expect(failingDataTable.initialize()).rejects.toThrow('Connection failed');
    });

    it('should handle query errors gracefully', async () => {
      // Mock DataLoader failure
      dataTable.dataLoader.load = vi.fn().mockRejectedValue(new Error('Query failed'));

      const csvFile = createTestCSVFile();
      
      await expect(dataTable.loadData(csvFile)).rejects.toThrow('Query failed');
    });
  });

  describe('Performance and Memory', () => {
    beforeEach(async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      await dataTable.initialize();
    });

    it('should track initialization performance', () => {
      expect(dataTable.performance.initStartTime).toBeDefined();
      expect(dataTable.performance.initEndTime).toBeDefined();
      expect(dataTable.performance.mode).toBe('Direct');
    });

    it('should handle caching configuration', () => {
      expect(dataTable.queryCache).toBeDefined();
      expect(dataTable.queryCache.enabled).toBe(true);
    });

    it('should monitor memory usage when available', () => {
      expect(dataTable.memoryMonitor.enabled).toBeDefined();
      if (dataTable.memoryMonitor.enabled) {
        expect(dataTable.performance.memoryUsage).toBeDefined();
      }
    });
  });
});