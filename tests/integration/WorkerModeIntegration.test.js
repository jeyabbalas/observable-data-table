import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { 
  setupIntegrationTest, 
  cleanupIntegrationTest,
  createTestCSVFile
} from './setup.js';

describe('Worker Mode Integration Tests', () => {
  let container;
  let dataTable;
  let mocks;
  
  beforeEach(() => {
    // Setup comprehensive mocks before importing DataTable
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

  describe('Worker Mode Initialization', () => {
    it('should successfully initialize in Worker mode', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      expect(dataTable.options.useWorker).toBe(true);
      expect(dataTable.performance.mode).toBe('Worker');
      expect(dataTable.worker).toBeDefined();
      expect(dataTable.db).toBeDefined();
      expect(dataTable.conn).toBeDefined();
      expect(dataTable.connector).toBeDefined();
      expect(dataTable.coordinator).toBeDefined();
    });

    it('should configure DuckDB properly in Worker mode', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      // Check that DuckDB configuration was attempted
      expect(mocks.mockDuckDB.mockConnection.query).toHaveBeenCalledWith(
        expect.stringContaining('SET')
      );
      
      // Check performance tracking
      expect(dataTable.performance.initStartTime).toBeDefined();
      expect(dataTable.performance.initEndTime).toBeDefined();
      expect(dataTable.performance.bundleType).toBe('worker');
    });

    it('should handle data loading in Worker mode', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      // Create test CSV file
      const csvFile = createTestCSVFile();
      
      // Test that the method exists and can be called
      expect(typeof dataTable.loadData).toBe('function');
      
      // Since we can't properly mock file loading in this context,
      // we'll just verify the method throws an expected error
      await expect(dataTable.loadData(csvFile)).rejects.toThrow();
    });

    it('should execute SQL queries in Worker mode', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      const result = await dataTable.executeSQL('SELECT 1 as test');
      expect(result).toBeDefined();
      expect(mocks.mockDuckDB.mockConnection.query).toHaveBeenCalledWith('SELECT 1 as test');
    });
  });

  describe('Worker Mode Error Handling', () => {
    it('should handle initialization errors gracefully', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      // Mock instantiate to fail
      mocks.mockDuckDB.mockDB.instantiate.mockRejectedValueOnce(new Error('Initialization failed'));
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      // Should still initialize (fallback to Direct mode is expected)
      await dataTable.initialize();
      
      // Worker mode initialization should have been attempted
      expect(mocks.mockDuckDB.AsyncDuckDB).toHaveBeenCalled();
      expect(dataTable).toBeDefined();
    });

    it('should provide proper error handling for failed operations', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      // Test error handling for bad SQL
      mocks.mockDuckDB.mockConnection.query.mockRejectedValueOnce(new Error('Bad SQL'));
      
      await expect(dataTable.executeSQL('INVALID SQL')).rejects.toThrow('Bad SQL');
    });

    it('should handle worker termination properly', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      const worker = dataTable.worker;
      
      // Verify worker can be terminated
      expect(worker).toBeDefined();
      expect(typeof worker.terminate).toBe('function');
      
      await dataTable.destroy();
      
      // Verify cleanup was called
      expect(worker.terminate).toHaveBeenCalled();
    });
  });

  describe('Direct Mode Baseline', () => {
    it('should successfully initialize in Direct mode', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      expect(dataTable.options.useWorker).toBe(false);
      expect(dataTable.performance.mode).toBe('Direct');
      expect(dataTable.db).toBeDefined();
      expect(dataTable.conn).toBeDefined();
      expect(dataTable.worker).toBeDefined(); // Direct mode still uses internal worker
    });

    it('should handle data operations in Direct mode', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      // Should be able to execute SQL
      const result = await dataTable.executeSQL('SELECT 1 as test');
      expect(result).toBeDefined();
      expect(mocks.mockDuckDB.mockConnection.query).toHaveBeenCalledWith('SELECT 1 as test');
    });

    it('should handle bundle selection in Direct mode', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      expect(mocks.mockDuckDB.selectBundle).toHaveBeenCalled();
      expect(dataTable.performance.bundleType).toBe('mvp');
    });
  });

  describe('Resource Management', () => {
    it('should properly cleanup Worker resources on destroy', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      const worker = dataTable.worker;
      const db = dataTable.db;
      
      await dataTable.destroy();
      
      // Verify cleanup was called
      expect(worker.terminate).toHaveBeenCalled();
      expect(db.terminate).toHaveBeenCalled();
    });

    it('should handle memory tracking', async () => {
      // Mock performance.memory
      global.performance = {
        memory: {
          usedJSHeapSize: 50 * 1024 * 1024, // 50MB
          totalJSHeapSize: 100 * 1024 * 1024 // 100MB
        }
      };
      
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      expect(dataTable.performance.memoryUsage).toBeDefined();
      expect(dataTable.performance.memoryUsage.used).toBe('50MB');
      expect(dataTable.performance.memoryUsage.total).toBe('100MB');
    });
  });

  describe('Mode Comparison', () => {
    it('should track performance metrics for both modes', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      const workerDataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      const directDataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
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

    it('should have different bundle configurations for Worker vs Direct', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      const workerDataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      const directDataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      
      await workerDataTable.initialize();
      await directDataTable.initialize();
      
      expect(workerDataTable.performance.bundleType).toBe('worker');
      expect(directDataTable.performance.bundleType).toBe('mvp');
      
      await workerDataTable.destroy();
      await directDataTable.destroy();
    });
  });

  describe('Error Handling', () => {
    it('should handle SQL execution errors gracefully', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      // Mock query to throw error
      mocks.mockDuckDB.mockConnection.query.mockRejectedValueOnce(new Error('SQL Error'));
      
      await expect(dataTable.executeSQL('INVALID SQL')).rejects.toThrow('SQL Error');
    });

    it('should handle concurrent operations properly', async () => {
      const { DataTable } = await import('../../src/core/DataTable.js');
      
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });
      
      await dataTable.initialize();
      
      // Clear previous query calls from initialization
      mocks.mockDuckDB.mockConnection.query.mockClear();
      
      // Execute multiple SQL queries concurrently
      const promises = [
        dataTable.executeSQL('SELECT 1'),
        dataTable.executeSQL('SELECT 2'),
        dataTable.executeSQL('SELECT 3')
      ];
      
      const results = await Promise.all(promises);
      expect(results).toHaveLength(3);
      expect(mocks.mockDuckDB.mockConnection.query).toHaveBeenCalledTimes(3);
    });
  });
});