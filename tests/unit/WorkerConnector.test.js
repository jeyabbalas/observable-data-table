import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { WorkerConnector } from '../../src/connectors/WorkerConnector.js';

describe('WorkerConnector', () => {
  let workerConnector;
  let mockDataTable;

  beforeEach(() => {
    mockDataTable = {
      sendToWorker: vi.fn()
    };
    workerConnector = new WorkerConnector(mockDataTable);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('Constructor', () => {
    it('should create WorkerConnector with DataTable reference', () => {
      expect(workerConnector.dataTable).toBe(mockDataTable);
    });
  });

  describe('query() method', () => {
    it('should handle exec type queries (no results)', async () => {
      const queryRequest = {
        type: 'exec',
        sql: 'CREATE TABLE test AS SELECT 1'
      };

      mockDataTable.sendToWorker.mockResolvedValue(undefined);

      const result = await workerConnector.query(queryRequest);

      expect(mockDataTable.sendToWorker).toHaveBeenCalledWith('exec', {
        sql: 'CREATE TABLE test AS SELECT 1'
      });
      expect(result).toBeUndefined();
    });

    it('should handle json type queries (with results)', async () => {
      const queryRequest = {
        type: 'json',
        sql: 'SELECT * FROM test LIMIT 5'
      };

      const mockResults = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' }
      ];

      mockDataTable.sendToWorker.mockResolvedValue(mockResults);

      const result = await workerConnector.query(queryRequest);

      expect(mockDataTable.sendToWorker).toHaveBeenCalledWith('exec', {
        sql: 'SELECT * FROM test LIMIT 5'
      });
      expect(result).toEqual(mockResults);
    });

    it('should handle arrow type queries (returns json for now)', async () => {
      const queryRequest = {
        type: 'arrow',
        sql: 'SELECT * FROM test LIMIT 5'
      };

      const mockResults = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' }
      ];

      mockDataTable.sendToWorker.mockResolvedValue(mockResults);

      const result = await workerConnector.query(queryRequest);

      expect(mockDataTable.sendToWorker).toHaveBeenCalledWith('exec', {
        sql: 'SELECT * FROM test LIMIT 5'
      });
      expect(result).toEqual(mockResults);
    });

    it('should handle complex SQL queries', async () => {
      const complexSQL = `
        SELECT 
          column1,
          COUNT(*) as count,
          AVG(column2) as avg_value
        FROM test_table 
        WHERE column1 > 100 
        GROUP BY column1 
        ORDER BY count DESC 
        LIMIT 10
      `;

      const queryRequest = {
        type: 'json',
        sql: complexSQL
      };

      mockDataTable.sendToWorker.mockResolvedValue([]);

      await workerConnector.query(queryRequest);

      expect(mockDataTable.sendToWorker).toHaveBeenCalledWith('exec', {
        sql: complexSQL
      });
    });

    it('should propagate worker errors', async () => {
      const queryRequest = {
        type: 'json',
        sql: 'SELECT * FROM nonexistent_table'
      };

      const workerError = new Error('Table does not exist');
      mockDataTable.sendToWorker.mockRejectedValue(workerError);

      await expect(workerConnector.query(queryRequest)).rejects.toThrow('Table does not exist');

      expect(mockDataTable.sendToWorker).toHaveBeenCalledWith('exec', {
        sql: 'SELECT * FROM nonexistent_table'
      });
    });

    it('should handle worker timeout errors', async () => {
      const queryRequest = {
        type: 'json',
        sql: 'SELECT * FROM large_table'
      };

      const timeoutError = new Error('Worker operation timeout');
      mockDataTable.sendToWorker.mockRejectedValue(timeoutError);

      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      await expect(workerConnector.query(queryRequest)).rejects.toThrow('Worker operation timeout');

      expect(consoleSpy).toHaveBeenCalledWith('WorkerConnector query failed:', timeoutError);
      
      consoleSpy.mockRestore();
    });

    it('should handle queries with additional options', async () => {
      const queryRequest = {
        type: 'json',
        sql: 'SELECT * FROM test',
        cache: true,
        priority: 1
      };

      mockDataTable.sendToWorker.mockResolvedValue([]);

      await workerConnector.query(queryRequest);

      // Should still only pass sql to worker (options are handled by coordinator)
      expect(mockDataTable.sendToWorker).toHaveBeenCalledWith('exec', {
        sql: 'SELECT * FROM test'
      });
    });
  });

  describe('getDuckDB() method', () => {
    it('should throw error as DuckDB instance is not available in worker mode', async () => {
      await expect(workerConnector.getDuckDB()).rejects.toThrow(
        'DuckDB instance not available in worker mode'
      );
    });
  });

  describe('getConnection() method', () => {
    it('should throw error as DuckDB connection is not available in worker mode', async () => {
      await expect(workerConnector.getConnection()).rejects.toThrow(
        'DuckDB connection not available in worker mode'
      );
    });
  });

  describe('Integration with Mosaic QueryManager', () => {
    it('should match the Mosaic Connector interface signature', () => {
      // Verify that WorkerConnector has the required methods
      expect(typeof workerConnector.query).toBe('function');
      expect(typeof workerConnector.getDuckDB).toBe('function');
      expect(typeof workerConnector.getConnection).toBe('function');
    });

    it('should handle the format that Mosaic QueryManager sends', async () => {
      // This is the exact format that Mosaic QueryManager sends
      const mosaicQueryRequest = {
        type: 'json',
        sql: 'SELECT * FROM data LIMIT 100 OFFSET 0'
      };

      mockDataTable.sendToWorker.mockResolvedValue([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ]);

      const result = await workerConnector.query(mosaicQueryRequest);

      expect(result).toHaveLength(2);
      expect(result[0].name).toBe('Alice');
      expect(result[1].name).toBe('Bob');
    });

    it('should work with Mosaic SQL Query objects converted to strings', async () => {
      // Mosaic converts Query objects to SQL strings
      const mosaicQueryRequest = {
        type: 'json',
        sql: 'SELECT * FROM data WHERE name = \'Alice\' ORDER BY age ASC LIMIT 100 OFFSET 0'
      };

      mockDataTable.sendToWorker.mockResolvedValue([
        { name: 'Alice', age: 30 }
      ]);

      const result = await workerConnector.query(mosaicQueryRequest);

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });
  });

  describe('Error Handling', () => {
    it('should handle null/undefined sql gracefully', async () => {
      const queryRequest = {
        type: 'json',
        sql: null
      };

      mockDataTable.sendToWorker.mockRejectedValue(new Error('Invalid SQL'));

      await expect(workerConnector.query(queryRequest)).rejects.toThrow('Invalid SQL');
    });

    it('should handle malformed query requests', async () => {
      const queryRequest = {
        // Missing type and sql
      };

      mockDataTable.sendToWorker.mockRejectedValue(new Error('Missing required parameters'));

      await expect(workerConnector.query(queryRequest)).rejects.toThrow('Missing required parameters');
    });

    it('should handle worker not being initialized', async () => {
      const queryRequest = {
        type: 'json',
        sql: 'SELECT 1'
      };

      const workerError = new Error('Worker not initialized');
      mockDataTable.sendToWorker.mockRejectedValue(workerError);

      await expect(workerConnector.query(queryRequest)).rejects.toThrow('Worker not initialized');
    });
  });
});