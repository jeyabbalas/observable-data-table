import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { BatchingConnector } from '../../src/connectors/BatchingConnector.js';

describe('BatchingConnector', () => {
  let batchingConnector;
  let mockBaseConnector;

  beforeEach(() => {
    vi.useFakeTimers();
    
    mockBaseConnector = {
      query: vi.fn().mockImplementation(async (request) => {
        return { data: [{ result: request.sql }], sql: request.sql };
      })
    };

    vi.clearAllMocks();
  });

  afterEach(() => {
    if (batchingConnector) {
      batchingConnector.destroy();
      batchingConnector = null;
    }
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  describe('Constructor', () => {
    it('should create BatchingConnector with default options', () => {
      batchingConnector = new BatchingConnector(mockBaseConnector);
      
      expect(batchingConnector.baseConnector).toBe(mockBaseConnector);
      expect(batchingConnector.batchWindow).toBe(10);
      expect(batchingConnector.maxBatchSize).toBe(10);
      expect(batchingConnector.enabled).toBe(true);
      expect(batchingConnector.pendingQueries).toEqual([]);
      expect(batchingConnector.batchTimer).toBeNull();
      expect(batchingConnector.executing).toBe(false);
    });

    it('should create BatchingConnector with custom options', () => {
      batchingConnector = new BatchingConnector(mockBaseConnector, {
        batchWindow: 20,
        maxBatchSize: 5,
        enabled: false
      });
      
      expect(batchingConnector.batchWindow).toBe(20);
      expect(batchingConnector.maxBatchSize).toBe(5);
      expect(batchingConnector.enabled).toBe(false);
    });

    it('should initialize statistics', () => {
      batchingConnector = new BatchingConnector(mockBaseConnector);
      
      expect(batchingConnector.stats).toEqual({
        queriesProcessed: 0,
        batchesExecuted: 0,
        totalBatchTime: 0,
        avgBatchSize: 0
      });
    });
  });

  describe('shouldNotBatch Logic', () => {
    beforeEach(() => {
      batchingConnector = new BatchingConnector(mockBaseConnector);
    });

    it('should not batch DDL operations', () => {
      const ddlQueries = [
        { sql: 'CREATE TABLE test (id INT)' },
        { sql: 'DROP TABLE test' },
        { sql: 'ALTER TABLE test ADD COLUMN name VARCHAR' }
      ];
      
      ddlQueries.forEach(request => {
        expect(batchingConnector.shouldNotBatch(request)).toBe(true);
      });
    });

    it('should not batch SET and PRAGMA commands', () => {
      const configQueries = [
        { sql: 'SET search_path TO public' },
        { sql: 'PRAGMA table_info(test)' }
      ];
      
      configQueries.forEach(request => {
        expect(batchingConnector.shouldNotBatch(request)).toBe(true);
      });
    });

    it('should not batch very large queries', () => {
      const largeQuery = {
        sql: 'SELECT * FROM test WHERE ' + 'condition AND '.repeat(200) + 'final_condition'
      };
      
      expect(batchingConnector.shouldNotBatch(largeQuery)).toBe(true);
    });

    it('should batch regular SELECT queries', () => {
      const regularQueries = [
        { sql: 'SELECT * FROM test' },
        { sql: 'SELECT COUNT(*) FROM users' },
        { sql: 'SELECT id, name FROM products WHERE price > 100' }
      ];
      
      regularQueries.forEach(request => {
        expect(batchingConnector.shouldNotBatch(request)).toBe(false);
      });
    });
  });

  describe('Query Execution - Disabled Batching', () => {
    beforeEach(() => {
      batchingConnector = new BatchingConnector(mockBaseConnector, { enabled: false });
    });

    it('should execute immediately when batching disabled', async () => {
      const request = { sql: 'SELECT * FROM test' };
      
      const resultPromise = batchingConnector.query(request);
      
      expect(mockBaseConnector.query).toHaveBeenCalledWith(request);
      expect(batchingConnector.pendingQueries).toHaveLength(0);
      
      const result = await resultPromise;
      expect(result.sql).toBe(request.sql);
    });
  });

  describe('Query Execution - Non-Batchable', () => {
    beforeEach(() => {
      batchingConnector = new BatchingConnector(mockBaseConnector);
    });

    it('should execute DDL operations immediately', async () => {
      const ddlRequest = { sql: 'CREATE TABLE test (id INT)' };
      
      const resultPromise = batchingConnector.query(ddlRequest);
      
      expect(mockBaseConnector.query).toHaveBeenCalledWith(ddlRequest);
      expect(batchingConnector.pendingQueries).toHaveLength(0);
      
      const result = await resultPromise;
      expect(result.sql).toBe(ddlRequest.sql);
    });
  });

  describe('Statistics', () => {
    beforeEach(() => {
      batchingConnector = new BatchingConnector(mockBaseConnector);
    });

    it('should provide statistics', () => {
      const stats = batchingConnector.getStats();
      
      expect(stats).toEqual({
        queriesProcessed: 0,
        batchesExecuted: 0,
        totalBatchTime: 0,
        avgBatchSize: 0,
        avgExecutionTime: 0,
        enabled: true,
        pendingQueries: 0
      });
    });
  });

  describe('Configuration', () => {
    beforeEach(() => {
      batchingConnector = new BatchingConnector(mockBaseConnector);
    });

    it('should allow enabling/disabling', () => {
      expect(batchingConnector.enabled).toBe(true);
      
      batchingConnector.setEnabled(false);
      expect(batchingConnector.enabled).toBe(false);
      
      batchingConnector.setEnabled(true);
      expect(batchingConnector.enabled).toBe(true);
    });
  });

  describe('Cleanup', () => {
    beforeEach(() => {
      batchingConnector = new BatchingConnector(mockBaseConnector);
    });

    it('should destroy properly', async () => {
      const clearTimeoutSpy = vi.spyOn(global, 'clearTimeout');
      
      // Start a batch and handle the rejection
      const promise = batchingConnector.query({ sql: 'SELECT 1' });
      
      expect(batchingConnector.pendingQueries).toHaveLength(1);
      expect(batchingConnector.batchTimer).toBeTruthy();
      
      batchingConnector.destroy();
      
      // Expect the promise to reject with "Connector destroyed"
      await expect(promise).rejects.toThrow('Connector destroyed');
      
      expect(clearTimeoutSpy).toHaveBeenCalled();
      expect(batchingConnector.batchTimer).toBeNull();
      expect(batchingConnector.pendingQueries).toEqual([]);
    });
  });
});