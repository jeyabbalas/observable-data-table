import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { QueryCache } from '../../src/core/QueryCache.js';

describe('QueryCache', () => {
  let queryCache;
  
  beforeEach(() => {
    // Use fake timers to control time-based behavior
    vi.useFakeTimers();
  });

  afterEach(() => {
    if (queryCache) {
      queryCache.destroy();
      queryCache = null;
    }
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  describe('Constructor', () => {
    it('should create cache with default options', () => {
      queryCache = new QueryCache();
      
      expect(queryCache.cache).toBeInstanceOf(Map);
      expect(queryCache.ttl).toBe(60000); // 1 minute
      expect(queryCache.maxSize).toBe(100);
      expect(queryCache.enabled).toBe(true);
      expect(queryCache.stats.hits).toBe(0);
      expect(queryCache.stats.misses).toBe(0);
      expect(queryCache.stats.evictions).toBe(0);
      expect(queryCache.cleanupInterval).toBeDefined();
    });

    it('should create cache with custom options', () => {
      queryCache = new QueryCache({
        ttl: 30000,
        maxSize: 50,
        enabled: false
      });
      
      expect(queryCache.ttl).toBe(30000);
      expect(queryCache.maxSize).toBe(50);
      expect(queryCache.enabled).toBe(false);
    });

    it('should start cleanup interval', () => {
      const setIntervalSpy = vi.spyOn(global, 'setInterval');
      
      queryCache = new QueryCache({ ttl: 60000 });
      
      expect(setIntervalSpy).toHaveBeenCalledWith(
        expect.any(Function),
        30000 // ttl / 2
      );
    });
  });

  describe('Cache Key Generation', () => {
    beforeEach(() => {
      queryCache = new QueryCache();
    });

    it('should normalize SQL whitespace', () => {
      const sql1 = 'SELECT   *   FROM    table   WHERE  id = 1';
      const sql2 = 'select * from table where id = 1';
      
      const key1 = queryCache.generateKey(sql1);
      const key2 = queryCache.generateKey(sql2);
      
      expect(key1).toBe(key2);
      expect(key1).toBe('select * from table where id = 1');
    });

    it('should include options in key', () => {
      const sql = 'SELECT * FROM table';
      
      const key1 = queryCache.generateKey(sql, {});
      const key2 = queryCache.generateKey(sql, { streaming: true });
      const key3 = queryCache.generateKey(sql, { limit: 100, offset: 50 });
      
      expect(key1).toBe('select * from table');
      expect(key2).toBe('select * from table|streaming');
      expect(key3).toBe('select * from table|limit:100|offset:50');
    });

    it('should handle empty SQL', () => {
      const key = queryCache.generateKey('', {});
      expect(key).toBe('');
    });
  });

  describe('Cache Operations', () => {
    beforeEach(() => {
      queryCache = new QueryCache({ ttl: 60000 });
    });

    it('should return null for cache miss', () => {
      const result = queryCache.get('SELECT * FROM test');
      
      expect(result).toBeNull();
      expect(queryCache.stats.misses).toBe(1);
      expect(queryCache.stats.hits).toBe(0);
    });

    it('should cache and retrieve results', () => {
      const sql = 'SELECT * FROM test';
      const data = [{ id: 1, name: 'Alice' }];
      
      queryCache.set(sql, data);
      const result = queryCache.get(sql);
      
      expect(result).toEqual(data);
      expect(queryCache.stats.hits).toBe(1);
      expect(queryCache.stats.misses).toBe(0);
    });

    it('should not cache when disabled', () => {
      queryCache = new QueryCache({ enabled: false });
      
      const sql = 'SELECT * FROM test';
      const data = [{ id: 1 }];
      
      queryCache.set(sql, data);
      const result = queryCache.get(sql);
      
      expect(result).toBeNull();
      expect(queryCache.cache.size).toBe(0);
    });

    it('should implement LRU ordering', () => {
      const sql1 = 'SELECT * FROM table1';
      const sql2 = 'SELECT * FROM table2';
      const data1 = [{ id: 1 }];
      const data2 = [{ id: 2 }];
      
      queryCache.set(sql1, data1);
      queryCache.set(sql2, data2);
      
      // Access sql1 to move it to end
      queryCache.get(sql1);
      
      // Check internal order (most recently used should be at end)
      const keys = Array.from(queryCache.cache.keys());
      expect(keys[keys.length - 1]).toBe(queryCache.generateKey(sql1));
    });
  });

  describe('TTL and Expiration', () => {
    beforeEach(() => {
      queryCache = new QueryCache({ ttl: 60000 }); // 1 minute
    });

    it('should expire entries after TTL', () => {
      const sql = 'SELECT * FROM test';
      const data = [{ id: 1 }];
      
      queryCache.set(sql, data);
      
      // Fast forward time beyond TTL
      vi.advanceTimersByTime(70000); // 70 seconds
      
      const result = queryCache.get(sql);
      
      expect(result).toBeNull();
      expect(queryCache.stats.misses).toBe(1);
      expect(queryCache.cache.size).toBe(0); // Entry should be deleted
    });

    it('should not expire entries within TTL', () => {
      const sql = 'SELECT * FROM test';
      const data = [{ id: 1 }];
      
      queryCache.set(sql, data);
      
      // Fast forward time within TTL
      vi.advanceTimersByTime(30000); // 30 seconds
      
      const result = queryCache.get(sql);
      
      expect(result).toEqual(data);
      expect(queryCache.stats.hits).toBe(1);
    });

    it('should run periodic cleanup', () => {
      const sql1 = 'SELECT * FROM table1';
      const sql2 = 'SELECT * FROM table2';
      
      queryCache.set(sql1, [{ id: 1 }]);
      queryCache.set(sql2, [{ id: 2 }]);
      
      expect(queryCache.cache.size).toBe(2);
      
      // Fast forward past TTL
      vi.advanceTimersByTime(70000);
      
      // Trigger cleanup interval
      vi.advanceTimersByTime(30000);
      
      expect(queryCache.cache.size).toBe(0);
    });
  });

  describe('Size Limits and Eviction', () => {
    beforeEach(() => {
      queryCache = new QueryCache({ maxSize: 3, ttl: 60000 });
    });

    it('should enforce max size with LRU eviction', () => {
      const queries = [
        'SELECT * FROM table1',
        'SELECT * FROM table2', 
        'SELECT * FROM table3',
        'SELECT * FROM table4' // This should trigger eviction
      ];
      
      // Fill cache to max size
      queries.forEach((sql, index) => {
        queryCache.set(sql, [{ id: index }]);
      });
      
      expect(queryCache.cache.size).toBe(3);
      expect(queryCache.stats.evictions).toBe(1);
      
      // First query should be evicted
      const result1 = queryCache.get(queries[0]);
      const result4 = queryCache.get(queries[3]);
      
      expect(result1).toBeNull();
      expect(result4).toEqual([{ id: 3 }]);
    });

    it('should not cache very large results', () => {
      const sql = 'SELECT * FROM large_table';
      const largeData = new Array(1000000).fill({ id: 1, data: 'x'.repeat(100) });
      
      queryCache.set(sql, largeData);
      
      // Should not be cached due to size limit
      expect(queryCache.cache.size).toBe(0);
    });

    it('should estimate result size correctly', () => {
      // Test with Array
      const arrayResult = [{ name: 'Alice', age: 30 }];
      const arraySize = queryCache.estimateSize(arrayResult);
      expect(arraySize).toBeGreaterThan(0);
      
      // Test with Arrow-like object
      const arrowResult = {
        numRows: 100,
        schema: { fields: [{ name: 'id' }, { name: 'name' }] }
      };
      const arrowSize = queryCache.estimateSize(arrowResult);
      expect(arrowSize).toBe(4000); // 100 rows * 2 fields * 20 bytes
      
      // Test with null
      expect(queryCache.estimateSize(null)).toBe(0);
    });
  });

  describe('Query Type Filtering', () => {
    beforeEach(() => {
      queryCache = new QueryCache();
    });

    it('should not cache DDL operations', () => {
      const ddlQueries = [
        'CREATE TABLE test (id INT)',
        'DROP TABLE test',
        'ALTER TABLE test ADD COLUMN name VARCHAR',
        'INSERT INTO test VALUES (1)',
        'UPDATE test SET name = "Alice"',
        'DELETE FROM test WHERE id = 1'
      ];
      
      ddlQueries.forEach(sql => {
        expect(queryCache.shouldNotCache(sql.toLowerCase())).toBe(true);
      });
    });

    it('should not cache PRAGMA and SET commands', () => {
      const configQueries = [
        'PRAGMA table_info(test)',
        'SET search_path TO public',
        'pragma foreign_keys=ON'
      ];
      
      configQueries.forEach(sql => {
        expect(queryCache.shouldNotCache(sql.toLowerCase())).toBe(true);
      });
    });

    it('should not cache queries with random functions', () => {
      const randomQueries = [
        'SELECT random() FROM test',
        'SELECT uuid() as id FROM test',
        'SELECT now() as timestamp FROM test',
        'SELECT current_timestamp FROM test'
      ];
      
      randomQueries.forEach(sql => {
        expect(queryCache.shouldNotCache(sql.toLowerCase())).toBe(true);
      });
    });

    it('should cache regular SELECT queries', () => {
      const selectQueries = [
        'SELECT * FROM test',
        'SELECT id, name FROM users WHERE age > 18',
        'SELECT COUNT(*) FROM products'
      ];
      
      selectQueries.forEach(sql => {
        expect(queryCache.shouldNotCache(sql.toLowerCase())).toBe(false);
      });
    });

    it('should respect shouldNotCache in set method', () => {
      const ddlSql = 'CREATE TABLE test (id INT)';
      const selectSql = 'SELECT * FROM test';
      
      queryCache.set(ddlSql, []);
      queryCache.set(selectSql, [{ id: 1 }]);
      
      expect(queryCache.cache.size).toBe(1); // Only SELECT should be cached
      expect(queryCache.get(ddlSql)).toBeNull();
      expect(queryCache.get(selectSql)).toEqual([{ id: 1 }]);
    });
  });

  describe('Statistics Tracking', () => {
    beforeEach(() => {
      queryCache = new QueryCache();
    });

    it('should track hits and misses', () => {
      const sql = 'SELECT * FROM test';
      const data = [{ id: 1 }];
      
      // Miss
      queryCache.get(sql);
      expect(queryCache.stats.misses).toBe(1);
      expect(queryCache.stats.hits).toBe(0);
      
      // Set and hit
      queryCache.set(sql, data);
      queryCache.get(sql);
      expect(queryCache.stats.hits).toBe(1);
      
      // Another hit
      queryCache.get(sql);
      expect(queryCache.stats.hits).toBe(2);
    });

    it('should track evictions', () => {
      queryCache = new QueryCache({ maxSize: 2 });
      
      queryCache.set('SELECT 1', [1]);
      queryCache.set('SELECT 2', [2]);
      queryCache.set('SELECT 3', [3]); // Should evict first
      
      expect(queryCache.stats.evictions).toBe(1);
    });

    it('should provide cache statistics', () => {
      const sql = 'SELECT * FROM test';
      queryCache.set(sql, [{ id: 1 }]);
      queryCache.get(sql); // hit
      queryCache.get('nonexistent'); // miss
      
      const stats = queryCache.getStats();
      
      expect(stats.hits).toBe(1);
      expect(stats.misses).toBe(1);
      expect(stats.evictions).toBe(0);
      expect(stats.size).toBe(1);
      expect(stats.enabled).toBe(true);
      expect(stats.hitRate).toBe('50.0%'); // 1 hit / (1 hit + 1 miss)
    });
  });

  describe('Cache Management', () => {
    beforeEach(() => {
      queryCache = new QueryCache();
    });

    it('should enable and disable caching', () => {
      const sql = 'SELECT * FROM test';
      const data = [{ id: 1 }];
      
      // Initially enabled
      queryCache.set(sql, data);
      expect(queryCache.get(sql)).toEqual(data);
      
      // Disable
      queryCache.setEnabled(false);
      expect(queryCache.get(sql)).toBeNull();
      
      // Re-enable
      queryCache.setEnabled(true);
      queryCache.set(sql, data);
      expect(queryCache.get(sql)).toEqual(data);
    });

    it('should clear all cache entries', () => {
      queryCache.set('SELECT 1', [1]);
      queryCache.set('SELECT 2', [2]);
      queryCache.get('SELECT 1'); // Create some stats
      
      expect(queryCache.cache.size).toBe(2);
      expect(queryCache.stats.hits).toBe(1);
      
      queryCache.clear();
      
      expect(queryCache.cache.size).toBe(0);
      expect(queryCache.stats.hits).toBe(0);
      expect(queryCache.stats.misses).toBe(0);
      expect(queryCache.stats.evictions).toBe(0);
    });

    it('should handle manual cleanup', () => {
      const sql1 = 'SELECT 1';
      const sql2 = 'SELECT 2';
      
      queryCache.set(sql1, [1]);
      queryCache.set(sql2, [2]);
      
      // Fast forward past TTL
      vi.advanceTimersByTime(70000);
      
      queryCache.cleanup();
      
      expect(queryCache.cache.size).toBe(0);
    });
  });

  describe('Result Cloning', () => {
    beforeEach(() => {
      queryCache = new QueryCache();
    });

    it('should clone cached results on set to prevent source mutation', () => {
      const sql = 'SELECT * FROM test';
      const originalData = [{ id: 1, name: 'Alice' }];
      
      queryCache.set(sql, originalData);
      
      // Modify original data after caching
      originalData[0].name = 'Bob';
      
      // Cached result should still have original value
      const cachedResult = queryCache.get(sql);
      expect(cachedResult[0].name).toBe('Alice');
    });

    it('should handle non-cloneable results gracefully', () => {
      const sql = 'SELECT * FROM test';
      const cyclicalObj = {};
      cyclicalObj.self = cyclicalObj;
      
      // Should not throw error
      expect(() => {
        queryCache.set(sql, cyclicalObj);
      }).not.toThrow();
    });
  });

  describe('Cleanup and Destruction', () => {
    it('should clear interval on destroy', () => {
      const clearIntervalSpy = vi.spyOn(global, 'clearInterval');
      
      queryCache = new QueryCache();
      const intervalId = queryCache.cleanupInterval;
      
      queryCache.destroy();
      
      expect(clearIntervalSpy).toHaveBeenCalledWith(intervalId);
      expect(queryCache.cleanupInterval).toBeNull();
    });

    it('should handle destroy when already destroyed', () => {
      queryCache = new QueryCache();
      
      queryCache.destroy();
      
      // Should not throw
      expect(() => {
        queryCache.destroy();
      }).not.toThrow();
    });
  });
});