// DuckDBHelpers.test.js - Comprehensive tests for DuckDB utility functions
import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  detectSchema,
  getTableInfo,
  getRowCount,
  getColumnStats,
  getDistinctValues,
  getDataProfile,
  formatSchemaForUI,
  detectColumnType
} from '../../src/data/DuckDBHelpers.js';

describe('DuckDBHelpers', () => {
  let mockConn;
  let mockDb;
  
  beforeEach(() => {
    // Mock DuckDB connection
    mockConn = {
      query: vi.fn()
    };
    
    // Mock DuckDB database instance
    mockDb = {
      conn: mockConn
    };
    
    vi.clearAllMocks();
  });

  describe('detectSchema', () => {
    it('should detect schema from DESCRIBE query result', async () => {
      const mockSchemaResult = {
        toArray: () => [
          { column_name: 'id', column_type: 'INTEGER', null: 'NO' },
          { column_name: 'name', column_type: 'VARCHAR', null: 'YES' },
          { column_name: 'price', column_type: 'DOUBLE', null: 'NO' },
          { column_name: 'created_at', column_type: 'TIMESTAMP', null: 'YES' }
        ]
      };
      
      mockConn.query.mockResolvedValue(mockSchemaResult);
      
      const schema = await detectSchema(mockConn, 'test_table');
      
      expect(mockConn.query).toHaveBeenCalledWith('DESCRIBE test_table');
      expect(schema).toEqual({
        id: {
          type: 'INTEGER',
          nullable: false,
          vizType: 'histogram'
        },
        name: {
          type: 'VARCHAR',
          nullable: true,
          vizType: 'categorical'
        },
        price: {
          type: 'DOUBLE',
          nullable: false,
          vizType: 'histogram'
        },
        created_at: {
          type: 'TIMESTAMP',
          nullable: true,
          vizType: 'temporal'
        }
      });
    });

    it('should work with db.conn pattern', async () => {
      const mockSchemaResult = {
        toArray: () => [
          { column_name: 'id', column_type: 'INTEGER', null: 'NO' }
        ]
      };
      
      mockConn.query.mockResolvedValue(mockSchemaResult);
      
      const schema = await detectSchema(mockDb, 'test_table');
      
      expect(mockConn.query).toHaveBeenCalledWith('DESCRIBE test_table');
      expect(schema.id).toBeDefined();
    });

    it('should throw error for invalid connection', async () => {
      await expect(detectSchema(null, 'test_table')).rejects.toThrow();
      await expect(detectSchema({}, 'test_table')).rejects.toThrow('Invalid DuckDB connection provided');
    });

    it('should throw error when query fails', async () => {
      mockConn.query.mockRejectedValue(new Error('Table not found'));
      
      await expect(detectSchema(mockConn, 'nonexistent_table')).rejects.toThrow(
        "Failed to detect schema for table 'nonexistent_table': Table not found"
      );
    });
  });

  describe('getTableInfo', () => {
    it('should return table info with row count and sample data', async () => {
      const mockCountResult = {
        toArray: () => [{ count: 1000 }]
      };
      
      const mockSampleResult = {
        toArray: () => [
          { id: 1, name: 'Alice', price: 10.50 },
          { id: 2, name: 'Bob', price: 15.25 },
          { id: 3, name: 'Charlie', price: 8.75 }
        ]
      };
      
      mockConn.query
        .mockResolvedValueOnce(mockCountResult)
        .mockResolvedValueOnce(mockSampleResult);
      
      const tableInfo = await getTableInfo(mockConn, 'products', 3);
      
      expect(mockConn.query).toHaveBeenNthCalledWith(1, 'SELECT COUNT(*) as count FROM products');
      expect(mockConn.query).toHaveBeenNthCalledWith(2, 'SELECT * FROM products LIMIT 3');
      
      expect(tableInfo).toEqual({
        tableName: 'products',
        rowCount: 1000,
        sampleData: [
          { id: 1, name: 'Alice', price: 10.50 },
          { id: 2, name: 'Bob', price: 15.25 },
          { id: 3, name: 'Charlie', price: 8.75 }
        ],
        sampleSize: 3
      });
    });

    it('should use default sample size of 5', async () => {
      const mockCountResult = { toArray: () => [{ count: 100 }] };
      const mockSampleResult = { toArray: () => [] };
      
      mockConn.query
        .mockResolvedValueOnce(mockCountResult)
        .mockResolvedValueOnce(mockSampleResult);
      
      await getTableInfo(mockConn, 'test_table');
      
      expect(mockConn.query).toHaveBeenNthCalledWith(2, 'SELECT * FROM test_table LIMIT 5');
    });
  });

  describe('getRowCount', () => {
    it('should return row count for table', async () => {
      const mockResult = {
        toArray: () => [{ count: 2500 }]
      };
      
      mockConn.query.mockResolvedValue(mockResult);
      
      const count = await getRowCount(mockConn, 'large_table');
      
      expect(mockConn.query).toHaveBeenCalledWith('SELECT COUNT(*) as count FROM large_table');
      expect(count).toBe(2500);
    });
  });

  describe('getColumnStats', () => {
    beforeEach(() => {
      // Mock DESCRIBE query for all tests
      const mockDescribeResult = {
        toArray: () => [
          { column_name: 'id', column_type: 'INTEGER', null: 'NO' },
          { column_name: 'name', column_type: 'VARCHAR', null: 'YES' },
          { column_name: 'price', column_type: 'DOUBLE', null: 'NO' },
          { column_name: 'created_at', column_type: 'TIMESTAMP', null: 'YES' }
        ]
      };
      mockConn.query.mockResolvedValue(mockDescribeResult);
    });

    it('should return numeric statistics for numeric columns', async () => {
      const mockStatsResult = {
        toArray: () => [{
          min_value: 1,
          max_value: 1000,
          avg_value: 245.67,
          non_null_count: 950,
          null_count: 50
        }]
      };
      
      mockConn.query
        .mockResolvedValueOnce({ toArray: () => [{ column_name: 'price', column_type: 'DOUBLE', null: 'NO' }] })
        .mockResolvedValueOnce(mockStatsResult);
      
      const stats = await getColumnStats(mockConn, 'products', 'price');
      
      expect(stats).toEqual({
        columnName: 'price',
        type: 'DOUBLE',
        nullable: false,
        minValue: 1,
        maxValue: 1000,
        avgValue: 245.67,
        nonNullCount: 950,
        nullCount: 50
      });
    });

    it('should return categorical statistics for text columns', async () => {
      const mockStatsResult = {
        toArray: () => [{
          distinct_count: 25,
          non_null_count: 950,
          null_count: 50
        }]
      };
      
      mockConn.query
        .mockResolvedValueOnce({ toArray: () => [{ column_name: 'name', column_type: 'VARCHAR', null: 'YES' }] })
        .mockResolvedValueOnce(mockStatsResult);
      
      const stats = await getColumnStats(mockConn, 'products', 'name');
      
      expect(stats).toEqual({
        columnName: 'name',
        type: 'VARCHAR',
        nullable: true,
        distinctCount: 25,
        nonNullCount: 950,
        nullCount: 50
      });
    });

    it('should return temporal statistics for date/time columns', async () => {
      const mockStatsResult = {
        toArray: () => [{
          min_date: '2023-01-01T00:00:00Z',
          max_date: '2023-12-31T23:59:59Z',
          non_null_count: 900,
          null_count: 100
        }]
      };
      
      mockConn.query
        .mockResolvedValueOnce({ toArray: () => [{ column_name: 'created_at', column_type: 'TIMESTAMP', null: 'YES' }] })
        .mockResolvedValueOnce(mockStatsResult);
      
      const stats = await getColumnStats(mockConn, 'products', 'created_at');
      
      expect(stats).toEqual({
        columnName: 'created_at',
        type: 'TIMESTAMP',
        nullable: true,
        minDate: '2023-01-01T00:00:00Z',
        maxDate: '2023-12-31T23:59:59Z',
        nonNullCount: 900,
        nullCount: 100
      });
    });

    it('should throw error for non-existent column', async () => {
      mockConn.query.mockResolvedValueOnce({ toArray: () => [] });
      
      await expect(getColumnStats(mockConn, 'products', 'nonexistent')).rejects.toThrow(
        "Column 'nonexistent' not found in table 'products'"
      );
    });
  });

  describe('getDistinctValues', () => {
    it('should return distinct values with counts', async () => {
      const mockResult = {
        toArray: () => [
          { value: 'Electronics', count: 150 },
          { value: 'Clothing', count: 120 },
          { value: 'Books', count: 80 },
          { value: 'Sports', count: 45 }
        ]
      };
      
      mockConn.query.mockResolvedValue(mockResult);
      
      const distinctValues = await getDistinctValues(mockConn, 'products', 'category', 10);
      
      expect(mockConn.query).toHaveBeenCalledWith(expect.stringContaining('GROUP BY category'));
      expect(mockConn.query).toHaveBeenCalledWith(expect.stringContaining('LIMIT 10'));
      expect(distinctValues).toHaveLength(4);
      expect(distinctValues[0]).toEqual({ value: 'Electronics', count: 150 });
    });

    it('should use default limit of 50', async () => {
      mockConn.query.mockResolvedValue({ toArray: () => [] });
      
      await getDistinctValues(mockConn, 'products', 'category');
      
      expect(mockConn.query).toHaveBeenCalledWith(expect.stringContaining('LIMIT 50'));
    });
  });

  describe('getDataProfile', () => {
    it('should return comprehensive data profile', async () => {
      // Mock getTableInfo
      const mockCountResult = { toArray: () => [{ count: 1000 }] };
      const mockSampleResult = { toArray: () => [{ id: 1, name: 'Test' }] };
      
      // Mock detectSchema
      const mockSchemaResult = {
        toArray: () => [
          { column_name: 'id', column_type: 'INTEGER', null: 'NO' },
          { column_name: 'name', column_type: 'VARCHAR', null: 'YES' }
        ]
      };
      
      // Mock column stats
      const mockNumericStats = {
        toArray: () => [{
          min_value: 1,
          max_value: 1000,
          avg_value: 500,
          non_null_count: 1000,
          null_count: 0
        }]
      };
      
      const mockCategoricalStats = {
        toArray: () => [{
          distinct_count: 25,
          non_null_count: 980,
          null_count: 20
        }]
      };
      
      mockConn.query
        // getTableInfo calls
        .mockResolvedValueOnce(mockCountResult)
        .mockResolvedValueOnce(mockSampleResult)
        // detectSchema call
        .mockResolvedValueOnce(mockSchemaResult)
        // getColumnStats for 'id' (numeric)
        .mockResolvedValueOnce({ toArray: () => [{ column_name: 'id', column_type: 'INTEGER', null: 'NO' }] })
        .mockResolvedValueOnce(mockNumericStats)
        // getColumnStats for 'name' (categorical)
        .mockResolvedValueOnce({ toArray: () => [{ column_name: 'name', column_type: 'VARCHAR', null: 'YES' }] })
        .mockResolvedValueOnce(mockCategoricalStats)
        // getDistinctValues for 'name'
        .mockResolvedValueOnce({ toArray: () => [{ value: 'Alice', count: 100 }] });
      
      const profile = await getDataProfile(mockConn, 'test_table');
      
      expect(profile).toHaveProperty('table');
      expect(profile).toHaveProperty('schema');
      expect(profile).toHaveProperty('columns');
      expect(profile).toHaveProperty('profiledAt');
      expect(profile.table.tableName).toBe('test_table');
      expect(profile.table.rowCount).toBe(1000);
      expect(profile.totalColumns).toBe(2);
      expect(profile.profiledColumns).toBe(2);
    });

    it('should handle column profiling errors gracefully', async () => {
      // Mock basic table info and schema
      mockConn.query
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] })
        .mockResolvedValueOnce({ toArray: () => [] })
        .mockResolvedValueOnce({ toArray: () => [{ column_name: 'bad_column', column_type: 'INTEGER', null: 'NO' }] })
        .mockResolvedValueOnce({ toArray: () => [{ column_name: 'bad_column', column_type: 'INTEGER', null: 'NO' }] })
        .mockRejectedValueOnce(new Error('Column statistics failed'));
      
      const profile = await getDataProfile(mockConn, 'test_table');
      
      expect(profile.columns.bad_column).toHaveProperty('error');
      expect(profile.columns.bad_column.error).toContain('Column statistics failed');
    });
  });

  describe('formatSchemaForUI', () => {
    it('should format schema for UI display', () => {
      const schema = {
        id: { type: 'INTEGER', nullable: false, vizType: 'histogram' },
        name: { type: 'VARCHAR', nullable: true, vizType: 'categorical' },
        price: { type: 'DOUBLE', nullable: false, vizType: 'histogram' }
      };
      
      const formatted = formatSchemaForUI(schema);
      
      expect(formatted).toHaveLength(3);
      expect(formatted[0]).toEqual({
        name: 'id',
        type: 'INTEGER',
        nullable: false,
        vizType: 'histogram',
        displayType: 'Int'
      });
      expect(formatted[1]).toEqual({
        name: 'name',
        type: 'VARCHAR',
        nullable: true,
        vizType: 'categorical',
        displayType: 'Text'
      });
    });
  });

  describe('detectColumnType', () => {
    it('should detect visualization types correctly', () => {
      expect(detectColumnType('INTEGER')).toBe('histogram');
      expect(detectColumnType('DOUBLE')).toBe('histogram');
      expect(detectColumnType('VARCHAR')).toBe('categorical');
      expect(detectColumnType('TIMESTAMP')).toBe('temporal');
      expect(detectColumnType('DATE')).toBe('temporal');
      expect(detectColumnType('BOOLEAN')).toBe('categorical');
    });
  });

  describe('Error Handling', () => {
    it('should handle connection errors consistently', async () => {
      const invalidConn = { query: null };
      
      await expect(detectSchema(invalidConn, 'test')).rejects.toThrow('Invalid DuckDB connection');
      await expect(getTableInfo(invalidConn, 'test')).rejects.toThrow('Invalid DuckDB connection');
      await expect(getRowCount(invalidConn, 'test')).rejects.toThrow('Invalid DuckDB connection');
      await expect(getColumnStats(invalidConn, 'test', 'col')).rejects.toThrow('Invalid DuckDB connection');
      await expect(getDistinctValues(invalidConn, 'test', 'col')).rejects.toThrow('Invalid DuckDB connection');
      await expect(getDataProfile(invalidConn, 'test')).rejects.toThrow('Invalid DuckDB connection');
    });

    it('should provide helpful error messages', async () => {
      mockConn.query.mockRejectedValue(new Error('Connection timeout'));
      
      await expect(detectSchema(mockConn, 'test')).rejects.toThrow(
        "Failed to detect schema for table 'test': Connection timeout"
      );
      
      await expect(getTableInfo(mockConn, 'test')).rejects.toThrow(
        "Failed to get table info for 'test': Connection timeout"
      );
    });
  });
});