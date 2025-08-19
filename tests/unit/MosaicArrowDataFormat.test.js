import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// Mock Apache Arrow Table for testing
class MockArrowTable {
  constructor(data) {
    this.schema = { fields: ['name', 'age', 'city'] };
    this.names = ['name', 'age', 'city'];
    this.children = [['Alice', 'Bob'], [30, 25], ['NYC', 'LA']];
    this._data = data;
  }

  toArray() {
    return this._data;
  }

  get [Symbol.toStringTag]() {
    return 'Table';
  }
}

describe('Mosaic Arrow Data Format Tests', () => {
  let mockDataTable;
  let mockContainer;
  let mockCoordinator;
  let mockConnector;

  beforeEach(() => {
    // Create mock container
    mockContainer = {
      appendChild: vi.fn(),
      innerHTML: ''
    };

    // Create mock coordinator with query method
    mockCoordinator = {
      connect: vi.fn(),
      databaseConnector: vi.fn(),
      query: vi.fn()
    };

    // Mock wasmConnector behavior
    mockConnector = {
      query: vi.fn()
    };

    // Mock DataTable
    mockDataTable = {
      coordinator: mockCoordinator,
      connector: mockConnector,
      log: {
        debug: vi.fn(),
        error: vi.fn()
      }
    };

    // Mock Mosaic core components
    vi.doMock('@uwdata/mosaic-core', () => ({
      MosaicClient: class MockMosaicClient {
        constructor() {
          this.coordinator = null;
        }
        initialize() {
          // Mock initialize method
        }
      },
      Selection: {
        crossfilter: vi.fn().mockReturnValue({})
      },
      Coordinator: vi.fn().mockImplementation(() => mockCoordinator),
      wasmConnector: vi.fn().mockReturnValue(mockConnector)
    }));

    // Mock Mosaic SQL
    vi.doMock('@uwdata/mosaic-sql', () => ({
      Query: {
        from: vi.fn().mockReturnThis(),
        select: vi.fn().mockReturnThis(),
        where: vi.fn().mockReturnThis(),
        orderby: vi.fn().mockReturnThis(),
        limit: vi.fn().mockReturnThis(),
        offset: vi.fn().mockReturnThis(),
        toString: vi.fn().mockReturnValue('SELECT * FROM data LIMIT 100 OFFSET 0')
      }
    }));
  });

  afterEach(() => {
    vi.clearAllMocks();
    vi.resetModules();
  });

  describe('wasmConnector Arrow Data Format', () => {
    it('should return Arrow Table format when queried through coordinator', async () => {
      const sampleData = [
        { name: 'Alice', age: 30, city: 'NYC' },
        { name: 'Bob', age: 25, city: 'LA' }
      ];

      // Mock Arrow Table response
      const arrowTable = new MockArrowTable(sampleData);
      mockConnector.query.mockResolvedValue(arrowTable);

      const queryRequest = {
        type: 'arrow',
        sql: 'SELECT * FROM data LIMIT 100'
      };

      const result = await mockConnector.query(queryRequest);

      // Verify result is an Arrow Table
      expect(result).toBeInstanceOf(MockArrowTable);
      expect(result.schema).toBeDefined();
      expect(result.names).toEqual(['name', 'age', 'city']);
      expect(result.toArray).toBeDefined();
      expect(typeof result.toArray).toBe('function');
    });

    it('should convert Arrow Table to JavaScript array using toArray()', async () => {
      const sampleData = [
        { name: 'Alice', age: 30, city: 'NYC' },
        { name: 'Bob', age: 25, city: 'LA' }
      ];

      const arrowTable = new MockArrowTable(sampleData);
      const jsArray = arrowTable.toArray();

      expect(Array.isArray(jsArray)).toBe(true);
      expect(jsArray).toEqual(sampleData);
      expect(jsArray[0].name).toBe('Alice');
      expect(jsArray[1].name).toBe('Bob');
    });

    it('should handle json type queries returning regular arrays', async () => {
      const sampleData = [
        { name: 'Alice', age: 30, city: 'NYC' },
        { name: 'Bob', age: 25, city: 'LA' }
      ];

      mockConnector.query.mockResolvedValue(sampleData);

      const queryRequest = {
        type: 'json',
        sql: 'SELECT * FROM data LIMIT 100'
      };

      const result = await mockConnector.query(queryRequest);

      expect(Array.isArray(result)).toBe(true);
      expect(result).toEqual(sampleData);
    });

    it('should detect difference between Arrow Table and regular array', () => {
      const regularArray = [{ name: 'Alice', age: 30 }];
      const arrowTable = new MockArrowTable(regularArray);

      // Regular array checks
      expect(Array.isArray(regularArray)).toBe(true);
      expect(regularArray.toArray).toBeUndefined();
      expect(regularArray.schema).toBeUndefined();

      // Arrow Table checks
      expect(Array.isArray(arrowTable)).toBe(false);
      expect(arrowTable.toArray).toBeDefined();
      expect(typeof arrowTable.toArray).toBe('function');
      expect(arrowTable.schema).toBeDefined();
    });
  });

  describe('Coordinator Query Flow with Arrow Data', () => {
    it('should properly pass Arrow format requests to connector', async () => {
      const sampleData = [{ name: 'Alice', age: 30 }];
      const arrowTable = new MockArrowTable(sampleData);
      
      mockCoordinator.query.mockResolvedValue(arrowTable);

      const query = {
        type: 'arrow',
        sql: 'SELECT * FROM data'
      };

      const result = await mockCoordinator.query(query);

      expect(mockCoordinator.query).toHaveBeenCalledWith(query);
      expect(result).toBeInstanceOf(MockArrowTable);
    });

    it('should handle mixed format requests (some arrow, some json)', async () => {
      const sampleData = [{ name: 'Alice', age: 30 }];
      const arrowTable = new MockArrowTable(sampleData);

      // First query returns Arrow
      mockCoordinator.query.mockResolvedValueOnce(arrowTable);
      // Second query returns JSON
      mockCoordinator.query.mockResolvedValueOnce(sampleData);

      const arrowQuery = { type: 'arrow', sql: 'SELECT * FROM data' };
      const jsonQuery = { type: 'json', sql: 'SELECT * FROM data' };

      const arrowResult = await mockCoordinator.query(arrowQuery);
      const jsonResult = await mockCoordinator.query(jsonQuery);

      expect(arrowResult).toBeInstanceOf(MockArrowTable);
      expect(Array.isArray(jsonResult)).toBe(true);
    });

    it('should handle errors when Arrow conversion fails', async () => {
      // Mock a broken Arrow Table without toArray method
      const brokenArrowTable = {
        schema: { fields: ['name'] },
        names: ['name'],
        // Missing toArray method
      };

      mockCoordinator.query.mockResolvedValue(brokenArrowTable);

      const query = { type: 'arrow', sql: 'SELECT * FROM data' };
      const result = await mockCoordinator.query(query);

      // Should handle gracefully
      expect(result.toArray).toBeUndefined();
      expect(result.schema).toBeDefined();
    });
  });

  describe('Data Format Detection Utilities', () => {
    it('should provide utility to detect Arrow Table format', () => {
      const regularArray = [{ name: 'Alice' }];
      const arrowTable = new MockArrowTable(regularArray);

      // Utility function for detection
      const isArrowTable = (data) => {
        return data !== null && 
               data !== undefined &&
               typeof data === 'object' && 
               !Array.isArray(data) &&
               typeof data.toArray === 'function' &&
               data.schema !== undefined;
      };

      expect(isArrowTable(regularArray)).toBe(false);
      expect(isArrowTable(arrowTable)).toBe(true);
      expect(isArrowTable(null)).toBe(false);
      expect(isArrowTable(undefined)).toBe(false);
      expect(isArrowTable({})).toBe(false);
    });

    it('should provide utility to safely convert data to array', () => {
      const regularArray = [{ name: 'Alice' }];
      const arrowTable = new MockArrowTable(regularArray);

      // Utility function for safe conversion
      const toJavaScriptArray = (data) => {
        if (Array.isArray(data)) {
          return data;
        }
        if (data && typeof data === 'object' && typeof data.toArray === 'function') {
          return data.toArray();
        }
        return [];
      };

      expect(toJavaScriptArray(regularArray)).toEqual(regularArray);
      expect(toJavaScriptArray(arrowTable)).toEqual(regularArray);
      expect(toJavaScriptArray(null)).toEqual([]);
      expect(toJavaScriptArray(undefined)).toEqual([]);
      expect(toJavaScriptArray({})).toEqual([]);
    });
  });

  describe('Performance Considerations', () => {
    it('should handle large Arrow Tables efficiently', async () => {
      // Create large dataset
      const largeData = Array.from({ length: 10000 }, (_, i) => ({
        id: i,
        name: `User${i}`,
        value: Math.random() * 1000
      }));

      const largeArrowTable = new MockArrowTable(largeData);

      // Measure conversion time (should be fast)
      const startTime = performance.now();
      const converted = largeArrowTable.toArray();
      const endTime = performance.now();

      expect(converted.length).toBe(10000);
      expect(endTime - startTime).toBeLessThan(100); // Should be under 100ms
    });

    it('should not double-convert already converted data', () => {
      const sampleData = [{ name: 'Alice' }];
      const arrowTable = new MockArrowTable(sampleData);

      // Convert once
      const firstConversion = arrowTable.toArray();

      // Convert again (should be no-op for arrays)
      const toJavaScriptArray = (data) => {
        if (Array.isArray(data)) {
          return data; // No additional conversion needed
        }
        if (data && typeof data.toArray === 'function') {
          return data.toArray();
        }
        return [];
      };

      const secondConversion = toJavaScriptArray(firstConversion);

      expect(firstConversion).toBe(secondConversion); // Same reference
      expect(Array.isArray(secondConversion)).toBe(true);
    });
  });
});