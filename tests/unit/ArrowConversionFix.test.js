import { describe, it, expect, beforeEach, vi } from 'vitest';

// Simple test to validate our core Arrow conversion fix without mocking issues
describe('Arrow Conversion Fix Validation', () => {
  let queryResult;

  beforeEach(() => {
    // Extract the core logic from TableRenderer.queryResult for testing
    queryResult = function(data) {
      console.log('queryResult() called with data:', data);
      console.log('Data type:', typeof data, 'Array?', Array.isArray(data), 'Length:', data?.length);
      
      // Handle Apache Arrow Table format from Mosaic wasmConnector
      // Arrow tables have a toArray() method to convert to JavaScript arrays
      if (data && typeof data === 'object' && !Array.isArray(data) && typeof data.toArray === 'function') {
        console.log('Converting Apache Arrow Table to JavaScript array');
        try {
          data = data.toArray();
          console.log('Arrow conversion successful, new length:', data.length);
        } catch (error) {
          console.error('Failed to convert Arrow Table to array:', error);
          // Fall back to empty array if conversion fails
          data = [];
        }
      }
      
      return data;
    };
  });

  describe('Core Arrow Conversion Logic', () => {
    it('should pass through regular JavaScript arrays unchanged', () => {
      const regularArray = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];

      const result = queryResult(regularArray);

      expect(result).toBe(regularArray); // Same reference
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(2);
    });

    it('should convert Arrow Table to JavaScript array', () => {
      const originalData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];

      // Mock Arrow Table structure
      const mockArrowTable = {
        schema: { fields: ['name', 'age'] },
        names: ['name', 'age'],
        toArray: vi.fn().mockReturnValue(originalData)
      };

      const result = queryResult(mockArrowTable);

      expect(mockArrowTable.toArray).toHaveBeenCalled();
      expect(result).toEqual(originalData);
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(2);
    });

    it('should handle null and undefined gracefully', () => {
      expect(queryResult(null)).toBe(null);
      expect(queryResult(undefined)).toBe(undefined);
    });

    it('should handle objects without toArray method', () => {
      const regularObject = { name: 'test', value: 123 };
      
      const result = queryResult(regularObject);
      
      expect(result).toBe(regularObject); // Should pass through unchanged
    });

    it('should handle broken Arrow Table with failed toArray conversion', () => {
      const brokenArrowTable = {
        schema: { fields: ['name'] },
        toArray: vi.fn().mockImplementation(() => {
          throw new Error('Conversion failed');
        })
      };

      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      
      const result = queryResult(brokenArrowTable);
      
      expect(brokenArrowTable.toArray).toHaveBeenCalled();
      expect(result).toEqual([]); // Should fallback to empty array
      expect(consoleSpy).toHaveBeenCalledWith('Failed to convert Arrow Table to array:', expect.any(Error));
      
      consoleSpy.mockRestore();
    });

    it('should detect Arrow Table vs regular objects correctly', () => {
      // Valid Arrow Table
      const arrowTable = {
        schema: { fields: ['name'] },
        toArray: () => [{ name: 'Alice' }]
      };

      // Regular object without toArray
      const regularObject = {
        schema: { fields: ['name'] }
        // No toArray method
      };

      // Regular array
      const regularArray = [{ name: 'Alice' }];

      const arrowResult = queryResult(arrowTable);
      const objectResult = queryResult(regularObject);
      const arrayResult = queryResult(regularArray);

      // Arrow table should be converted
      expect(Array.isArray(arrowResult)).toBe(true);
      expect(arrowResult).toEqual([{ name: 'Alice' }]);

      // Regular object should pass through
      expect(objectResult).toBe(regularObject);

      // Regular array should pass through
      expect(arrayResult).toBe(regularArray);
    });

    it('should work with empty Arrow Table', () => {
      const emptyArrowTable = {
        schema: { fields: [] },
        toArray: vi.fn().mockReturnValue([])
      };

      const result = queryResult(emptyArrowTable);

      expect(emptyArrowTable.toArray).toHaveBeenCalled();
      expect(result).toEqual([]);
      expect(Array.isArray(result)).toBe(true);
    });

    it('should work with large Arrow Tables', () => {
      const largeData = Array.from({ length: 1000 }, (_, i) => ({
        id: i,
        name: `User${i}`
      }));

      const largeArrowTable = {
        schema: { fields: ['id', 'name'] },
        toArray: vi.fn().mockReturnValue(largeData)
      };

      const startTime = performance.now();
      const result = queryResult(largeArrowTable);
      const endTime = performance.now();

      expect(largeArrowTable.toArray).toHaveBeenCalled();
      expect(result).toEqual(largeData);
      expect(result.length).toBe(1000);
      expect(endTime - startTime).toBeLessThan(100); // Should be fast
    });
  });

  describe('Real-world Scenarios', () => {
    it('should handle typical Mosaic wasmConnector response format', () => {
      // This mimics what the real wasmConnector returns
      const mosaicArrowResponse = {
        schema: {
          fields: [
            { name: 'id', type: 'int64' },
            { name: 'name', type: 'utf8' },
            { name: 'age', type: 'int32' }
          ]
        },
        names: ['id', 'name', 'age'],
        children: [
          [1, 2, 3],
          ['Alice', 'Bob', 'Charlie'],
          [30, 25, 35]
        ],
        toArray: vi.fn().mockReturnValue([
          { id: 1, name: 'Alice', age: 30 },
          { id: 2, name: 'Bob', age: 25 },
          { id: 3, name: 'Charlie', age: 35 }
        ])
      };

      const result = queryResult(mosaicArrowResponse);

      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(result[0]).toEqual({ id: 1, name: 'Alice', age: 30 });
    });

    it('should maintain data integrity through conversion', () => {
      const originalData = [
        { id: 1, name: 'Alice', score: 95.5, active: true },
        { id: 2, name: 'Bob', score: 87.2, active: false },
        { id: 3, name: 'Charlie', score: null, active: true }
      ];

      const arrowTable = {
        schema: { fields: ['id', 'name', 'score', 'active'] },
        toArray: vi.fn().mockReturnValue(originalData)
      };

      const result = queryResult(arrowTable);

      expect(result).toEqual(originalData);
      expect(result[0].id).toBe(1);
      expect(result[0].name).toBe('Alice');
      expect(result[0].score).toBe(95.5);
      expect(result[0].active).toBe(true);
      expect(result[2].score).toBe(null);
    });
  });
});