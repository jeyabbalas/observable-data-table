import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// Mock Apache Arrow Table for testing (same as previous test)
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

describe('TableRenderer Arrow Data Handling', () => {
  let TableRenderer;
  let tableRenderer;
  let mockContainer;
  let mockCoordinator;
  let mockConnection;

  beforeEach(async () => {
    // Create mock DOM elements
    mockContainer = {
      appendChild: vi.fn(),
      innerHTML: ''
    };

    // Mock table element structure
    const mockTableElement = {
      appendChild: vi.fn(),
      className: '',
      style: {}
    };

    const mockThead = {
      appendChild: vi.fn(),
      innerHTML: ''
    };

    const mockTbody = {
      appendChild: vi.fn(),
      innerHTML: ''
    };

    // Mock DOM methods
    global.document = {
      createElement: vi.fn().mockImplementation((tag) => {
        if (tag === 'table') return mockTableElement;
        if (tag === 'thead') return mockThead;
        if (tag === 'tbody') return mockTbody;
        if (tag === 'div') return { 
          appendChild: vi.fn(), 
          className: '', 
          style: {},
          addEventListener: vi.fn()
        };
        if (tag === 'tr') return { 
          appendChild: vi.fn(), 
          style: {},
          addEventListener: vi.fn()
        };
        if (tag === 'th') return { 
          appendChild: vi.fn(), 
          style: {},
          addEventListener: vi.fn(),
          textContent: ''
        };
        if (tag === 'td') return { 
          appendChild: vi.fn(), 
          style: {},
          textContent: ''
        };
        return { 
          appendChild: vi.fn(), 
          addEventListener: vi.fn(),
          style: {},
          innerHTML: '',
          textContent: ''
        };
      }),
      createDocumentFragment: vi.fn().mockReturnValue({
        appendChild: vi.fn()
      })
    };

    mockCoordinator = {
      connect: vi.fn(),
      query: vi.fn()
    };

    mockConnection = {
      query: vi.fn()
    };

    // Mock Mosaic components
    vi.doMock('@uwdata/mosaic-core', () => ({
      MosaicClient: class MockMosaicClient {
        constructor() {
          this.coordinator = null;
        }
        initialize() {
          // Mock parent initialize
        }
      },
      Selection: {
        crossfilter: vi.fn().mockReturnValue({})
      }
    }));

    // Mock Mosaic SQL Query
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

    // Mock signals
    vi.doMock('@preact/signals-core', () => ({
      signal: vi.fn().mockImplementation((value) => ({
        value: value
      }))
    }));

    // Import TableRenderer after mocking
    const module = await import('../../src/core/TableRenderer.js');
    TableRenderer = module.TableRenderer;

    // Create TableRenderer instance
    tableRenderer = new TableRenderer({
      table: 'data',
      schema: { name: { type: 'string' }, age: { type: 'number' } },
      container: mockContainer,
      coordinator: mockCoordinator,
      connection: mockConnection
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
    vi.resetModules();
  });

  describe('queryResult() Method with Arrow Data', () => {
    it('should handle regular JavaScript array data', () => {
      const regularData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];

      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      const result = tableRenderer.queryResult(regularData);

      expect(renderRowsSpy).toHaveBeenCalledWith(regularData);
      expect(result).toBe(tableRenderer); // Should return this for chaining
    });

    it('should convert Arrow Table to JavaScript array', () => {
      const sampleData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];
      const arrowTable = new MockArrowTable(sampleData);

      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      const result = tableRenderer.queryResult(arrowTable);

      // Should call renderRows with converted array, not Arrow table
      expect(renderRowsSpy).toHaveBeenCalledWith(sampleData);
      expect(renderRowsSpy).not.toHaveBeenCalledWith(arrowTable);
      expect(result).toBe(tableRenderer);
    });

    it('should handle null/undefined data gracefully', () => {
      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      tableRenderer.queryResult(null);
      expect(renderRowsSpy).toHaveBeenCalledWith(null);

      tableRenderer.queryResult(undefined);
      expect(renderRowsSpy).toHaveBeenCalledWith(undefined);
    });

    it('should handle malformed Arrow Table without toArray method', () => {
      const malformedArrowTable = {
        schema: { fields: ['name'] },
        names: ['name'],
        // Missing toArray method
      };

      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      // Should not crash, should pass through as-is
      tableRenderer.queryResult(malformedArrowTable);
      expect(renderRowsSpy).toHaveBeenCalledWith(malformedArrowTable);
    });
  });

  describe('renderRows() Method Validation', () => {
    it('should accept JavaScript arrays', () => {
      const regularData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];

      const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
      
      tableRenderer.renderRows(regularData);

      // Should not warn about invalid data
      expect(consoleSpy).not.toHaveBeenCalled();
      
      consoleSpy.mockRestore();
    });

    it('should reject Arrow Table format directly', () => {
      const sampleData = [{ name: 'Alice', age: 30 }];
      const arrowTable = new MockArrowTable(sampleData);

      const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
      
      tableRenderer.renderRows(arrowTable);

      // Should warn about invalid data (expects array, got object)
      expect(consoleSpy).toHaveBeenCalledWith('Invalid data provided to renderRows:', arrowTable);
      
      consoleSpy.mockRestore();
    });

    it('should reject null and undefined', () => {
      const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
      
      tableRenderer.renderRows(null);
      expect(consoleSpy).toHaveBeenCalledWith('Invalid data provided to renderRows:', null);

      tableRenderer.renderRows(undefined);
      expect(consoleSpy).toHaveBeenCalledWith('Invalid data provided to renderRows:', undefined);
      
      consoleSpy.mockRestore();
    });
  });

  describe('Integration with Mosaic Coordinator', () => {
    it('should properly handle Arrow data from coordinator flow', async () => {
      const sampleData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];
      const arrowTable = new MockArrowTable(sampleData);

      // Mock the coordinator to return Arrow format
      mockCoordinator.query.mockResolvedValue(arrowTable);

      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      const queryResultSpy = vi.spyOn(tableRenderer, 'queryResult');

      // Simulate Mosaic coordinator calling queryResult with Arrow data
      tableRenderer.queryResult(arrowTable);

      expect(queryResultSpy).toHaveBeenCalledWith(arrowTable);
      expect(renderRowsSpy).toHaveBeenCalledWith(sampleData); // Converted to array
    });

    it('should work with initialize() flow and prevent fallback', async () => {
      const sampleData = [{ name: 'Alice', age: 30 }];
      const arrowTable = new MockArrowTable(sampleData);

      // Mock super.initialize() to immediately call queryResult
      const originalInitialize = tableRenderer.initialize;
      tableRenderer.initialize = async function() {
        this.connected = true;
        // Simulate Mosaic coordinator calling queryResult
        setTimeout(() => {
          this.queryResult(arrowTable);
        }, 100);
        return this;
      };

      const fallbackSpy = vi.spyOn(tableRenderer, 'fallbackDataLoad');
      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');

      await tableRenderer.initialize();

      // Use fake timers for immediate resolution
      vi.useFakeTimers();
      vi.advanceTimersByTime(200);
      vi.useRealTimers();

      // Should have received data and not called fallback
      expect(renderRowsSpy).toHaveBeenCalledWith(sampleData);
      expect(tableRenderer.data.length).toBeGreaterThan(0);
      expect(fallbackSpy).not.toHaveBeenCalled();
    });
  });

  describe('Data Format Conversion Edge Cases', () => {
    it('should handle empty Arrow Table', () => {
      const emptyArrowTable = new MockArrowTable([]);
      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      tableRenderer.queryResult(emptyArrowTable);
      
      expect(renderRowsSpy).toHaveBeenCalledWith([]);
    });

    it('should handle Arrow Table with complex nested data', () => {
      const complexData = [
        { 
          name: 'Alice', 
          age: 30, 
          address: { street: '123 Main St', city: 'NYC' },
          hobbies: ['reading', 'swimming']
        }
      ];
      const complexArrowTable = new MockArrowTable(complexData);
      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      tableRenderer.queryResult(complexArrowTable);
      
      expect(renderRowsSpy).toHaveBeenCalledWith(complexData);
    });

    it('should handle very large Arrow Tables', () => {
      const largeData = Array.from({ length: 1000 }, (_, i) => ({
        id: i,
        name: `User${i}`,
        value: Math.random()
      }));
      const largeArrowTable = new MockArrowTable(largeData);
      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      const startTime = performance.now();
      tableRenderer.queryResult(largeArrowTable);
      const endTime = performance.now();
      
      expect(renderRowsSpy).toHaveBeenCalledWith(largeData);
      expect(endTime - startTime).toBeLessThan(500); // Should be reasonably fast
    });
  });

  describe('Error Recovery', () => {
    it('should handle Arrow Table with broken toArray method', () => {
      const brokenArrowTable = {
        schema: { fields: ['name'] },
        names: ['name'],
        toArray: vi.fn().mockImplementation(() => {
          throw new Error('toArray conversion failed');
        })
      };

      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      // Should handle the error gracefully
      expect(() => {
        tableRenderer.queryResult(brokenArrowTable);
      }).not.toThrow();
      
      consoleSpy.mockRestore();
    });

    it('should fallback to empty array when Arrow conversion fails', () => {
      const brokenArrowTable = {
        schema: { fields: ['name'] },
        toArray: () => { throw new Error('Conversion failed'); }
      };

      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      // Arrow conversion fails, should fall back to empty array
      tableRenderer.queryResult(brokenArrowTable);
      
      // Should call renderRows with empty array as fallback
      expect(renderRowsSpy).toHaveBeenCalledWith([]);
    });
  });
});