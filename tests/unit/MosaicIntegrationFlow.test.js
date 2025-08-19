import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// Mock Apache Arrow Table
class MockArrowTable {
  constructor(data) {
    this.schema = { fields: Object.keys(data[0] || {}) };
    this.names = Object.keys(data[0] || {});
    this._data = data;
  }

  toArray() {
    return this._data;
  }

  get [Symbol.toStringTag]() {
    return 'Table';
  }
}

describe('Mosaic Integration Flow Tests', () => {
  let DataTable;
  let dataTable;
  let mockContainer;

  beforeEach(async () => {
    // Mock DOM environment
    mockContainer = {
      appendChild: vi.fn(),
      innerHTML: ''
    };

    global.document = {
      createElement: vi.fn().mockImplementation((tag) => ({
        appendChild: vi.fn(),
        addEventListener: vi.fn(),
        style: {},
        className: '',
        innerHTML: '',
        textContent: ''
      })),
      createDocumentFragment: vi.fn().mockReturnValue({
        appendChild: vi.fn()
      })
    };

    global.performance = {
      now: vi.fn().mockReturnValue(Date.now())
    };

    // Mock Mosaic Core
    vi.doMock('@uwdata/mosaic-core', () => {
      const mockConnector = {
        query: vi.fn()
      };

      const mockCoordinator = {
        connect: vi.fn(),
        databaseConnector: vi.fn(),
        query: vi.fn()
      };

      return {
        MosaicClient: class MockMosaicClient {
          constructor() {
            this.coordinator = null;
          }
          initialize() {
            // Will be overridden by TableRenderer
          }
        },
        Selection: {
          crossfilter: vi.fn().mockReturnValue({})
        },
        Coordinator: vi.fn().mockImplementation(() => mockCoordinator),
        wasmConnector: vi.fn().mockReturnValue(mockConnector)
      };
    });

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

    // Mock DuckDB WASM
    vi.doMock('@duckdb/duckdb-wasm', () => ({
      AsyncDuckDB: vi.fn().mockImplementation(() => ({
        instantiate: vi.fn().mockResolvedValue(),
        connect: vi.fn().mockResolvedValue({
          query: vi.fn().mockResolvedValue({
            toArray: vi.fn().mockReturnValue([])
          })
        }),
        registerFileText: vi.fn().mockResolvedValue()
      })),
      selectBundle: vi.fn().mockResolvedValue({
        mainModule: 'mock-module.wasm',
        pthreadWorker: 'mock-worker.js'
      }),
      ConsoleLogger: vi.fn(),
      LogLevel: { WARNING: 1 }
    }));

    // Mock signals
    vi.doMock('@preact/signals-core', () => ({
      signal: vi.fn().mockImplementation((value) => ({ value }))
    }));

    // Mock DuckDBHelpers
    vi.doMock('../../src/data/DuckDBHelpers.js', () => ({
      detectSchema: vi.fn().mockResolvedValue({
        name: { type: 'string' },
        age: { type: 'number' }
      }),
      getRowCount: vi.fn().mockResolvedValue(1000n),
      getDataProfile: vi.fn().mockResolvedValue({
        tableName: 'data',
        rowCount: 1000,
        columnCount: 2
      })
    }));

    // Import DataTable after mocking
    const module = await import('../../src/core/DataTable.js');
    DataTable = module.DataTable;
  });

  afterEach(() => {
    vi.clearAllMocks();
    vi.resetModules();
  });

  describe('Full Direct Mode Flow', () => {
    it('should complete full flow: DataTable → Coordinator → wasmConnector → Arrow → TableRenderer', async () => {
      const sampleData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];
      const arrowTable = new MockArrowTable(sampleData);

      // Mock wasmConnector to return Arrow format
      const { wasmConnector } = await import('@uwdata/mosaic-core');
      wasmConnector.mockReturnValue({
        query: vi.fn().mockResolvedValue(arrowTable)
      });

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      // Verify wasmConnector was configured with DuckDB instances
      expect(wasmConnector).toHaveBeenCalledWith({
        duckdb: expect.any(Object),
        connection: expect.any(Object)
      });

      // Verify coordinator was configured with connector
      expect(dataTable.coordinator.databaseConnector).toHaveBeenCalledWith(
        expect.objectContaining({
          query: expect.any(Function)
        })
      );
    });

    it('should handle data loading with Mosaic integration', async () => {
      const { wasmConnector, Coordinator } = await import('@uwdata/mosaic-core');
      
      const mockConnector = {
        query: vi.fn().mockResolvedValue(new MockArrowTable([
          { name: 'Alice', age: 30 },
          { name: 'Bob', age: 25 }
        ]))
      };

      const mockCoordinator = {
        connect: vi.fn(),
        databaseConnector: vi.fn(),
        query: vi.fn().mockResolvedValue(new MockArrowTable([
          { name: 'Alice', age: 30 },
          { name: 'Bob', age: 25 }
        ]))
      };

      wasmConnector.mockReturnValue(mockConnector);
      Coordinator.mockReturnValue(mockCoordinator);

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      // Simulate loading data with Mosaic
      const csvData = 'name,age\nAlice,30\nBob,25';
      await dataTable.loadData('csv', new TextEncoder().encode(csvData), {
        tableName: 'data'
      });

      expect(dataTable.tableRenderer).toBeDefined();
      expect(mockCoordinator.connect).toHaveBeenCalledWith(dataTable.tableRenderer);
    });

    it('should prevent fallback queries when Mosaic connector works', async () => {
      const sampleData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];

      const { wasmConnector, Coordinator } = await import('@uwdata/mosaic-core');
      
      const mockConnector = {
        query: vi.fn().mockResolvedValue(new MockArrowTable(sampleData))
      };

      const mockCoordinator = {
        connect: vi.fn().mockImplementation((tableRenderer) => {
          // Simulate immediate data response from coordinator
          setTimeout(() => {
            tableRenderer.queryResult(new MockArrowTable(sampleData));
          }, 50);
        }),
        databaseConnector: vi.fn(),
        query: vi.fn()
      };

      wasmConnector.mockReturnValue(mockConnector);
      Coordinator.mockReturnValue(mockCoordinator);

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      const csvData = 'name,age\nAlice,30\nBob,25';
      await dataTable.loadData('csv', new TextEncoder().encode(csvData), {
        tableName: 'data'
      });

      // Spy on fallback method
      const fallbackSpy = vi.spyOn(dataTable.tableRenderer, 'fallbackDataLoad');

      // Wait for coordinator response
      await new Promise(resolve => setTimeout(resolve, 200));

      // Fallback should NOT be called because Mosaic provided data
      expect(fallbackSpy).not.toHaveBeenCalled();
      expect(dataTable.tableRenderer.data.length).toBeGreaterThan(0);
    });
  });

  describe('Worker Mode Integration', () => {
    it('should setup WorkerConnector for Mosaic in worker mode', async () => {
      // Mock Worker
      global.Worker = vi.fn().mockImplementation(() => ({
        onmessage: null,
        postMessage: vi.fn(),
        terminate: vi.fn()
      }));

      const { Coordinator } = await import('@uwdata/mosaic-core');
      
      const mockCoordinator = {
        connect: vi.fn(),
        databaseConnector: vi.fn(),
        query: vi.fn()
      };

      Coordinator.mockReturnValue(mockCoordinator);

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: true
      });

      await dataTable.initialize();

      // Should use WorkerConnector, not wasmConnector
      expect(dataTable.connector).toBeDefined();
      expect(dataTable.connector.constructor.name).toBe('WorkerConnector');

      // Coordinator should be configured with WorkerConnector
      expect(mockCoordinator.databaseConnector).toHaveBeenCalledWith(
        expect.objectContaining({
          query: expect.any(Function)
        })
      );
    });

    it('should handle Arrow data consistently between Direct and Worker modes', async () => {
      const sampleData = [{ name: 'Alice', age: 30 }];

      // Test both modes
      for (const useWorker of [false, true]) {
        if (useWorker) {
          global.Worker = vi.fn().mockImplementation(() => ({
            onmessage: null,
            postMessage: vi.fn(),
            terminate: vi.fn()
          }));
        }

        const { wasmConnector, Coordinator } = await import('@uwdata/mosaic-core');
        
        const mockConnector = {
          query: vi.fn().mockResolvedValue(useWorker ? sampleData : new MockArrowTable(sampleData))
        };

        const mockCoordinator = {
          connect: vi.fn(),
          databaseConnector: vi.fn(),
          query: vi.fn()
        };

        if (useWorker) {
          Coordinator.mockReturnValue(mockCoordinator);
        } else {
          wasmConnector.mockReturnValue(mockConnector);
          Coordinator.mockReturnValue(mockCoordinator);
        }

        const testDataTable = new DataTable({
          container: mockContainer,
          useWorker
        });

        await testDataTable.initialize();

        // Both modes should work without fallback
        expect(testDataTable.connector).toBeDefined();
        expect(testDataTable.coordinator).toBeDefined();
      }
    });
  });

  describe('Error Handling Integration', () => {
    it('should gracefully handle wasmConnector initialization failures', async () => {
      const { wasmConnector } = await import('@uwdata/mosaic-core');
      wasmConnector.mockImplementation(() => {
        throw new Error('Connector initialization failed');
      });

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await expect(dataTable.initialize()).rejects.toThrow();
    });

    it('should handle coordinator connection failures', async () => {
      const { wasmConnector, Coordinator } = await import('@uwdata/mosaic-core');
      
      const mockConnector = { query: vi.fn() };
      const mockCoordinator = {
        connect: vi.fn().mockImplementation(() => {
          throw new Error('Connection failed');
        }),
        databaseConnector: vi.fn()
      };

      wasmConnector.mockReturnValue(mockConnector);
      Coordinator.mockReturnValue(mockCoordinator);

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      const csvData = 'name,age\nAlice,30';
      
      // Should handle connection error gracefully
      await expect(dataTable.loadData('csv', new TextEncoder().encode(csvData), {
        tableName: 'data'
      })).resolves.not.toThrow();
    });

    it('should handle mixed data format scenarios', async () => {
      const { wasmConnector, Coordinator } = await import('@uwdata/mosaic-core');
      
      let callCount = 0;
      const mockConnector = {
        query: vi.fn().mockImplementation(() => {
          callCount++;
          // First call returns Arrow, second returns JSON
          if (callCount === 1) {
            return Promise.resolve(new MockArrowTable([{ name: 'Alice' }]));
          } else {
            return Promise.resolve([{ name: 'Bob' }]);
          }
        })
      };

      const mockCoordinator = {
        connect: vi.fn(),
        databaseConnector: vi.fn(),
        query: vi.fn()
      };

      wasmConnector.mockReturnValue(mockConnector);
      Coordinator.mockReturnValue(mockCoordinator);

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      const csvData = 'name,age\nAlice,30';
      await dataTable.loadData('csv', new TextEncoder().encode(csvData), {
        tableName: 'data'
      });

      // TableRenderer should handle both Arrow and JSON formats correctly
      expect(dataTable.tableRenderer).toBeDefined();
      
      // Simulate different query results
      dataTable.tableRenderer.queryResult(new MockArrowTable([{ name: 'Alice' }]));
      dataTable.tableRenderer.queryResult([{ name: 'Bob' }]);

      // Both should work without errors
      expect(dataTable.tableRenderer.data).toBeDefined();
    });
  });

  describe('Performance Integration', () => {
    it('should handle large datasets efficiently through Mosaic', async () => {
      const largeData = Array.from({ length: 1000 }, (_, i) => ({
        id: i,
        name: `User${i}`,
        value: Math.random()
      }));

      const { wasmConnector, Coordinator } = await import('@uwdata/mosaic-core');
      
      const mockConnector = {
        query: vi.fn().mockResolvedValue(new MockArrowTable(largeData))
      };

      const mockCoordinator = {
        connect: vi.fn(),
        databaseConnector: vi.fn(),
        query: vi.fn()
      };

      wasmConnector.mockReturnValue(mockConnector);
      Coordinator.mockReturnValue(mockCoordinator);

      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      const startTime = performance.now();
      await dataTable.initialize();
      const endTime = performance.now();

      expect(endTime - startTime).toBeLessThan(1000); // Should be fast
      expect(dataTable.tableRenderer).toBeDefined();
    });

    it('should track initialization performance metrics', async () => {
      dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      expect(dataTable.performance).toBeDefined();
      expect(dataTable.performance.initTime).toBeGreaterThan(0);
      expect(dataTable.performance.bundleType).toBeDefined();
    });
  });
});