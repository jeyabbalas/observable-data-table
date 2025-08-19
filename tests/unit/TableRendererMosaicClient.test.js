import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// Mock Mosaic components with more realistic behavior
vi.mock('@uwdata/mosaic-core', () => {
  const realMosaicClient = {
    _filterBy: undefined,
    _requestUpdate: vi.fn(),
    _coordinator: null,
    _pending: Promise.resolve(),
    _enabled: true,
    _initialized: false,
    _request: null,
    
    get coordinator() { return this._coordinator; },
    set coordinator(coordinator) { this._coordinator = coordinator; },
    get enabled() { return this._enabled; },
    set enabled(state) { this._enabled = !!state; },
    get pending() { return this._pending; },
    get filterBy() { return this._filterBy; },
    get filterStable() { return true; },
    
    async prepare() { return Promise.resolve(); },
    query() { return null; },
    queryPending() { return this; },
    queryResult() { return this; },
    queryError() { return this; },
    update() { return this; },
    destroy() {},
    
    requestQuery(query) {
      if (this._enabled && this._coordinator) {
        const q = query || this.query(this.filterBy?.predicate?.(this));
        return this._coordinator.requestQuery(this, q);
      }
      return null;
    },
    
    requestUpdate() {
      if (this._enabled) {
        this._requestUpdate();
      } else {
        this.requestQuery();
      }
    },
    
    initialize() {
      if (!this._enabled) {
        this._initialized = false;
      } else if (this._coordinator) {
        this._initialized = true;
        this._pending = this.prepare().then(() => this.requestQuery());
      }
    }
  };

  return {
    MosaicClient: class MockMosaicClient {
      constructor(filterSelection) {
        Object.assign(this, { ...realMosaicClient });
        this._filterBy = filterSelection;
        this._requestUpdate = vi.fn(() => this.requestQuery());
      }
    },
    Selection: {
      crossfilter: vi.fn(() => ({}))
    },
    Coordinator: vi.fn().mockImplementation(() => ({
      connect: vi.fn().mockImplementation((client) => {
        client.coordinator = this;
        client.initialize(); // This is key - coordinator calls initialize!
      }),
      databaseConnector: vi.fn(),
      query: vi.fn().mockResolvedValue([]),
      requestQuery: vi.fn().mockImplementation((client, query) => {
        // Simulate async query processing
        return new Promise((resolve) => {
          setTimeout(() => {
            const mockData = [
              { name: 'Alice', age: 30 },
              { name: 'Bob', age: 25 }
            ];
            client.queryResult(mockData);
            resolve(mockData);
          }, 50); // 50ms delay to simulate real query
        });
      })
    }))
  };
});

vi.mock('@uwdata/mosaic-sql', () => ({
  Query: {
    from: vi.fn(() => ({
      select: vi.fn(() => ({
        where: vi.fn(() => ({
          orderby: vi.fn(() => ({
            limit: vi.fn(() => ({
              offset: vi.fn(() => ({
                toString: vi.fn(() => 'SELECT * FROM data LIMIT 100 OFFSET 0')
              }))
            }))
          }))
        }))
      }))
    }))
  }
}));

vi.mock('@preact/signals-core', () => ({
  signal: vi.fn((initial) => ({
    value: initial,
    subscribe: vi.fn(),
    peek: vi.fn(() => initial)
  }))
}));

import { TableRenderer } from '../../src/core/TableRenderer.js';

describe('TableRenderer as MosaicClient', () => {
  let tableRenderer;
  let container;
  let mockCoordinator;

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    
    const { Coordinator } = await import('@uwdata/mosaic-core');
    mockCoordinator = new Coordinator();
    
    vi.clearAllMocks();
  });

  afterEach(() => {
    if (tableRenderer) {
      tableRenderer.destroy();
    }
    if (container.parentNode) {
      container.parentNode.removeChild(container);
    }
  });

  describe('MosaicClient Interface', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should inherit from MosaicClient', () => {
      expect(tableRenderer).toHaveProperty('coordinator');
      expect(tableRenderer).toHaveProperty('enabled');
      expect(tableRenderer).toHaveProperty('pending');
      expect(typeof tableRenderer.query).toBe('function');
      expect(typeof tableRenderer.queryResult).toBe('function');
      expect(typeof tableRenderer.requestQuery).toBe('function');
    });

    it('should have correct initial state', () => {
      expect(tableRenderer.enabled).toBe(true);
      expect(tableRenderer._initialized).toBe(false);
      expect(tableRenderer.coordinator).toBe(mockCoordinator);
    });

    it('should generate correct SQL queries', () => {
      const query = tableRenderer.query();
      
      expect(query.toString()).toBe('SELECT * FROM data LIMIT 100 OFFSET 0');
    });
  });

  describe('Initialization Sequence', () => {
    it('should follow correct MosaicClient initialization flow', async () => {
      const initializationLog = [];
      
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      // Mock methods to track call order
      const originalInitialize = tableRenderer.initialize.bind(tableRenderer);
      const originalPrepare = tableRenderer.prepare.bind(tableRenderer);
      const originalRequestQuery = tableRenderer.requestQuery.bind(tableRenderer);
      const originalQueryResult = tableRenderer.queryResult.bind(tableRenderer);

      tableRenderer.initialize = vi.fn().mockImplementation(async (...args) => {
        initializationLog.push('initialize');
        return originalInitialize(...args);
      });

      tableRenderer.prepare = vi.fn().mockImplementation(async (...args) => {
        initializationLog.push('prepare');
        return originalPrepare(...args);
      });

      tableRenderer.requestQuery = vi.fn().mockImplementation((...args) => {
        initializationLog.push('requestQuery');
        return originalRequestQuery(...args);
      });

      tableRenderer.queryResult = vi.fn().mockImplementation((...args) => {
        initializationLog.push('queryResult');
        return originalQueryResult(...args);
      });

      await tableRenderer.initialize();

      // Verify the MosaicClient initialization sequence
      expect(initializationLog).toContain('initialize');
      expect(initializationLog).toContain('prepare');
      expect(initializationLog).toContain('requestQuery');
    });

    it('should connect to coordinator and trigger initialization', async () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      const initializeSpy = vi.spyOn(tableRenderer, 'initialize');
      
      await tableRenderer.initialize();

      expect(mockCoordinator.connect).toHaveBeenCalledWith(tableRenderer);
      expect(initializeSpy).toHaveBeenCalled();
      expect(tableRenderer._initialized).toBe(true);
    });
  });

  describe('Query Execution Flow', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should request queries through coordinator', async () => {
      await tableRenderer.initialize();

      // Manual call to requestData should use coordinator
      tableRenderer.requestData();

      expect(mockCoordinator.requestQuery).toHaveBeenCalledWith(tableRenderer);
    });

    it('should handle query results via queryResult callback', async () => {
      const queryResultSpy = vi.spyOn(tableRenderer, 'queryResult');
      
      await tableRenderer.initialize();
      
      // Wait for the async query to complete
      await new Promise(resolve => setTimeout(resolve, 100));

      expect(queryResultSpy).toHaveBeenCalled();
      
      const callArgs = queryResultSpy.mock.calls[0][0];
      expect(Array.isArray(callArgs)).toBe(true);
      expect(callArgs.length).toBeGreaterThan(0);
    });
  });

  describe('**CRITICAL: Initialization Timing Issue**', () => {
    it('should NOT call requestData immediately in initialize', async () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      const requestDataSpy = vi.spyOn(tableRenderer, 'requestData');
      
      // The initialize method should NOT call requestData directly
      await tableRenderer.initialize();

      // This is the key test - TableRenderer should NOT call requestData immediately
      // Instead, it should let MosaicClient's initialize() handle the initial query
      expect(requestDataSpy).not.toHaveBeenCalled();
    });

    it('should let MosaicClient parent handle initial query', async () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      const parentRequestQuerySpy = vi.spyOn(tableRenderer, 'requestQuery');
      
      await tableRenderer.initialize();
      
      // Wait for async initialization to complete
      await tableRenderer.pending;

      // MosaicClient parent should handle the initial query via requestQuery
      expect(parentRequestQuerySpy).toHaveBeenCalled();
    });

    it('should NOT trigger fallback when coordinator responds quickly', async () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      const fallbackSpy = vi.spyOn(tableRenderer, 'fallbackDataLoad');
      
      await tableRenderer.initialize();
      
      // Wait for query to complete (50ms) but less than fallback timeout (1000ms)
      await new Promise(resolve => setTimeout(resolve, 200));

      // Fallback should NOT be triggered when coordinator responds quickly
      expect(fallbackSpy).not.toHaveBeenCalled();
    });

    it('should trigger fallback only when coordinator fails to respond', async () => {
      // Create a coordinator that doesn't respond
      const slowCoordinator = {
        connect: vi.fn().mockImplementation((client) => {
          client.coordinator = this;
          client.initialize();
        }),
        requestQuery: vi.fn().mockImplementation(() => {
          // Never resolve - simulate coordinator not responding
          return new Promise(() => {});
        })
      };

      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: slowCoordinator,
        connection: {
          query: vi.fn().mockResolvedValue({
            toArray: vi.fn().mockReturnValue([{ name: 'Alice' }])
          })
        }
      });

      const fallbackSpy = vi.spyOn(tableRenderer, 'fallbackDataLoad');
      
      await tableRenderer.initialize();
      
      // Wait for fallback timeout (1000ms)
      await new Promise(resolve => setTimeout(resolve, 1100));

      // Fallback should be triggered when coordinator doesn't respond
      expect(fallbackSpy).toHaveBeenCalled();
    });
  });

  describe('Error Handling', () => {
    it('should handle coordinator connection errors', async () => {
      const errorCoordinator = {
        connect: vi.fn().mockImplementation(() => {
          throw new Error('Connection failed');
        })
      };

      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: errorCoordinator
      });

      // Should not throw - should handle gracefully
      await expect(tableRenderer.initialize()).resolves.toBe(tableRenderer);
    });

    it('should handle query errors via queryError callback', () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      const queryErrorSpy = vi.spyOn(tableRenderer, 'queryError');
      const testError = new Error('Query failed');
      
      tableRenderer.queryError(testError);

      expect(queryErrorSpy).toHaveBeenCalledWith(testError);
    });
  });

  describe('Data Flow', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should render data when queryResult is called', async () => {
      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      const testData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];

      tableRenderer.queryResult(testData);

      expect(renderRowsSpy).toHaveBeenCalledWith(testData);
      expect(tableRenderer.data).toEqual(testData);
    });

    it('should handle empty query results', () => {
      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      tableRenderer.queryResult([]);

      expect(renderRowsSpy).toHaveBeenCalledWith([]);
      expect(tableRenderer.data).toEqual([]);
    });
  });
});