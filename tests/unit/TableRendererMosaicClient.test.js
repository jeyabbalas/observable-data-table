import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// Mock Mosaic components with simpler behavior
vi.mock('@uwdata/mosaic-core', () => ({
  MosaicClient: class MockMosaicClient {
    constructor() {
      this._enabled = true;
      this._initialized = false;
      this._coordinator = null;
      this._pending = Promise.resolve();
    }
    
    get coordinator() { return this._coordinator; }
    set coordinator(coordinator) { this._coordinator = coordinator; }
    get enabled() { return this._enabled; }
    get pending() { return this._pending; }
    get filterBy() { return undefined; }
    get filterStable() { return true; }
    
    initialize() {
      this._initialized = true;
      return this;
    }
    
    query() { return null; }
    queryResult() { return this; }
    queryError() { return this; }
    queryPending() { return this; }
    requestQuery() { return this; }
    requestUpdate() { return this; }
    prepare() { return Promise.resolve(); }
    update() { return this; }
    destroy() {}
  },
  Selection: {
    crossfilter: vi.fn(() => ({}))
  }
}));

vi.mock('@uwdata/mosaic-sql', () => {
  const mockQuery = {
    from: vi.fn().mockReturnThis(),
    select: vi.fn().mockReturnThis(),
    where: vi.fn().mockReturnThis(),
    orderby: vi.fn().mockReturnThis(),
    limit: vi.fn().mockReturnThis(),
    offset: vi.fn().mockReturnThis(),
    toString: vi.fn().mockReturnValue('SELECT * FROM test_table LIMIT 100 OFFSET 0')
  };

  return {
    Query: mockQuery,
    asc: vi.fn((field) => ({ field, order: 'ASC' })),
    desc: vi.fn((field) => ({ field, order: 'DESC' }))
  };
});

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
    
    mockCoordinator = {
      connect: vi.fn(),
      query: vi.fn().mockResolvedValue([]),
      requestQuery: vi.fn()
    };
    
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

    it('should have query method that returns an object', () => {
      const query = tableRenderer.query();
      
      // The query method should return the mock query object
      expect(query).toBeTruthy();
      expect(typeof query).toBe('object');
      expect(query.toString()).toBe('SELECT * FROM test_table LIMIT 100 OFFSET 0');
    });
  });

  describe('Initialization', () => {
    it('should initialize successfully', async () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      const result = await tableRenderer.initialize();
      expect(result).toBe(tableRenderer);
      expect(tableRenderer.connected).toBe(true);
    });

    it('should handle initialization without coordinator', async () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: null
      });

      const result = await tableRenderer.initialize();
      expect(result).toBe(tableRenderer);
      expect(tableRenderer.connected).toBe(true);
    });
  });

  describe('Data Flow Integration', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should handle queryResult callback properly', () => {
      // Create DOM structure first
      tableRenderer.renderHeader(['name', 'age']);
      
      const testData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];

      // Test queryResult functionality
      const result = tableRenderer.queryResult(testData);
      expect(result).toBe(tableRenderer);
      
      // Verify data was processed and stored
      expect(tableRenderer.data.length).toBeGreaterThan(0);
      expect(tableRenderer.data).toContain(testData[0]);
      expect(tableRenderer.data).toContain(testData[1]);
    });

    it('should handle empty query results', () => {
      // Create DOM structure first
      tableRenderer.renderHeader(['name', 'age']);
      
      const result = tableRenderer.queryResult([]);
      expect(result).toBe(tableRenderer);
      expect(tableRenderer.data).toEqual([]);
    });

    it('should handle queryError callback', () => {
      const testError = new Error('Query failed');
      const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      
      const result = tableRenderer.queryError(testError);
      expect(result).toBe(tableRenderer);
      expect(consoleErrorSpy).toHaveBeenCalledWith('Query error for table:', 'test_table', testError);
      
      consoleErrorSpy.mockRestore();
    });
  });

  describe('Request Flow', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should request data through coordinator when connected', async () => {
      await tableRenderer.initialize();
      
      // Connect to coordinator to enable requests
      tableRenderer.coordinator = mockCoordinator;
      tableRenderer.connected = true;
      
      const requestQuerySpy = vi.spyOn(tableRenderer, 'requestQuery');
      
      tableRenderer.requestData();
      
      expect(requestQuerySpy).toHaveBeenCalled();
    });

    it('should warn when coordinator is not available', async () => {
      await tableRenderer.initialize();
      
      // Ensure no coordinator is available
      tableRenderer.coordinator = null;
      tableRenderer.connected = false;
      
      const consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
      
      tableRenderer.requestData();
      
      expect(consoleWarnSpy).toHaveBeenCalledWith('Cannot request data: coordinator not available or not connected');
      consoleWarnSpy.mockRestore();
    });
  });

  describe('Fallback Behavior', () => {
    it('should NOT call requestData during initialization', async () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      const requestDataSpy = vi.spyOn(tableRenderer, 'requestData');
      
      await tableRenderer.initialize();

      // TableRenderer should NOT call requestData immediately during initialize
      expect(requestDataSpy).not.toHaveBeenCalled();
    });

    it('should trigger fallback when no data is received', async () => {
      // Use fake timers from the start to control async behavior
      vi.useFakeTimers();
      
      try {
        tableRenderer = new TableRenderer({
          table: 'test_table',
          schema: { name: { type: 'string' } },
          container,
          coordinator: null,
          connection: {
            query: vi.fn().mockResolvedValue({
              toArray: vi.fn().mockReturnValue([{ name: 'Alice' }])
            })
          }
        });

        const fallbackSpy = vi.spyOn(tableRenderer, 'fallbackDataLoad');
        
        // Initialize and immediately advance time to trigger fallback
        const initPromise = tableRenderer.initialize();
        vi.advanceTimersByTime(1100);
        await initPromise;

        expect(fallbackSpy).toHaveBeenCalled();
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe('Table Functionality', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should manage sorting state', () => {
      tableRenderer.toggleSort('name');
      expect(tableRenderer.orderBy.value).toEqual([{ field: 'name', order: 'ASC' }]);

      tableRenderer.toggleSort('name');
      expect(tableRenderer.orderBy.value).toEqual([{ field: 'name', order: 'DESC' }]);

      tableRenderer.toggleSort('name');
      expect(tableRenderer.orderBy.value).toEqual([]);
    });

    it('should manage filter state', () => {
      const filter = 'name = "Alice"';
      tableRenderer.applyFilter(filter);
      expect(tableRenderer.filters.value).toContain(filter);

      tableRenderer.removeFilter(filter);
      expect(tableRenderer.filters.value).not.toContain(filter);

      tableRenderer.applyFilter(filter);
      tableRenderer.clearFilters();
      expect(tableRenderer.filters.value).toEqual([]);
    });

    it('should clear data properly', () => {
      tableRenderer.data = [{ name: 'Alice' }];
      tableRenderer.offset = 100;
      
      tableRenderer.clearData();
      
      expect(tableRenderer.data).toEqual([]);
      expect(tableRenderer.offset).toBe(0);
    });
  });

  describe('Cleanup', () => {
    it('should destroy properly', () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      expect(container.children.length).toBeGreaterThan(0);
      
      tableRenderer.destroy();
      
      expect(container.children.length).toBe(0);
      expect(tableRenderer.coordinator).toBe(null);
      expect(tableRenderer.data).toEqual([]);
    });
  });
});