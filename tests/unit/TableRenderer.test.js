import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { TableRenderer } from '../../src/core/TableRenderer.js';

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

// Mock Mosaic components
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
      return Promise.resolve();
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

describe('TableRenderer', () => {
  let container;
  let tableRenderer;
  let mockCoordinator;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    
    mockCoordinator = {
      connect: vi.fn(),
      query: vi.fn(() => Promise.resolve([{ name: 'Alice', age: 30 }])),
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

  describe('Constructor and Initialization', () => {
    it('should create TableRenderer with required options', () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container,
        coordinator: mockCoordinator
      });

      expect(tableRenderer.table).toBe('test_table');
      expect(tableRenderer.schema).toEqual({ name: { type: 'string' }, age: { type: 'number' } });
      expect(tableRenderer.container).toBe(container);
      expect(tableRenderer.coordinator).toBe(mockCoordinator);
    });

    it('should initialize with default state', () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: {},
        container,
        coordinator: mockCoordinator
      });

      expect(tableRenderer.offset).toBe(0);
      expect(tableRenderer.limit).toBe(100);
      expect(tableRenderer.data).toEqual([]);
    });

    it('should create table structure in DOM', () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: {},
        container,
        coordinator: mockCoordinator
      });

      const scrollContainer = container.querySelector('.datatable-scroll');
      const table = container.querySelector('.datatable');
      const thead = container.querySelector('thead');
      const tbody = container.querySelector('tbody');

      expect(scrollContainer).toBeTruthy();
      expect(table).toBeTruthy();
      expect(thead).toBeTruthy();
      expect(tbody).toBeTruthy();
    });

    it('should initialize successfully', async () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      await expect(tableRenderer.initialize()).resolves.toBe(tableRenderer);
      expect(tableRenderer.connected).toBe(true);
    });

    it('should handle initialization without coordinator', async () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: null
      });

      await expect(tableRenderer.initialize()).resolves.toBe(tableRenderer);
    });
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
      
      expect(query).toBeTruthy();
      expect(typeof query).toBe('object');
      expect(query.toString()).toBe('SELECT * FROM test_table LIMIT 100 OFFSET 0');
    });
  });

  describe('Header Rendering', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should render header with field names', () => {
      const fields = ['name', 'age'];
      tableRenderer.renderHeader(fields);

      const headerCells = container.querySelectorAll('th');
      expect(headerCells).toHaveLength(2);
      expect(headerCells[0].textContent).toBe('name');
      expect(headerCells[1].textContent).toBe('age');
    });

    it('should add click handlers for sorting', () => {
      const fields = ['name', 'age'];
      tableRenderer.renderHeader(fields);

      const headerCells = container.querySelectorAll('th');
      expect(headerCells[0].style.cursor).toBe('pointer');
      expect(headerCells[1].style.cursor).toBe('pointer');
    });

    it('should create visualization containers', () => {
      const fields = ['name', 'age'];
      tableRenderer.renderHeader(fields);

      const vizContainers = container.querySelectorAll('.column-viz');
      expect(vizContainers).toHaveLength(2);
    });
  });

  describe('Data Rendering', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should render data rows', () => {
      const testData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];

      tableRenderer.renderRows(testData);

      const rows = container.querySelectorAll('tbody tr');
      expect(rows).toHaveLength(2);
      
      const firstRowCells = rows[0].querySelectorAll('td');
      expect(firstRowCells[0].textContent).toBe('Alice');
      expect(firstRowCells[1].textContent).toBe('30');
    });

    it('should handle empty data', () => {
      tableRenderer.renderRows([]);
      
      const rows = container.querySelectorAll('tbody tr');
      expect(rows).toHaveLength(0);
    });

    it('should handle invalid data', () => {
      const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
      
      tableRenderer.renderRows(null);
      tableRenderer.renderRows('invalid');
      
      expect(consoleSpy).toHaveBeenCalledTimes(2);
      consoleSpy.mockRestore();
    });
  });

  describe('Arrow Data Handling', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should handle regular JavaScript array data', () => {
      const regularData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];

      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      const result = tableRenderer.queryResult(regularData);

      expect(renderRowsSpy).toHaveBeenCalledWith(regularData);
      expect(result).toBe(tableRenderer);
    });

    it('should convert Arrow Table to JavaScript array', () => {
      const sampleData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];
      const arrowTable = new MockArrowTable(sampleData);

      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      const result = tableRenderer.queryResult(arrowTable);

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
        names: ['name']
      };

      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      tableRenderer.queryResult(malformedArrowTable);
      expect(renderRowsSpy).toHaveBeenCalledWith(malformedArrowTable);
    });

    it('should handle empty Arrow Table', () => {
      const emptyArrowTable = new MockArrowTable([]);
      const renderRowsSpy = vi.spyOn(tableRenderer, 'renderRows');
      
      tableRenderer.queryResult(emptyArrowTable);
      
      expect(renderRowsSpy).toHaveBeenCalledWith([]);
    });

    it('should handle large Arrow Tables efficiently', () => {
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
      expect(endTime - startTime).toBeLessThan(2000); // Should be reasonably fast
    });
  });

  describe('Value Formatting', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: {},
        container,
        coordinator: mockCoordinator
      });
    });

    it('should format null and undefined values', () => {
      expect(tableRenderer.formatValue(null)).toBe('null');
      expect(tableRenderer.formatValue(undefined)).toBe('null');
    });

    it('should format numbers with locale formatting', () => {
      expect(tableRenderer.formatValue(1000)).toBe('1,000');
      expect(tableRenderer.formatValue(1000.123)).toBe('1,000.123');
    });

    it('should format dates', () => {
      const date = new Date('2023-01-15T10:30:00Z');
      expect(tableRenderer.formatValue(date)).toBe('2023-01-15');
    });

    it('should format booleans', () => {
      expect(tableRenderer.formatValue(true)).toBe('true');
      expect(tableRenderer.formatValue(false)).toBe('false');
    });

    it('should format strings', () => {
      expect(tableRenderer.formatValue('test string')).toBe('test string');
    });
  });

  describe('Sorting', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should toggle sort order', () => {
      // First click - add ASC sort
      tableRenderer.toggleSort('name');
      expect(tableRenderer.orderBy.value).toEqual([{ field: 'name', order: 'ASC' }]);

      // Second click - change to DESC
      tableRenderer.toggleSort('name');
      expect(tableRenderer.orderBy.value).toEqual([{ field: 'name', order: 'DESC' }]);

      // Third click - remove sort
      tableRenderer.toggleSort('name');
      expect(tableRenderer.orderBy.value).toEqual([]);
    });

    it('should support multiple column sorting', () => {
      tableRenderer.toggleSort('name');
      tableRenderer.toggleSort('age');
      
      expect(tableRenderer.orderBy.value).toHaveLength(2);
      expect(tableRenderer.orderBy.value[0].field).toBe('name');
      expect(tableRenderer.orderBy.value[1].field).toBe('age');
    });

    it('should clear data when sorting changes', () => {
      tableRenderer.data = [{ name: 'Alice' }];
      tableRenderer.clearData = vi.fn();
      tableRenderer.requestData = vi.fn();

      tableRenderer.toggleSort('name');

      expect(tableRenderer.clearData).toHaveBeenCalled();
      expect(tableRenderer.requestData).toHaveBeenCalled();
    });
  });

  describe('Filtering', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });
    });

    it('should apply filters', () => {
      const filter = 'name = "Alice"';
      tableRenderer.clearData = vi.fn();
      tableRenderer.requestData = vi.fn();

      tableRenderer.applyFilter(filter);

      expect(tableRenderer.filters.value).toContain(filter);
      expect(tableRenderer.clearData).toHaveBeenCalled();
      expect(tableRenderer.requestData).toHaveBeenCalled();
    });

    it('should remove filters', () => {
      const filter1 = 'name = "Alice"';
      const filter2 = 'age > 25';
      
      tableRenderer.filters.value = [filter1, filter2];
      tableRenderer.clearData = vi.fn();
      tableRenderer.requestData = vi.fn();

      tableRenderer.removeFilter(filter1);

      expect(tableRenderer.filters.value).not.toContain(filter1);
      expect(tableRenderer.filters.value).toContain(filter2);
    });

    it('should clear all filters', () => {
      tableRenderer.filters.value = ['filter1', 'filter2'];
      tableRenderer.clearData = vi.fn();
      tableRenderer.requestData = vi.fn();

      tableRenderer.clearFilters();

      expect(tableRenderer.filters.value).toEqual([]);
      expect(tableRenderer.clearData).toHaveBeenCalled();
      expect(tableRenderer.requestData).toHaveBeenCalled();
    });
  });

  describe('Scroll Handling', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: {},
        container,
        coordinator: mockCoordinator
      });
    });

    it('should load more data when scrolling near bottom', () => {
      tableRenderer.loadMoreData = vi.fn();

      const scrollEvent = {
        target: {
          scrollTop: 300,
          scrollHeight: 500,
          clientHeight: 200
        }
      };

      tableRenderer.handleScroll(scrollEvent);

      expect(tableRenderer.loadMoreData).toHaveBeenCalled();
    });

    it('should not load more data when not near bottom', () => {
      tableRenderer.loadMoreData = vi.fn();

      const scrollEvent = {
        target: {
          scrollTop: 100,
          scrollHeight: 500,
          clientHeight: 200
        }
      };

      tableRenderer.handleScroll(scrollEvent);

      expect(tableRenderer.loadMoreData).not.toHaveBeenCalled();
    });
  });

  describe('Data Management', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: {},
        container,
        coordinator: mockCoordinator
      });
    });

    it('should clear data properly', () => {
      tableRenderer.offset = 100;
      tableRenderer.data = [{ name: 'Alice' }];
      
      const tbody = container.querySelector('tbody');
      tbody.innerHTML = '<tr><td>Test</td></tr>';

      tableRenderer.clearData();

      expect(tableRenderer.offset).toBe(0);
      expect(tableRenderer.data).toEqual([]);
      expect(tbody.innerHTML).toBe('');
    });

    it('should handle load more data', () => {
      tableRenderer.requestData = vi.fn();
      tableRenderer.loading = false;
      tableRenderer.offset = 0;
      tableRenderer.limit = 100;

      tableRenderer.loadMoreData();

      expect(tableRenderer.offset).toBe(100);
      expect(tableRenderer.requestData).toHaveBeenCalled();
    });

    it('should prevent multiple simultaneous loads', () => {
      tableRenderer.requestData = vi.fn();
      tableRenderer.loading = true;

      tableRenderer.loadMoreData();

      expect(tableRenderer.requestData).not.toHaveBeenCalled();
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
      tableRenderer.renderHeader(['name', 'age']);
      
      const testData = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ];

      const result = tableRenderer.queryResult(testData);
      expect(result).toBe(tableRenderer);
      
      expect(tableRenderer.data.length).toBeGreaterThan(0);
      expect(tableRenderer.data).toContain(testData[0]);
      expect(tableRenderer.data).toContain(testData[1]);
    });

    it('should handle empty query results', () => {
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
      
      tableRenderer.coordinator = mockCoordinator;
      tableRenderer.connected = true;
      
      const requestQuerySpy = vi.spyOn(tableRenderer, 'requestQuery');
      
      tableRenderer.requestData();
      
      expect(requestQuerySpy).toHaveBeenCalled();
    });

    it('should warn when coordinator is not available', async () => {
      await tableRenderer.initialize();
      
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

      expect(requestDataSpy).not.toHaveBeenCalled();
    });

    it('should trigger fallback when no data is received', async () => {
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
        
        const initPromise = tableRenderer.initialize();
        vi.advanceTimersByTime(1100);
        await initPromise;

        expect(fallbackSpy).toHaveBeenCalled();
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe('Error Recovery', () => {
    beforeEach(() => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });
    });

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
      
      tableRenderer.queryResult(brokenArrowTable);
      
      expect(renderRowsSpy).toHaveBeenCalledWith([]);
    });
  });

  describe('Cleanup', () => {
    it('should destroy properly', () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: {},
        container,
        coordinator: mockCoordinator
      });

      const scrollContainer = container.querySelector('.datatable-scroll');
      expect(scrollContainer).toBeTruthy();

      tableRenderer.destroy();

      const remainingScrollContainer = container.querySelector('.datatable-scroll');
      expect(remainingScrollContainer).toBeFalsy();
    });

    it('should clean up all resources', () => {
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