import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { TableRenderer } from '../../src/core/TableRenderer.js';

// Mock Mosaic components
vi.mock('@uwdata/mosaic-core', () => ({
  MosaicClient: class MockMosaicClient {
    constructor() {}
    initialize() {
      // Mock parent initialize method
      return Promise.resolve();
    }
  },
  Selection: {
    crossfilter: vi.fn(() => ({}))
  }
}));

vi.mock('@uwdata/mosaic-sql', () => ({
  Query: {
    from: vi.fn(() => ({
      select: vi.fn(() => ({
        where: vi.fn(() => ({
          orderby: vi.fn(() => ({
            limit: vi.fn(() => ({
              offset: vi.fn(() => ({}))
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

  describe('Constructor', () => {
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
  });

  describe('Initialization', () => {
    it('should initialize successfully', async () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: { name: { type: 'string' } },
        container,
        coordinator: mockCoordinator
      });

      await expect(tableRenderer.initialize()).resolves.toBe(tableRenderer);
      expect(mockCoordinator.connect).toHaveBeenCalledWith(tableRenderer);
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

      // Mock scroll event near bottom
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

      // Mock scroll event not near bottom
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
      
      // Add some rows to tbody
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

  describe('Cleanup', () => {
    it('should destroy properly', () => {
      tableRenderer = new TableRenderer({
        table: 'test_table',
        schema: {},
        container,
        coordinator: mockCoordinator
      });

      // Add table to container
      const scrollContainer = container.querySelector('.datatable-scroll');
      expect(scrollContainer).toBeTruthy();

      tableRenderer.destroy();

      // Should remove table from container
      const remainingScrollContainer = container.querySelector('.datatable-scroll');
      expect(remainingScrollContainer).toBeFalsy();
    });
  });
});