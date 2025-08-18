import { MosaicClient, Selection } from '@uwdata/mosaic-core';
import { Query } from '@uwdata/mosaic-sql';
import { signal } from '@preact/signals-core';

export class TableRenderer extends MosaicClient {
  constructor(options) {
    super(Selection.crossfilter());
    
    this.table = options.table;
    this.schema = options.schema;
    this.container = options.container;
    this.coordinator = options.coordinator;
    
    // UI elements
    this.tableElement = null;
    this.thead = null;
    this.tbody = null;
    
    // State
    this.offset = 0;
    this.limit = 100;
    this.orderBy = signal([]);
    this.filters = signal([]);
    this.data = [];
    
    // Create table structure
    this.createTable();
  }
  
  async initialize() {
    try {
      // Connect to coordinator
      if (this.coordinator) {
        this.coordinator.connect(this);
      }
      
      // Get field info and render header
      await this.prepare();
      
      // Load initial data
      this.requestData();
      
      return this;
    } catch (error) {
      console.error('Failed to initialize TableRenderer:', error);
      throw error;
    }
  }
  
  createTable() {
    // Create main table structure
    this.tableElement = document.createElement('table');
    this.tableElement.className = 'datatable';
    this.tableElement.style.width = '100%';
    this.tableElement.style.borderCollapse = 'collapse';
    
    this.thead = document.createElement('thead');
    this.tbody = document.createElement('tbody');
    
    this.tableElement.appendChild(this.thead);
    this.tableElement.appendChild(this.tbody);
    
    // Create scrollable container
    const scrollContainer = document.createElement('div');
    scrollContainer.className = 'datatable-scroll';
    scrollContainer.style.overflowX = 'auto';
    scrollContainer.style.overflowY = 'auto';
    scrollContainer.style.maxHeight = '400px';
    scrollContainer.appendChild(this.tableElement);
    
    this.container.appendChild(scrollContainer);
    
    // Setup infinite scroll
    scrollContainer.addEventListener('scroll', this.handleScroll.bind(this));
  }
  
  query(filter = []) {
    return Query
      .from(this.table)
      .select('*')
      .where(filter.concat(this.filters.value))
      .orderby(this.orderBy.value)
      .limit(this.limit)
      .offset(this.offset);
  }
  
  queryResult(data) {
    this.renderRows(data);
    return this;
  }
  
  async prepare() {
    // Get column information
    const fields = Object.keys(this.schema);
    
    if (fields.length === 0) {
      // Try to infer schema from first few rows
      try {
        const sampleQuery = Query.from(this.table).select('*').limit(1);
        const sample = await this.coordinator.query(sampleQuery);
        
        if (sample && sample.length > 0) {
          const firstRow = sample[0];
          this.schema = Object.keys(firstRow).reduce((acc, key) => {
            acc[key] = { type: typeof firstRow[key] };
            return acc;
          }, {});
          fields.push(...Object.keys(this.schema));
        }
      } catch (error) {
        console.error('Failed to infer schema:', error);
      }
    }
    
    this.renderHeader(fields);
  }
  
  renderHeader(fields) {
    const headerRow = document.createElement('tr');
    headerRow.style.backgroundColor = '#f5f5f5';
    headerRow.style.borderBottom = '2px solid #ddd';
    
    fields.forEach((field) => {
      const th = document.createElement('th');
      th.textContent = field;
      th.style.padding = '8px 12px';
      th.style.textAlign = 'left';
      th.style.cursor = 'pointer';
      th.style.userSelect = 'none';
      th.style.position = 'relative';
      
      // Add sort functionality
      th.addEventListener('click', () => this.toggleSort(field));
      
      // Add hover effect
      th.addEventListener('mouseenter', () => {
        th.style.backgroundColor = '#e0e0e0';
      });
      th.addEventListener('mouseleave', () => {
        th.style.backgroundColor = '#f5f5f5';
      });
      
      // Create visualization container
      const vizContainer = document.createElement('div');
      vizContainer.className = 'column-viz';
      vizContainer.style.marginTop = '4px';
      vizContainer.style.height = '40px';
      th.appendChild(vizContainer);
      
      // TODO: Create appropriate visualization based on field type
      // This will be implemented when we create the visualization components
      
      headerRow.appendChild(th);
    });
    
    this.thead.appendChild(headerRow);
  }
  
  renderRows(data) {
    if (!data || !Array.isArray(data)) {
      console.warn('Invalid data provided to renderRows:', data);
      return;
    }
    
    const fragment = document.createDocumentFragment();
    
    data.forEach((row, index) => {
      const tr = document.createElement('tr');
      tr.style.borderBottom = '1px solid #eee';
      
      // Add hover effect
      tr.addEventListener('mouseenter', () => {
        tr.style.backgroundColor = '#f9f9f9';
      });
      tr.addEventListener('mouseleave', () => {
        tr.style.backgroundColor = '';
      });
      
      Object.values(row).forEach(value => {
        const td = document.createElement('td');
        td.textContent = this.formatValue(value);
        td.style.padding = '8px 12px';
        td.style.borderRight = '1px solid #eee';
        tr.appendChild(td);
      });
      
      fragment.appendChild(tr);
    });
    
    this.tbody.appendChild(fragment);
    this.data.push(...data);
  }
  
  formatValue(value) {
    if (value === null || value === undefined) {
      return 'null';
    }
    if (typeof value === 'number') {
      // Format numbers with proper locale formatting
      if (Number.isInteger(value)) {
        return value.toLocaleString();
      } else {
        return value.toLocaleString(undefined, { 
          minimumFractionDigits: 0,
          maximumFractionDigits: 6 
        });
      }
    }
    if (value instanceof Date) {
      return value.toISOString().split('T')[0]; // YYYY-MM-DD format
    }
    if (typeof value === 'boolean') {
      return value ? 'true' : 'false';
    }
    return String(value);
  }
  
  handleScroll(event) {
    const { scrollTop, scrollHeight, clientHeight } = event.target;
    
    // Load more data when near bottom (within 150px)
    if (scrollHeight - scrollTop < clientHeight + 150) {
      this.loadMoreData();
    }
  }
  
  loadMoreData() {
    if (this.loading) return;
    
    this.loading = true;
    this.offset += this.limit;
    this.requestData();
  }
  
  requestData() {
    if (!this.coordinator) return;
    
    try {
      // Request data through the coordinator
      this.coordinator.requestQuery(this);
    } catch (error) {
      console.error('Failed to request data:', error);
    } finally {
      this.loading = false;
    }
  }
  
  toggleSort(field) {
    const current = this.orderBy.value;
    const existingIndex = current.findIndex(o => o.field === field);
    
    let newOrderBy;
    if (existingIndex === -1) {
      // Add new sort
      newOrderBy = [...current, { field, order: 'ASC' }];
    } else {
      const currentOrder = current[existingIndex].order;
      if (currentOrder === 'ASC') {
        // Change to DESC
        newOrderBy = current.map(o => 
          o.field === field ? { field, order: 'DESC' } : o
        );
      } else {
        // Remove sort
        newOrderBy = current.filter(o => o.field !== field);
      }
    }
    
    this.orderBy.value = newOrderBy;
    
    // Reset and reload data
    this.clearData();
    this.requestData();
  }
  
  clearData() {
    this.offset = 0;
    this.data = [];
    this.tbody.innerHTML = '';
  }
  
  applyFilter(filter) {
    this.filters.value = [...this.filters.value, filter];
    this.clearData();
    this.requestData();
  }
  
  removeFilter(filterToRemove) {
    this.filters.value = this.filters.value.filter(f => f !== filterToRemove);
    this.clearData();
    this.requestData();
  }
  
  clearFilters() {
    this.filters.value = [];
    this.clearData();
    this.requestData();
  }
  
  destroy() {
    if (this.container && this.tableElement) {
      this.container.removeChild(this.tableElement.parentNode);
    }
  }
}