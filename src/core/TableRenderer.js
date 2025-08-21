import { MosaicClient, Selection } from '@uwdata/mosaic-core';
import { Query, asc, desc } from '@uwdata/mosaic-sql';
import { signal } from '@preact/signals-core';

export class TableRenderer extends MosaicClient {
  constructor(options) {
    super(Selection.crossfilter());
    
    this.table = options.table;
    this.schema = options.schema;
    this.container = options.container;
    this.coordinator = options.coordinator;
    this.connection = options.connection; // Direct DuckDB connection for fallback queries
    
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
    this.connected = false;
    
    // Create table structure
    this.createTable();
  }
  
  async initialize() {
    try {
      // Reset connection state and mark as connected 
      // (coordinator.connect() will have been called by DataTable)
      this.connected = true;
      
      // Call parent MosaicClient initialize() to handle the proper Mosaic flow
      // This will call prepare() then requestQuery() through the coordinator
      super.initialize();
      
      // Fallback: If Mosaic coordinator doesn't trigger queryResult, 
      // manually fetch and display initial data
      setTimeout(async () => {
        if (this.data.length === 0) {
          console.log('No data received via coordinator, attempting direct query...');
          await this.fallbackDataLoad();
        }
      }, 1000); // Wait 1 second for coordinator to respond
      
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
    // Convert orderBy array to Mosaic SQL format
    const orderByExprs = this.orderBy.value.map(({ field, order }) => 
      order === 'DESC' ? desc(field) : asc(field)
    );
    
    const query = Query
      .from(this.table)
      .select('*')
      .where(filter.concat(this.filters.value))
      .orderby(...orderByExprs)
      .limit(this.limit)
      .offset(this.offset);
    
    console.log('TableRenderer.query() generated:', query.toString());
    console.log('OrderBy expressions:', orderByExprs);
    console.log('Current orderBy value:', this.orderBy.value);
    return query;
  }
  
  queryResult(data) {
    console.log('TableRenderer.queryResult() called with data:', data);
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
    
    this.renderRows(data);
    return this;
  }

  queryPending() {
    console.log('Query pending for table:', this.table);
    // Could add loading indicator here if needed
  }

  queryError(error) {
    console.error('Query error for table:', this.table, error);
    // Try to recover or show error to user
    if (this.fallbackDataLoad) {
      console.log('Attempting fallback data load due to query error');
      this.fallbackDataLoad();
    }
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
    // Clear existing headers to prevent duplicates
    this.thead.innerHTML = '';
    
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
    if (!this.coordinator || !this.connected) {
      console.warn('Cannot request data: coordinator not available or not connected');
      return;
    }
    
    try {
      console.log('Requesting data for table:', this.table);
      // FIXED: Use the MosaicClient's requestQuery method which handles everything correctly
      this.requestQuery();
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
  
  async fallbackDataLoad() {
    try {
      console.log('Attempting fallback data load for table:', this.table);
      
      // Try direct DuckDB query first (most reliable)
      if (this.connection) {
        console.log('Using direct DuckDB connection for fallback query');
        
        // Build SQL query without quotes around table name
        const sql = `SELECT * FROM ${this.table} LIMIT ${this.limit} OFFSET ${this.offset}`;
        console.log('Executing SQL directly:', sql);
        
        const result = await this.connection.query(sql);
        const data = result.toArray();
        
        if (data && data.length > 0) {
          console.log('Fallback data load successful via direct DuckDB, received', data.length, 'rows');
          this.queryResult(data);
          return;
        }
      }
      
      // Fallback to coordinator query if direct connection failed
      if (this.coordinator && this.coordinator.query) {
        console.log('Trying coordinator query as secondary fallback');
        const query = this.query(); // Get our query
        const result = await this.coordinator.query(query);
        
        if (result && result.length > 0) {
          console.log('Fallback data load successful via coordinator, received', result.length, 'rows');
          this.queryResult(result);
          return;
        }
      }
      
      console.warn('Fallback data load failed - no data received from any method');
    } catch (error) {
      console.error('Fallback data load error:', error);
    }
  }

  destroy() {
    // Disconnect from coordinator if connected
    if (this.coordinator && this.connected) {
      try {
        // Clear any cached queries for this specific table
        if (this.coordinator.cache && this.table) {
          // Clear all cache entries that reference this table
          if (typeof this.coordinator.cache.clear === 'function') {
            // Clear entire cache to be safe - table name conflicts are dangerous
            this.coordinator.cache.clear();
            console.log(`TableRenderer.destroy(): Cleared coordinator cache for table ${this.table}`);
          } else if (this.coordinator.cache instanceof Map) {
            // Clear entire Map cache
            this.coordinator.cache.clear();
            console.log(`TableRenderer.destroy(): Cleared Map cache for table ${this.table}`);
          } else if (this.coordinator.cache.delete) {
            // Try to find and remove specific cache entries
            const keysToDelete = [];
            for (const [key] of this.coordinator.cache) {
              const keyStr = String(key);
              if (keyStr.includes && (keyStr.includes(this.table) || keyStr.includes(`FROM ${this.table}`))) {
                keysToDelete.push(key);
              }
            }
            keysToDelete.forEach(key => this.coordinator.cache.delete(key));
            console.log(`TableRenderer.destroy(): Removed ${keysToDelete.length} cache entries for table ${this.table}`);
          }
        }
        
        // Clear query cache if it exists separately
        if (this.coordinator.queryCache && this.table) {
          if (typeof this.coordinator.queryCache.clear === 'function') {
            this.coordinator.queryCache.clear();
            console.log(`TableRenderer.destroy(): Cleared query cache for table ${this.table}`);
          }
        }
        
        // Remove this component from coordinator's clients
        if (this.coordinator.clients) {
          if (this.coordinator.clients.has && this.coordinator.clients.has(this)) {
            this.coordinator.clients.delete(this);
          } else if (this.coordinator.clients instanceof Set) {
            this.coordinator.clients.delete(this);
          } else if (Array.isArray(this.coordinator.clients)) {
            const index = this.coordinator.clients.indexOf(this);
            if (index > -1) {
              this.coordinator.clients.splice(index, 1);
            }
          }
          console.log(`TableRenderer.destroy(): Removed from coordinator clients for table ${this.table}`);
        }
        
      } catch (error) {
        console.warn('Error during coordinator cleanup:', error);
      }
      
      this.connected = false;
    }
    
    // Clear container completely
    if (this.container) {
      this.container.innerHTML = '';
    }
    
    // Clear all data and state
    this.data = [];
    this.offset = 0;
    
    // Clear filters and ordering
    if (this.filters && this.filters.value) {
      this.filters.value = [];
    }
    if (this.orderBy && this.orderBy.value) {
      this.orderBy.value = [];
    }
    
    // Clear references
    this.tableElement = null;
    this.thead = null;
    this.tbody = null;
    this.coordinator = null;
    this.schema = null;
    this.table = null;
  }
}