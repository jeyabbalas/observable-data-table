import { MosaicClient, Selection } from '@uwdata/mosaic-core';
import { Query, asc, desc } from '@uwdata/mosaic-sql';
import { signal } from '@preact/signals-core';
import { Type } from '@uwdata/flechette';
import { Histogram } from '../visualizations/Histogram.js';
import { DateHistogram } from '../visualizations/DateHistogram.js';

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
    
    // Column visualizations
    this.visualizations = new Map();
    
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
    
    return query;
  }
  
  queryResult(data) {
    
    // Handle Apache Arrow Table format from Mosaic wasmConnector
    // Arrow tables have a toArray() method to convert to JavaScript arrays
    if (data && typeof data === 'object' && !Array.isArray(data) && typeof data.toArray === 'function') {
      try {
        data = data.toArray();
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
    // Could add loading indicator here if needed
  }

  queryError(error) {
    console.error('Query error for table:', this.table, error);
    // Try to recover or show error to user
    if (this.fallbackDataLoad) {
      this.fallbackDataLoad();
    }
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
    // Clear existing headers to prevent duplicates
    this.thead.innerHTML = '';
    
    // Clear existing visualizations
    this.visualizations.forEach(viz => viz.destroy());
    this.visualizations.clear();
    
    const headerRow = document.createElement('tr');
    headerRow.style.backgroundColor = '#f5f5f5';
    headerRow.style.borderBottom = '2px solid #ddd';
    
    fields.forEach((fieldName) => {
      const th = document.createElement('th');
      th.style.padding = '8px 12px';
      th.style.textAlign = 'left';
      th.style.cursor = 'pointer';
      th.style.userSelect = 'none';
      th.style.position = 'relative';
      th.style.verticalAlign = 'top';
      
      // Create column header structure
      const headerContent = document.createElement('div');
      headerContent.className = 'column-header';
      
      // Header top row (name and sort indicator)
      const headerTop = document.createElement('div');
      headerTop.className = 'header-top';
      headerTop.style.display = 'flex';
      headerTop.style.justifyContent = 'space-between';
      headerTop.style.alignItems = 'flex-start';
      headerTop.style.marginBottom = '4px';
      
      // Column name
      const columnName = document.createElement('div');
      columnName.textContent = fieldName;
      columnName.style.fontWeight = 'bold';
      columnName.style.flex = '1';
      headerTop.appendChild(columnName);
      
      // Sort indicator
      const sortIndicator = this.createSortIndicator(fieldName);
      headerTop.appendChild(sortIndicator);
      
      headerContent.appendChild(headerTop);
      
      // Data type label
      const dataType = this.getFieldType(fieldName);
      const typeLabel = document.createElement('div');
      typeLabel.textContent = dataType;
      typeLabel.className = 'gray';  // Add class for hover updates
      typeLabel.style.fontSize = '11px';
      typeLabel.style.color = '#666';
      typeLabel.style.marginBottom = '4px';
      headerContent.appendChild(typeLabel);
      
      // Stats display for visualization hover/selection info
      const statsDisplay = document.createElement('div');
      statsDisplay.className = 'stats-display';
      statsDisplay.style.fontSize = '10px';
      statsDisplay.style.color = '#666';
      statsDisplay.style.fontWeight = '500';
      statsDisplay.style.minHeight = '12px';
      statsDisplay.style.marginBottom = '4px';
      statsDisplay.style.textAlign = 'center';
      headerContent.appendChild(statsDisplay);
      
      // Visualization container
      const vizContainer = document.createElement('div');
      vizContainer.className = 'column-viz';
      vizContainer.style.height = '52px';  // Increased to accommodate 50px histogram
      vizContainer.style.marginBottom = '4px';
      headerContent.appendChild(vizContainer);
      
      // Create visualization if appropriate
      this.createVisualization(fieldName, vizContainer, statsDisplay);
      
      th.appendChild(headerContent);
      
      // Add sort functionality
      th.addEventListener('click', (e) => {
        // Allow sorting from column header or sort indicator
        if (!e.target.closest('.column-viz')) {
          this.toggleSort(fieldName);
        }
      });
      
      // Add hover effect
      th.addEventListener('mouseenter', () => {
        th.style.backgroundColor = '#e0e0e0';
      });
      th.addEventListener('mouseleave', () => {
        th.style.backgroundColor = '#f5f5f5';
      });
      
      headerRow.appendChild(th);
    });
    
    this.thead.appendChild(headerRow);
  }
  
  /**
   * Create sort indicator for a column
   * @param {string} fieldName - Name of the field
   * @returns {HTMLElement} Sort indicator element
   */
  createSortIndicator(fieldName) {
    const container = document.createElement('div');
    container.className = 'sort-indicator';
    container.style.position = 'relative';
    container.style.width = '20px';
    container.style.height = '20px';
    container.style.display = 'flex';
    container.style.alignItems = 'center';
    container.style.justifyContent = 'center';
    container.style.cursor = 'pointer';
    container.dataset.field = fieldName;
    
    // Create SVG with arrows
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('width', '12');
    svg.setAttribute('height', '16');
    svg.setAttribute('viewBox', '0 0 12 16');
    svg.style.pointerEvents = 'none';
    
    // Up arrow (descending)
    const upArrow = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    upArrow.setAttribute('d', 'M6 2 L10 6 L2 6 Z');
    upArrow.setAttribute('class', 'sort-arrow-up');
    upArrow.setAttribute('fill', '#cbd5e1'); // Default light gray
    
    // Down arrow (ascending)
    const downArrow = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    downArrow.setAttribute('d', 'M6 14 L2 10 L10 10 Z');
    downArrow.setAttribute('class', 'sort-arrow-down');
    downArrow.setAttribute('fill', '#cbd5e1'); // Default light gray
    
    svg.appendChild(upArrow);
    svg.appendChild(downArrow);
    container.appendChild(svg);
    
    // Sort order badge (hidden initially)
    const badge = document.createElement('div');
    badge.className = 'sort-order-badge';
    badge.style.position = 'absolute';
    badge.style.top = '-8px';
    badge.style.right = '-8px';
    badge.style.width = '16px';
    badge.style.height = '16px';
    badge.style.borderRadius = '50%';
    badge.style.backgroundColor = 'var(--primary-color)';
    badge.style.color = 'white';
    badge.style.fontSize = '10px';
    badge.style.fontWeight = 'bold';
    badge.style.display = 'none';
    badge.style.alignItems = 'center';
    badge.style.justifyContent = 'center';
    badge.style.lineHeight = '1';
    container.appendChild(badge);
    
    // Update initial state
    this.updateSortIndicator(fieldName);
    
    return container;
  }

  /**
   * Update sort indicator visual state
   * @param {string} fieldName - Name of the field
   */
  updateSortIndicator(fieldName) {
    const indicator = this.thead.querySelector(`.sort-indicator[data-field="${fieldName}"]`);
    if (!indicator) return;
    
    const upArrow = indicator.querySelector('.sort-arrow-up');
    const downArrow = indicator.querySelector('.sort-arrow-down');
    const badge = indicator.querySelector('.sort-order-badge');
    
    // Find current sort state for this field
    const currentSort = this.orderBy.value.find(o => o.field === fieldName);
    const sortIndex = this.orderBy.value.findIndex(o => o.field === fieldName);
    
    if (currentSort) {
      // Field is currently sorted
      if (currentSort.order === 'ASC') {
        // Ascending: highlight down arrow
        upArrow.setAttribute('fill', '#cbd5e1');
        downArrow.setAttribute('fill', 'var(--primary-color)');
      } else {
        // Descending: highlight up arrow
        upArrow.setAttribute('fill', 'var(--primary-color)');
        downArrow.setAttribute('fill', '#cbd5e1');
      }
      
      // Show sort order badge if multiple columns are sorted
      if (this.orderBy.value.length > 1) {
        badge.textContent = (sortIndex + 1).toString();
        badge.style.display = 'flex';
      } else {
        badge.style.display = 'none';
      }
    } else {
      // Field is not sorted: show both arrows in light gray
      upArrow.setAttribute('fill', '#cbd5e1');
      downArrow.setAttribute('fill', '#cbd5e1');
      badge.style.display = 'none';
    }
  }

  /**
   * Update all sort indicators
   */
  updateAllSortIndicators() {
    const indicators = this.thead.querySelectorAll('.sort-indicator');
    indicators.forEach(indicator => {
      const fieldName = indicator.dataset.field;
      this.updateSortIndicator(fieldName);
    });
  }

  /**
   * Get field type from schema
   * @param {string} fieldName - Name of the field
   * @returns {string} Type description
   */
  getFieldType(fieldName) {
    const fieldSchema = this.schema[fieldName];
    if (!fieldSchema) return 'unknown';
    
    // Handle Arrow schema format (has typeId)
    if (fieldSchema.type && fieldSchema.type.typeId !== undefined) {
      switch (fieldSchema.type.typeId) {
        case Type.Int:
          return `int${fieldSchema.type.bitWidth || ''}`;
        case Type.Float:
          return `float${fieldSchema.type.precision === 1 ? '32' : '64'}`;
        case Type.Utf8:
        case Type.LargeUtf8:
          return 'string';
        case Type.Bool:
          return 'boolean';
        case Type.Date:
          return 'date';
        case Type.Timestamp:
          return 'timestamp';
        default:
          return 'other';
      }
    }
    
    // Handle DuckDB string types from detectSchema
    if (typeof fieldSchema.type === 'string') {
      const duckdbType = fieldSchema.type.toUpperCase();
      
      // Numeric types
      if (duckdbType.includes('INTEGER') || duckdbType.includes('BIGINT') || duckdbType.includes('SMALLINT') || duckdbType.includes('TINYINT')) {
        return 'integer';
      }
      if (duckdbType.includes('DOUBLE') || duckdbType.includes('REAL') || duckdbType.includes('FLOAT')) {
        return 'float';
      }
      if (duckdbType.includes('DECIMAL') || duckdbType.includes('NUMERIC')) {
        return 'decimal';
      }
      
      // String types
      if (duckdbType.includes('VARCHAR') || duckdbType.includes('TEXT') || duckdbType.includes('STRING')) {
        return 'string';
      }
      
      // Boolean types
      if (duckdbType.includes('BOOLEAN') || duckdbType.includes('BOOL')) {
        return 'boolean';
      }
      
      // Date/time types
      if (duckdbType.includes('DATE')) {
        return 'date';
      }
      if (duckdbType.includes('TIMESTAMP') || duckdbType.includes('DATETIME')) {
        return 'timestamp';
      }
      if (duckdbType.includes('TIME')) {
        return 'time';
      }
      
      return duckdbType.toLowerCase();
    }
    
    // Simple type inference fallback
    if (fieldSchema.type === 'number') return 'number';
    if (fieldSchema.type === 'string') return 'string';
    if (fieldSchema.type === 'boolean') return 'boolean';
    
    return fieldSchema.type || 'unknown';
  }
  
  /**
   * Create appropriate visualization for a field
   * @param {string} fieldName - Name of the field
   * @param {HTMLElement} container - Container element for visualization
   */
  createVisualization(fieldName, container, statsDisplay) {
    const fieldSchema = this.schema[fieldName];
    if (!fieldSchema) return;
    
    // Create mock field object for compatibility with visualization classes
    // Handle both Arrow schema and DuckDB string types
    let mockTypeObject;
    if (fieldSchema.type && fieldSchema.type.typeId !== undefined) {
      // Already an Arrow type object
      mockTypeObject = fieldSchema.type;
    } else {
      // Create a mock Arrow-like type object for DuckDB string types
      mockTypeObject = {
        typeId: this.isNumericField(fieldSchema) ? 'numeric' : 'string',
        toString: () => fieldSchema.type
      };
    }
    
    const mockField = {
      name: fieldName,
      type: mockTypeObject,
      // Include original DuckDB type for formatting decisions
      duckdbType: fieldSchema.type
    };
    
    // Check field type and create appropriate visualization
    if (this.isTemporalField(fieldSchema)) {
      // Create date histogram for temporal fields
      try {
        const dateHistogram = new DateHistogram({
          table: this.table,
          column: fieldName,
          field: mockField,
          filterBy: this.filterBy
        });
        
        // Connect to coordinator
        if (this.coordinator) {
          this.coordinator.connect(dateHistogram);
        }
        
        // Add to container
        container.appendChild(dateHistogram.node());
        
        // Store reference for cleanup
        this.visualizations.set(fieldName, dateHistogram);
        
      } catch (error) {
        console.error(`Failed to create date histogram for ${fieldName}:`, error);
        this.createPlaceholderViz(container, 'Error');
      }
    } else if (this.isNumericField(fieldSchema)) {
      // Create histogram for numeric fields
      try {
        const histogram = new Histogram({
          table: this.table,
          column: fieldName,
          field: mockField,
          filterBy: this.filterBy,
          statsDisplay: statsDisplay
        });
        
        // Connect to coordinator
        if (this.coordinator) {
          this.coordinator.connect(histogram);
        }
        
        // Add to container
        container.appendChild(histogram.node());
        
        // Store reference for cleanup
        this.visualizations.set(fieldName, histogram);
        
      } catch (error) {
        console.error(`Failed to create histogram for ${fieldName}:`, error);
        this.createPlaceholderViz(container, 'Error');
      }
    } else {
      // For non-numeric, non-temporal fields, show placeholder for now
      this.createPlaceholderViz(container, 'Text');
    }
  }
  
  /**
   * Check if field is numeric
   * @param {Object} fieldSchema - Field schema object
   * @returns {boolean}
   */
  isNumericField(fieldSchema) {
    // Handle Arrow schema format (has typeId)
    if (fieldSchema.type && fieldSchema.type.typeId !== undefined) {
      const typeId = fieldSchema.type.typeId;
      return typeId === Type.Int || typeId === Type.Float || typeId === Type.Decimal;
    }
    
    // Handle DuckDB string types from detectSchema
    if (typeof fieldSchema.type === 'string') {
      const duckdbType = fieldSchema.type.toUpperCase();
      
      return (
        duckdbType.includes('INTEGER') ||
        duckdbType.includes('BIGINT') ||
        duckdbType.includes('SMALLINT') ||
        duckdbType.includes('TINYINT') ||
        duckdbType.includes('DOUBLE') ||
        duckdbType.includes('REAL') ||
        duckdbType.includes('FLOAT') ||
        duckdbType.includes('DECIMAL') ||
        duckdbType.includes('NUMERIC')
      );
    }
    
    // Fallback to simple type check
    return fieldSchema.type === 'number';
  }
  
  /**
   * Check if field represents a temporal (date/time) type
   * @param {Object} fieldSchema - Field schema object
   * @returns {boolean}
   */
  isTemporalField(fieldSchema) {
    if (!fieldSchema || !fieldSchema.type) return false;
    
    // Handle Arrow types
    if (fieldSchema.type.typeId !== undefined) {
      const typeId = fieldSchema.type.typeId;
      return typeId === Type.Date || 
             typeId === Type.Timestamp;
    }
    
    // Handle DuckDB string types from detectSchema
    if (typeof fieldSchema.type === 'string') {
      const duckdbType = fieldSchema.type.toUpperCase();
      return (
        duckdbType.includes('DATE') ||
        duckdbType.includes('TIMESTAMP') ||
        duckdbType.includes('DATETIME') ||
        duckdbType.includes('TIME')
      );
    }
    
    return false;
  }
  
  /**
   * Create placeholder visualization
   * @param {HTMLElement} container - Container element
   * @param {string} label - Label to display
   */
  createPlaceholderViz(container, label) {
    const placeholder = document.createElement('div');
    placeholder.style.width = '125px';
    placeholder.style.height = '40px';
    placeholder.style.display = 'flex';
    placeholder.style.alignItems = 'center';
    placeholder.style.justifyContent = 'center';
    placeholder.style.backgroundColor = '#f9f9f9';
    placeholder.style.border = '1px solid #e0e0e0';
    placeholder.style.borderRadius = '4px';
    placeholder.style.color = '#999';
    placeholder.style.fontSize = '11px';
    placeholder.textContent = label;
    container.appendChild(placeholder);
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
    
    // Update all sort indicators to reflect new state
    this.updateAllSortIndicators();
    
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
      
      // Try direct DuckDB query first (most reliable)
      if (this.connection) {
        
        // Build SQL query without quotes around table name
        const sql = `SELECT * FROM ${this.table} LIMIT ${this.limit} OFFSET ${this.offset}`;
        
        const result = await this.connection.query(sql);
        const data = result.toArray();
        
        if (data && data.length > 0) {
          this.queryResult(data);
          return;
        }
      }
      
      // Fallback to coordinator query if direct connection failed
      if (this.coordinator && this.coordinator.query) {
        const query = this.query(); // Get our query
        const result = await this.coordinator.query(query);
        
        if (result && result.length > 0) {
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
    // Clean up visualizations first
    if (this.visualizations) {
      this.visualizations.forEach(viz => viz.destroy());
      this.visualizations.clear();
    }
    
    // Disconnect from coordinator if connected
    if (this.coordinator && this.connected) {
      try {
        // Clear any cached queries for this specific table
        if (this.coordinator.cache && this.table) {
          // Clear all cache entries that reference this table
          if (typeof this.coordinator.cache.clear === 'function') {
            // Clear entire cache to be safe - table name conflicts are dangerous
            this.coordinator.cache.clear();
          } else if (this.coordinator.cache instanceof Map) {
            // Clear entire Map cache
            this.coordinator.cache.clear();
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
          }
        }
        
        // Clear query cache if it exists separately
        if (this.coordinator.queryCache && this.table) {
          if (typeof this.coordinator.queryCache.clear === 'function') {
            this.coordinator.queryCache.clear();
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