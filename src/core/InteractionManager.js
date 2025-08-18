import { signal } from '@preact/signals-core';
import { Query } from '@uwdata/mosaic-sql';

export class InteractionManager {
  constructor(options = {}) {
    this.dataTable = options.dataTable;
    this.coordinator = options.coordinator;
    
    // State management
    this.interactions = signal([]);
    this.activeFilters = signal(new Map());
    this.generatedSQL = signal('');
    
    // Bind methods
    this.handleVisualizationInteraction = this.handleVisualizationInteraction.bind(this);
    this.generateSQL = this.generateSQL.bind(this);
  }
  
  registerVisualization(visualization) {
    // Register event listeners for visualization interactions
    if (visualization.on) {
      visualization.on('filter', this.handleVisualizationInteraction);
      visualization.on('brush', this.handleVisualizationInteraction);
      visualization.on('select', this.handleVisualizationInteraction);
    }
  }
  
  handleVisualizationInteraction(event) {
    const { source, type, field, value, predicate } = event;
    
    // Update active filters
    const filters = new Map(this.activeFilters.value);
    
    if (value === null || value === undefined) {
      // Remove filter
      filters.delete(field);
    } else {
      // Add or update filter
      filters.set(field, {
        type,
        field,
        value,
        predicate,
        source: source.constructor.name
      });
    }
    
    this.activeFilters.value = filters;
    
    // Record interaction
    this.interactions.value = [...this.interactions.value, {
      id: crypto.randomUUID(),
      timestamp: Date.now(),
      type,
      field,
      value,
      source: source.constructor.name
    }];
    
    // Generate SQL
    this.generateSQL();
    
    // Notify coordinator of filter change
    if (this.coordinator) {
      this.coordinator.filterBy(Array.from(filters.values()).map(f => f.predicate).filter(Boolean));
    }
  }
  
  generateSQL() {
    const tableName = this.dataTable?.tableName?.value || 'data';
    const filters = Array.from(this.activeFilters.value.values());
    
    if (filters.length === 0) {
      this.generatedSQL.value = `SELECT * FROM ${tableName}`;
      return;
    }
    
    // Build WHERE clause
    const whereConditions = filters.map(filter => {
      switch (filter.type) {
        case 'range':
          if (Array.isArray(filter.value) && filter.value.length === 2) {
            const [min, max] = filter.value;
            return `${filter.field} BETWEEN ${min} AND ${max}`;
          }
          break;
          
        case 'categorical':
          if (Array.isArray(filter.value)) {
            const values = filter.value.map(v => `'${v}'`).join(', ');
            return `${filter.field} IN (${values})`;
          } else {
            return `${filter.field} = '${filter.value}'`;
          }
          
        case 'temporal':
          if (Array.isArray(filter.value) && filter.value.length === 2) {
            const [start, end] = filter.value;
            return `${filter.field} BETWEEN '${start}' AND '${end}'`;
          }
          break;
          
        case 'text':
          return `${filter.field} LIKE '%${filter.value}%'`;
          
        default:
          // Custom predicate
          if (filter.predicate) {
            return filter.predicate;
          }
      }
      
      return null;
    }).filter(Boolean);
    
    const sql = `SELECT * FROM ${tableName}${whereConditions.length > 0 ? ' WHERE ' + whereConditions.join(' AND ') : ''}`;
    this.generatedSQL.value = sql;
    
    // Update DataTable's current SQL
    if (this.dataTable) {
      this.dataTable.currentSQL.value = sql;
    }
  }
  
  addFilter(field, type, value, predicate = null) {
    const event = {
      source: { constructor: { name: 'Manual' } },
      type,
      field,
      value,
      predicate
    };
    
    this.handleVisualizationInteraction(event);
  }
  
  removeFilter(field) {
    const filters = new Map(this.activeFilters.value);
    filters.delete(field);
    this.activeFilters.value = filters;
    this.generateSQL();
    
    // Notify coordinator
    if (this.coordinator) {
      this.coordinator.filterBy(Array.from(filters.values()).map(f => f.predicate).filter(Boolean));
    }
  }
  
  clearAllFilters() {
    this.activeFilters.value = new Map();
    this.generateSQL();
    
    // Notify coordinator
    if (this.coordinator) {
      this.coordinator.filterBy([]);
    }
  }
  
  getActiveFilters() {
    return Array.from(this.activeFilters.value.values());
  }
  
  getGeneratedSQL() {
    return this.generatedSQL.value;
  }
  
  getInteractionHistory() {
    return this.interactions.value;
  }
  
  copySQL() {
    const sql = this.generatedSQL.value;
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(sql).then(() => {
        console.log('SQL copied to clipboard:', sql);
      }).catch(err => {
        console.error('Failed to copy SQL to clipboard:', err);
        this.fallbackCopySQL(sql);
      });
    } else {
      this.fallbackCopySQL(sql);
    }
  }
  
  fallbackCopySQL(sql) {
    // Fallback for browsers that don't support clipboard API
    const textArea = document.createElement('textarea');
    textArea.value = sql;
    textArea.style.position = 'fixed';
    textArea.style.opacity = '0';
    document.body.appendChild(textArea);
    textArea.select();
    
    try {
      document.execCommand('copy');
      console.log('SQL copied to clipboard (fallback):', sql);
    } catch (err) {
      console.error('Failed to copy SQL (fallback):', err);
    }
    
    document.body.removeChild(textArea);
  }
  
  exportInteractions() {
    return {
      interactions: this.interactions.value,
      activeFilters: Array.from(this.activeFilters.value.entries()),
      generatedSQL: this.generatedSQL.value,
      timestamp: Date.now()
    };
  }
  
  importInteractions(data) {
    if (data.interactions) {
      this.interactions.value = data.interactions;
    }
    
    if (data.activeFilters) {
      this.activeFilters.value = new Map(data.activeFilters);
    }
    
    if (data.generatedSQL) {
      this.generatedSQL.value = data.generatedSQL;
    }
    
    // Regenerate SQL to ensure consistency
    this.generateSQL();
  }
}