import { MosaicClient, queryFieldInfo } from '@uwdata/mosaic-core';
import { Query, count, sql } from '@uwdata/mosaic-sql';

/**
 * Base class for column visualizations that extend MosaicClient
 * Provides common functionality for histogram, value counts, and other column-level visualizations
 */
export class ColumnVisualization extends MosaicClient {
  constructor(options = {}) {
    super(options.filterBy);
    
    this.table = options.table;
    this.column = options.column;
    this.field = options.field;
    this.type = options.type || 'auto';
    
    // UI elements
    this.container = document.createElement('div');
    this.container.className = 'column-visualization';
    
    // State
    this.fieldInfo = null;
    this.initialized = false;
  }
  
  /**
   * MosaicClient prepare phase - fetch field metadata
   */
  async prepare() {
    try {
      const info = await queryFieldInfo(
        this.coordinator,
        [{
          table: this.table,
          column: this.column,
          stats: ['min', 'max', 'distinct', 'nulls']
        }]
      );
      
      this.fieldInfo = info[0];
      return this;
    } catch (error) {
      console.error(`Failed to prepare field info for ${this.column}:`, error);
      throw error;
    }
  }
  
  /**
   * MosaicClient query method - should be overridden by subclasses
   * @param {Array} filter - Filter expressions to apply
   * @returns {Query} The SQL query for this visualization
   */
  query(filter = []) {
    // Default implementation - should be overridden
    return Query
      .from(this.table)
      .select({ count: count() })
      .where(filter);
  }
  
  /**
   * MosaicClient queryResult method - handle data from coordinator
   * @param {Object} data - Result data from query
   * @returns {this}
   */
  queryResult(data) {
    try {
      this.render(data);
      this.initialized = true;
      return this;
    } catch (error) {
      console.error(`Failed to render ${this.column} visualization:`, error);
      return this;
    }
  }
  
  /**
   * Render the visualization - should be overridden by subclasses
   * @param {Object} data - Data to render
   */
  render(data) {
    // Default implementation - should be overridden
    this.container.innerHTML = '<div style="padding: 10px; color: #666;">No visualization</div>';
  }
  
  /**
   * Get the DOM node for this visualization
   * @returns {HTMLElement}
   */
  node() {
    return this.container;
  }
  
  /**
   * Check if this visualization type is appropriate for the given field
   * @param {Object} field - Arrow field object
   * @returns {boolean}
   */
  static isApplicable(field) {
    // Default implementation - should be overridden
    return false;
  }
  
  /**
   * Cleanup resources when visualization is destroyed
   */
  destroy() {
    if (this.coordinator) {
      this.coordinator.disconnect(this);
    }
    
    if (this.container && this.container.parentNode) {
      this.container.parentNode.removeChild(this.container);
    }
  }
}