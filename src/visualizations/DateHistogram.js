import { Type } from '@uwdata/flechette';
import { Query, count, sql } from '@uwdata/mosaic-sql';
import { ColumnVisualization } from './ColumnVisualization.js';
import { createDateHistogram } from './utils/HistogramRenderer.js';

/**
 * Date/time histogram visualization for temporal data columns
 * Extends ColumnVisualization with temporal binning and date-specific functionality
 */
export class DateHistogram extends ColumnVisualization {
  constructor(options = {}) {
    super(options);
    
    this.bins = options.bins || 12; // Fewer bins for dates (months/quarters)
    this.container.className = 'column-visualization date-histogram-visualization';
  }
  
  /**
   * Generate SQL query for temporal histogram bins
   * @param {Array} filter - Filter expressions to apply
   * @returns {Query} Temporal binning query for this column
   */
  query(filter = []) {
    if (!this.fieldInfo) {
      // Fallback query if field info not available yet
      return super.query(filter);
    }
    
    const { min, max } = this.fieldInfo;
    
    if (min == null || max == null || min === max) {
      // Handle edge case with no range
      return Query
        .from(this.table)
        .select({
          x0: sql`${min}`,
          x1: sql`${min}`,
          count: count()
        })
        .where(filter)
        .where(sql`${this.column} IS NOT NULL`);
    }
    
    // Determine appropriate temporal binning based on date range
    const timespan = new Date(max) - new Date(min);
    const daysSpan = timespan / (1000 * 60 * 60 * 24);
    
    let interval;
    if (daysSpan <= 7) {
      // Less than a week - bin by day
      interval = sql`DATE_TRUNC('day', ${this.column})`;
    } else if (daysSpan <= 31) {
      // Less than a month - bin by day
      interval = sql`DATE_TRUNC('day', ${this.column})`;
    } else if (daysSpan <= 365) {
      // Less than a year - bin by month
      interval = sql`DATE_TRUNC('month', ${this.column})`;
    } else {
      // More than a year - bin by year
      interval = sql`DATE_TRUNC('year', ${this.column})`;
    }
    
    return Query
      .from(this.table)
      .select({
        x0: interval,
        x1: interval, // For dates, x0 and x1 are the same (period start)
        count: count()
      })
      .where(filter)
      .where(sql`${this.column} IS NOT NULL`)
      .groupby(interval)
      .orderby(interval);
  }
  
  /**
   * Render date histogram from query result data
   * @param {Object} data - Arrow table or array of bin objects
   */
  render(data) {
    try {
      // Convert Arrow table to JavaScript array if needed
      let bins = [];
      if (data && typeof data.toArray === 'function') {
        bins = data.toArray();
      } else if (Array.isArray(data)) {
        bins = data;
      }
      
      // Pass actual min/max from fieldInfo for accurate display
      const actualRange = this.fieldInfo ? {
        min: this.fieldInfo.min,
        max: this.fieldInfo.max
      } : null;
      
      // Create date histogram visualization
      const histogramSVG = createDateHistogram(bins, this.field, {
        width: 125,
        height: 40,
        actualRange
      });
      
      // Clear container and add histogram
      this.container.innerHTML = '';
      this.container.appendChild(histogramSVG);
      
    } catch (error) {
      console.error(`Failed to render date histogram for ${this.column}:`, error);
      this.renderError();
    }
  }
  
  /**
   * Render error state
   */
  renderError() {
    this.container.innerHTML = `
      <div style="width: 125px; height: 40px; display: flex; align-items: center; justify-content: center; color: #ef4444; font-size: 11px;">
        Error
      </div>
    `;
  }
  
  /**
   * Check if date histogram is appropriate for this field type
   * @param {Object} field - Arrow field object or mock field
   * @returns {boolean}
   */
  static isApplicable(field) {
    if (!field) return false;
    
    // Check DuckDB type from mock field (preferred)
    if (field.duckdbType && typeof field.duckdbType === 'string') {
      const typeStr = field.duckdbType.toUpperCase();
      return typeStr.includes('DATE') || 
             typeStr.includes('TIMESTAMP') || 
             typeStr.includes('DATETIME') ||
             typeStr.includes('TIME');
    }
    
    // Check field type string (from DuckDB schema)
    if (field.type && typeof field.type === 'string') {
      const typeStr = field.type.toUpperCase();
      return typeStr.includes('DATE') || 
             typeStr.includes('TIMESTAMP') || 
             typeStr.includes('DATETIME') ||
             typeStr.includes('TIME');
    }
    
    // Check Arrow type object
    if (field.type && field.type.typeId) {
      return field.type.typeId === Type.Date || 
             field.type.typeId === Type.Timestamp;
    }
    
    return false;
  }
  
  /**
   * Get display name for this visualization type
   * @returns {string}
   */
  static getDisplayName() {
    return 'Date Histogram';
  }
}