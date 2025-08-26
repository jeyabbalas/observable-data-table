import { Type } from '@uwdata/flechette';
import { Query, count, sql } from '@uwdata/mosaic-sql';
import { ColumnVisualization } from './ColumnVisualization.js';
import { createHistogram } from './utils/HistogramRenderer.js';

/**
 * Numeric histogram visualization for continuous data columns
 * Extends ColumnVisualization with binning and histogram-specific functionality
 */
export class Histogram extends ColumnVisualization {
  constructor(options = {}) {
    super(options);
    
    this.bins = options.bins || 18; // Default bin count following Quak pattern
    this.container.className = 'column-visualization histogram-visualization';
  }
  
  /**
   * Generate SQL query for histogram bins
   * @param {Array} filter - Filter expressions to apply
   * @returns {Query} Binning query for this column
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
    
    // Calculate bin width
    const binWidth = (max - min) / this.bins;
    
    return Query
      .from(this.table)
      .select({
        x0: sql`floor((${this.column} - ${min}) / ${binWidth}) * ${binWidth} + ${min}`,
        x1: sql`(floor((${this.column} - ${min}) / ${binWidth}) + 1) * ${binWidth} + ${min}`,
        count: count()
      })
      .where(filter)
      .where(sql`${this.column} IS NOT NULL`)
      .groupby('x0', 'x1')
      .orderby('x0');
  }
  
  /**
   * Render histogram from query result data
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
      
      // Create histogram visualization
      const histogramSVG = createHistogram(bins, this.field, {
        width: 125,
        height: 40
      });
      
      // Clear container and add histogram
      this.container.innerHTML = '';
      this.container.appendChild(histogramSVG);
      
    } catch (error) {
      console.error(`Failed to render histogram for ${this.column}:`, error);
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
   * Check if histogram is appropriate for this field type
   * @param {Object} field - Arrow field object
   * @returns {boolean}
   */
  static isApplicable(field) {
    if (!field || !field.type) return false;
    
    const typeId = field.type.typeId;
    return typeId === Type.Int || 
           typeId === Type.Float || 
           typeId === Type.Decimal;
  }
  
  /**
   * Get display name for this visualization type
   * @returns {string}
   */
  static getDisplayName() {
    return 'Histogram';
  }
}