import { Type } from '@uwdata/flechette';
import { Query, count, sql } from '@uwdata/mosaic-sql';
import { ColumnVisualization } from './ColumnVisualization.js';
import { createHistogram } from './utils/HistogramRenderer.js';
import { createInteractiveHistogram } from './utils/InteractiveHistogram.js';
import { createInteractionHandler, InteractionHandler } from './utils/InteractionHandler.js';

/**
 * Numeric histogram visualization for continuous data columns
 * Extends ColumnVisualization with binning and histogram-specific functionality
 */
export class Histogram extends ColumnVisualization {
  constructor(options = {}) {
    super(options);
    
    this.bins = options.bins || 18; // Default bin count following Quak pattern
    this.container.className = 'column-visualization histogram-visualization';
    
    // External stats display element from column header
    this.statsDisplay = options.statsDisplay || null;
    
    // Interactive mode flag
    this.interactive = options.interactive !== false; // Default to interactive
    
    // Interaction state
    this.interactionHandler = null;
    this.currentHistogram = null;
    
    // Store actual total count for correct proportion calculations
    this.actualTotalCount = 0;
    
    if (this.interactive) {
      // Create interaction handler
      this.interactionHandler = createInteractionHandler({
        table: this.table,
        column: this.column,
        field: this.field,
        filterBy: this.filterBy,
        client: this,
        debounceDelay: 50
      });
    }
  }

  /**
   * Override prepare to calculate quartiles for Freedman-Diaconis rule
   */
  async prepare() {
    try {
      // First call parent prepare to get basic field info
      await super.prepare();

      // Then calculate quartiles using SQL query
      if (this.coordinator && this.fieldInfo) {
        const quartileQuery = Query
          .from(this.table)
          .select({
            q1: sql`QUANTILE(${this.column}, 0.25)`,
            q3: sql`QUANTILE(${this.column}, 0.75)`,
            count: sql`COUNT(${this.column})`
          })
          .where(sql`${this.column} IS NOT NULL`);

        const quartileResult = await this.coordinator.query(quartileQuery);

        if (quartileResult && quartileResult.length > 0) {
          const quartileData = Array.isArray(quartileResult) ? quartileResult[0] : quartileResult.toArray()[0];
          this.q1 = quartileData.q1;
          this.q3 = quartileData.q3;
          this.nonNullCount = quartileData.count;
        }
      }

      return this;
    } catch (error) {
      console.error(`Failed to prepare histogram for ${this.column}:`, error);
      // Continue without quartiles if calculation fails
      return this;
    }
  }

  /**
   * Generate SQL query for histogram bins including null count
   * @param {Array} filter - Filter expressions to apply
   * @returns {Query} Binning query for this column with null handling
   */
  query(filter = []) {
    if (!this.fieldInfo) {
      // Fallback query if field info not available yet
      return super.query(filter);
    }

    const { min, max } = this.fieldInfo;

    if (min == null || max == null || min === max) {
      // Handle edge case with no range - still include null count
      const validQuery = Query
        .from(this.table)
        .select({
          x0: sql`${min}`,
          x1: sql`${min}`,
          count: count(),
          is_null: sql`false`
        })
        .where(filter)
        .where(sql`${this.column} IS NOT NULL`);

      const nullQuery = Query
        .from(this.table)
        .select({
          x0: sql`NULL`,
          x1: sql`NULL`,
          count: count(),
          is_null: sql`true`
        })
        .where(filter)
        .where(sql`${this.column} IS NULL`);

      return Query.unionAll(validQuery, nullQuery);
    }

    // Calculate number of bins using Freedman-Diaconis rule
    let numBins;

    if (this.q1 != null && this.q3 != null && this.nonNullCount != null) {
      const IQR = this.q3 - this.q1;
      const n = this.nonNullCount; // Non-null count

      if (IQR > 0 && n > 0) {
        const binWidth = 2 * IQR / Math.pow(n, 1/3);
        numBins = Math.ceil((max - min) / binWidth);
      } else {
        // Fall back to Sturges' formula if IQR is 0
        numBins = Math.ceil(1 + Math.log2(n));
      }

      // Clamp to reasonable range
      numBins = Math.max(5, Math.min(100, numBins));
    } else {
      // Fall back to default if quartiles unavailable
      numBins = this.bins;
    }

    // Calculate bin width
    const binWidth = (max - min) / numBins;
    
    // Query for regular histogram bins
    const binQuery = Query
      .from(this.table)
      .select({
        x0: sql`floor((${this.column} - ${min}) / ${binWidth}) * ${binWidth} + ${min}`,
        x1: sql`(floor((${this.column} - ${min}) / ${binWidth}) + 1) * ${binWidth} + ${min}`,
        count: count(),
        is_null: sql`false`
      })
      .where(filter)
      .where(sql`${this.column} IS NOT NULL`)
      .groupby('x0', 'x1');
    
    // Query for null values (includes NULL, NaN, invalid strings, etc.)
    const nullQuery = Query
      .from(this.table)
      .select({
        x0: sql`NULL`,
        x1: sql`NULL`, 
        count: count(),
        is_null: sql`true`
      })
      .where(filter)
      .where(sql`${this.column} IS NULL`);
    
    // Combine both queries with UNION ALL
    return Query.unionAll(binQuery, nullQuery);
  }
  
  /**
   * Render histogram from query result data
   * @param {Object} data - Arrow table or array of bin objects
   */
  render(data) {
    try {
      // Convert Arrow table to JavaScript array if needed
      let allBins = [];
      if (data && typeof data.toArray === 'function') {
        allBins = data.toArray();
      } else if (Array.isArray(data)) {
        allBins = data;
      }
      
      // Separate null count from regular bins
      let nullCount = 0;
      let regularBins = [];
      
      allBins.forEach(bin => {
        if (bin.is_null === true || bin.x0 === null || bin.x1 === null) {
          nullCount = bin.count;
        } else {
          regularBins.push({
            x0: bin.x0,
            x1: bin.x1,
            count: bin.count
          });
        }
      });
      
      // Pass actual min/max from fieldInfo for accurate display
      const actualRange = this.fieldInfo ? {
        min: this.fieldInfo.min,
        max: this.fieldInfo.max
      } : null;
      
      // Clear container first
      this.container.innerHTML = '';
      
      // Calculate and store actual total count for proportion calculations
      this.actualTotalCount = regularBins.reduce((sum, bin) => sum + bin.count, 0) + nullCount;
      
      // Initialize external stats display with total count
      if (this.statsDisplay) {
        this.statsDisplay.textContent = `${this.actualTotalCount.toLocaleString()} rows`;
      }
      
      if (this.interactive) {
        this.renderInteractive(regularBins, actualRange, nullCount);
      } else {
        this.renderStatic(regularBins, actualRange, nullCount);
      }
      
    } catch (error) {
      console.error(`Failed to render histogram for ${this.column}:`, error);
      this.renderError();
    }
  }
  
  /**
   * Render static histogram
   * @param {Array} regularBins - Regular histogram bins
   * @param {Object} actualRange - Actual data range
   * @param {number} nullCount - Count of null values
   */
  renderStatic(regularBins, actualRange, nullCount) {
    const histogramSVG = createHistogram(regularBins, this.field, {
      width: 125,
      height: 50,          // Increased to match interactive version
      actualRange,
      nullCount
    });
    
    this.container.appendChild(histogramSVG);
  }
  
  /**
   * Render interactive histogram
   * @param {Array} regularBins - Regular histogram bins
   * @param {Object} actualRange - Actual data range
   * @param {number} nullCount - Count of null values
   */
  renderInteractive(regularBins, actualRange, nullCount) {
    // Clean up previous instance
    if (this.currentHistogram) {
      this.currentHistogram.destroy();
      this.currentHistogram = null;
    }
    
    // Create interactive histogram with external stats display
    this.currentHistogram = createInteractiveHistogram(
      regularBins, 
      this.field, 
      {
        width: 125,
        height: 50,  // Further increased to show x-axis labels properly
        actualRange,
        nullCount,
        statsDisplay: this.statsDisplay,  // Pass external stats element
        totalCount: this.actualTotalCount  // Pass actual total count
      },
      (selection, isFinal) => this.handleSelectionChange(selection, isFinal),
      (hoverData) => this.handleHover(hoverData)
    );
    
    // Add to container
    this.container.appendChild(this.currentHistogram.node());
  }
  
  /**
   * Handle selection changes from interactive histogram
   * @param {Array|string|null} selection - The selection data
   * @param {boolean} isFinal - Whether this is a final selection
   */
  handleSelectionChange(selection, isFinal) {
    if (!this.interactionHandler) return;
    
    if (selection === null) {
      // Clear selection
      this.interactionHandler.handleSelectionChange(null, 'interval', isFinal);
    } else if (selection === 'null') {
      // Null value selected
      this.interactionHandler.handleSelectionChange(null, 'null', isFinal);
    } else if (Array.isArray(selection) && selection.length === 2) {
      // Range selection
      this.interactionHandler.handleSelectionChange(selection, 'interval', isFinal);
    } else {
      console.warn('Unexpected selection format:', selection);
    }
  }
  
  /**
   * Handle hover events from interactive histogram
   * @param {Object|null} hoverData - Hover data or null when not hovering
   */
  handleHover(hoverData) {
    // Update external stats display if available
    if (this.statsDisplay) {
      if (hoverData) {
        const { count, bin, isNull } = hoverData;
        const percentage = (count / this.actualTotalCount) * 100;

        if (isNull) {
          const label = 'null value';
          this.statsDisplay.textContent = `${count.toLocaleString()} ${label}${count === 1 ? '' : 's'} (${percentage.toFixed(1)}%)`;
        } else if (bin && bin.x0 != null && bin.x1 != null) {
          // Create a temporary InteractionHandler instance for formatting
          const formatter = new InteractionHandler();
          const formattedX0 = formatter.formatNumericValue(bin.x0);
          const formattedX1 = formatter.formatNumericValue(bin.x1);
          this.statsDisplay.textContent = `[${formattedX0}..${formattedX1}]: ${count.toLocaleString()} rows (${percentage.toFixed(1)}%)`;
        } else {
          const label = 'row';
          this.statsDisplay.textContent = `${count.toLocaleString()} ${label}${count === 1 ? '' : 's'} (${percentage.toFixed(1)}%)`;
        }
      } else {
        // Reset to total count
        this.statsDisplay.textContent = `${this.actualTotalCount.toLocaleString()} rows`;
      }
    }
    
    // Also update column header type label if available (for backward compatibility)
    const headerLabel = this.container.closest('th')?.querySelector('.gray');
    if (headerLabel && !hoverData) {
      // Reset to field type when not hovering
      const fieldType = this.getFieldType();
      headerLabel.textContent = fieldType;
    }
  }
  
  /**
   * Get total count for percentage calculations
   * @returns {number} Total count
   */
  getTotalCount() {
    return this.actualTotalCount;
  }
  
  /**
   * Get field type string for display
   * @returns {string} Field type
   */
  getFieldType() {
    if (this.field && this.field.duckdbType) {
      return this.field.duckdbType.toLowerCase();
    }
    return 'numeric';
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
  
  /**
   * Clean up resources when visualization is destroyed
   */
  destroy() {
    // Clean up interactive components
    if (this.currentHistogram) {
      this.currentHistogram.destroy();
      this.currentHistogram = null;
    }
    
    if (this.interactionHandler) {
      this.interactionHandler.destroy();
      this.interactionHandler = null;
    }
    
    // Call parent cleanup
    super.destroy();
  }
}