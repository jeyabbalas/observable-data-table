import { clauseInterval, clausePoint } from '@uwdata/mosaic-core';
import * as d3 from 'd3';

/**
 * Utility class for handling interactions in column visualizations
 * Provides reusable methods for creating selection clauses and managing interaction state
 */
export class InteractionHandler {
  constructor(options = {}) {
    this.table = options.table;
    this.column = options.column;
    this.field = options.field;
    this.filterBy = options.filterBy;
    this.client = options.client; // The MosaicClient instance
    
    // Debounce settings
    this.debounceDelay = options.debounceDelay || 50;
    this.debounceTimer = null;
    
    // State
    this.currentSelection = null;
    this.isSelecting = false;
  }
  
  /**
   * Create an interval selection clause for numeric ranges
   * @param {Array} range - [min, max] range values
   * @param {Object} options - Additional options for the clause
   * @returns {Object} Selection clause
   */
  createIntervalClause(range, options = {}) {
    if (!range || range.length !== 2) {
      return null;
    }
    
    const [min, max] = range;
    
    // Ensure proper order
    const orderedRange = min <= max ? [min, max] : [max, min];
    
    return clauseInterval(this.column, orderedRange, {
      source: this.client,
      clients: new Set([this.client]),
      pixelSize: options.pixelSize || 1,
      ...options
    });
  }
  
  /**
   * Create a point selection clause for categorical values
   * @param {*} value - The value to select
   * @param {Object} options - Additional options for the clause
   * @returns {Object} Selection clause
   */
  createPointClause(value, options = {}) {
    if (value === null || value === undefined) {
      return null;
    }
    
    return clausePoint(this.column, value, {
      source: this.client,
      clients: new Set([this.client]),
      ...options
    });
  }
  
  /**
   * Create a special clause for null value selection
   * @returns {Object} Selection clause for null values
   */
  createNullClause() {
    return clausePoint(this.column, null, {
      source: this.client,
      clients: new Set([this.client])
    });
  }
  
  /**
   * Handle selection changes with debouncing
   * @param {*} selection - The new selection value (range, point, or null)
   * @param {string} type - Type of selection ('interval', 'point', 'null')
   * @param {boolean} isFinal - Whether this is a final selection or intermediate
   * @param {Object} options - Additional options
   */
  handleSelectionChange(selection, type = 'interval', isFinal = true, options = {}) {
    // Clear existing debounce timer
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
    
    // Store the selection
    this.currentSelection = { selection, type, options };
    
    // If this is not a final selection and we want to debounce, delay the update
    if (!isFinal && this.debounceDelay > 0) {
      this.debounceTimer = setTimeout(() => {
        this.applySelection();
        this.debounceTimer = null;
      }, this.debounceDelay);
      return;
    }
    
    // Apply immediately
    this.applySelection();
  }
  
  /**
   * Apply the current selection to the filter
   */
  applySelection() {
    if (!this.filterBy) {
      console.warn('No filterBy selection provided to InteractionHandler');
      return;
    }
    
    let clause = null;
    
    if (this.currentSelection) {
      const { selection, type, options } = this.currentSelection;
      
      switch (type) {
        case 'interval':
          clause = this.createIntervalClause(selection, options);
          break;
        case 'point':
          clause = this.createPointClause(selection, options);
          break;
        case 'null':
          clause = this.createNullClause();
          break;
        default:
          console.warn(`Unknown selection type: ${type}`);
      }
    }
    
    // Update the selection
    if (clause) {
      this.filterBy.update(clause);
    } else {
      // Clear the selection
      this.clearSelection();
    }
  }
  
  /**
   * Clear the current selection
   */
  clearSelection() {
    this.currentSelection = null;
    
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
    
    if (this.filterBy) {
      // Remove this client's clauses from the selection
      this.filterBy.remove(this.client);
    }
  }
  
  /**
   * Get the current selection
   * @returns {Object|null} Current selection object
   */
  getCurrentSelection() {
    return this.currentSelection;
  }
  
  /**
   * Check if there's an active selection
   * @returns {boolean}
   */
  hasSelection() {
    return this.currentSelection !== null;
  }
  
  /**
   * Format a numeric value for display
   * @param {number} value - The value to format
   * @returns {string} Formatted value
   */
  formatNumericValue(value) {
    if (value == null || isNaN(value)) return 'N/A';

    const absValue = Math.abs(value);

    // Use different formatting based on magnitude
    if (absValue >= 1000000) {
      return d3.format('.2s')(value); // SI notation for large numbers
    } else if (absValue >= 1000) {
      return d3.format(',.0f')(value); // Comma separated for thousands
    } else if (absValue % 1 === 0) {
      return value.toString(); // Integer as-is
    } else if (absValue >= 10) {
      return d3.format('.1f')(value); // One decimal place for values >= 10
    } else if (absValue >= 0.1) {
      return d3.format('.1f')(value); // One decimal place for values 0.1-10
    } else {
      // For very small values, show significant digits
      return d3.format('.3g')(value); // Up to 3 significant digits
    }
  }
  
  /**
   * Format a percentage for display
   * @param {number} ratio - The ratio (0-1) to format as percentage
   * @returns {string} Formatted percentage
   */
  formatPercentage(ratio) {
    const percentage = ratio * 100;
    
    if (percentage >= 10) {
      return percentage.toFixed(0) + '%';
    } else if (percentage >= 1) {
      return percentage.toFixed(1) + '%';
    } else {
      return percentage.toFixed(2) + '%';
    }
  }
  
  /**
   * Create hover data object for tooltips
   * @param {*} value - The hovered value
   * @param {number} count - The count for this value
   * @param {number} totalCount - The total count for percentage calculation
   * @param {Object} additionalData - Any additional data to include
   * @returns {Object} Hover data object
   */
  createHoverData(value, count, totalCount, additionalData = {}) {
    const percentage = totalCount > 0 ? count / totalCount : 0;
    
    return {
      value,
      count,
      totalCount,
      percentage,
      formattedValue: this.formatDisplayValue(value),
      formattedCount: count.toLocaleString(),
      formattedPercentage: this.formatPercentage(percentage),
      ...additionalData
    };
  }
  
  /**
   * Format a value for display based on its type
   * @param {*} value - The value to format
   * @returns {string} Formatted value
   */
  formatDisplayValue(value) {
    if (value === null || value === undefined) {
      return 'null';
    }
    
    if (typeof value === 'number') {
      return this.formatNumericValue(value);
    }
    
    if (value instanceof Date) {
      return value.toLocaleDateString();
    }
    
    return String(value);
  }
  
  /**
   * Cleanup resources
   */
  destroy() {
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
    
    this.clearSelection();
  }
}

/**
 * Create an interaction handler for a specific field
 * @param {Object} options - Configuration options
 * @returns {InteractionHandler} New interaction handler instance
 */
export function createInteractionHandler(options) {
  return new InteractionHandler(options);
}

/**
 * Utility functions for common interaction patterns
 */
export const InteractionUtils = {
  /**
   * Clamp a value to a range
   * @param {number} value - Value to clamp
   * @param {number} min - Minimum value
   * @param {number} max - Maximum value
   * @returns {number} Clamped value
   */
  clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  },
  
  /**
   * Check if two ranges overlap
   * @param {Array} range1 - First range [min, max]
   * @param {Array} range2 - Second range [min, max]
   * @returns {boolean} True if ranges overlap
   */
  rangesOverlap(range1, range2) {
    if (!range1 || !range2 || range1.length !== 2 || range2.length !== 2) {
      return false;
    }
    
    const [min1, max1] = range1;
    const [min2, max2] = range2;
    
    return max1 >= min2 && max2 >= min1;
  },
  
  /**
   * Calculate the intersection of two ranges
   * @param {Array} range1 - First range [min, max]
   * @param {Array} range2 - Second range [min, max]
   * @returns {Array|null} Intersection range or null if no overlap
   */
  rangeIntersection(range1, range2) {
    if (!this.rangesOverlap(range1, range2)) {
      return null;
    }
    
    const [min1, max1] = range1;
    const [min2, max2] = range2;
    
    return [Math.max(min1, min2), Math.min(max1, max2)];
  },
  
  /**
   * Debounce a function call
   * @param {Function} func - Function to debounce
   * @param {number} delay - Delay in milliseconds
   * @returns {Function} Debounced function
   */
  debounce(func, delay) {
    let timeoutId;
    return function (...args) {
      clearTimeout(timeoutId);
      timeoutId = setTimeout(() => func.apply(this, args), delay);
    };
  },
  
  /**
   * Throttle a function call
   * @param {Function} func - Function to throttle
   * @param {number} delay - Delay in milliseconds
   * @returns {Function} Throttled function
   */
  throttle(func, delay) {
    let lastCall = 0;
    return function (...args) {
      const now = Date.now();
      if (now - lastCall >= delay) {
        lastCall = now;
        return func.apply(this, args);
      }
    };
  }
};