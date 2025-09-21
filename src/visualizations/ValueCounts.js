import { Type } from '@uwdata/flechette';
import { Query, count, sql } from '@uwdata/mosaic-sql';
import { ColumnVisualization } from './ColumnVisualization.js';
import { createInteractionHandler, InteractionHandler } from './utils/InteractionHandler.js';
import * as d3 from 'd3';

/**
 * Value counts visualization for categorical (string and boolean) data columns
 * Extends ColumnVisualization with horizontal proportion bars showing category distributions
 */
export class ValueCounts extends ColumnVisualization {
  constructor(options = {}) {
    super(options);

    this.container.className = 'column-visualization value-counts-visualization';

    // External stats display element from column header
    this.statsDisplay = options.statsDisplay || null;

    // Interactive mode flag
    this.interactive = options.interactive !== false; // Default to interactive

    // Interaction state
    this.interactionHandler = null;

    // Store actual total count for correct proportion calculations
    this.actualTotalCount = 0;

    // Store total number of categories
    this.totalCategoryCount = 0;

    // Configuration
    this.maxCategories = 10; // Show up to 10 categories, aggregate the rest
    this.minCategoryCount = 2; // Categories with count >= 2 are shown individually

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
   * Generate SQL query for value counts including null count and unique value aggregation
   * @param {Array} filter - Filter expressions to apply
   * @returns {Query} Value counts query for this column with null handling
   */
  query(filter = []) {
    if (!this.fieldInfo) {
      // Fallback query if field info not available yet
      return super.query(filter);
    }

    // Query for individual category counts (excluding nulls)
    // Note: ORDER BY is handled in JavaScript during processCategories()
    const categoryQuery = Query
      .from(this.table)
      .select({
        value: this.column,
        count: count(),
        is_null: sql`false`,
        is_unique: sql`false`
      })
      .where(filter)
      .where(sql`${this.column} IS NOT NULL`)
      .groupby(this.column);

    // Query for null values
    const nullQuery = Query
      .from(this.table)
      .select({
        value: sql`NULL`,
        count: count(),
        is_null: sql`true`,
        is_unique: sql`false`
      })
      .where(filter)
      .where(sql`${this.column} IS NULL`);

    // Combine both queries with UNION ALL
    return Query.unionAll(categoryQuery, nullQuery);
  }

  /**
   * Render value counts from query result data
   * @param {Object} data - Arrow table or array of category objects
   */
  render(data) {
    try {
      // Convert Arrow table to JavaScript array if needed
      let allCategories = [];
      if (data && typeof data.toArray === 'function') {
        allCategories = data.toArray();
      } else if (Array.isArray(data)) {
        allCategories = data;
      }

      // Separate null count from regular categories
      let nullCount = 0;
      let regularCategories = [];

      allCategories.forEach(cat => {
        if (cat.is_null === true || cat.value === null) {
          nullCount = cat.count;
        } else {
          regularCategories.push({
            value: cat.value,
            count: cat.count
          });
        }
      });

      // Process categories to handle unique values aggregation
      const processedCategories = this.processCategories(regularCategories);

      // Clear container first
      this.container.innerHTML = '';

      // Use the actual total row count from the base class for accurate proportion calculations
      this.actualTotalCount = this.totalRowCount;

      // Calculate total number of categories (including nulls if present)
      this.totalCategoryCount = regularCategories.length + (nullCount > 0 ? 1 : 0);

      // Initialize external stats display with total count and category count
      if (this.statsDisplay) {
        this.statsDisplay.textContent = `${this.actualTotalCount.toLocaleString()} rows; ${this.totalCategoryCount} categories`;
      }

      if (this.interactive) {
        this.renderInteractive(processedCategories, nullCount);
      } else {
        this.renderStatic(processedCategories, nullCount);
      }

    } catch (error) {
      console.error(`Failed to render value counts for ${this.column}:`, error);
      this.renderError();
    }
  }

  /**
   * Process categories to handle unique values aggregation
   * @param {Array} categories - Array of category objects with value and count
   * @returns {Array} Processed categories with unique values aggregated
   */
  processCategories(categories) {
    // Sort by count descending
    categories.sort((a, b) => b.count - a.count);

    let regularCategories = [];
    let uniqueValueCount = 0;
    let uniqueValueSum = 0;

    categories.forEach(cat => {
      if (cat.count >= this.minCategoryCount && regularCategories.length < this.maxCategories) {
        // Show this category individually
        regularCategories.push(cat);
      } else {
        // Aggregate into unique values
        uniqueValueCount++;
        uniqueValueSum += cat.count;
      }
    });

    // Add unique values category if there are any
    if (uniqueValueCount > 0) {
      regularCategories.push({
        value: `${uniqueValueCount} unique values`,
        count: uniqueValueSum,
        isUnique: true
      });
    }

    return regularCategories;
  }

  /**
   * Render static value counts
   * @param {Array} categories - Processed categories
   * @param {number} nullCount - Count of null values
   */
  renderStatic(categories, nullCount) {
    const svg = this.createValueCountsSVG(categories, nullCount, {
      width: 150,
      height: 50
    });

    this.container.appendChild(svg);
  }

  /**
   * Render interactive value counts
   * @param {Array} categories - Processed categories
   * @param {number} nullCount - Count of null values
   */
  renderInteractive(categories, nullCount) {
    const svg = this.createValueCountsSVG(categories, nullCount, {
      width: 150,
      height: 50,
      interactive: true
    });

    this.container.appendChild(svg);
  }

  /**
   * Create SVG visualization for value counts
   * @param {Array} categories - Processed categories
   * @param {number} nullCount - Count of null values
   * @param {Object} options - Rendering options
   * @returns {SVGElement} SVG element
   */
  createValueCountsSVG(categories, nullCount, options = {}) {
    const { width = 150, height = 50, interactive = false } = options;

    // Calculate total for proportions
    const categoriesTotal = categories.reduce((sum, cat) => sum + cat.count, 0);
    const grandTotal = categoriesTotal + nullCount;

    // Create SVG
    const svg = d3.create('svg')
      .attr('width', width)
      .attr('height', height)
      .style('display', 'block');

    const margin = { top: 0, right: 15, bottom: 10, left: 4 };
    const plotWidth = width - margin.left - margin.right;
    const plotHeight = height - margin.top - margin.bottom;

    const g = svg.append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // Add a clipping path for overall rounded corners
    const defs = svg.append('defs');
    const clipPath = defs.append('clipPath')
      .attr('id', `value-counts-clip-${Math.random().toString(36).substr(2, 9)}`);

    clipPath.append('rect')
      .attr('x', 0)
      .attr('y', 0)
      .attr('width', plotWidth)
      .attr('height', plotHeight)
      .attr('rx', 4)
      .attr('ry', 4);

    // Apply clipping to the entire visualization group
    g.attr('clip-path', `url(#${clipPath.attr('id')})`);

    // Create scale for proportions
    let currentX = 0;

    // Add categories
    categories.forEach((cat, i) => {
      const proportion = cat.count / grandTotal;
      const barWidth = proportion * plotWidth;

      // Determine color
      let barColor = cat.isUnique ? '#d1d5db' : '#2563eb'; // Light gray for unique, blue for regular

      // Create bar
      const barGroup = g.append('g')
        .attr('class', 'category-bar')
        .style('cursor', interactive ? 'pointer' : 'default');

      const rect = barGroup.append('rect')
        .attr('x', currentX)
        .attr('y', 0)
        .attr('width', barWidth)
        .attr('height', plotHeight)
        .attr('fill', barColor)
        .attr('stroke', '#e5e7eb')
        .attr('stroke-width', 0.5);

      // Add text label if bar is wide enough
      if (barWidth > 15) {
        const originalText = cat.isUnique ? cat.value : String(cat.value);  // Show full "# unique values" text
        const truncatedText = this.truncateText(originalText, barWidth, '10px');

        // Determine text color based on background
        const textColor = cat.isUnique ? '#374151' : 'white'; // Dark gray for light gray bars, white for blue

        barGroup.append('text')
          .attr('x', currentX + barWidth / 2)
          .attr('y', plotHeight / 2)
          .attr('dy', '0.35em')
          .attr('text-anchor', 'middle')
          .attr('font-size', '10px')
          .attr('font-weight', '500')
          .attr('font-family', 'var(--font-sans, system-ui)')
          .style('pointer-events', 'none')
          .style('fill', textColor)  // Use style instead of attr for fill
          .text(truncatedText);
      }

      // Add interaction handlers if interactive
      if (interactive) {
        this.addCategoryInteraction(barGroup, cat, rect);
      }

      currentX += barWidth;
    });

    // Add null bar if there are null values
    if (nullCount > 0) {
      const nullProportion = nullCount / grandTotal;
      const nullBarWidth = nullProportion * plotWidth;

      const nullBarGroup = g.append('g')
        .attr('class', 'null-bar')
        .style('cursor', interactive ? 'pointer' : 'default');

      const nullRect = nullBarGroup.append('rect')
        .attr('x', currentX)
        .attr('y', 0)
        .attr('width', nullBarWidth)
        .attr('height', plotHeight)
        .attr('fill', 'var(--secondary, #fbbf24)') // Gold color for nulls
        .attr('stroke', '#e5e7eb')
        .attr('stroke-width', 0.5);

      // Add null symbol if bar is wide enough
      if (nullBarWidth > 15) {
        nullBarGroup.append('text')
          .attr('x', currentX + nullBarWidth / 2)
          .attr('y', plotHeight / 2)
          .attr('dy', '0.35em')
          .attr('text-anchor', 'middle')
          .attr('font-size', '12px')
          .attr('font-weight', '600')
          .attr('font-family', 'var(--font-sans, system-ui)')
          .style('pointer-events', 'none')
          .style('fill', '#92400e')  // Use style instead of attr for fill - Dark amber for gold background
          .text('∅');
      }

      // Add interaction handlers if interactive
      if (interactive) {
        this.addNullInteraction(nullBarGroup, nullCount, nullRect);
      }
    }

    return svg.node();
  }

  /**
   * Helper function to truncate text to fit within available width
   * @param {string} text - Original text
   * @param {number} availableWidth - Available width in pixels
   * @param {string} fontSize - Font size (e.g., '10px')
   * @returns {string} Truncated text
   */
  truncateText(text, availableWidth, fontSize = '10px') {
    // Approximate character width based on font size
    const charWidth = parseInt(fontSize) * 0.6; // Rough estimate
    const maxChars = Math.floor((availableWidth - 8) / charWidth); // Leave some padding

    if (text.length <= maxChars || maxChars < 3) {
      return text;
    }

    // Truncate with ellipsis
    return text.substring(0, maxChars - 1) + '…';
  }

  /**
   * Add interaction handlers for category bars
   * @param {d3.Selection} barGroup - D3 selection of bar group
   * @param {Object} category - Category data
   * @param {d3.Selection} rect - D3 selection of rectangle
   */
  addCategoryInteraction(barGroup, category, rect) {
    barGroup
      .on('mouseenter', () => {
        // Highlight on hover
        rect.attr('opacity', 0.8);
        this.handleHover({
          value: category.value,
          count: category.count,
          isUnique: category.isUnique
        });
      })
      .on('mouseleave', () => {
        // Remove highlight
        rect.attr('opacity', 1);
        this.handleHover(null);
      })
      .on('click', () => {
        // Handle click selection
        if (this.interactionHandler) {
          this.interactionHandler.handleSelectionChange(category.value, 'point', true);
        }
      });
  }

  /**
   * Add interaction handlers for null bars
   * @param {d3.Selection} barGroup - D3 selection of bar group
   * @param {number} nullCount - Count of null values
   * @param {d3.Selection} rect - D3 selection of rectangle
   */
  addNullInteraction(barGroup, nullCount, rect) {
    barGroup
      .on('mouseenter', () => {
        // Highlight on hover
        rect.attr('opacity', 0.8);
        this.handleHover({
          value: null,
          count: nullCount,
          isNull: true
        });
      })
      .on('mouseleave', () => {
        // Remove highlight
        rect.attr('opacity', 1);
        this.handleHover(null);
      })
      .on('click', () => {
        // Handle click selection for null values
        if (this.interactionHandler) {
          this.interactionHandler.handleSelectionChange(null, 'null', true);
        }
      });
  }

  /**
   * Handle hover events
   * @param {Object|null} hoverData - Hover data or null when not hovering
   */
  handleHover(hoverData) {
    // Update external stats display if available
    if (this.statsDisplay) {
      if (hoverData) {
        const { value, count, isUnique, isNull } = hoverData;
        const percentage = (count / this.actualTotalCount) * 100;

        if (isNull) {
          this.statsDisplay.textContent = `∅: ${count.toLocaleString()} rows (${percentage.toFixed(1)}%)`;
        } else if (isUnique) {
          this.statsDisplay.textContent = `${value}: ${count.toLocaleString()} rows (${percentage.toFixed(1)}%)`;
        } else {
          this.statsDisplay.textContent = `${value}: ${count.toLocaleString()} rows (${percentage.toFixed(1)}%)`;
        }
      } else {
        // Reset to total count and category count
        this.statsDisplay.textContent = `${this.actualTotalCount.toLocaleString()} rows; ${this.totalCategoryCount} categories`;
      }
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
    return 'categorical';
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
   * Check if value counts visualization is appropriate for this field type
   * @param {Object} field - Arrow field object
   * @returns {boolean}
   */
  static isApplicable(field) {
    if (!field || !field.type) return false;

    const typeId = field.type.typeId;
    return typeId === Type.Utf8 ||
           typeId === Type.LargeUtf8 ||
           typeId === Type.Bool;
  }

  /**
   * Get display name for this visualization type
   * @returns {string}
   */
  static getDisplayName() {
    return 'Value Counts';
  }

  /**
   * Clean up resources when visualization is destroyed
   */
  destroy() {
    if (this.interactionHandler) {
      this.interactionHandler.destroy();
      this.interactionHandler = null;
    }

    // Call parent cleanup
    super.destroy();
  }
}