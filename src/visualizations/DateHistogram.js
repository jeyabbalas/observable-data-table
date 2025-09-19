import { Type } from '@uwdata/flechette';
import { Query, count, sql } from '@uwdata/mosaic-sql';
import { ColumnVisualization } from './ColumnVisualization.js';
import { createDateHistogram } from './utils/HistogramRenderer.js';
import { createInteractiveHistogram } from './utils/InteractiveHistogram.js';
import { createInteractionHandler, InteractionHandler } from './utils/InteractionHandler.js';

/**
 * Intelligent date/time histogram visualization for temporal data columns
 * Automatically detects temporal data types and applies optimal binning strategies
 */
export class DateHistogram extends ColumnVisualization {
  constructor(options = {}) {
    super(options);

    this.targetBins = options.bins || 20; // Target number of bins for optimal visualization
    this.container.className = 'column-visualization date-histogram-visualization';

    // External stats display element from column header
    this.statsDisplay = options.statsDisplay || null;

    // Interactive mode flag
    this.interactive = options.interactive !== false; // Default to interactive

    // Interaction state
    this.interactionHandler = null;
    this.currentHistogram = null;

    // Store actual total count for correct proportion calculations
    this.actualTotalCount = 0;

    // Temporal type detection
    this.temporalType = null; // 'DATE', 'TIMESTAMP', 'TIME', 'INTERVAL'
    this.optimalInterval = null; // Calculated optimal binning interval

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
   * Override prepare to detect temporal type and calculate optimal binning
   */
  async prepare() {
    try {
      // First call parent prepare to get basic field info
      await super.prepare();

      // Detect temporal type
      this.temporalType = this.detectTemporalType();

      // Calculate optimal interval for temporal types
      if (this.fieldInfo && (this.temporalType === 'DATE' || this.temporalType === 'TIMESTAMP')) {
        this.optimalInterval = this.calculateOptimalInterval(this.fieldInfo.min, this.fieldInfo.max);
      }

      return this;
    } catch (error) {
      console.error(`Failed to prepare date histogram for ${this.column}:`, error);
      return this;
    }
  }

  /**
   * Detect the temporal data type based on field metadata
   * @returns {string} 'DATE', 'TIMESTAMP', 'TIME', 'INTERVAL'
   */
  detectTemporalType() {
    if (!this.field) return 'DATE'; // Default fallback

    // Check DuckDB type from mock field (preferred)
    if (this.field.duckdbType && typeof this.field.duckdbType === 'string') {
      const typeStr = this.field.duckdbType.toUpperCase();
      if (typeStr.includes('TIMESTAMP')) return 'TIMESTAMP';
      if (typeStr.includes('INTERVAL')) return 'INTERVAL';
      if (typeStr.includes('TIME') && !typeStr.includes('TIMESTAMP')) return 'TIME';
      if (typeStr.includes('DATE')) return 'DATE';
    }

    // Check field type string (from DuckDB schema)
    if (this.field.type && typeof this.field.type === 'string') {
      const typeStr = this.field.type.toUpperCase();
      if (typeStr.includes('TIMESTAMP')) return 'TIMESTAMP';
      if (typeStr.includes('INTERVAL')) return 'INTERVAL';
      if (typeStr.includes('TIME') && !typeStr.includes('TIMESTAMP')) return 'TIME';
      if (typeStr.includes('DATE')) return 'DATE';
    }

    // Default to DATE for temporal fields
    return 'DATE';
  }

  /**
   * Calculate optimal interval for DATE/TIMESTAMP binning
   * @param {*} minVal - Minimum date value
   * @param {*} maxVal - Maximum date value
   * @returns {Object} Interval configuration
   */
  calculateOptimalInterval(minVal, maxVal) {
    const minDate = new Date(minVal);
    const maxDate = new Date(maxVal);
    const span = maxDate.getTime() - minDate.getTime(); // milliseconds

    // Define interval candidates with their thresholds and metadata
    const intervals = [
      { threshold: 60 * 1000, interval: 'second', duckdbInterval: '1 second', format: '%H:%M:%S' },
      { threshold: 60 * 60 * 1000, interval: 'minute', duckdbInterval: '1 minute', format: '%H:%M' },
      { threshold: 24 * 60 * 60 * 1000, interval: 'hour', duckdbInterval: '1 hour', format: '%b %d %H:00' },
      { threshold: 7 * 24 * 60 * 60 * 1000, interval: 'day', duckdbInterval: '1 day', format: '%b %d' },
      { threshold: 30 * 24 * 60 * 60 * 1000, interval: 'week', duckdbInterval: '7 days', format: 'Week of %b %d' },
      { threshold: 365 * 24 * 60 * 60 * 1000, interval: 'month', duckdbInterval: '1 month', format: '%b %Y' },
      { threshold: 3 * 365 * 24 * 60 * 60 * 1000, interval: 'quarter', duckdbInterval: '3 months', format: 'Q%q %Y' },
      { threshold: Infinity, interval: 'year', duckdbInterval: '1 year', format: '%Y' }
    ];

    // Find the appropriate interval based on span
    const selected = intervals.find(i => span <= i.threshold * this.targetBins);
    return selected || intervals[intervals.length - 1]; // Fallback to year
  }

  /**
   * Generate SQL query for temporal histogram bins including null count
   * @param {Array} filter - Filter expressions to apply
   * @returns {Query} Temporal binning query for this column with null handling
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

    // Generate query based on temporal type
    switch (this.temporalType) {
      case 'TIME':
        return this.generateTimeOfDayQuery(filter);
      case 'INTERVAL':
        return this.generateIntervalQuery(filter);
      case 'DATE':
      case 'TIMESTAMP':
      default:
        return this.generateTemporalBinQuery(filter);
    }
  }

  /**
   * Generate query for TIME-only columns (24-hour distribution with fixed bins)
   */
  generateTimeOfDayQuery(filter) {
    // For TIME columns, create fixed bins from 0-24 hours
    const numBins = Math.min(this.targetBins || 20, 24); // Max 24 bins for hours
    const binWidth = 24 / numBins; // Hours per bin

    // Extract hour and minute as decimal hours (e.g., 14:30 = 14.5)
    const hourExtract = sql`EXTRACT(hour FROM ${this.column}) + EXTRACT(minute FROM ${this.column}) / 60.0 + EXTRACT(second FROM ${this.column}) / 3600.0`;

    const binQuery = Query
      .from(this.table)
      .select({
        x0: sql`LEAST(floor(${hourExtract} / ${binWidth}), ${numBins - 1}) * ${binWidth}`,
        x1: sql`(LEAST(floor(${hourExtract} / ${binWidth}), ${numBins - 1}) + 1) * ${binWidth}`,
        count: count(),
        is_null: sql`false`
      })
      .where(filter)
      .where(sql`${this.column} IS NOT NULL`)
      .groupby('x0', 'x1');

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

    return Query.unionAll(binQuery, nullQuery);
  }

  /**
   * Generate query for INTERVAL columns (duration distribution with fixed bins)
   */
  generateIntervalQuery(filter) {
    const { min, max } = this.fieldInfo;

    // For INTERVAL types, the min/max are likely already in a format we can work with
    // Try to convert to seconds for consistent binning
    let minSeconds, maxSeconds;

    try {
      // Attempt to parse as duration or convert to epoch
      if (typeof min === 'string' || min instanceof Date) {
        minSeconds = new Date(min).getTime() / 1000;
        maxSeconds = new Date(max).getTime() / 1000;
      } else {
        // Already numeric - assume seconds
        minSeconds = min;
        maxSeconds = max;
      }
    } catch (error) {
      // Fallback to extracting from SQL
      console.warn('Could not parse INTERVAL min/max values, using SQL extraction');
      return this.generateIntervalQueryWithSQL(filter);
    }

    const range = maxSeconds - minSeconds;
    const numBins = this.targetBins || 20;
    const binWidth = range / numBins;

    // Handle edge case where range is 0
    if (range <= 0 || binWidth <= 0) {
      const validQuery = Query
        .from(this.table)
        .select({
          x0: sql`${minSeconds}`,
          x1: sql`${minSeconds}`,
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

    // Extract seconds from the interval column
    const columnSeconds = sql`EXTRACT(epoch FROM ${this.column})`;

    const binQuery = Query
      .from(this.table)
      .select({
        x0: sql`LEAST(floor((${columnSeconds} - ${minSeconds}) / ${binWidth}), ${numBins - 1}) * ${binWidth} + ${minSeconds}`,
        x1: sql`(LEAST(floor((${columnSeconds} - ${minSeconds}) / ${binWidth}), ${numBins - 1}) + 1) * ${binWidth} + ${minSeconds}`,
        count: count(),
        is_null: sql`false`
      })
      .where(filter)
      .where(sql`${this.column} IS NOT NULL`)
      .groupby('x0', 'x1');

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

    return Query.unionAll(binQuery, nullQuery);
  }

  /**
   * Fallback method for INTERVAL queries when JavaScript parsing fails
   */
  generateIntervalQueryWithSQL(filter) {
    const numBins = this.targetBins || 20;
    const columnSeconds = sql`EXTRACT(epoch FROM ${this.column})`;

    // Use SQL subqueries to calculate min/max and binning
    const binQuery = Query
      .from(this.table)
      .select({
        x0: sql`LEAST(floor((${columnSeconds} - (SELECT MIN(${columnSeconds}) FROM ${this.table} WHERE ${this.column} IS NOT NULL)) / ((SELECT MAX(${columnSeconds}) - MIN(${columnSeconds}) FROM ${this.table} WHERE ${this.column} IS NOT NULL) / ${numBins})), ${numBins - 1}) * ((SELECT MAX(${columnSeconds}) - MIN(${columnSeconds}) FROM ${this.table} WHERE ${this.column} IS NOT NULL) / ${numBins}) + (SELECT MIN(${columnSeconds}) FROM ${this.table} WHERE ${this.column} IS NOT NULL)`,
        x1: sql`(LEAST(floor((${columnSeconds} - (SELECT MIN(${columnSeconds}) FROM ${this.table} WHERE ${this.column} IS NOT NULL)) / ((SELECT MAX(${columnSeconds}) - MIN(${columnSeconds}) FROM ${this.table} WHERE ${this.column} IS NOT NULL) / ${numBins})), ${numBins - 1}) + 1) * ((SELECT MAX(${columnSeconds}) - MIN(${columnSeconds}) FROM ${this.table} WHERE ${this.column} IS NOT NULL) / ${numBins}) + (SELECT MIN(${columnSeconds}) FROM ${this.table} WHERE ${this.column} IS NOT NULL)`,
        count: count(),
        is_null: sql`false`
      })
      .where(filter)
      .where(sql`${this.column} IS NOT NULL`)
      .groupby('x0', 'x1');

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

    return Query.unionAll(binQuery, nullQuery);
  }

  /**
   * Generate query for DATE/TIMESTAMP columns using fixed binning
   */
  generateTemporalBinQuery(filter) {
    const { min, max } = this.fieldInfo;

    // Convert to epoch seconds in JavaScript to avoid complex SQL
    const minEpoch = new Date(min).getTime() / 1000;
    const maxEpoch = new Date(max).getTime() / 1000;
    const range = maxEpoch - minEpoch;

    // Calculate bin width for target number of bins
    const numBins = this.targetBins || 20;
    const binWidth = range / numBins;

    // Handle edge case where range is 0
    if (range <= 0 || binWidth <= 0) {
      const validQuery = Query
        .from(this.table)
        .select({
          x0: sql`${minEpoch}`,
          x1: sql`${minEpoch}`,
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

    // Extract epoch seconds from the column
    const columnEpoch = sql`EXTRACT(epoch FROM ${this.column}::TIMESTAMP)`;

    // Create binned query using JavaScript-calculated values
    const binQuery = Query
      .from(this.table)
      .select({
        x0: sql`LEAST(floor((${columnEpoch} - ${minEpoch}) / ${binWidth}), ${numBins - 1}) * ${binWidth} + ${minEpoch}`,
        x1: sql`(LEAST(floor((${columnEpoch} - ${minEpoch}) / ${binWidth}), ${numBins - 1}) + 1) * ${binWidth} + ${minEpoch}`,
        count: count(),
        is_null: sql`false`
      })
      .where(filter)
      .where(sql`${this.column} IS NOT NULL`)
      .groupby('x0', 'x1');

    // Query for null values
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

    return Query.unionAll(binQuery, nullQuery);
  }
  
  /**
   * Render date histogram from query result data
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
          // Convert epoch seconds back to dates for temporal bins
          const processedBin = {
            count: bin.count
          };

          if (this.temporalType === 'DATE' || this.temporalType === 'TIMESTAMP') {
            // For DATE/TIMESTAMP, x0 and x1 are epoch seconds - convert to dates
            processedBin.x0 = new Date(bin.x0 * 1000); // Convert seconds to milliseconds
            processedBin.x1 = new Date(bin.x1 * 1000);
          } else {
            // For TIME and INTERVAL, keep as numeric values
            processedBin.x0 = bin.x0;
            processedBin.x1 = bin.x1;
          }

          regularBins.push(processedBin);
        }
      });

      // Fill in missing bins to ensure continuous distribution
      if (regularBins.length > 0 && regularBins.length < (this.targetBins || 20)) {
        regularBins = this.fillMissingBins(regularBins);
      }

      // Pass actual min/max from fieldInfo for accurate display
      const actualRange = this.fieldInfo ? {
        min: this.fieldInfo.min,
        max: this.fieldInfo.max
      } : null;

      // Clear container first
      this.container.innerHTML = '';

      // Use the actual total row count from the base class for accurate proportion calculations
      this.actualTotalCount = this.totalRowCount;

      // Initialize external stats display with total count only
      if (this.statsDisplay) {
        this.statsDisplay.textContent = `${this.actualTotalCount.toLocaleString()} rows`;
      }

      if (this.interactive) {
        this.renderInteractive(regularBins, actualRange, nullCount);
      } else {
        this.renderStatic(regularBins, actualRange, nullCount);
      }

    } catch (error) {
      console.error(`Failed to render date histogram for ${this.column}:`, error);
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
    const histogramSVG = createDateHistogram(regularBins, this.field, {
      width: 125,
      height: 50,
      actualRange,
      nullCount
    });

    this.container.appendChild(histogramSVG);
  }

  /**
   * Render interactive histogram with appropriate scale type
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

    // Determine scale type based on temporal type
    const scaleType = (this.temporalType === 'DATE' || this.temporalType === 'TIMESTAMP') ? 'time' : 'linear';

    // Create interactive histogram with external stats display
    this.currentHistogram = createInteractiveHistogram(
      regularBins,
      this.field,
      {
        width: 125,
        height: 50,
        actualRange,
        nullCount,
        scaleType,  // Pass the appropriate scale type
        statsDisplay: this.statsDisplay,
        totalCount: this.actualTotalCount
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
   * Handle hover events from interactive histogram with context-aware formatting
   * @param {Object|null} hoverData - Hover data or null when not hovering
   */
  handleHover(hoverData) {
    // Update external stats display if available
    if (this.statsDisplay) {
      if (hoverData) {
        const { count, bin, isNull } = hoverData;
        const percentage = (count / this.actualTotalCount) * 100;

        if (isNull) {
          this.statsDisplay.textContent = `∅: ${count.toLocaleString()} rows (${percentage.toFixed(1)}%)`;
        } else if (bin && bin.x0 != null && bin.x1 != null) {
          // Context-aware formatting based on temporal type
          const formattedText = this.formatBinForHover(bin, count, percentage);
          this.statsDisplay.textContent = formattedText;
        } else {
          const label = 'row';
          this.statsDisplay.textContent = `${count.toLocaleString()} ${label}${count === 1 ? '' : 's'} (${percentage.toFixed(1)}%)`;
        }
      } else {
        // Reset to total count only
        this.statsDisplay.textContent = `${this.actualTotalCount.toLocaleString()} rows`;
      }
    }
  }

  /**
   * Format bin information for hover display based on temporal type
   * @param {Object} bin - The bin object with x0, x1, count
   * @param {number} count - Count of items in bin
   * @param {number} percentage - Percentage of total
   * @returns {string} Formatted hover text
   */
  formatBinForHover(bin, count, percentage) {
    switch (this.temporalType) {
      case 'TIME':
        // Format as hour range
        const startHour = Math.floor(bin.x0);
        const endHour = Math.floor(bin.x1);
        return `${startHour}:00 - ${endHour}:00: ${count.toLocaleString()} rows (${percentage.toFixed(1)}%)`;

      case 'INTERVAL':
        // Format as duration range
        const startDuration = this.formatDuration(bin.x0);
        const endDuration = this.formatDuration(bin.x1);
        return `${startDuration} - ${endDuration}: ${count.toLocaleString()} rows (${percentage.toFixed(1)}%)`;

      case 'DATE':
      case 'TIMESTAMP':
      default:
        // bin.x0 and bin.x1 are already Date objects (converted in render method)
        const startDate = bin.x0;
        const endDate = bin.x1;
        const span = endDate - startDate;

        let rangeText;
        if (span < 86400000) { // Less than a day - show time range
          const dateStr = startDate.toLocaleDateString([], { month: 'short', day: 'numeric' });
          const startTime = startDate.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
          const endTime = endDate.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
          rangeText = `${dateStr} ${startTime} - ${endTime}`;
        } else if (span < 2629746000) { // Less than a month - show date range
          const startDateStr = startDate.toLocaleDateString([], { month: 'short', day: 'numeric' });
          const endDateStr = endDate.toLocaleDateString([], { month: 'short', day: 'numeric', year: 'numeric' });
          rangeText = `${startDateStr} - ${endDateStr}`;
        } else { // Month or larger - show month/year range
          const startStr = startDate.toLocaleDateString([], { month: 'short', year: 'numeric' });
          const endStr = endDate.toLocaleDateString([], { month: 'short', year: 'numeric' });
          rangeText = `${startStr} - ${endStr}`;
        }

        return `${rangeText}: ${count.toLocaleString()} rows (${percentage.toFixed(1)}%)`;
    }
  }

  /**
   * Format temporal range based on interval type
   * @param {*} binStart - Start of bin
   * @param {Object} interval - Interval configuration
   * @returns {string} Formatted range string
   */
  formatTemporalRange(binStart, interval) {
    const date = new Date(binStart);

    switch (interval.interval) {
      case 'second':
        return date.toLocaleTimeString();
      case 'minute':
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      case 'hour':
        return `${date.toLocaleDateString([], { month: 'short', day: 'numeric' })} ${date.getHours()}:00`;
      case 'day':
        return date.toLocaleDateString([], { month: 'short', day: 'numeric', year: 'numeric' });
      case 'week':
        return `Week of ${date.toLocaleDateString([], { month: 'short', day: 'numeric' })}`;
      case 'month':
        return date.toLocaleDateString([], { month: 'short', year: 'numeric' });
      case 'quarter':
        const quarter = Math.floor(date.getMonth() / 3) + 1;
        return `Q${quarter} ${date.getFullYear()}`;
      case 'year':
        return date.getFullYear().toString();
      default:
        return date.toLocaleDateString();
    }
  }

  /**
   * Format duration in seconds to human-readable string
   * @param {number} seconds - Duration in seconds
   * @returns {string} Formatted duration
   */
  formatDuration(seconds) {
    if (seconds < 60) {
      return `${seconds.toFixed(1)}s`;
    } else if (seconds < 3600) {
      const minutes = seconds / 60;
      return `${minutes.toFixed(1)}m`;
    } else if (seconds < 86400) {
      const hours = seconds / 3600;
      return `${hours.toFixed(1)}h`;
    } else {
      const days = seconds / 86400;
      return `${days.toFixed(1)}d`;
    }
  }

  /**
   * Fill in missing bins to ensure continuous distribution
   * @param {Array} existingBins - Bins returned from query
   * @returns {Array} Complete set of bins with zeros for missing intervals
   */
  fillMissingBins(existingBins) {
    if (!existingBins || existingBins.length === 0) {
      return existingBins;
    }

    const targetBins = this.targetBins || 20;

    if (this.temporalType === 'DATE' || this.temporalType === 'TIMESTAMP') {
      // For temporal data, calculate the full range and create missing bins
      const { min, max } = this.fieldInfo;
      const minEpoch = new Date(min).getTime() / 1000;
      const maxEpoch = new Date(max).getTime() / 1000;
      const range = maxEpoch - minEpoch;
      const binWidth = range / targetBins;

      const fullBins = [];
      for (let i = 0; i < targetBins; i++) {
        const x0Epoch = minEpoch + i * binWidth;
        const x1Epoch = minEpoch + (i + 1) * binWidth;
        const x0 = new Date(x0Epoch * 1000);
        const x1 = new Date(x1Epoch * 1000);

        // Find if this bin exists in the data
        const existingBin = existingBins.find(bin => {
          const binX0Time = bin.x0.getTime();
          const expectedX0Time = x0.getTime();
          return Math.abs(binX0Time - expectedX0Time) < 1000; // Within 1 second tolerance
        });

        if (existingBin) {
          fullBins.push(existingBin);
        } else {
          // Create empty bin
          fullBins.push({ x0, x1, count: 0 });
        }
      }
      return fullBins;

    } else if (this.temporalType === 'TIME') {
      // For TIME data, fill 0-24 hour range
      const numBins = Math.min(targetBins, 24);
      const binWidth = 24 / numBins;

      const fullBins = [];
      for (let i = 0; i < numBins; i++) {
        const x0 = i * binWidth;
        const x1 = (i + 1) * binWidth;

        // Find if this bin exists in the data
        const existingBin = existingBins.find(bin =>
          Math.abs(bin.x0 - x0) < 0.1 // Within 0.1 hour tolerance
        );

        if (existingBin) {
          fullBins.push(existingBin);
        } else {
          // Create empty bin
          fullBins.push({ x0, x1, count: 0 });
        }
      }
      return fullBins;

    } else if (this.temporalType === 'INTERVAL') {
      // For INTERVAL data, fill based on min/max seconds
      const { min, max } = this.fieldInfo;
      let minSeconds, maxSeconds;

      try {
        if (typeof min === 'string' || min instanceof Date) {
          minSeconds = new Date(min).getTime() / 1000;
          maxSeconds = new Date(max).getTime() / 1000;
        } else {
          minSeconds = min;
          maxSeconds = max;
        }

        const range = maxSeconds - minSeconds;
        const binWidth = range / targetBins;

        const fullBins = [];
        for (let i = 0; i < targetBins; i++) {
          const x0 = minSeconds + i * binWidth;
          const x1 = minSeconds + (i + 1) * binWidth;

          // Find if this bin exists in the data
          const existingBin = existingBins.find(bin =>
            Math.abs(bin.x0 - x0) < binWidth * 0.1 // Within 10% of bin width tolerance
          );

          if (existingBin) {
            fullBins.push(existingBin);
          } else {
            // Create empty bin
            fullBins.push({ x0, x1, count: 0 });
          }
        }
        return fullBins;
      } catch (error) {
        console.warn('Could not fill missing bins for INTERVAL data:', error);
        return existingBins;
      }
    }

    // Fallback: return existing bins if type is unknown
    return existingBins;
  }

  /**
   * Format date range for summary statistics display
   * @param {*} min - Minimum date value
   * @param {*} max - Maximum date value
   * @returns {string} Formatted date range
   */
  formatDateRange(min, max) {
    if (!min || !max) return '';

    switch (this.temporalType) {
      case 'TIME':
        return 'time of day';

      case 'INTERVAL':
        const minSeconds = new Date(min).getTime() / 1000;
        const maxSeconds = new Date(max).getTime() / 1000;
        const minDuration = this.formatDuration(minSeconds);
        const maxDuration = this.formatDuration(maxSeconds);
        return `${minDuration} - ${maxDuration}`;

      case 'DATE':
      case 'TIMESTAMP':
      default:
        const minDate = new Date(min);
        const maxDate = new Date(max);
        const span = maxDate - minDate;

        if (span < 86400000) { // Less than a day
          return `${minDate.toLocaleDateString()} ${minDate.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} - ${maxDate.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
        } else if (span < 31536000000) { // Less than a year
          return `${minDate.toLocaleDateString([], { month: 'short', day: 'numeric' })} - ${maxDate.toLocaleDateString([], { month: 'short', day: 'numeric', year: 'numeric' })}`;
        } else {
          return `${minDate.getFullYear()} - ${maxDate.getFullYear()}`;
        }
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
             typeStr.includes('TIME') ||
             typeStr.includes('INTERVAL');
    }

    // Check field type string (from DuckDB schema)
    if (field.type && typeof field.type === 'string') {
      const typeStr = field.type.toUpperCase();
      return typeStr.includes('DATE') ||
             typeStr.includes('TIMESTAMP') ||
             typeStr.includes('DATETIME') ||
             typeStr.includes('TIME') ||
             typeStr.includes('INTERVAL');
    }

    // Check Arrow type object
    if (field.type && field.type.typeId) {
      return field.type.typeId === Type.Date ||
             field.type.typeId === Type.Timestamp;
    }

    return false;
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
    return this.temporalType ? this.temporalType.toLowerCase() : 'temporal';
  }

  /**
   * Get display name for this visualization type
   * @returns {string}
   */
  static getDisplayName() {
    return 'Timeline Histogram';
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