import * as d3 from 'd3';

/**
 * Configuration options for histogram rendering
 */
const DEFAULT_OPTIONS = {
  width: 125,
  height: 40,
  marginTop: 2,
  marginRight: 4,
  marginBottom: 12,
  marginLeft: 4,
  fillColor: '#2563eb',        // Blue-600
  backgroundColor: '#d1d5db',   // Gray-300
  textColor: '#6b7280',        // Gray-500
  nullColor: '#f59e0b',        // Amber-500 (gold for nulls)
  barPadding: 1                // Padding between bars in pixels
};

/**
 * Create a static histogram visualization using D3.js
 * @param {Array} bins - Array of bin objects with x0, x1, count properties
 * @param {Object} field - Arrow field metadata
 * @param {Object} options - Rendering options
 * @returns {SVGElement} The rendered histogram as SVG element
 */
export function createHistogram(bins, field, options = {}) {
  const opts = { ...DEFAULT_OPTIONS, ...options };
  const nullCount = options.nullCount || 0;
  
  if (!bins || bins.length === 0) {
    // If only null values, show special histogram
    if (nullCount > 0) {
      return createNullOnlyHistogram(nullCount, opts);
    }
    return createEmptyHistogram(opts);
  }
  
  // Calculate null bar dimensions
  const nullBarWidth = nullCount > 0 ? 5 : 0;
  const nullBarGap = nullCount > 0 ? 4 : 0;
  const totalNullSpace = nullBarWidth + nullBarGap;
  
  // Calculate scales with null bar space adjustment
  const xExtent = d3.extent(bins, d => d.x0).concat(d3.extent(bins, d => d.x1));
  const xDomain = [Math.min(...xExtent), Math.max(...xExtent)];
  const yMax = Math.max(d3.max(bins, d => d.count) || 1, nullCount);
  
  const xScale = d3.scaleLinear()
    .domain(xDomain)
    .range([opts.marginLeft + totalNullSpace, opts.width - opts.marginRight]);
    
  const yScale = d3.scaleLinear()
    .domain([0, yMax])
    .range([opts.height - opts.marginBottom, opts.marginTop]);
  
  // Create SVG
  const svg = d3.create('svg')
    .attr('width', opts.width)
    .attr('height', opts.height)
    .attr('viewBox', [0, 0, opts.width, opts.height])
    .style('max-width', '100%')
    .style('height', 'auto');
  
  // Render null bar if present
  if (nullCount > 0) {
    // Background null bar
    svg.append('rect')
      .attr('x', opts.marginLeft)
      .attr('width', nullBarWidth)
      .attr('y', yScale(nullCount))
      .attr('height', yScale(0) - yScale(nullCount))
      .attr('fill', opts.backgroundColor);
      
    // Foreground null bar (gold color)
    svg.append('rect')
      .attr('x', opts.marginLeft)
      .attr('width', nullBarWidth)
      .attr('y', yScale(nullCount))
      .attr('height', yScale(0) - yScale(nullCount))
      .attr('fill', opts.nullColor);
      
    // Null symbol below bar
    svg.append('text')
      .attr('x', opts.marginLeft + nullBarWidth / 2)
      .attr('y', opts.height - 2)
      .attr('text-anchor', 'middle')
      .attr('font-family', 'system-ui, sans-serif')
      .attr('font-size', '10px')
      .attr('font-weight', 'normal')
      .attr('fill', opts.nullColor)
      .text('∅');
  }
  
  // Background bars for regular histogram (for context)
  svg.append('g')
    .attr('fill', opts.backgroundColor)
    .selectAll('rect')
    .data(bins)
    .join('rect')
    .attr('x', d => xScale(d.x0) + opts.barPadding / 2)
    .attr('width', d => Math.max(1, xScale(d.x1) - xScale(d.x0) - opts.barPadding))
    .attr('y', d => yScale(d.count))
    .attr('height', d => yScale(0) - yScale(d.count));
  
  // Foreground bars for regular histogram (actual data)
  svg.append('g')
    .attr('fill', opts.fillColor)
    .selectAll('rect')
    .data(bins)
    .join('rect')
    .attr('x', d => xScale(d.x0) + opts.barPadding / 2)
    .attr('width', d => Math.max(1, xScale(d.x1) - xScale(d.x0) - opts.barPadding))
    .attr('y', d => yScale(d.count))
    .attr('height', d => yScale(0) - yScale(d.count));
  
  // Add min/max labels using actual data range if available
  const formatValue = getValueFormatter(field);
  let minVal, maxVal;
  
  if (options.actualRange) {
    // Use actual data min/max for accurate display
    minVal = options.actualRange.min;
    maxVal = options.actualRange.max;
  } else {
    // Fallback to bin range
    [minVal, maxVal] = xDomain;
  }
  
  // Min label (positioned after null bar space)
  svg.append('text')
    .attr('x', opts.marginLeft + totalNullSpace)
    .attr('y', opts.height - 2)
    .attr('text-anchor', 'start')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', '10px')
    .attr('fill', opts.textColor)
    .text(formatValue(minVal));
  
  // Max label  
  svg.append('text')
    .attr('x', opts.width - opts.marginRight)
    .attr('y', opts.height - 2)
    .attr('text-anchor', 'end')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', '10px')
    .attr('fill', opts.textColor)
    .text(formatValue(maxVal));
  
  return svg.node();
}

/**
 * Create a date histogram visualization using D3.js
 * @param {Array} bins - Array of bin objects with x0, x1, count properties for dates
 * @param {Object} field - Arrow field metadata
 * @param {Object} options - Rendering options
 * @returns {SVGElement} The rendered date histogram as SVG element
 */
export function createDateHistogram(bins, field, options = {}) {
  const opts = { ...DEFAULT_OPTIONS, ...options };
  const nullCount = options.nullCount || 0;
  
  if (!bins || bins.length === 0) {
    // If only null values, show special histogram
    if (nullCount > 0) {
      return createNullOnlyHistogram(nullCount, opts);
    }
    return createEmptyHistogram(opts);
  }
  
  // Calculate null bar dimensions
  const nullBarWidth = nullCount > 0 ? 5 : 0;
  const nullBarGap = nullCount > 0 ? 4 : 0;
  const totalNullSpace = nullBarWidth + nullBarGap;
  
  // Convert date strings to Date objects and calculate scales
  const dateExtent = d3.extent(bins, d => new Date(d.x0));
  const yMax = Math.max(d3.max(bins, d => d.count) || 1, nullCount);
  
  const xScale = d3.scaleTime()
    .domain(dateExtent)
    .range([opts.marginLeft + totalNullSpace, opts.width - opts.marginRight]);
    
  const yScale = d3.scaleLinear()
    .domain([0, yMax])
    .range([opts.height - opts.marginBottom, opts.marginTop]);
  
  // Calculate bar width based on data temporal spacing
  const availableWidth = opts.width - opts.marginLeft - opts.marginRight - totalNullSpace;
  const barWidth = Math.max(2, availableWidth / bins.length - opts.barPadding);
  
  // Create SVG
  const svg = d3.create('svg')
    .attr('width', opts.width)
    .attr('height', opts.height)
    .attr('viewBox', [0, 0, opts.width, opts.height])
    .style('max-width', '100%')
    .style('height', 'auto');
  
  // Render null bar if present
  if (nullCount > 0) {
    // Background null bar
    svg.append('rect')
      .attr('x', opts.marginLeft)
      .attr('width', nullBarWidth)
      .attr('y', yScale(nullCount))
      .attr('height', yScale(0) - yScale(nullCount))
      .attr('fill', opts.backgroundColor);
      
    // Foreground null bar (gold color)
    svg.append('rect')
      .attr('x', opts.marginLeft)
      .attr('width', nullBarWidth)
      .attr('y', yScale(nullCount))
      .attr('height', yScale(0) - yScale(nullCount))
      .attr('fill', opts.nullColor);
      
    // Null symbol below bar
    svg.append('text')
      .attr('x', opts.marginLeft + nullBarWidth / 2)
      .attr('y', opts.height - 2)
      .attr('text-anchor', 'middle')
      .attr('font-family', 'system-ui, sans-serif')
      .attr('font-size', '10px')
      .attr('font-weight', 'normal')
      .attr('fill', opts.nullColor)
      .text('∅');
  }
  
  // Background bars for date histogram (for context)
  svg.append('g')
    .attr('fill', opts.backgroundColor)
    .selectAll('rect')
    .data(bins)
    .join('rect')
    .attr('x', d => xScale(new Date(d.x0)) - barWidth / 2)
    .attr('width', barWidth)
    .attr('y', d => yScale(d.count))
    .attr('height', d => yScale(0) - yScale(d.count));
  
  // Foreground bars for date histogram (actual data)
  svg.append('g')
    .attr('fill', opts.fillColor)
    .selectAll('rect')
    .data(bins)
    .join('rect')
    .attr('x', d => xScale(new Date(d.x0)) - barWidth / 2)
    .attr('width', barWidth)
    .attr('y', d => yScale(d.count))
    .attr('height', d => yScale(0) - yScale(d.count));
  
  // Add min/max date labels using actual data range if available
  const formatDate = getDateFormatter();
  let minVal, maxVal;
  
  if (options.actualRange) {
    // Use actual data min/max for accurate display
    minVal = new Date(options.actualRange.min);
    maxVal = new Date(options.actualRange.max);
  } else {
    // Fallback to date extent
    [minVal, maxVal] = dateExtent;
  }
  
  // Min label (positioned after null bar space)
  svg.append('text')
    .attr('x', opts.marginLeft + totalNullSpace)
    .attr('y', opts.height - 2)
    .attr('text-anchor', 'start')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', '10px')
    .attr('fill', opts.textColor)
    .text(formatDate(minVal));
  
  // Max label  
  svg.append('text')
    .attr('x', opts.width - opts.marginRight)
    .attr('y', opts.height - 2)
    .attr('text-anchor', 'end')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', '10px')
    .attr('fill', opts.textColor)
    .text(formatDate(maxVal));
  
  return svg.node();
}

/**
 * Create an empty histogram placeholder
 * @param {Object} opts - Rendering options
 * @returns {SVGElement}
 */
function createEmptyHistogram(opts) {
  const svg = d3.create('svg')
    .attr('width', opts.width)
    .attr('height', opts.height)
    .attr('viewBox', [0, 0, opts.width, opts.height])
    .style('max-width', '100%')
    .style('height', 'auto');
  
  svg.append('text')
    .attr('x', opts.width / 2)
    .attr('y', opts.height / 2)
    .attr('text-anchor', 'middle')
    .attr('dominant-baseline', 'middle')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', '11px')
    .attr('fill', opts.textColor)
    .text('No data');
  
  return svg.node();
}

/**
 * Create a histogram with only null values
 * @param {number} nullCount - Number of null values
 * @param {Object} opts - Rendering options
 * @returns {SVGElement}
 */
function createNullOnlyHistogram(nullCount, opts) {
  const nullBarWidth = 5;
  const yScale = d3.scaleLinear()
    .domain([0, nullCount])
    .range([opts.height - opts.marginBottom, opts.marginTop]);
  
  const svg = d3.create('svg')
    .attr('width', opts.width)
    .attr('height', opts.height)
    .attr('viewBox', [0, 0, opts.width, opts.height])
    .style('max-width', '100%')
    .style('height', 'auto');
  
  // Background null bar
  svg.append('rect')
    .attr('x', opts.marginLeft)
    .attr('width', nullBarWidth)
    .attr('y', yScale(nullCount))
    .attr('height', yScale(0) - yScale(nullCount))
    .attr('fill', opts.backgroundColor);
    
  // Foreground null bar (gold color)
  svg.append('rect')
    .attr('x', opts.marginLeft)
    .attr('width', nullBarWidth)
    .attr('y', yScale(nullCount))
    .attr('height', yScale(0) - yScale(nullCount))
    .attr('fill', opts.nullColor);
    
  // Null symbol below bar
  svg.append('text')
    .attr('x', opts.marginLeft + nullBarWidth / 2)
    .attr('y', opts.height - 2)
    .attr('text-anchor', 'middle')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', '10px')
    .attr('font-weight', 'normal')
    .attr('fill', opts.nullColor)
    .text('∅');
  
  // "All null" label 
  svg.append('text')
    .attr('x', opts.marginLeft + nullBarWidth + 10)
    .attr('y', opts.height / 2)
    .attr('text-anchor', 'start')
    .attr('dominant-baseline', 'middle')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', '11px')
    .attr('fill', opts.textColor)
    .text('All null');
  
  return svg.node();
}

/**
 * Get appropriate value formatter based on field type
 * @param {Object} field - Arrow field metadata
 * @returns {Function} Formatter function
 */
function getValueFormatter(field) {
  return (value) => {
    if (value == null || isNaN(value)) return 'N/A';
    
    // Use SI format for all numeric values to handle large numbers compactly
    // .2s provides up to 2 significant digits: 18347 → "18k", 999987786 → "1.0G"
    const format = d3.format('.2s');
    return format(value);
  };
}

/**
 * Check if field represents an integer type
 * @param {Object} field - Arrow field metadata or mock field
 * @returns {boolean}
 */
function isIntegerField(field) {
  if (!field) return false;
  
  // Check DuckDB type from mock field (preferred)
  if (field.duckdbType && typeof field.duckdbType === 'string') {
    const typeStr = field.duckdbType.toUpperCase();
    return typeStr.includes('INTEGER') || 
           typeStr.includes('BIGINT') || 
           typeStr.includes('SMALLINT') || 
           typeStr.includes('TINYINT');
  }
  
  // Check field type string (from DuckDB schema)
  if (field.type && typeof field.type === 'string') {
    const typeStr = field.type.toUpperCase();
    return typeStr.includes('INTEGER') || 
           typeStr.includes('BIGINT') || 
           typeStr.includes('SMALLINT') || 
           typeStr.includes('TINYINT');
  }
  
  // Check Arrow type object
  if (field.type && field.type.typeId) {
    // This would be for actual Arrow types
    return field.type.typeId === 'Int';
  }
  
  return false;
}

/**
 * Get date formatter for temporal values
 * @returns {Function} Date formatter function
 */
function getDateFormatter() {
  return (date) => {
    if (!date || isNaN(date)) return 'N/A';
    
    // Format date as MMM DD or MMM YYYY depending on span
    const now = new Date();
    const year = date.getFullYear();
    
    if (year === now.getFullYear()) {
      // Same year, show month and day
      return d3.timeFormat('%b %d')(date);
    } else {
      // Different year, show month and year
      return d3.timeFormat('%b %Y')(date);
    }
  };
}