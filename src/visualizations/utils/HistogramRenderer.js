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
  nullColor: '#f59e0b'         // Amber-500 (gold for nulls)
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
  
  if (!bins || bins.length === 0) {
    return createEmptyHistogram(opts);
  }
  
  // Calculate scales
  const xExtent = d3.extent(bins, d => d.x0).concat(d3.extent(bins, d => d.x1));
  const xDomain = [Math.min(...xExtent), Math.max(...xExtent)];
  const yMax = d3.max(bins, d => d.count) || 1;
  
  const xScale = d3.scaleLinear()
    .domain(xDomain)
    .range([opts.marginLeft, opts.width - opts.marginRight]);
    
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
  
  // Background bars (for context)
  svg.append('g')
    .attr('fill', opts.backgroundColor)
    .selectAll('rect')
    .data(bins)
    .join('rect')
    .attr('x', d => xScale(d.x0))
    .attr('width', d => Math.max(1, xScale(d.x1) - xScale(d.x0)))
    .attr('y', d => yScale(d.count))
    .attr('height', d => yScale(0) - yScale(d.count));
  
  // Foreground bars (actual data)
  svg.append('g')
    .attr('fill', opts.fillColor)
    .selectAll('rect')
    .data(bins)
    .join('rect')
    .attr('x', d => xScale(d.x0))
    .attr('width', d => Math.max(1, xScale(d.x1) - xScale(d.x0)))
    .attr('y', d => yScale(d.count))
    .attr('height', d => yScale(0) - yScale(d.count));
  
  // Add min/max labels
  const formatValue = getValueFormatter(field);
  const [minVal, maxVal] = xDomain;
  
  // Min label
  svg.append('text')
    .attr('x', opts.marginLeft)
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
 * Get appropriate value formatter based on field type
 * @param {Object} field - Arrow field metadata
 * @returns {Function} Formatter function
 */
function getValueFormatter(field) {
  // For now, use basic numeric formatting
  // TODO: Add date/time formatting for temporal fields
  const format = d3.format('.3s');
  return (value) => {
    if (value == null || isNaN(value)) return 'N/A';
    return format(value);
  };
}