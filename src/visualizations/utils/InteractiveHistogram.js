import * as d3 from 'd3';

/**
 * Configuration options for interactive histogram rendering
 */
const DEFAULT_OPTIONS = {
  width: 125,
  height: 50,                  // Increased further for x-axis labels
  marginTop: 2,                // Back to original margin
  marginRight: 4,
  marginBottom: 16,            // More space for x-axis labels
  marginLeft: 4,
  fillColor: '#2563eb',        // Blue-600
  backgroundColor: '#d1d5db',   // Gray-300
  textColor: '#6b7280',        // Gray-500
  nullColor: '#f59e0b',        // Amber-500 (gold for nulls)
  nullHoverColor: '#fbbf24',   // Amber-400 (brighter gold for hover)
  barPadding: 1,               // Padding between bars in pixels
  selectionColor: '#1d4ed8',   // Darker blue for selection
  fadeOpacity: 0.3,            // Opacity for non-selected elements
  brushDebounce: 50            // Debounce delay for brush events in ms
};

/**
 * Create an interactive histogram visualization with D3.js brush and hover
 * @param {Array} bins - Array of bin objects with x0, x1, count properties
 * @param {Object} field - Arrow field metadata
 * @param {Object} options - Rendering options
 * @param {Function} onSelectionChange - Callback for selection changes
 * @param {Function} onHover - Callback for hover events
 * @returns {Object} The interactive histogram with methods
 */
export function createInteractiveHistogram(bins, field, options = {}, onSelectionChange = null, onHover = null) {
  const opts = { ...DEFAULT_OPTIONS, ...options };
  const nullCount = options.nullCount || 0;
  const statsDisplay = options.statsDisplay || null;  // External stats display element
  const totalCount = options.totalCount || (bins.reduce((sum, bin) => sum + bin.count, 0) + nullCount);
  const scaleType = options.scaleType || 'linear';  // 'linear', 'time'
  
  if (!bins || bins.length === 0) {
    if (nullCount > 0) {
      return createNullOnlyInteractiveHistogram(nullCount, totalCount, opts, onSelectionChange, onHover);
    }
    return createEmptyInteractiveHistogram(opts);
  }
  
  // Calculate dimensions
  const nullBarWidth = nullCount > 0 ? 5 : 0;
  const nullBarGap = nullCount > 0 ? 4 : 0;
  const totalNullSpace = nullBarWidth + nullBarGap;
  
  // Calculate scales based on scale type
  let xScale, xDomain;
  const yMax = Math.max(d3.max(bins, d => d.count) || 1, nullCount);

  if (scaleType === 'time') {
    // For time scales, convert to Date objects
    const timeExtent = d3.extent(bins.map(d => [new Date(d.x0), new Date(d.x1)]).flat());
    xDomain = timeExtent;
    xScale = d3.scaleUtc()
      .domain(xDomain)
      .range([opts.marginLeft + totalNullSpace, opts.width - opts.marginRight]);
  } else {
    // Default linear scale
    const xExtent = d3.extent(bins, d => d.x0).concat(d3.extent(bins, d => d.x1));
    xDomain = [Math.min(...xExtent), Math.max(...xExtent)];
    xScale = d3.scaleLinear()
      .domain(xDomain)
      .range([opts.marginLeft + totalNullSpace, opts.width - opts.marginRight]);
  }
    
  const yScale = d3.scaleLinear()
    .domain([0, yMax])
    .range([opts.height - opts.marginBottom, opts.marginTop]);
  
  // Create SVG container
  const svg = d3.create('svg')
    .attr('width', opts.width)
    .attr('height', opts.height)
    .attr('viewBox', [0, 0, opts.width, opts.height])
    .style('max-width', '100%')
    .style('height', 'auto')
    .style('overflow', 'visible');
  
  // State management
  let currentSelection = null;
  let hoveredValue = null;
  let brushDebounceTimer = null;
  
  // Initialize external stats display if provided
  if (statsDisplay) {
    statsDisplay.textContent = `${totalCount.toLocaleString()} rows`;
  }
  
  // X-axis value label variables (will be created after min/max labels for proper z-order)
  let valueLabel, valueLabelBg, valueLabelText;
  
  // Render null bar if present
  let nullBarGroup = null;
  let nullBarRect = null;
  if (nullCount > 0) {
    nullBarGroup = svg.append('g').attr('class', 'null-bars');

    // Background null bar
    nullBarGroup.append('rect')
      .attr('class', 'null-bg')
      .attr('x', opts.marginLeft)
      .attr('width', nullBarWidth)
      .attr('y', yScale(nullCount))
      .attr('height', yScale(0) - yScale(nullCount))
      .attr('fill', opts.backgroundColor);

    // Foreground null bar (gold color) - store reference for hover effects
    nullBarRect = nullBarGroup.append('rect')
      .attr('class', 'null-fg')
      .attr('x', opts.marginLeft)
      .attr('width', nullBarWidth)
      .attr('y', yScale(nullCount))
      .attr('height', yScale(0) - yScale(nullCount))
      .attr('fill', opts.nullColor)
      .style('cursor', 'pointer')
      .on('click', function() {
        if (onSelectionChange) {
          onSelectionChange('null', true);
        }
      });
      
    // Null symbol
    nullBarGroup.append('text')
      .attr('class', 'null-label')
      .attr('x', opts.marginLeft + nullBarWidth / 2)
      .attr('y', opts.height - 2)
      .attr('text-anchor', 'middle')
      .attr('font-family', 'system-ui, sans-serif')
      .attr('font-size', '10px')
      .attr('font-weight', 'normal')
      .attr('fill', opts.nullColor)
      .text('∅');
  }
  
  // Background bars for context
  const backgroundGroup = svg.append('g')
    .attr('class', 'background-bars')
    .attr('fill', opts.backgroundColor);
    
  const backgroundBars = backgroundGroup.selectAll('rect')
    .data(bins)
    .join('rect')
    .attr('x', d => {
      const x0 = scaleType === 'time' ? new Date(d.x0) : d.x0;
      return xScale(x0) + opts.barPadding / 2;
    })
    .attr('width', d => {
      const x0 = scaleType === 'time' ? new Date(d.x0) : d.x0;
      const x1 = scaleType === 'time' ? new Date(d.x1) : d.x1;
      return Math.max(1, xScale(x1) - xScale(x0) - opts.barPadding);
    })
    .attr('y', d => yScale(d.count))
    .attr('height', d => yScale(0) - yScale(d.count));
  
  // Foreground bars for actual data
  const foregroundGroup = svg.append('g')
    .attr('class', 'foreground-bars')
    .attr('fill', opts.fillColor);
    
  const foregroundBars = foregroundGroup.selectAll('rect')
    .data(bins)
    .join('rect')
    .attr('x', d => {
      const x0 = scaleType === 'time' ? new Date(d.x0) : d.x0;
      return xScale(x0) + opts.barPadding / 2;
    })
    .attr('width', d => {
      const x0 = scaleType === 'time' ? new Date(d.x0) : d.x0;
      const x1 = scaleType === 'time' ? new Date(d.x1) : d.x1;
      return Math.max(1, xScale(x1) - xScale(x0) - opts.barPadding);
    })
    .attr('y', d => yScale(d.count))
    .attr('height', d => yScale(0) - yScale(d.count));
  
  // Add min/max labels
  const formatValue = getValueFormatter(field, scaleType);
  let minVal, maxVal;
  
  if (options.actualRange) {
    minVal = options.actualRange.min;
    maxVal = options.actualRange.max;
  } else {
    [minVal, maxVal] = xDomain;
  }

  // Convert to appropriate format for time scales
  if (scaleType === 'time') {
    minVal = minVal instanceof Date ? minVal : new Date(minVal);
    maxVal = maxVal instanceof Date ? maxVal : new Date(maxVal);
  }
  
  // Calculate dynamic font size and format to prevent overlap
  const availableWidth = opts.width - opts.marginLeft - opts.marginRight - totalNullSpace;

  // Create adaptive date formatter based on available space
  const createAdaptiveDateFormatter = (value, size) => {
    if (scaleType !== 'time' || !(value instanceof Date)) {
      return formatValue(value);
    }

    switch (size) {
      case 'large':  // 10px font
        return d3.timeFormat('%b %d, %Y')(value);
      case 'medium': // 9px font
        return d3.timeFormat('%b %d, %y')(value);
      case 'small':  // 8px font
        return d3.timeFormat('%m/%d/%y')(value);
      case 'tiny':   // 7px font
        return d3.timeFormat('%m/%y')(value);
      default:
        return d3.timeFormat('%b %d, %Y')(value);
    }
  };

  // Try different sizes and formats until we find one that fits
  let fontSize = '10px';
  let sizeKey = 'large';
  let minText = createAdaptiveDateFormatter(minVal, sizeKey);
  let maxText = createAdaptiveDateFormatter(maxVal, sizeKey);

  // Better text width estimation (7px per char for dates, 6px for numbers)
  const charWidth = scaleType === 'time' ? 7 : 6;
  let estimatedMinWidth = minText.length * charWidth;
  let estimatedMaxWidth = maxText.length * charWidth;
  let totalTextWidth = estimatedMinWidth + estimatedMaxWidth;

  // Try smaller sizes if text doesn't fit (use 70% of available space for safety)
  if (totalTextWidth > availableWidth * 0.7) {
    fontSize = '9px';
    sizeKey = 'medium';
    minText = createAdaptiveDateFormatter(minVal, sizeKey);
    maxText = createAdaptiveDateFormatter(maxVal, sizeKey);
    estimatedMinWidth = minText.length * (charWidth * 0.9); // 9px is 90% of 10px
    estimatedMaxWidth = maxText.length * (charWidth * 0.9);
    totalTextWidth = estimatedMinWidth + estimatedMaxWidth;
  }

  if (totalTextWidth > availableWidth * 0.7) {
    fontSize = '8px';
    sizeKey = 'small';
    minText = createAdaptiveDateFormatter(minVal, sizeKey);
    maxText = createAdaptiveDateFormatter(maxVal, sizeKey);
    estimatedMinWidth = minText.length * (charWidth * 0.8); // 8px is 80% of 10px
    estimatedMaxWidth = maxText.length * (charWidth * 0.8);
    totalTextWidth = estimatedMinWidth + estimatedMaxWidth;
  }

  if (totalTextWidth > availableWidth * 0.7) {
    fontSize = '7px';
    sizeKey = 'tiny';
    minText = createAdaptiveDateFormatter(minVal, sizeKey);
    maxText = createAdaptiveDateFormatter(maxVal, sizeKey);
  }

  // Min label
  svg.append('text')
    .attr('class', 'min-label')
    .attr('x', opts.marginLeft + totalNullSpace)
    .attr('y', opts.height - 2)
    .attr('text-anchor', 'start')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', fontSize)
    .attr('fill', opts.textColor)
    .text(minText);

  // Max label
  svg.append('text')
    .attr('class', 'max-label')
    .attr('x', opts.width - opts.marginRight)
    .attr('y', opts.height - 2)
    .attr('text-anchor', 'end')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', fontSize)
    .attr('fill', opts.textColor)
    .text(maxText);
  
  // X-axis value label (follows cursor) - created after min/max for proper z-ordering
  valueLabel = svg.append('g').attr('class', 'value-label').style('display', 'none');
  
  valueLabelBg = valueLabel.append('rect')
    .attr('fill', 'white')
    .attr('fill-opacity', 0.8)    // More translucent
    .attr('stroke', '#e5e7eb')     // Lighter border
    .attr('stroke-width', 0.5)     // Thinner border
    .attr('rx', 2)
    .attr('ry', 2);
    
  valueLabelText = valueLabel.append('text')
    .attr('text-anchor', 'middle')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', '10px')
    .attr('font-weight', '500')
    .attr('fill', '#374151')
    .attr('dy', '0.35em');
  
  // Create brush for selection
  const brush = d3.brushX()
    .extent([[opts.marginLeft + totalNullSpace, opts.marginTop], 
             [opts.width - opts.marginRight, opts.height - opts.marginBottom]])
    .on('start', onBrushStart)
    .on('brush', onBrushMove)
    .on('end', onBrushEnd);
  
  const brushGroup = svg.append('g')
    .attr('class', 'brush')
    .call(brush);
  
  // Style the brush
  brushGroup.select('.overlay')
    .style('cursor', 'crosshair');
    
  brushGroup.select('.selection')
    .style('fill', opts.selectionColor)
    .style('fill-opacity', 0.2)
    .style('stroke', opts.selectionColor)
    .style('stroke-width', 1);
  
  // Create invisible overlay for hover detection
  const hoverOverlay = svg.append('rect')
    .attr('class', 'hover-overlay')
    .attr('x', opts.marginLeft + totalNullSpace)
    .attr('y', opts.marginTop)
    .attr('width', opts.width - opts.marginLeft - opts.marginRight - totalNullSpace)
    .attr('height', opts.height - opts.marginTop - opts.marginBottom)
    .attr('fill', 'transparent')
    .style('cursor', 'crosshair')
    .on('mousemove', onMouseMove)
    .on('mouseleave', onMouseLeave);

  // Add hover area for null bar if present
  let nullHoverArea = null;
  if (nullCount > 0) {
    nullHoverArea = svg.append('rect')
      .attr('class', 'null-hover-overlay')
      .attr('x', opts.marginLeft)
      .attr('y', opts.marginTop)
      .attr('width', nullBarWidth)
      .attr('height', opts.height - opts.marginTop - opts.marginBottom)
      .attr('fill', 'transparent')
      .style('cursor', 'pointer')
      .on('mouseenter', function() {
        updateHoverState('null');
      })
      .on('mouseleave', function() {
        updateHoverState(null);
      })
      .on('click', function() {
        if (onSelectionChange) {
          onSelectionChange('null', true);
        }
      });
  }
  
  // Brush event handlers
  function onBrushStart(event) {
    // Clear any existing hover state when starting brush
    updateHoverState(null);
  }
  
  function onBrushMove(event) {
    const selection = event.selection;
    if (selection) {
      const [x0, x1] = selection.map(xScale.invert);
      updateSelectionState([x0, x1], false);
    }
  }
  
  function onBrushEnd(event) {
    const selection = event.selection;
    
    // Clear debounce timer
    if (brushDebounceTimer) {
      clearTimeout(brushDebounceTimer);
    }
    
    // Debounce the final selection update
    brushDebounceTimer = setTimeout(() => {
      if (selection) {
        const [x0, x1] = selection.map(xScale.invert);
        // Ensure proper order
        const range = x0 <= x1 ? [x0, x1] : [x1, x0];
        updateSelectionState(range, true);
      } else {
        // Selection was cleared
        updateSelectionState(null, true);
      }
      brushDebounceTimer = null;
    }, opts.brushDebounce);
  }
  
  // Mouse event handlers for hover
  function onMouseMove(event) {
    if (event.defaultPrevented) return; // Ignore during brush
    
    const [mouseX] = d3.pointer(event, this);
    const dataValue = xScale.invert(mouseX);

    // Clamp to domain and handle time scales
    let clampedValue;
    if (scaleType === 'time') {
      const minTime = xDomain[0].getTime();
      const maxTime = xDomain[1].getTime();
      const valueTime = dataValue.getTime();
      clampedValue = new Date(Math.max(minTime, Math.min(maxTime, valueTime)));
    } else {
      clampedValue = Math.max(xDomain[0], Math.min(xDomain[1], dataValue));
    }
    updateHoverState(clampedValue);
  }
  
  function onMouseLeave(event) {
    updateHoverState(null);
  }
  
  // State update functions
  function updateSelectionState(range, isFinal) {
    currentSelection = range;
    updateVisualSelection();
    
    if (onSelectionChange) {
      onSelectionChange(range, isFinal);
    }
  }
  
  function updateHoverState(value) {
    hoveredValue = value;
    updateVisualHover();

    if (onHover) {
      let hoverData;
      if (value === 'null') {
        // Special case for null bar
        hoverData = { count: nullCount, bin: null, isNull: true };
      } else if (value !== null) {
        hoverData = getHoverData(value);
      } else {
        hoverData = null;
      }
      onHover(hoverData);
    }
  }
  
  function updateVisualSelection() {
    const hasSelection = currentSelection !== null;
    const opacity = hasSelection ? opts.fadeOpacity : 1.0;
    
    // Update foreground bars
    foregroundBars
      .transition()
      .duration(150)
      .attr('opacity', d => {
        if (!hasSelection) return 1.0;
        const [start, end] = currentSelection;
        return (d.x1 > start && d.x0 < end) ? 1.0 : opacity;
      });
    
    // Update null bars if present
    if (nullBarGroup) {
      nullBarGroup.selectAll('rect, text')
        .transition()
        .duration(150)
        .attr('opacity', hasSelection ? opts.fadeOpacity : 1.0);
    }
  }
  
  function updateVisualHover() {
    if (hoveredValue === null) {
      valueLabel.style('display', 'none');
      // Remove highlighting from all bars
      foregroundBars.attr('fill', opts.fillColor);
      // Reset null bar color if it exists
      if (nullBarRect) {
        nullBarRect.attr('fill', opts.nullColor);
      }
      // Update external stats display if provided
      if (statsDisplay) {
        statsDisplay.textContent = `${totalCount.toLocaleString()} rows`;
      }
      return;
    }

    // Check if hovering over null bar
    if (hoveredValue === 'null') {
      valueLabel.style('display', 'none');
      // Remove highlighting from regular bars
      foregroundBars.attr('fill', opts.fillColor);
      // Highlight null bar
      if (nullBarRect) {
        nullBarRect.attr('fill', opts.nullHoverColor);
      }
      // Visual update for null bar hover (onHover is now handled in updateHoverState)
      return;
    }

    const hoverData = getHoverData(hoveredValue);
    if (!hoverData) {
      valueLabel.style('display', 'none');
      // Remove highlighting from all bars
      foregroundBars.attr('fill', opts.fillColor);
      // Reset null bar color if it exists
      if (nullBarRect) {
        nullBarRect.attr('fill', opts.nullColor);
      }
      // Update external stats display if provided
      if (statsDisplay) {
        statsDisplay.textContent = `${totalCount.toLocaleString()} rows`;
      }
      return;
    }

    // Highlight the hovered regular bar
    foregroundBars.attr('fill', d =>
      d === hoverData.bin ? '#60a5fa' : opts.fillColor
    );
    // Reset null bar color when hovering regular bars
    if (nullBarRect) {
      nullBarRect.attr('fill', opts.nullColor);
    }

    // Update external stats display if provided
    if (statsDisplay) {
      const percentage = ((hoverData.count / totalCount) * 100).toFixed(1);
      statsDisplay.textContent = `${hoverData.count.toLocaleString()} rows (${percentage}%)`;
    }
    
    // Update x-axis value label with improved white background
    const formattedValue = formatValue(hoveredValue);
    const mouseX = scaleType === 'time' ?
      xScale(hoveredValue instanceof Date ? hoveredValue : new Date(hoveredValue)) :
      xScale(hoveredValue);
    
    // Position and size the label background with elegant minimal styling
    const textBBox = valueLabelText.text(formattedValue).node().getBBox();
    const padding = 2;  // Minimal padding for elegant appearance
    
    valueLabelBg
      .attr('x', mouseX - textBBox.width / 2 - padding)
      .attr('y', opts.height - opts.marginBottom + 2 - padding)  // Just above axis
      .attr('width', textBBox.width + padding * 2)
      .attr('height', textBBox.height + padding * 2);
    
    valueLabelText
      .attr('x', mouseX)
      .attr('y', opts.height - opts.marginBottom + 2 + textBBox.height / 2);
    
    valueLabel.style('display', 'block');
  }
  
  function getHoverData(value) {
    // Find the bin that contains this value, handling time scales
    const bin = bins.find(b => {
      if (scaleType === 'time') {
        const valueTime = value instanceof Date ? value.getTime() : new Date(value).getTime();
        const x0Time = new Date(b.x0).getTime();
        const x1Time = new Date(b.x1).getTime();
        return valueTime >= x0Time && valueTime < x1Time;
      } else {
        return value >= b.x0 && value < b.x1;
      }
    });

    if (bin) {
      return { count: bin.count, bin };
    }

    // Check if value is very close to a bin edge (within 1% of domain)
    let tolerance;
    if (scaleType === 'time') {
      tolerance = (xDomain[1].getTime() - xDomain[0].getTime()) * 0.01;
      const valueTime = value instanceof Date ? value.getTime() : new Date(value).getTime();
      const nearBin = bins.find(b => {
        const x0Time = new Date(b.x0).getTime();
        const x1Time = new Date(b.x1).getTime();
        return Math.abs(valueTime - x0Time) < tolerance || Math.abs(valueTime - x1Time) < tolerance;
      });
      return nearBin ? { count: nearBin.count, bin: nearBin } : null;
    } else {
      tolerance = (xDomain[1] - xDomain[0]) * 0.01;
      const nearBin = bins.find(b =>
        Math.abs(value - b.x0) < tolerance || Math.abs(value - b.x1) < tolerance
      );
      return nearBin ? { count: nearBin.count, bin: nearBin } : null;
    }
  }
  
  // Public methods
  const instance = {
    // Get the SVG DOM node
    node() {
      return svg.node();
    },
    
    // Get integrated display elements (no external tooltip needed)
    getTooltip() {
      return null; // No external tooltip - display is integrated
    },
    
    // Update with new data
    update(newBins, newOptions = {}) {
      // This could be implemented to update the existing visualization
      // For now, caller should create a new instance
      console.warn('Update not implemented - create new instance');
    },
    
    // Clear current selection
    clearSelection() {
      brushGroup.call(brush.clear);
      updateSelectionState(null, true);
    },
    
    // Get current selection
    getSelection() {
      return currentSelection;
    },
    
    // Set selection programmatically
    setSelection(range) {
      if (range && range.length === 2) {
        const [x0, x1] = range;
        const pixel0 = xScale(x0);
        const pixel1 = xScale(x1);
        brushGroup.call(brush.move, [pixel0, pixel1]);
      } else {
        brushGroup.call(brush.clear);
      }
    },
    
    // Destroy the instance
    destroy() {
      if (brushDebounceTimer) {
        clearTimeout(brushDebounceTimer);
      }
    }
  };
  
  return instance;
}

/**
 * Create interactive histogram with only null values
 */
function createNullOnlyInteractiveHistogram(nullCount, totalCount, opts, onSelectionChange, onHover) {
  const svg = d3.create('svg')
    .attr('width', opts.width)
    .attr('height', opts.height)
    .attr('viewBox', [0, 0, opts.width, opts.height])
    .style('max-width', '100%')
    .style('height', 'auto');
  
  const yScale = d3.scaleLinear()
    .domain([0, nullCount])
    .range([opts.height - opts.marginBottom, opts.marginTop]);
  
  const nullBarWidth = 5;
  const statsDisplay = opts.statsDisplay || null;
  
  // Update external stats display if provided
  if (statsDisplay) {
    statsDisplay.textContent = `${nullCount.toLocaleString()} rows (all null)`;
  }
  
  // Background null bar
  svg.append('rect')
    .attr('x', opts.marginLeft)
    .attr('width', nullBarWidth)
    .attr('y', yScale(nullCount))
    .attr('height', yScale(0) - yScale(nullCount))
    .attr('fill', opts.backgroundColor);
    
  // Foreground null bar with hover functionality
  const nullBar = svg.append('rect')
    .attr('x', opts.marginLeft)
    .attr('width', nullBarWidth)
    .attr('y', yScale(nullCount))
    .attr('height', yScale(0) - yScale(nullCount))
    .attr('fill', opts.nullColor)
    .style('cursor', 'pointer')
    .on('mouseenter', function() {
      if (onHover) {
        onHover({ count: nullCount, bin: null, isNull: true });
      }
    })
    .on('mouseleave', function() {
      if (onHover) {
        onHover(null);
      }
    });
    
  // Null symbol
  svg.append('text')
    .attr('x', opts.marginLeft + nullBarWidth / 2)
    .attr('y', opts.height - 2)
    .attr('text-anchor', 'middle')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', '10px')
    .attr('font-weight', 'normal')
    .attr('fill', opts.nullColor)
    .text('∅');
  
  // Label
  svg.append('text')
    .attr('x', opts.marginLeft + nullBarWidth + 10)
    .attr('y', opts.height / 2)
    .attr('text-anchor', 'start')
    .attr('dominant-baseline', 'middle')
    .attr('font-family', 'system-ui, sans-serif')
    .attr('font-size', '11px')
    .attr('fill', opts.textColor)
    .text('All null');
  
  // Add click handler for null selection
  nullBar.on('click', (event) => {
    if (onSelectionChange) {
      onSelectionChange('null', true);
    }
  });
  
  return {
    node() { return svg.node(); },
    getTooltip() { return null; },
    clearSelection() {},
    getSelection() { return null; },
    setSelection() {},
    destroy() {}
  };
}

/**
 * Create empty interactive histogram placeholder
 */
function createEmptyInteractiveHistogram(opts) {
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
  
  return {
    node() { return svg.node(); },
    getTooltip() { return null; },
    clearSelection() {},
    getSelection() { return null; },
    setSelection() {},
    destroy() {}
  };
}

/**
 * Get appropriate value formatter based on field type and scale type
 */
function getValueFormatter(field, scaleType = 'linear') {
  return (value) => {
    if (value == null) return 'N/A';

    if (scaleType === 'time') {
      // For time scales, format as date
      if (value instanceof Date) {
        return d3.timeFormat('%b %d, %Y')(value);
      } else {
        const date = new Date(value);
        if (isNaN(date)) return 'N/A';
        return d3.timeFormat('%b %d, %Y')(date);
      }
    }

    // Default numeric formatting
    if (isNaN(value)) return 'N/A';
    const format = d3.format('.2s');
    return format(value);
  };
}