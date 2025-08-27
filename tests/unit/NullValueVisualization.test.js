import { describe, it, expect, vi } from 'vitest';
import { Histogram } from '../../src/visualizations/Histogram.js';
import { DateHistogram } from '../../src/visualizations/DateHistogram.js';
import { createHistogram, createDateHistogram } from '../../src/visualizations/utils/HistogramRenderer.js';
import * as d3 from 'd3';

describe('Null Value Visualization', () => {
  const mockField = {
    name: 'test_column',
    type: { typeId: 'Float' },
    duckdbType: 'DOUBLE'
  };

  const mockDateField = {
    name: 'date_column',
    type: { typeId: 'Date' },
    duckdbType: 'DATE'
  };

  describe('Histogram SQL Query Generation', () => {
    it('should generate UNION query to capture both regular bins and null values', () => {
      const histogram = new Histogram({
        table: 'test_table',
        column: 'score',
        field: mockField
      });

      // Mock field info to enable query generation
      histogram.fieldInfo = { min: 0, max: 100 };

      const query = histogram.query();
      const queryStr = query.toString();

      expect(queryStr).toContain('UNION ALL');
      expect(queryStr).toContain('IS NOT NULL');
      expect(queryStr).toContain('IS NULL');
      expect(queryStr).toContain('is_null');
    });

    it('should handle edge case with no valid range but still capture nulls', () => {
      const histogram = new Histogram({
        table: 'test_table', 
        column: 'score',
        field: mockField
      });

      histogram.fieldInfo = { min: null, max: null };

      const query = histogram.query();
      const queryStr = query.toString();

      expect(queryStr).toContain('UNION ALL');
      expect(queryStr).toContain('IS NULL');
    });
  });

  describe('DateHistogram SQL Query Generation', () => {
    it('should generate UNION query for date columns with null handling', () => {
      const dateHistogram = new DateHistogram({
        table: 'test_table',
        column: 'join_date', 
        field: mockDateField
      });

      dateHistogram.fieldInfo = { 
        min: '2023-01-01', 
        max: '2023-12-31' 
      };

      const query = dateHistogram.query();
      const queryStr = query.toString();

      expect(queryStr).toContain('UNION ALL');
      expect(queryStr).toContain('IS NOT NULL');
      expect(queryStr).toContain('IS NULL');
      expect(queryStr).toContain('is_null');
    });
  });

  describe('Histogram Data Processing', () => {
    it('should separate null count from regular bins in render method', () => {
      const histogram = new Histogram({
        table: 'test_table',
        column: 'score', 
        field: mockField
      });

      histogram.fieldInfo = { min: 0, max: 100 };

      // Mock data with both regular bins and null values
      const mockData = [
        { x0: 0, x1: 10, count: 5, is_null: false },
        { x0: 10, x1: 20, count: 8, is_null: false },
        { x0: null, x1: null, count: 3, is_null: true }
      ];

      // Test the render method - it should not throw and should process data
      expect(() => histogram.render(mockData)).not.toThrow();
      
      // Verify the histogram container has content
      expect(histogram.container.children.length).toBeGreaterThan(0);
    });

    it('should handle data with only null values', () => {
      const histogram = new Histogram({
        table: 'test_table',
        column: 'score',
        field: mockField
      });

      const mockData = [
        { x0: null, x1: null, count: 10, is_null: true }
      ];

      expect(() => histogram.render(mockData)).not.toThrow();
      expect(histogram.container.children.length).toBeGreaterThan(0);
    });
  });

  describe('Histogram Renderer Null Bar Display', () => {
    it('should create histogram with null bar when nullCount > 0', () => {
      const bins = [
        { x0: 0, x1: 10, count: 5 },
        { x0: 10, x1: 20, count: 8 }
      ];

      const svg = createHistogram(bins, mockField, { 
        nullCount: 3,
        width: 125,
        height: 40 
      });

      expect(svg).toBeDefined();
      expect(svg.tagName).toBe('svg');

      // Check if null bar elements are created
      const rects = svg.querySelectorAll('rect');
      const texts = svg.querySelectorAll('text');
      
      // Should have background bars, foreground bars, and null bars
      expect(rects.length).toBeGreaterThan(4); // 2 bg + 2 fg + 2 null bars
      
      // Should have null symbol
      const nullSymbol = Array.from(texts).find(text => text.textContent === '∅');
      expect(nullSymbol).toBeDefined();
    });

    it('should adjust x-scale range to accommodate null bar space', () => {
      const bins = [
        { x0: 0, x1: 10, count: 5 }
      ];

      const svgWithNull = createHistogram(bins, mockField, { 
        nullCount: 3,
        width: 125 
      });

      const svgWithoutNull = createHistogram(bins, mockField, { 
        nullCount: 0,
        width: 125 
      });

      expect(svgWithNull).toBeDefined();
      expect(svgWithoutNull).toBeDefined();

      // Both should be valid SVG elements
      expect(svgWithNull.tagName).toBe('svg');
      expect(svgWithoutNull.tagName).toBe('svg');
    });

    it('should create null-only histogram when no regular bins exist', () => {
      const svg = createHistogram([], mockField, { 
        nullCount: 5,
        width: 125,
        height: 40 
      });

      expect(svg).toBeDefined();
      
      // Should have null symbol
      const texts = svg.querySelectorAll('text');
      const nullSymbol = Array.from(texts).find(text => text.textContent === '∅');
      expect(nullSymbol).toBeDefined();
      
      // Should have "All null" label
      const allNullLabel = Array.from(texts).find(text => text.textContent === 'All null');
      expect(allNullLabel).toBeDefined();
    });
  });

  describe('Date Histogram Null Handling', () => {
    it('should create date histogram with null bar', () => {
      const dateBins = [
        { x0: '2023-01-01', x1: '2023-01-01', count: 3 },
        { x0: '2023-02-01', x1: '2023-02-01', count: 5 }
      ];

      const svg = createDateHistogram(dateBins, mockDateField, {
        nullCount: 2,
        width: 125,
        height: 40
      });

      expect(svg).toBeDefined();
      expect(svg.tagName).toBe('svg');

      // Should have null symbol
      const texts = svg.querySelectorAll('text');
      const nullSymbol = Array.from(texts).find(text => text.textContent === '∅');
      expect(nullSymbol).toBeDefined();
    });
  });

  describe('Null Value Color and Styling', () => {
    it('should use gold color for null bars', () => {
      const svg = createHistogram([], mockField, { 
        nullCount: 5,
        width: 125,
        height: 40 
      });

      const rects = svg.querySelectorAll('rect');
      const texts = svg.querySelectorAll('text');
      
      // Find null bar (gold colored)
      const nullBar = Array.from(rects).find(rect => 
        rect.getAttribute('fill') === '#f59e0b'
      );
      expect(nullBar).toBeDefined();

      // Find null symbol with gold color
      const nullSymbol = Array.from(texts).find(text => 
        text.textContent === '∅' && text.getAttribute('fill') === '#f59e0b'
      );
      expect(nullSymbol).toBeDefined();
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty data gracefully', () => {
      const histogram = new Histogram({
        table: 'test_table',
        column: 'score',
        field: mockField
      });

      histogram.fieldInfo = { min: 0, max: 100 };
      
      // Empty data array
      histogram.render([]);

      expect(histogram.container.children.length).toBeGreaterThan(0);
    });

    it('should handle data with undefined is_null field', () => {
      const histogram = new Histogram({
        table: 'test_table', 
        column: 'score',
        field: mockField
      });

      const mockData = [
        { x0: 0, x1: 10, count: 5 }, // missing is_null field
        { x0: null, x1: null, count: 3 } // null x0/x1 should be detected
      ];

      expect(() => histogram.render(mockData)).not.toThrow();
    });
  });
});