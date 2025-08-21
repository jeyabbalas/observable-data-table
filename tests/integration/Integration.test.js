import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

describe('Integration Tests', () => {
  let container;
  let dataTable;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
  });

  afterEach(async () => {
    if (dataTable) {
      await dataTable.destroy();
      dataTable = null;
    }
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
  });

  describe('DataTable Initialization', () => {
    it('should initialize successfully in direct mode', async () => {
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });

      await dataTable.initialize();

      expect(dataTable.performance.mode).toBe('Direct');
      expect(dataTable.coordinator).toBeDefined();
      expect(dataTable.db).toBeDefined();
    }, 30000);

    it('should handle worker mode with fallback', async () => {
      dataTable = new DataTable({
        container,
        useWorker: true,
        logLevel: 'error'
      });

      await dataTable.initialize();

      // Should initialize in some mode (worker or direct fallback)
      expect(dataTable.performance.mode).toMatch(/Worker|Direct/);
      expect(dataTable.coordinator).toBeDefined();
    }, 30000);
  });

  describe('Data Loading Integration', () => {
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      await dataTable.initialize();
    }, 30000);

    it('should load CSV data successfully', async () => {
      const csvPath = join(__dirname, '../fixtures/sample.csv');
      const csvFile = new File([readFileSync(csvPath)], 'sample.csv', {
        type: 'text/csv'
      });

      const result = await dataTable.loadData(csvFile);

      expect(result).toBeDefined();
      expect(dataTable.tableName.value).toMatch(/sample_\d+_\w+/);
      expect(Object.keys(dataTable.schema.value).length).toBeGreaterThan(0);
      expect(dataTable.tableRenderer).toBeDefined();
    }, 30000);

    it('should load JSON data successfully', async () => {
      const jsonPath = join(__dirname, '../fixtures/sample.json');
      const jsonFile = new File([readFileSync(jsonPath)], 'sample.json', {
        type: 'application/json'
      });

      const result = await dataTable.loadData(jsonFile);

      expect(result).toBeDefined();
      expect(dataTable.tableName.value).toMatch(/sample_\d+_\w+/);
      expect(Object.keys(dataTable.schema.value).length).toBeGreaterThan(0);
      expect(dataTable.tableRenderer).toBeDefined();
    }, 30000);
  });

  describe('Mosaic Integration', () => {
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      await dataTable.initialize();

      // Load test data
      const csvPath = join(__dirname, '../fixtures/sample.csv');
      const csvFile = new File([readFileSync(csvPath)], 'sample.csv', {
        type: 'text/csv'
      });
      await dataTable.loadData(csvFile);
    }, 30000);

    it('should create table renderer with Mosaic integration', () => {
      expect(dataTable.tableRenderer).toBeDefined();
      expect(dataTable.tableRenderer.coordinator).toBeDefined();
      expect(dataTable.tableRenderer.table).toMatch(/sample_\d+_\w+/);
    }, 10000);

    it('should handle table renderer queries', async () => {
      const renderer = dataTable.tableRenderer;
      expect(renderer.query()).toBeDefined();
      expect(renderer.orderBy.value).toEqual([]);
      expect(renderer.filters.value).toEqual([]);
    }, 10000);

    it('should clean up resources properly', async () => {
      const renderer = dataTable.tableRenderer;
      expect(renderer).toBeDefined();

      await dataTable.destroy();

      // After cleanup, renderer should be destroyed
      expect(dataTable.tableRenderer).toBeNull();
    }, 10000);
  });

  describe('Error Handling', () => {
    beforeEach(async () => {
      dataTable = new DataTable({
        container,
        useWorker: false,
        logLevel: 'error'
      });
      await dataTable.initialize();
    }, 30000);

    it('should handle invalid file gracefully', async () => {
      const invalidFile = new File(['invalid data'], 'invalid.txt', {
        type: 'text/plain'
      });

      // Should not throw since unknown formats default to CSV
      const result = await dataTable.loadData(invalidFile);
      expect(result).toBeDefined();
      expect(result.format).toBe('csv');
    }, 10000);

    it('should handle missing container gracefully', () => {
      expect(() => {
        new DataTable({ container: null });
      }).not.toThrow();
    }, 5000);
  });
});