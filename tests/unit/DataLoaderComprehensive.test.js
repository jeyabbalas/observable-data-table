import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DataLoader } from '../../src/data/DataLoader.js';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

describe('DataLoader Comprehensive Tests', () => {
  let dataLoader;
  let mockDataTable;
  let mockDb;
  let mockConn;

  beforeEach(() => {
    // Mock DuckDB connection
    mockConn = {
      query: vi.fn().mockResolvedValue({
        toArray: vi.fn().mockReturnValue([{ count: 10 }])
      })
    };

    // Mock DuckDB instance
    mockDb = {
      registerFileText: vi.fn().mockResolvedValue(),
      registerFileBuffer: vi.fn().mockResolvedValue()
    };

    // Mock DataTable
    mockDataTable = {
      log: {
        info: vi.fn(),
        debug: vi.fn(),
        error: vi.fn()
      },
      options: {
        useWorker: false
      },
      db: mockDb,
      conn: mockConn,
      sendToWorker: vi.fn().mockResolvedValue({
        status: 'loaded',
        tableName: 'data',
        schema: {},
        rowCount: 10,
        format: 'csv'
      })
    };

    // Mock DuckDBHelpers
    vi.doMock('../../src/data/DuckDBHelpers.js', () => ({
      detectSchema: vi.fn().mockResolvedValue({
        name: { type: 'string', nullable: false, vizType: 'categorical' },
        age: { type: 'integer', nullable: false, vizType: 'histogram' },
        score: { type: 'double', nullable: false, vizType: 'histogram' },
        active: { type: 'boolean', nullable: false, vizType: 'categorical' }
      }),
      getRowCount: vi.fn().mockResolvedValue(10n)
    }));

    dataLoader = new DataLoader(mockDataTable);
  });

  afterEach(() => {
    vi.clearAllMocks();
    vi.resetModules();
  });

  describe('CSV Loading Tests', () => {
    it('should load CSV from text string in Direct mode', async () => {
      const csvData = 'name,age,city\nAlice,30,NYC\nBob,25,LA';
      
      const result = await dataLoader.loadCSV(csvData, {
        tableName: 'test_table'
      });

      expect(mockDb.registerFileText).toHaveBeenCalledWith('test_table.csv', csvData);
      expect(mockConn.query).toHaveBeenCalledWith(
        expect.stringContaining('CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_csv_auto')
      );
      expect(result.tableName).toBe('test_table');
      expect(result.format).toBe('csv');
    });

    it('should load CSV from ArrayBuffer in Direct mode', async () => {
      const csvText = 'name,age,city\nAlice,30,NYC\nBob,25,LA';
      const csvData = new TextEncoder().encode(csvText);

      const result = await dataLoader.loadCSV(csvData, {
        tableName: 'buffer_test'
      });

      expect(mockDb.registerFileText).toHaveBeenCalledWith('buffer_test.csv', csvText);
      expect(result.tableName).toBe('buffer_test');
    });

    it('should handle different delimiters', async () => {
      const tsvData = 'name\tage\tcity\nAlice\t30\tNYC\nBob\t25\tLA';
      
      await dataLoader.loadCSV(tsvData, {
        tableName: 'tsv_test',
        delimiter: '\t'
      });

      expect(mockConn.query).toHaveBeenCalledWith(
        expect.stringMatching(/delim='\t'/)
      );
    });

    it('should use loadCSV for TSV with tab delimiter', async () => {
      const tsvData = 'name\tage\tcity\nAlice\t30\tNYC';
      const loadCSVSpy = vi.spyOn(dataLoader, 'loadCSV');

      await dataLoader.loadTSV(tsvData);

      expect(loadCSVSpy).toHaveBeenCalledWith(tsvData, {
        delimiter: '\t'
      });
    });

    it('should handle CSV with custom options', async () => {
      const csvData = 'name,age,city\nAlice,30,NYC';
      
      await dataLoader.loadCSV(csvData, {
        tableName: 'custom_table',
        delimiter: ',',
        customOption: 'test'
      });

      expect(mockDb.registerFileText).toHaveBeenCalledWith('custom_table.csv', csvData);
      expect(mockDataTable.log.info).toHaveBeenCalledWith('Loading CSV data into table: custom_table');
    });

    it('should handle CSV loading errors gracefully', async () => {
      const csvData = 'invalid,csv,data';
      mockDb.registerFileText.mockRejectedValue(new Error('File registration failed'));

      await expect(dataLoader.loadCSV(csvData)).rejects.toThrow(
        'Failed to load CSV data: File registration failed'
      );

      expect(mockDataTable.log.error).toHaveBeenCalledWith(
        'DuckDB operation failed:',
        expect.any(Error)
      );
    });

    it('should load CSV in Worker mode', async () => {
      mockDataTable.options.useWorker = true;
      const csvData = 'name,age\nAlice,30\nBob,25';

      const result = await dataLoader.loadCSV(csvData, {
        tableName: 'worker_test'
      });

      expect(mockDataTable.sendToWorker).toHaveBeenCalledWith('load', {
        format: 'csv',
        data: csvData,
        tableName: 'worker_test',
        options: { delimiter: ',' }
      });
      
      expect(result.status).toBe('loaded');
      expect(result.format).toBe('csv');
    });
  });

  describe('JSON Loading Tests', () => {
    it('should load JSON array in Direct mode', async () => {
      const jsonData = JSON.stringify([
        { name: 'Alice', age: 30, city: 'NYC' },
        { name: 'Bob', age: 25, city: 'LA' }
      ]);

      const result = await dataLoader.loadJSON(jsonData, {
        tableName: 'json_test'
      });

      expect(mockDb.registerFileText).toHaveBeenCalledWith('json_test.json', jsonData);
      expect(mockConn.query).toHaveBeenCalledWith(
        expect.stringContaining('CREATE OR REPLACE TABLE json_test AS SELECT * FROM read_json_auto')
      );
      expect(result.tableName).toBe('json_test');
      expect(result.format).toBe('json');
    });

    it('should load JSON from ArrayBuffer in Direct mode', async () => {
      const jsonText = JSON.stringify([{ name: 'Alice', age: 30 }]);
      const jsonData = new TextEncoder().encode(jsonText);

      const result = await dataLoader.loadJSON(jsonData);

      expect(mockDb.registerFileText).toHaveBeenCalledWith('data.json', jsonText);
      expect(result.tableName).toBe('data');
    });

    it('should handle nested JSON objects', async () => {
      const jsonData = JSON.stringify([
        {
          name: 'Alice',
          age: 30,
          address: { street: '123 Main St', city: 'NYC' },
          hobbies: ['reading', 'swimming']
        }
      ]);

      const result = await dataLoader.loadJSON(jsonData, {
        tableName: 'nested_json'
      });

      expect(mockDb.registerFileText).toHaveBeenCalledWith('nested_json.json', jsonData);
      expect(result.format).toBe('json');
    });

    it('should handle JSON loading errors', async () => {
      const jsonData = '{"invalid": json}'; // Invalid JSON
      mockConn.query.mockRejectedValue(new Error('Invalid JSON format'));

      await expect(dataLoader.loadJSON(jsonData)).rejects.toThrow(
        'Failed to load JSON data: Invalid JSON format'
      );
    });

    it('should load JSON in Worker mode', async () => {
      mockDataTable.options.useWorker = true;
      const jsonData = JSON.stringify([{ name: 'Alice', age: 30 }]);

      mockDataTable.sendToWorker.mockResolvedValue({
        status: 'loaded',
        tableName: 'worker_json',
        schema: {},
        rowCount: 1,
        format: 'json'
      });

      const result = await dataLoader.loadJSON(jsonData, {
        tableName: 'worker_json'
      });

      expect(mockDataTable.sendToWorker).toHaveBeenCalledWith('load', {
        format: 'json',
        data: jsonData,
        tableName: 'worker_json',
        options: {}
      });

      expect(result.status).toBe('loaded');
      expect(result.format).toBe('json');
    });
  });

  describe('Parquet Loading Tests', () => {
    it('should load Parquet from Uint8Array in Direct mode', async () => {
      const parquetData = new Uint8Array([80, 65, 82, 49]); // Mock Parquet magic bytes

      const result = await dataLoader.loadParquet(parquetData, {
        tableName: 'parquet_test'
      });

      expect(mockDb.registerFileBuffer).toHaveBeenCalledWith('parquet_test.parquet', parquetData);
      expect(mockConn.query).toHaveBeenCalledWith(
        expect.stringContaining('CREATE OR REPLACE TABLE parquet_test AS SELECT * FROM parquet_scan')
      );
      expect(result.tableName).toBe('parquet_test');
      expect(result.format).toBe('parquet');
    });

    it('should convert ArrayBuffer to Uint8Array', async () => {
      const arrayBuffer = new ArrayBuffer(10);
      const view = new Uint8Array(arrayBuffer);
      view.set([80, 65, 82, 49]); // Mock Parquet magic bytes

      await dataLoader.loadParquet(arrayBuffer, {
        tableName: 'buffer_parquet'
      });

      expect(mockDb.registerFileBuffer).toHaveBeenCalledWith(
        'buffer_parquet.parquet',
        expect.any(Uint8Array)
      );
    });

    it('should handle Parquet loading errors', async () => {
      const parquetData = new Uint8Array([1, 2, 3, 4]); // Invalid data
      mockConn.query.mockRejectedValue(new Error('Invalid Parquet file'));

      await expect(dataLoader.loadParquet(parquetData)).rejects.toThrow(
        'Failed to load Parquet data: Invalid Parquet file'
      );
    });

    it('should load Parquet in Worker mode', async () => {
      mockDataTable.options.useWorker = true;
      const parquetData = new Uint8Array([80, 65, 82, 49]);

      mockDataTable.sendToWorker.mockResolvedValue({
        status: 'loaded',
        tableName: 'worker_parquet',
        schema: {},
        rowCount: 100,
        format: 'parquet'
      });

      const result = await dataLoader.loadParquet(parquetData, {
        tableName: 'worker_parquet'
      });

      expect(mockDataTable.sendToWorker).toHaveBeenCalledWith('load', {
        format: 'parquet',
        data: parquetData,
        tableName: 'worker_parquet',
        options: {}
      });

      expect(result.status).toBe('loaded');
      expect(result.format).toBe('parquet');
    });
  });

  describe('File Loading Integration', () => {
    it('should detect format from filename and load CSV file', async () => {
      const csvContent = 'name,age\nAlice,30\nBob,25';
      const mockFile = {
        name: 'test.csv',
        arrayBuffer: vi.fn().mockResolvedValue(new TextEncoder().encode(csvContent))
      };

      const loadCSVSpy = vi.spyOn(dataLoader, 'loadCSV');

      await dataLoader.loadFile(mockFile);

      expect(loadCSVSpy).toHaveBeenCalledWith(
        expect.any(ArrayBuffer),
        expect.objectContaining({
          filename: 'test.csv'
        })
      );
    });

    it('should detect format from filename and load JSON file', async () => {
      const jsonContent = JSON.stringify([{ name: 'Alice', age: 30 }]);
      const mockFile = {
        name: 'test.json',
        arrayBuffer: vi.fn().mockResolvedValue(new TextEncoder().encode(jsonContent))
      };

      const loadJSONSpy = vi.spyOn(dataLoader, 'loadJSON');

      await dataLoader.loadFile(mockFile);

      expect(loadJSONSpy).toHaveBeenCalledWith(
        expect.any(ArrayBuffer),
        expect.objectContaining({
          filename: 'test.json'
        })
      );
    });

    it('should handle unsupported file formats', async () => {
      const mockFile = {
        name: 'test.xlsx',
        arrayBuffer: vi.fn().mockResolvedValue(new ArrayBuffer(10))
      };

      await expect(dataLoader.loadFile(mockFile)).rejects.toThrow(
        'Unsupported format: xlsx'
      );
    });

    it('should override format detection with explicit format option', async () => {
      const csvContent = 'name,age\nAlice,30';
      const mockFile = {
        name: 'data.txt',
        arrayBuffer: vi.fn().mockResolvedValue(new TextEncoder().encode(csvContent))
      };

      const loadCSVSpy = vi.spyOn(dataLoader, 'loadCSV');

      await dataLoader.loadFile(mockFile, { format: 'csv' });

      expect(loadCSVSpy).toHaveBeenCalled();
    });
  });

  describe('Error Handling and Edge Cases', () => {
    it('should handle empty data gracefully', async () => {
      const emptyData = '';

      await dataLoader.loadCSV(emptyData);

      expect(mockDb.registerFileText).toHaveBeenCalledWith('data.csv', '');
    });

    it('should handle DuckDB not initialized error', async () => {
      mockDataTable.db = null;
      mockDataTable.conn = null;

      await expect(dataLoader.loadCSV('test')).rejects.toThrow(
        'DuckDB not properly initialized in direct mode'
      );
    });

    it('should handle worker not available in worker mode', async () => {
      mockDataTable.options.useWorker = true;
      mockDataTable.sendToWorker.mockRejectedValue(new Error('Worker not available'));

      await expect(dataLoader.loadCSV('test')).rejects.toThrow('Worker not available');
    });

    it('should handle very large files efficiently', async () => {
      // Create large CSV data
      const largeCSV = 'name,value\n' + Array.from({ length: 10000 }, (_, i) => 
        `User${i},${Math.random()}`
      ).join('\n');

      const startTime = performance.now();
      await dataLoader.loadCSV(largeCSV);
      const endTime = performance.now();

      expect(endTime - startTime).toBeLessThan(1000); // Should process quickly
      expect(mockDb.registerFileText).toHaveBeenCalledWith('data.csv', largeCSV);
    });
  });

  describe('Schema Detection Integration', () => {
    it('should return detected schema and row count', async () => {
      const { detectSchema, getRowCount } = await import('../../src/data/DuckDBHelpers.js');
      
      detectSchema.mockResolvedValue({
        name: { type: 'string', nullable: false },
        age: { type: 'integer', nullable: false }
      });
      getRowCount.mockResolvedValue(100n);

      const result = await dataLoader.loadCSV('name,age\nAlice,30');

      expect(result.schema).toEqual({
        name: { type: 'string', nullable: false },
        age: { type: 'integer', nullable: false }
      });
      expect(result.rowCount).toBe(100n);
    });

    it('should call DuckDBHelpers with correct parameters', async () => {
      const { detectSchema, getRowCount } = await import('../../src/data/DuckDBHelpers.js');

      await dataLoader.loadJSON('[]', { tableName: 'json_table' });

      expect(detectSchema).toHaveBeenCalledWith(mockConn, 'json_table');
      expect(getRowCount).toHaveBeenCalledWith(mockConn, 'json_table');
    });
  });

  describe('Data Profile Integration', () => {
    it('should provide data profiling capabilities', async () => {
      const mockProfile = {
        table: {
          tableName: 'data',
          rowCount: 100,
          sampleData: [{ name: 'Alice', age: 30 }]
        },
        schema: {
          name: { type: 'string', vizType: 'categorical' },
          age: { type: 'integer', vizType: 'histogram' }
        },
        columns: {
          name: { distinctCount: 50 },
          age: { minValue: 18, maxValue: 65 }
        }
      };

      mockDataTable.conn = mockConn;
      
      vi.doMock('../../src/data/DuckDBHelpers.js', () => ({
        getDataProfile: vi.fn().mockResolvedValue(mockProfile)
      }));

      const profile = await dataLoader.getDataProfile('data');

      expect(profile).toEqual(mockProfile);
    });

    it('should handle data profiling errors', async () => {
      mockDataTable.conn = null;

      await expect(dataLoader.getDataProfile('test')).rejects.toThrow(
        'No DuckDB connection available'
      );
    });
  });
});