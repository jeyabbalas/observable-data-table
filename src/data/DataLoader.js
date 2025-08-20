// DataLoader for handling various data formats
import { detectSchema, getRowCount, getTableInfo } from './DuckDBHelpers.js';

export class DataLoader {
  constructor(dataTable) {
    this.dataTable = dataTable;
    this.supportedFormats = new Map([
      ['csv', this.loadCSV.bind(this)],
      ['tsv', this.loadTSV.bind(this)],
      ['json', this.loadJSON.bind(this)],
      ['parquet', this.loadParquet.bind(this)]
    ]);
  }
  
  async load(source, options = {}) {
    // Detect source type
    if (source instanceof File) {
      return this.loadFile(source, options);
    } else if (typeof source === 'string') {
      if (source.startsWith('http://') || source.startsWith('https://')) {
        return this.loadURL(source, options);
      } else {
        // Assume it's raw data
        return this.loadRawData(source, options);
      }
    } else if (source instanceof ArrayBuffer) {
      return this.loadArrayBuffer(source, options);
    }
    
    throw new Error('Unsupported data source type');
  }
  
  async loadFile(file, options = {}) {
    const format = options.format || this.detectFormat(file.name);
    const arrayBuffer = await file.arrayBuffer();
    
    this.dataTable.log.info(`Loading ${format} file: ${file.name}`);
    
    const loader = this.supportedFormats.get(format);
    if (!loader) {
      throw new Error(`Unsupported format: ${format}`);
    }
    
    return loader(arrayBuffer, { ...options, filename: file.name });
  }
  
  detectFormat(path) {
    const extension = path.split('.').pop().toLowerCase();
    const formatMap = {
      'csv': 'csv',
      'tsv': 'tsv', 
      'json': 'json',
      'parquet': 'parquet'
    };
    
    return formatMap[extension] || 'csv';
  }
  
  async loadCSV(data, options = {}) {
    const tableName = options.tableName || 'data';
    const delimiter = options.delimiter || ',';
    
    this.dataTable.log.info(`Loading CSV data into table: ${tableName}`);
    
    // Both worker and direct modes use the same connection now
    if (!this.dataTable.db || !this.dataTable.conn) {
      throw new Error('DuckDB not properly initialized');
    }
    
    const text = typeof data === 'string' ? data : new TextDecoder().decode(data);
    
    try {
      // Register the CSV data as a file in DuckDB's virtual filesystem
      const fileName = `${tableName}.csv`;
      this.dataTable.log.debug(`Registering CSV file: ${fileName} (${text.length} characters)`);
      await this.dataTable.db.registerFileText(fileName, text);
      
      // Use DuckDB's read_csv_auto for automatic schema detection
      const sql = `CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM read_csv_auto('${fileName}', delim='${delimiter}', header=true, auto_detect=true, sample_size=1000)`;
      
      this.dataTable.log.debug(`Executing SQL: ${sql}`);
      await this.dataTable.conn.query(sql);
      
    } catch (duckdbError) {
      this.dataTable.log.error('DuckDB operation failed:', duckdbError);
      throw new Error(`Failed to load CSV data: ${duckdbError.message}`);
    }
    
    // Get schema information using DuckDBHelpers
    const schema = await detectSchema(this.dataTable.conn, tableName);
    const rowCount = await getRowCount(this.dataTable.conn, tableName);
    
    this.dataTable.log.info(`CSV loaded: ${rowCount} rows, ${Object.keys(schema).length} columns`);
    
    return {
      tableName,
      schema,
      rowCount,
      format: 'csv'
    };
  }
  
  async loadTSV(data, options = {}) {
    return this.loadCSV(data, { ...options, delimiter: '\t' });
  }
  
  async loadJSON(data, options = {}) {
    const tableName = options.tableName || 'data';
    
    this.dataTable.log.info(`Loading JSON data into table: ${tableName}`);
    
    // Both worker and direct modes use the same connection now
    if (!this.dataTable.db || !this.dataTable.conn) {
      throw new Error('DuckDB not properly initialized');
    }
    
    const text = typeof data === 'string' ? data : new TextDecoder().decode(data);
    
    try {
      // Register the JSON data as a file in DuckDB's virtual filesystem
      const fileName = `${tableName}.json`;
      this.dataTable.log.debug(`Registering JSON file: ${fileName} (${text.length} characters)`);
      await this.dataTable.db.registerFileText(fileName, text);
      
      // Use DuckDB's read_json_auto for automatic schema detection
      const sql = `CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM read_json_auto('${fileName}')`;
      
      this.dataTable.log.debug(`Executing SQL: ${sql}`);
      await this.dataTable.conn.query(sql);
      
    } catch (duckdbError) {
      this.dataTable.log.error('DuckDB operation failed:', duckdbError);
      throw new Error(`Failed to load JSON data: ${duckdbError.message}`);
    }
    
    // Get schema information using DuckDBHelpers
    const schema = await detectSchema(this.dataTable.conn, tableName);
    const rowCount = await getRowCount(this.dataTable.conn, tableName);
    
    this.dataTable.log.info(`JSON loaded: ${rowCount} rows, ${Object.keys(schema).length} columns`);
    
    return {
      tableName,
      schema,
      rowCount,
      format: 'json'
    };
  }
  
  async loadParquet(data, options = {}) {
    const tableName = options.tableName || 'data';
    
    this.dataTable.log.info(`Loading Parquet data into table: ${tableName}`);
    
    // Both worker and direct modes use the same connection now
    if (!this.dataTable.db || !this.dataTable.conn) {
      throw new Error('DuckDB not properly initialized');
    }
    
    try {
      // Register the Parquet data as a file in DuckDB's virtual filesystem
      const fileName = `${tableName}.parquet`;
      
      // Convert data to Uint8Array if it's an ArrayBuffer
      const uint8Data = data instanceof ArrayBuffer ? new Uint8Array(data) : data;
      
      this.dataTable.log.debug(`Registering Parquet file: ${fileName} (${uint8Data.length} bytes)`);
      await this.dataTable.db.registerFileBuffer(fileName, uint8Data);
      
      // Use DuckDB's parquet_scan
      const sql = `CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM parquet_scan('${fileName}')`;
      
      this.dataTable.log.debug(`Executing SQL: ${sql}`);
      await this.dataTable.conn.query(sql);
      
    } catch (duckdbError) {
      this.dataTable.log.error('DuckDB operation failed:', duckdbError);
      throw new Error(`Failed to load Parquet data: ${duckdbError.message}`);
    }
    
    // Get schema information using DuckDBHelpers
    const schema = await detectSchema(this.dataTable.conn, tableName);
    const rowCount = await getRowCount(this.dataTable.conn, tableName);
    
    this.dataTable.log.info(`Parquet loaded: ${rowCount} rows, ${Object.keys(schema).length} columns`);
    
    return {
      tableName,
      schema,
      rowCount,
      format: 'parquet'
    };
  }
  
  async loadURL(url, options = {}) {
    // TODO: Implement URL loading
    console.log('URL loading - Coming soon!');
    return { tableName: 'data', schema: {} };
  }
  
  async loadRawData(data, options = {}) {
    // TODO: Implement raw data loading
    console.log('Raw data loading - Coming soon!');
    return { tableName: 'data', schema: {} };
  }
  
  async loadArrayBuffer(buffer, options = {}) {
    // TODO: Implement ArrayBuffer loading
    console.log('ArrayBuffer loading - Coming soon!');
    return { tableName: 'data', schema: {} };
  }
  
  /**
   * Get comprehensive data profile for the loaded table
   * @param {string} tableName - Name of the table to profile
   * @returns {Object} Complete data profile including schema and statistics
   */
  async getDataProfile(tableName) {
    if (!this.dataTable.conn) {
      throw new Error('No DuckDB connection available');
    }
    
    const { getDataProfile } = await import('./DuckDBHelpers.js');
    return getDataProfile(this.dataTable.conn, tableName);
  }
}