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
  
  generateUniqueTableName(baseFileName = 'data') {
    // Generate a unique table name using timestamp and random suffix
    const timestamp = Date.now();
    const randomSuffix = Math.random().toString(36).substring(2, 8);
    
    // Clean filename for SQL safety
    const cleanName = baseFileName
      .replace(/[^a-zA-Z0-9_]/g, '_')
      .replace(/^[0-9]/, 'table_$&')
      .substring(0, 20);
    
    const tableName = `${cleanName}_${timestamp}_${randomSuffix}`;
    this.dataTable.log.debug(`Generated unique table name: ${tableName}`);
    return tableName;
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
    
    this.dataTable.log.info(`Loading ${format} file: ${file.name} (${(file.size / 1024 / 1024).toFixed(1)}MB)`);
    
    const loader = this.supportedFormats.get(format);
    if (!loader) {
      throw new Error(`Unsupported format: ${format}`);
    }
    
    // Use streaming for large files (>10MB)
    const useStreaming = file.size > 10 * 1024 * 1024 && options.streaming !== false;
    
    if (useStreaming) {
      this.dataTable.log.info('Using streaming mode for large file');
      return this.loadFileStreaming(file, format, loader, options);
    } else {
      const arrayBuffer = await file.arrayBuffer();
      return loader(arrayBuffer, { ...options, filename: file.name });
    }
  }
  
  async loadFileStreaming(file, format, loader, options = {}) {
    const chunkSize = 1024 * 1024; // 1MB chunks
    const totalChunks = Math.ceil(file.size / chunkSize);
    let processedChunks = 0;
    
    this.dataTable.log.info(`Streaming ${file.name} in ${totalChunks} chunks`);
    
    // Progress callback
    const onProgress = options.onProgress || (() => {});
    
    try {
      if (format === 'csv' || format === 'tsv') {
        return this.loadCSVStreaming(file, options, onProgress);
      } else {
        // For non-CSV formats, still load as chunks but process sequentially
        const chunks = [];
        
        for (let i = 0; i < totalChunks; i++) {
          const start = i * chunkSize;
          const end = Math.min(start + chunkSize, file.size);
          const chunk = file.slice(start, end);
          const arrayBuffer = await chunk.arrayBuffer();
          chunks.push(new Uint8Array(arrayBuffer));
          
          processedChunks++;
          onProgress({
            loaded: processedChunks,
            total: totalChunks,
            percent: Math.round((processedChunks / totalChunks) * 100)
          });
        }
        
        // Combine all chunks
        const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
        const combined = new Uint8Array(totalLength);
        let offset = 0;
        
        for (const chunk of chunks) {
          combined.set(chunk, offset);
          offset += chunk.length;
        }
        
        return loader(combined.buffer, { ...options, filename: file.name, streaming: true });
      }
    } catch (error) {
      this.dataTable.log.error('Streaming load failed:', error);
      throw error;
    }
  }
  
  async loadCSVStreaming(file, options = {}, onProgress = () => {}) {
    const baseFileName = options.filename ? 
      options.filename.replace(/\.[^/.]+$/, '') : 
      file.name.replace(/\.[^/.]+$/, '');
    const tableName = options.tableName || this.generateUniqueTableName(baseFileName);
    const delimiter = options.delimiter || (file.name.endsWith('.tsv') ? '\t' : ',');
    
    this.dataTable.log.info(`Streaming CSV data into table: ${tableName}`);
    
    if (!this.dataTable.db || !this.dataTable.conn) {
      throw new Error('DuckDB not properly initialized');
    }
    
    const chunkSize = 1024 * 1024; // 1MB chunks
    const totalChunks = Math.ceil(file.size / chunkSize);
    let processedChunks = 0;
    let header = null;
    let buffer = '';
    let isFirstChunk = true;
    
    try {
      for (let i = 0; i < totalChunks; i++) {
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, file.size);
        const chunk = file.slice(start, end);
        const text = await chunk.text();
        
        buffer += text;
        const lines = buffer.split('\n');
        
        // Keep incomplete line for next chunk
        buffer = lines.pop() || '';
        
        if (isFirstChunk) {
          // Extract header from first chunk
          header = lines[0];
          
          // Create table with proper schema
          const fileName = `${tableName}_chunk_0.csv`;
          const headerData = header + '\n' + (lines.slice(1).join('\n') || '');
          await this.dataTable.db.registerFileText(fileName, headerData);
          
          const sql = `CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM read_csv_auto('${fileName}', delim='${delimiter}', header=true, auto_detect=true, sample_size=1000)`;
          await this.dataTable.conn.query(sql);
          
          isFirstChunk = false;
        } else if (lines.length > 0) {
          // Append subsequent chunks
          const fileName = `${tableName}_chunk_${i}.csv`;
          const chunkData = header + '\n' + lines.join('\n');
          await this.dataTable.db.registerFileText(fileName, chunkData);
          
          const sql = `INSERT INTO ${tableName} SELECT * FROM read_csv_auto('${fileName}', delim='${delimiter}', header=true, auto_detect=true)`;
          await this.dataTable.conn.query(sql);
        }
        
        processedChunks++;
        onProgress({
          loaded: processedChunks,
          total: totalChunks,
          percent: Math.round((processedChunks / totalChunks) * 100),
          stage: 'processing'
        });
      }
      
      // Process any remaining buffer
      if (buffer.trim()) {
        const fileName = `${tableName}_final.csv`;
        const finalData = header + '\n' + buffer;
        await this.dataTable.db.registerFileText(fileName, finalData);
        
        const sql = `INSERT INTO ${tableName} SELECT * FROM read_csv_auto('${fileName}', delim='${delimiter}', header=true, auto_detect=true)`;
        await this.dataTable.conn.query(sql);
      }
      
    } catch (error) {
      this.dataTable.log.error('CSV streaming failed:', error);
      throw new Error(`Failed to stream CSV data: ${error.message}`);
    }
    
    // Get final schema and row count
    const schema = await detectSchema(this.dataTable.conn, tableName);
    const rowCount = await getRowCount(this.dataTable.conn, tableName);
    
    this.dataTable.log.info(`CSV streaming completed: ${rowCount} rows, ${Object.keys(schema).length} columns`);
    
    onProgress({
      loaded: totalChunks,
      total: totalChunks,
      percent: 100,
      stage: 'complete'
    });
    
    return {
      tableName,
      schema,
      rowCount,
      format: 'csv',
      streaming: true
    };
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
    const baseFileName = options.filename ? 
      options.filename.replace(/\.[^/.]+$/, '') : // Remove extension
      'data';
    const tableName = options.tableName || this.generateUniqueTableName(baseFileName);
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
    const baseFileName = options.filename ? 
      options.filename.replace(/\.[^/.]+$/, '') : // Remove extension
      'data';
    const tableName = options.tableName || this.generateUniqueTableName(baseFileName);
    
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
    const baseFileName = options.filename ? 
      options.filename.replace(/\.[^/.]+$/, '') : // Remove extension
      'data';
    const tableName = options.tableName || this.generateUniqueTableName(baseFileName);
    
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
    // URL loading implementation pending for Phase 2
    return { tableName: 'data', schema: {} };
  }
  
  async loadRawData(data, options = {}) {
    // TODO: Implement raw data loading
    // Raw data loading implementation pending for Phase 2
    return { tableName: 'data', schema: {} };
  }
  
  async loadArrayBuffer(buffer, options = {}) {
    // TODO: Implement ArrayBuffer loading
    // ArrayBuffer loading implementation pending for Phase 2
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