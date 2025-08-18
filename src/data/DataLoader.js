// DataLoader for handling various data formats
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
    
    if (this.dataTable.options.useWorker) {
      // Worker mode
      const text = typeof data === 'string' ? data : new TextDecoder().decode(data);
      const result = await this.dataTable.sendToWorker('load', {
        format: 'csv',
        data: text,
        tableName,
        options: { delimiter }
      });
      
      return result;
    } else {
      // Direct mode
      if (!this.dataTable.db || !this.dataTable.conn) {
        throw new Error('DuckDB not properly initialized in direct mode');
      }
      
      const text = typeof data === 'string' ? data : new TextDecoder().decode(data);
      
      // Register the CSV data as a file in DuckDB's virtual filesystem
      const fileName = `${tableName}.csv`;
      await this.dataTable.db.registerFileText(fileName, text);
      
      // Use DuckDB's read_csv_auto for automatic schema detection
      const sql = `
        CREATE OR REPLACE TABLE ${tableName} AS 
        SELECT * FROM read_csv_auto('${fileName}', 
          delim='${delimiter}',
          header=true,
          auto_detect=true,
          sample_size=1000
        )
      `;
      
      await this.dataTable.conn.query(sql);
      
      // Get schema information
      const schema = await this.getTableSchema(tableName);
      const rowCount = await this.getRowCount(tableName);
      
      this.dataTable.log.info(`CSV loaded: ${rowCount} rows, ${Object.keys(schema).length} columns`);
      
      return {
        tableName,
        schema,
        rowCount,
        format: 'csv'
      };
    }
  }
  
  async loadTSV(data, options = {}) {
    return this.loadCSV(data, { ...options, delimiter: '\t' });
  }
  
  async loadJSON(data, options = {}) {
    // TODO: Implement JSON loading
    console.log('JSON loading - Coming soon!');
    return { tableName: 'data', schema: {} };
  }
  
  async loadParquet(data, options = {}) {
    // TODO: Implement Parquet loading
    console.log('Parquet loading - Coming soon!');
    return { tableName: 'data', schema: {} };
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
   * Helper function to get table schema in direct mode
   */
  async getTableSchema(tableName) {
    if (!this.dataTable.conn) {
      throw new Error('No DuckDB connection available');
    }
    
    const result = await this.dataTable.conn.query(`DESCRIBE ${tableName}`);
    const columns = result.toArray();
    
    return columns.reduce((schema, col) => {
      schema[col.column_name] = {
        type: col.column_type,
        nullable: col.null === 'YES'
      };
      return schema;
    }, {});
  }
  
  /**
   * Helper function to get row count in direct mode
   */
  async getRowCount(tableName) {
    if (!this.dataTable.conn) {
      throw new Error('No DuckDB connection available');
    }
    
    const result = await this.dataTable.conn.query(`SELECT COUNT(*) as count FROM ${tableName}`);
    const rows = result.toArray();
    return rows[0].count;
  }
}