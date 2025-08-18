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
    // TODO: Implement CSV loading with DuckDB
    const tableName = options.tableName || 'data';
    console.log('CSV loading - Coming soon!');
    return {
      tableName,
      schema: { name: { type: 'string' }, age: { type: 'number' } }
    };
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
}