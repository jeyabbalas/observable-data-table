// DuckDB Web Worker - Full Implementation
import * as duckdb from '@duckdb/duckdb-wasm';
import { detectSchema, getRowCount, getTableInfo, getDataProfile } from '../data/DuckDBHelpers.js';

console.log('DuckDB Worker loading...');

// Global state
let db = null;
let conn = null;
let initialized = false;

// Bundle configuration for Vite/Rollup compatibility
const MANUAL_BUNDLES = {
  mvp: {
    mainModule: new URL('@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm', import.meta.url).toString(),
    mainWorker: new URL('@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js', import.meta.url).toString(),
  },
  eh: {
    mainModule: new URL('@duckdb/duckdb-wasm/dist/duckdb-eh.wasm', import.meta.url).toString(),
    mainWorker: new URL('@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js', import.meta.url).toString(),
  },
};

// Message handler
self.onmessage = async (event) => {
  const { id, type, payload } = event.data;
  
  try {
    let result;
    
    switch(type) {
      case 'init':
        result = await initializeDuckDB(payload);
        break;
      
      case 'exec':
        result = await executeQuery(payload.sql);
        break;
      
      case 'load':
        result = await loadData(payload);
        break;
      
      case 'export':
        result = await exportData(payload);
        break;
        
      default:
        throw new Error(`Unknown message type: ${type}`);
    }
    
    self.postMessage({ id, success: true, result });
  } catch (error) {
    console.error('Worker error:', error);
    self.postMessage({ 
      id, 
      success: false, 
      error: error.message,
      stack: error.stack
    });
  }
};

/**
 * Initialize DuckDB-WASM with automatic bundle selection
 */
async function initializeDuckDB(options = {}) {
  if (initialized) {
    return { status: 'already_initialized' };
  }
  
  console.log('Initializing DuckDB-WASM in worker...');
  
  try {
    // Try to use JSDelivr bundles first, fallback to manual bundles
    let bundles;
    try {
      bundles = duckdb.getJsDelivrBundles();
    } catch (e) {
      console.warn('JSDelivr bundles not available, using manual bundles');
      bundles = MANUAL_BUNDLES;
    }
    
    // Select the best bundle for this browser
    const bundle = await duckdb.selectBundle(bundles);
    console.log('Selected DuckDB bundle:', bundle.mainModule.includes('eh') ? 'eh' : 'mvp');
    
    // Create logger
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    
    // For workers, we don't need to create a worker instance since we ARE the worker
    // Initialize DuckDB directly
    db = new duckdb.AsyncDuckDB(logger);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    
    // Create a connection
    conn = await db.connect();
    
    // Configure DuckDB for optimal performance
    const config = {
      'max_memory': '512MB',
      'threads': '1', // Single thread in worker context
      'enable_progress_bar': 'false',
      ...options.config
    };
    
    for (const [key, value] of Object.entries(config)) {
      try {
        await conn.query(`SET ${key} = '${value}'`);
      } catch (configError) {
        console.warn(`Failed to set config ${key}=${value}:`, configError.message);
      }
    }
    
    initialized = true;
    console.log('DuckDB-WASM initialized successfully in worker');
    
    return { 
      status: 'initialized',
      version: await getVersion(),
      config: config
    };
    
  } catch (error) {
    console.error('Failed to initialize DuckDB-WASM:', error);
    throw new Error(`DuckDB initialization failed: ${error.message}`);
  }
}

/**
 * Execute a SQL query and return results
 */
async function executeQuery(sql) {
  if (!initialized || !conn) {
    throw new Error('DuckDB not initialized. Call init first.');
  }
  
  console.log('Executing query:', sql.substring(0, 100) + (sql.length > 100 ? '...' : ''));
  
  try {
    const result = await conn.query(sql);
    const data = result.toArray();
    
    console.log(`Query executed successfully, returned ${data.length} rows`);
    return data;
  } catch (error) {
    console.error('Query execution failed:', error);
    throw new Error(`Query failed: ${error.message}`);
  }
}

/**
 * Load data from various formats into DuckDB
 */
async function loadData({ format, data, tableName, options = {} }) {
  if (!initialized || !db || !conn) {
    throw new Error('DuckDB not initialized. Call init first.');
  }
  
  console.log(`Loading ${format} data into table: ${tableName}`);
  
  try {
    const finalTableName = tableName || 'data';
    
    switch(format.toLowerCase()) {
      case 'csv':
        return await loadCSVData(data, finalTableName, options);
      
      case 'tsv':
        return await loadCSVData(data, finalTableName, { ...options, delimiter: '\t' });
      
      case 'json':
        return await loadJSONData(data, finalTableName, options);
      
      case 'parquet':
        return await loadParquetData(data, finalTableName, options);
      
      default:
        throw new Error(`Unsupported format: ${format}`);
    }
  } catch (error) {
    console.error(`Failed to load ${format} data:`, error);
    throw new Error(`Data loading failed: ${error.message}`);
  }
}

/**
 * Load CSV data into DuckDB
 */
async function loadCSVData(data, tableName, options = {}) {
  const delimiter = options.delimiter || ',';
  const hasHeader = options.header !== false; // Default to true
  
  // Register the CSV data as a file in DuckDB's virtual filesystem
  const fileName = `${tableName}.csv`;
  await db.registerFileText(fileName, data);
  
  // Use DuckDB's read_csv_auto for automatic schema detection
  const sql = `
    CREATE OR REPLACE TABLE ${tableName} AS 
    SELECT * FROM read_csv_auto('${fileName}', 
      delim='${delimiter}',
      header=${hasHeader},
      auto_detect=true,
      sample_size=1000
    )
  `;
  
  await conn.query(sql);
  
  // Get schema information using DuckDBHelpers
  const schema = await detectSchema(conn, tableName);
  const rowCount = await getRowCount(conn, tableName);
  
  console.log(`CSV loaded: ${rowCount} rows, ${Object.keys(schema).length} columns`);
  
  return {
    status: 'loaded',
    tableName,
    schema,
    rowCount,
    format: 'csv'
  };
}

/**
 * Load JSON data into DuckDB
 */
async function loadJSONData(data, tableName, options = {}) {
  // Register the JSON data as a file
  const fileName = `${tableName}.json`;
  await db.registerFileText(fileName, data);
  
  // Use DuckDB's read_json_auto for automatic schema detection
  const sql = `
    CREATE OR REPLACE TABLE ${tableName} AS 
    SELECT * FROM read_json_auto('${fileName}')
  `;
  
  await conn.query(sql);
  
  const schema = await detectSchema(conn, tableName);
  const rowCount = await getRowCount(conn, tableName);
  
  console.log(`JSON loaded: ${rowCount} rows, ${Object.keys(schema).length} columns`);
  
  return {
    status: 'loaded',
    tableName,
    schema,
    rowCount,
    format: 'json'
  };
}

/**
 * Load Parquet data into DuckDB
 */
async function loadParquetData(data, tableName, options = {}) {
  // Register the Parquet data as a file (binary data)
  const fileName = `${tableName}.parquet`;
  
  // Convert data to Uint8Array if it's an ArrayBuffer
  const uint8Data = data instanceof ArrayBuffer ? new Uint8Array(data) : data;
  await db.registerFileBuffer(fileName, uint8Data);
  
  // Use DuckDB's parquet_scan
  const sql = `
    CREATE OR REPLACE TABLE ${tableName} AS 
    SELECT * FROM parquet_scan('${fileName}')
  `;
  
  await conn.query(sql);
  
  const schema = await detectSchema(conn, tableName);
  const rowCount = await getRowCount(conn, tableName);
  
  console.log(`Parquet loaded: ${rowCount} rows, ${Object.keys(schema).length} columns`);
  
  return {
    status: 'loaded',
    tableName,
    schema,
    rowCount,
    format: 'parquet'
  };
}

/**
 * Export data from DuckDB
 */
async function exportData(payload) {
  if (!initialized || !conn) {
    throw new Error('DuckDB not initialized. Call init first.');
  }
  
  const { tableName, format = 'csv', query } = payload;
  
  console.log(`Exporting data from ${tableName || 'query'} as ${format}`);
  
  try {
    const sql = query || `SELECT * FROM ${tableName}`;
    const result = await conn.query(sql);
    const data = result.toArray();
    
    return {
      status: 'exported',
      data,
      format,
      rowCount: data.length
    };
  } catch (error) {
    throw new Error(`Export failed: ${error.message}`);
  }
}

/**
 * Get comprehensive data profile for a table in worker context
 */
async function getWorkerDataProfile(tableName) {
  if (!initialized || !conn) {
    throw new Error('DuckDB not initialized. Call init first.');
  }
  
  try {
    return await getDataProfile(conn, tableName);
  } catch (error) {
    throw new Error(`Data profiling failed: ${error.message}`);
  }
}

/**
 * Get DuckDB version
 */
async function getVersion() {
  try {
    const result = await conn.query('SELECT version() as version');
    const rows = result.toArray();
    return rows[0].version;
  } catch (error) {
    return 'unknown';
  }
}

// Log that worker is ready
console.log('DuckDB Worker ready for messages');