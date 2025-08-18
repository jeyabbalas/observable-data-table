// DuckDBHelpers.js - Centralized utilities for DuckDB operations
// Provides schema detection, table introspection, and data profiling utilities

/**
 * Detect and parse table schema from DuckDB
 * @param {Object} dbOrConn - DuckDB database instance or connection
 * @param {string} tableName - Name of the table to analyze
 * @returns {Object} Schema object with column information
 */
export async function detectSchema(dbOrConn, tableName) {
  // Handle both db.conn and direct conn objects
  const conn = dbOrConn.conn || dbOrConn;
  
  if (!conn || !conn.query) {
    throw new Error('Invalid DuckDB connection provided');
  }
  
  try {
    const result = await conn.query(`DESCRIBE ${tableName}`);
    const columns = result.toArray();
    
    return columns.reduce((schema, col) => {
      schema[col.column_name] = {
        type: col.column_type,
        nullable: col.null === 'YES',
        // Infer visualization type for later use
        vizType: inferVisualizationType(col.column_type)
      };
      return schema;
    }, {});
  } catch (error) {
    throw new Error(`Failed to detect schema for table '${tableName}': ${error.message}`);
  }
}

/**
 * Get comprehensive table information including row count and sample data
 * @param {Object} dbOrConn - DuckDB database instance or connection
 * @param {string} tableName - Name of the table to analyze
 * @param {number} sampleSize - Number of sample rows to retrieve (default: 5)
 * @returns {Object} Table information object
 */
export async function getTableInfo(dbOrConn, tableName, sampleSize = 5) {
  const conn = dbOrConn.conn || dbOrConn;
  
  if (!conn || !conn.query) {
    throw new Error('Invalid DuckDB connection provided');
  }
  
  try {
    // Get row count
    const countResult = await conn.query(`SELECT COUNT(*) as count FROM ${tableName}`);
    const rowCount = countResult.toArray()[0].count;
    
    // Get sample data
    const sampleResult = await conn.query(`SELECT * FROM ${tableName} LIMIT ${sampleSize}`);
    const sampleData = sampleResult.toArray();
    
    return {
      tableName,
      rowCount,
      sampleData,
      sampleSize: sampleData.length
    };
  } catch (error) {
    throw new Error(`Failed to get table info for '${tableName}': ${error.message}`);
  }
}

/**
 * Get row count for a table
 * @param {Object} dbOrConn - DuckDB database instance or connection
 * @param {string} tableName - Name of the table
 * @returns {number} Row count
 */
export async function getRowCount(dbOrConn, tableName) {
  const conn = dbOrConn.conn || dbOrConn;
  
  if (!conn || !conn.query) {
    throw new Error('Invalid DuckDB connection provided');
  }
  
  try {
    const result = await conn.query(`SELECT COUNT(*) as count FROM ${tableName}`);
    const rows = result.toArray();
    return rows[0].count;
  } catch (error) {
    throw new Error(`Failed to get row count for table '${tableName}': ${error.message}`);
  }
}

/**
 * Get statistical information for a specific column
 * @param {Object} dbOrConn - DuckDB database instance or connection
 * @param {string} tableName - Name of the table
 * @param {string} columnName - Name of the column to analyze
 * @returns {Object} Column statistics
 */
export async function getColumnStats(dbOrConn, tableName, columnName) {
  const conn = dbOrConn.conn || dbOrConn;
  
  if (!conn || !conn.query) {
    throw new Error('Invalid DuckDB connection provided');
  }
  
  try {
    // First get column type
    const schemaResult = await conn.query(`DESCRIBE ${tableName}`);
    const columns = schemaResult.toArray();
    const column = columns.find(col => col.column_name === columnName);
    
    if (!column) {
      throw new Error(`Column '${columnName}' not found in table '${tableName}'`);
    }
    
    const columnType = column.column_type.toLowerCase();
    const stats = {
      columnName,
      type: column.column_type,
      nullable: column.null === 'YES'
    };
    
    // Numeric statistics
    if (isNumericType(columnType)) {
      const numericResult = await conn.query(`
        SELECT 
          MIN(${columnName}) as min_value,
          MAX(${columnName}) as max_value,
          AVG(${columnName}) as avg_value,
          COUNT(${columnName}) as non_null_count,
          COUNT(*) - COUNT(${columnName}) as null_count
        FROM ${tableName}
      `);
      const numericStats = numericResult.toArray()[0];
      
      Object.assign(stats, {
        minValue: numericStats.min_value,
        maxValue: numericStats.max_value,
        avgValue: numericStats.avg_value,
        nonNullCount: numericStats.non_null_count,
        nullCount: numericStats.null_count
      });
    }
    
    // Categorical statistics
    else if (isTextType(columnType)) {
      const categoricalResult = await conn.query(`
        SELECT 
          COUNT(DISTINCT ${columnName}) as distinct_count,
          COUNT(${columnName}) as non_null_count,
          COUNT(*) - COUNT(${columnName}) as null_count
        FROM ${tableName}
      `);
      const categoricalStats = categoricalResult.toArray()[0];
      
      Object.assign(stats, {
        distinctCount: categoricalStats.distinct_count,
        nonNullCount: categoricalStats.non_null_count,
        nullCount: categoricalStats.null_count
      });
    }
    
    // Date/time statistics
    else if (isTemporalType(columnType)) {
      const temporalResult = await conn.query(`
        SELECT 
          MIN(${columnName}) as min_date,
          MAX(${columnName}) as max_date,
          COUNT(${columnName}) as non_null_count,
          COUNT(*) - COUNT(${columnName}) as null_count
        FROM ${tableName}
      `);
      const temporalStats = temporalResult.toArray()[0];
      
      Object.assign(stats, {
        minDate: temporalStats.min_date,
        maxDate: temporalStats.max_date,
        nonNullCount: temporalStats.non_null_count,
        nullCount: temporalStats.null_count
      });
    }
    
    return stats;
  } catch (error) {
    throw new Error(`Failed to get column stats for '${columnName}': ${error.message}`);
  }
}

/**
 * Get distinct values for a categorical column
 * @param {Object} dbOrConn - DuckDB database instance or connection
 * @param {string} tableName - Name of the table
 * @param {string} columnName - Name of the column
 * @param {number} limit - Maximum number of distinct values to return (default: 50)
 * @returns {Array} Array of distinct values with counts
 */
export async function getDistinctValues(dbOrConn, tableName, columnName, limit = 50) {
  const conn = dbOrConn.conn || dbOrConn;
  
  if (!conn || !conn.query) {
    throw new Error('Invalid DuckDB connection provided');
  }
  
  try {
    const result = await conn.query(`
      SELECT 
        ${columnName} as value,
        COUNT(*) as count
      FROM ${tableName}
      WHERE ${columnName} IS NOT NULL
      GROUP BY ${columnName}
      ORDER BY count DESC
      LIMIT ${limit}
    `);
    
    return result.toArray();
  } catch (error) {
    throw new Error(`Failed to get distinct values for '${columnName}': ${error.message}`);
  }
}

/**
 * Get comprehensive data profile for a table
 * @param {Object} dbOrConn - DuckDB database instance or connection
 * @param {string} tableName - Name of the table to profile
 * @returns {Object} Complete data profile
 */
export async function getDataProfile(dbOrConn, tableName) {
  const conn = dbOrConn.conn || dbOrConn;
  
  if (!conn || !conn.query) {
    throw new Error('Invalid DuckDB connection provided');
  }
  
  try {
    // Get basic table info
    const tableInfo = await getTableInfo(dbOrConn, tableName);
    const schema = await detectSchema(dbOrConn, tableName);
    
    // Get column profiles for each column
    const columnProfiles = {};
    const columnNames = Object.keys(schema);
    
    // Limit detailed profiling to first 20 columns to avoid performance issues
    const columnsToProfile = columnNames.slice(0, 20);
    
    for (const columnName of columnsToProfile) {
      try {
        const columnStats = await getColumnStats(dbOrConn, tableName, columnName);
        
        // Add distinct values for categorical columns
        if (isTextType(schema[columnName].type.toLowerCase()) && tableInfo.rowCount < 10000) {
          const distinctValues = await getDistinctValues(dbOrConn, tableName, columnName, 20);
          columnStats.topValues = distinctValues;
        }
        
        columnProfiles[columnName] = columnStats;
      } catch (error) {
        // If individual column profiling fails, continue with others
        console.warn(`Failed to profile column '${columnName}':`, error.message);
        columnProfiles[columnName] = {
          columnName,
          type: schema[columnName].type,
          error: error.message
        };
      }
    }
    
    return {
      table: tableInfo,
      schema,
      columns: columnProfiles,
      profiledAt: new Date().toISOString(),
      totalColumns: columnNames.length,
      profiledColumns: columnsToProfile.length
    };
  } catch (error) {
    throw new Error(`Failed to generate data profile for table '${tableName}': ${error.message}`);
  }
}

/**
 * Format schema for UI display
 * @param {Object} schema - Schema object from detectSchema
 * @returns {Object} UI-friendly schema format
 */
export function formatSchemaForUI(schema) {
  return Object.entries(schema).map(([columnName, columnInfo]) => ({
    name: columnName,
    type: columnInfo.type,
    nullable: columnInfo.nullable,
    vizType: columnInfo.vizType,
    displayType: formatTypeForDisplay(columnInfo.type)
  }));
}

/**
 * Detect the best visualization type for a column based on its data type
 * @param {string} columnType - DuckDB column type
 * @returns {string} Visualization type (histogram, categorical, temporal, text)
 */
export function detectColumnType(columnType) {
  return inferVisualizationType(columnType);
}

// Helper functions

/**
 * Infer visualization type from DuckDB column type
 * @param {string} columnType - DuckDB column type
 * @returns {string} Visualization type
 */
function inferVisualizationType(columnType) {
  const type = columnType.toLowerCase();
  
  if (isNumericType(type)) {
    return 'histogram';
  } else if (isTemporalType(type)) {
    return 'temporal';
  } else if (isBooleanType(type)) {
    return 'categorical';
  } else {
    return 'categorical';
  }
}

/**
 * Check if column type is numeric
 * @param {string} type - Column type in lowercase
 * @returns {boolean}
 */
function isNumericType(type) {
  return /^(integer|bigint|smallint|tinyint|double|float|real|decimal|numeric)/.test(type);
}

/**
 * Check if column type is text/string
 * @param {string} type - Column type in lowercase
 * @returns {boolean}
 */
function isTextType(type) {
  return /^(varchar|char|text|string)/.test(type);
}

/**
 * Check if column type is temporal (date/time)
 * @param {string} type - Column type in lowercase
 * @returns {boolean}
 */
function isTemporalType(type) {
  return /^(date|time|timestamp|datetime)/.test(type);
}

/**
 * Check if column type is boolean
 * @param {string} type - Column type in lowercase
 * @returns {boolean}
 */
function isBooleanType(type) {
  return /^(boolean|bool)/.test(type);
}

/**
 * Format column type for UI display
 * @param {string} type - Raw DuckDB type
 * @returns {string} Formatted type
 */
function formatTypeForDisplay(type) {
  const typeMap = {
    'INTEGER': 'Int',
    'BIGINT': 'BigInt',
    'DOUBLE': 'Number',
    'VARCHAR': 'Text',
    'DATE': 'Date',
    'TIMESTAMP': 'DateTime',
    'BOOLEAN': 'Boolean'
  };
  
  const upperType = type.toUpperCase();
  return typeMap[upperType] || type;
}