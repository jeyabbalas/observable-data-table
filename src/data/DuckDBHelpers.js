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

    const baseSchema = columns.reduce((schema, col) => {
      schema[col.column_name] = {
        type: col.column_type,
        nullable: col.null === 'YES',
        // Infer visualization type for later use
        vizType: inferVisualizationType(col.column_type)
      };
      return schema;
    }, {});

    // Detect and mark timestamp columns in VARCHAR fields
    try {
      const enhancedSchema = await detectAndMarkTimestampColumns(dbOrConn, tableName, baseSchema);
      return enhancedSchema;
    } catch (timestampError) {
      console.warn(`Timestamp detection failed for table '${tableName}':`, timestampError.message);
      // Return base schema if timestamp detection fails
      return baseSchema;
    }
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

/**
 * Detect if a VARCHAR column contains timestamp data based on patterns
 * @param {Object} dbOrConn - DuckDB database instance or connection
 * @param {string} tableName - Name of the table
 * @param {string} columnName - Name of the column
 * @param {number} sampleSize - Number of rows to sample for pattern detection (default: 10)
 * @returns {Object|null} Timestamp type info if detected, null otherwise
 */
export async function detectTimestampColumn(dbOrConn, tableName, columnName, sampleSize = 10) {
  const conn = dbOrConn.conn || dbOrConn;

  if (!conn || !conn.query) {
    throw new Error('Invalid DuckDB connection provided');
  }

  try {
    // Sample non-null values from the column
    const sampleResult = await conn.query(`
      SELECT ${columnName}
      FROM ${tableName}
      WHERE ${columnName} IS NOT NULL
      LIMIT ${sampleSize}
    `);
    const samples = sampleResult.toArray();

    if (samples.length === 0) {
      return null;
    }

    // Check if values match timestamp patterns
    const timestampInfo = analyzeTimestampPatterns(samples.map(row => row[columnName]));

    if (timestampInfo) {
      // For patterns with milliseconds and 'Z' suffix, skip strptime and use direct casting
      // as DuckDB's strptime has issues with these formats
      const testValue = samples[0][columnName];
      const hasMillisecondsZ = /\.\d{3}Z$/.test(testValue);

      if (hasMillisecondsZ) {
        // Skip strptime for patterns known to cause issues, use direct casting
        try {
          const castResult = await conn.query(`
            SELECT '${testValue}'::TIMESTAMP as cast_timestamp
          `);
          const cast = castResult.toArray();

          if (cast.length > 0 && cast[0].cast_timestamp !== null) {
            return {
              detectedType: timestampInfo.type,
              format: 'auto',
              isTimestamp: true,
              originalType: 'VARCHAR'
            };
          }
        } catch (castError) {
          // Direct casting failed
        }
      } else {
        // Try strptime first for simpler formats
        try {
          const parseResult = await conn.query(`
            SELECT strptime('${testValue}', '${timestampInfo.format}') as parsed_timestamp
          `);
          const parsed = parseResult.toArray();

          if (parsed.length > 0 && parsed[0].parsed_timestamp !== null) {
            return {
              detectedType: timestampInfo.type,
              format: timestampInfo.format,
              isTimestamp: true,
              originalType: 'VARCHAR'
            };
          }
        } catch (parseError) {
          // If strptime fails, try direct casting as fallback
          try {
            const castResult = await conn.query(`
              SELECT '${testValue}'::TIMESTAMP as cast_timestamp
            `);
            const cast = castResult.toArray();

            if (cast.length > 0 && cast[0].cast_timestamp !== null) {
              return {
                detectedType: timestampInfo.type,
                format: 'auto',
                isTimestamp: true,
                originalType: 'VARCHAR'
              };
            }
          } catch (castError) {
            // Neither strptime nor casting worked
          }
        }
      }
    }

    return null;
  } catch (error) {
    console.warn(`Failed to detect timestamp pattern for ${columnName}:`, error.message);
    return null;
  }
}

/**
 * Analyze string values to detect timestamp patterns
 * @param {Array<string>} values - Array of string values to analyze
 * @returns {Object|null} Pattern info if detected, null otherwise
 */
function analyzeTimestampPatterns(values) {
  if (!values || values.length === 0) {
    return null;
  }

  // Define patterns and their corresponding DuckDB types
  const patterns = [
    {
      regex: /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/,
      type: 'TIMESTAMP',
      format: '%Y-%m-%dT%H:%M:%S',
      description: 'ISO 8601 timestamp'
    },
    {
      regex: /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
      type: 'TIMESTAMP',
      format: '%Y-%m-%dT%H:%M:%S.%gZ',
      description: 'ISO 8601 timestamp with milliseconds and Z'
    },
    {
      regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{3})?$/,
      type: 'TIMESTAMP',
      format: '%Y-%m-%d %H:%M:%S',
      description: 'Standard timestamp format'
    },
    {
      regex: /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}$/,
      type: 'TIMESTAMP',
      format: '%Y-%m-%dT%H:%M:%S%z',
      description: 'ISO 8601 with timezone offset'
    }
  ];

  // Check each pattern against all values
  for (const pattern of patterns) {
    let matchCount = 0;
    let totalNonNull = 0;

    for (const value of values) {
      if (value !== null && value !== undefined && value !== '') {
        totalNonNull++;
        if (pattern.regex.test(value)) {
          matchCount++;
        }
      }
    }

    // If most values match this pattern (at least 80%), consider it a match
    if (totalNonNull > 0 && (matchCount / totalNonNull) >= 0.8) {
      return pattern;
    }
  }

  return null;
}

/**
 * Detect all timestamp columns in a table and update schema metadata
 * @param {Object} dbOrConn - DuckDB database instance or connection
 * @param {string} tableName - Name of the table
 * @param {Object} schema - Existing schema object to update
 * @returns {Object} Updated schema with timestamp detection metadata
 */
export async function detectAndMarkTimestampColumns(dbOrConn, tableName, schema) {
  if (!schema) {
    return schema;
  }

  const updatedSchema = { ...schema };

  // Find VARCHAR columns that might contain timestamps
  const varcharColumns = Object.entries(schema).filter(([_, columnInfo]) => {
    const type = typeof columnInfo.type === 'string' ?
      columnInfo.type.toUpperCase() :
      (columnInfo.type?.toString?.().toUpperCase() || '');
    return type.includes('VARCHAR') || type.includes('TEXT') || type.includes('STRING');
  });

  // Check each VARCHAR column for timestamp patterns
  for (const [columnName, columnInfo] of varcharColumns) {
    try {
      const timestampInfo = await detectTimestampColumn(dbOrConn, tableName, columnName);

      if (timestampInfo) {
        // Update the schema metadata to indicate this is a timestamp column
        updatedSchema[columnName] = {
          ...columnInfo,
          detectedTimestampType: timestampInfo.detectedType,
          timestampFormat: timestampInfo.format,
          isDetectedTimestamp: true,
          originalType: columnInfo.type,
          // Update the visualization type
          vizType: 'temporal'
        };

      }
    } catch (error) {
      console.warn(`Failed to check timestamp pattern for column ${columnName}:`, error.message);
    }
  }

  return updatedSchema;
}