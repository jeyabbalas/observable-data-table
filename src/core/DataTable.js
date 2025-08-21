import { Coordinator, wasmConnector } from '@uwdata/mosaic-core';
import { Query } from '@uwdata/mosaic-sql';
import { signal } from '@preact/signals-core';
import { DataLoader } from '../data/DataLoader.js';
import { PersistenceManager } from '../storage/PersistenceManager.js';
import { VersionControl } from '../storage/VersionControl.js';
import { TableRenderer } from './TableRenderer.js';
import { QueryCache } from './QueryCache.js';
import { detectSchema, getDataProfile } from '../data/DuckDBHelpers.js';
import { BatchingConnector } from '../connectors/BatchingConnector.js';

export class DataTable {
  constructor(options = {}) {
    this.options = {
      container: options.container || document.body,
      height: options.height || 500,
      persistSession: options.persistSession || false,
      useWorker: options.useWorker !== false,
      logLevel: options.logLevel || 'info',
      ...options
    };
    
    // Core components
    this.coordinator = new Coordinator();
    this.connector = null;
    this.db = null;
    this.worker = null;
    this.conn = null;
    
    // State management with signals
    this.tableName = signal(null);
    this.schema = signal({});
    this.currentSQL = signal('');
    this.queryHistory = [];
    
    // UI components
    this.container = null;
    this.tableRenderer = null;
    this.visualizations = new Map();
    
    // Managers
    this.dataLoader = new DataLoader(this);
    this.persistenceManager = null;
    this.versionControl = null;
    this.queryCache = new QueryCache({
      ttl: options.cacheTTL || 60000, // 1 minute default
      maxSize: options.cacheMaxSize || 100,
      enabled: options.enableCache !== false
    });
    
    // Performance tracking
    this.performance = {
      initStartTime: null,
      initEndTime: null,
      mode: null
    };
    
    // Memory monitoring
    this.memoryMonitor = {
      enabled: typeof performance !== 'undefined' && performance.memory,
      lastCheck: 0,
      checkInterval: 10000, // Check every 10 seconds
      thresholds: {
        warning: 0.8, // 80% memory usage
        critical: 0.9 // 90% memory usage
      },
      callbacks: new Set()
    };
    
    // Progress event system
    this.progressCallbacks = new Set();
    
    // Setup logging
    this.setupLogging();
  }
  
  setupLogging() {
    const levels = { error: 0, warn: 1, info: 2, debug: 3 };
    const currentLevel = levels[this.options.logLevel] || 2;
    
    this.log = {
      error: (...args) => currentLevel >= 0 && console.error('[DataTable]', ...args),
      warn: (...args) => currentLevel >= 1 && console.warn('[DataTable]', ...args),
      info: (...args) => currentLevel >= 2 && console.log('[DataTable]', ...args),
      debug: (...args) => currentLevel >= 3 && console.debug('[DataTable]', ...args)
    };
  }
  
  
  async initialize() {
    try {
      // Start timing initialization
      this.performance.initStartTime = Date.now();
      
      this.log.info('Initializing DataTable...');
      
      // Setup DuckDB connection
      if (this.options.useWorker) {
        await this.initializeWorker();
        this.performance.mode = 'Worker';
      } else {
        await this.initializeDirect();
        this.performance.mode = 'Direct';
      }
      
      // Setup persistence if enabled
      if (this.options.persistSession) {
        await this.initializePersistence();
      }
      
      // Setup version control
      this.versionControl = new VersionControl({
        strategy: 'hybrid',
        maxCommands: 50,
        maxSnapshots: 3
      });
      
      // Create UI container
      this.createContainer();
      
      // Complete initialization timing
      this.performance.initEndTime = Date.now();
      
      this.log.info('DataTable initialized successfully');
      
      
      return this;
    } catch (error) {
      this.log.error('Failed to initialize:', error);
      throw error;
    }
  }
  
  async initializeDirect() {
    this.log.debug('Initializing direct DuckDB connection...');
    
    try {
      // Track direct mode initialization
      
      // Import DuckDB-WASM for direct mode
      const duckdb = await import('@duckdb/duckdb-wasm');
      
      // Try to use JSDelivr bundles first, fallback to manual bundles  
      let bundles;
      try {
        bundles = duckdb.getJsDelivrBundles();
      } catch (e) {
        this.log.warn('JSDelivr bundles not available, using manual bundles');
        bundles = {
          mvp: {
            mainModule: new URL('@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm', import.meta.url).toString(),
            mainWorker: new URL('@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js', import.meta.url).toString(),
          },
          eh: {
            mainModule: new URL('@duckdb/duckdb-wasm/dist/duckdb-eh.wasm', import.meta.url).toString(),
            mainWorker: new URL('@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js', import.meta.url).toString(),
          },
        };
      }
      
      // Select the best bundle for this browser
      const bundle = await duckdb.selectBundle(bundles);
      
      // Capture bundle type
      this.performance.bundleType = bundle.mainModule.includes('eh') ? 'eh' : 'mvp';
      this.log.debug('Selected DuckDB bundle:', this.performance.bundleType);
      
      // Create logger - use ERROR level to reduce console noise
      const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR);
      
      // Even in "direct" mode, AsyncDuckDB requires a worker
      // Create a worker internally but don't expose worker management to the user
      
      // Create worker using the bundle URL directly or via Blob (browser only)
      let workerUrl = bundle.mainWorker;
      let needsCleanup = false;
      
      // In browsers, try to use Blob to avoid CORS issues
      if (typeof Blob !== 'undefined' && typeof URL !== 'undefined' && URL.createObjectURL) {
        try {
          const workerCode = `importScripts("${bundle.mainWorker}");`;
          const workerBlob = new Blob([workerCode], { type: 'text/javascript' });
          workerUrl = URL.createObjectURL(workerBlob);
          needsCleanup = true;
        } catch (e) {
          // Fallback to direct URL if Blob approach fails
          this.log.debug('Blob worker creation failed, using direct URL:', e.message);
          workerUrl = bundle.mainWorker;
        }
      }
      
      // Create worker instance
      this.worker = new Worker(workerUrl);
      
      // Setup basic error handling for the internal worker
      this.worker.onerror = (error) => {
        this.log.error('Internal worker error in direct mode:', error);
      };
      
      // Initialize DuckDB with the worker (required by AsyncDuckDB)
      this.db = new duckdb.AsyncDuckDB(logger, this.worker);
      await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      
      // Clean up the blob URL if we created one
      if (needsCleanup && typeof URL !== 'undefined' && URL.revokeObjectURL) {
        URL.revokeObjectURL(workerUrl);
      }
      
      // Create a connection
      this.conn = await this.db.connect();
      
      // Get DuckDB version
      try {
        const versionResult = await this.conn.query('SELECT version() as version');
        const versionData = versionResult.toArray();
        this.performance.duckdbVersion = versionData[0]?.version || 'unknown';
      } catch (e) {
        this.performance.duckdbVersion = 'unknown';
      }
      
      // Setup Mosaic connector with the DuckDB connection
      const baseConnector = wasmConnector({ 
        duckdb: this.db,
        connection: this.conn 
      });
      
      // Optionally wrap with batching connector for performance
      if (this.options.enableQueryBatching !== false) {
        this.connector = new BatchingConnector(baseConnector, {
          batchWindow: this.options.batchWindow || 10,
          maxBatchSize: this.options.maxBatchSize || 10,
          enabled: true
        });
      } else {
        this.connector = baseConnector;
      }
      
      this.coordinator.databaseConnector(this.connector);
      
      // Capture memory usage if available
      if (performance.memory) {
        this.performance.memoryUsage = {
          used: Math.round(performance.memory.usedJSHeapSize / 1024 / 1024) + 'MB',
          total: Math.round(performance.memory.totalJSHeapSize / 1024 / 1024) + 'MB'
        };
      }
      
      this.log.debug('Direct DuckDB connection established');
      
    } catch (error) {
      this.log.error('Failed to initialize direct DuckDB connection:', error);
      throw new Error(`Direct DuckDB initialization failed: ${error.message}`);
    }
  }

  
  async initializeWorker() {
    this.log.debug('Initializing DuckDB with Web Worker...');
    
    try {
      // Track worker initialization
      
      // Import DuckDB-WASM for worker mode
      const duckdb = await import('@duckdb/duckdb-wasm');
      
      // Get DuckDB bundles and select the best one for this browser
      const bundles = duckdb.getJsDelivrBundles();
      const bundle = await duckdb.selectBundle(bundles);
      
      this.log.debug('Selected DuckDB bundle:', bundle.mainModule.includes('eh') ? 'eh' : 'mvp');
      
      // Create worker using the bundle URL directly or via Blob to avoid CORS issues
      let workerUrl = bundle.mainWorker;
      let needsCleanup = false;
      
      // In browsers, try to use Blob to avoid CORS issues
      if (typeof Blob !== 'undefined' && typeof URL !== 'undefined' && URL.createObjectURL) {
        try {
          const workerCode = `importScripts("${bundle.mainWorker}");`;
          const workerBlob = new Blob([workerCode], { type: 'text/javascript' });
          workerUrl = URL.createObjectURL(workerBlob);
          needsCleanup = true;
        } catch (e) {
          // Fallback to direct URL if Blob approach fails
          this.log.debug('Blob worker creation failed, using direct URL:', e.message);
          workerUrl = bundle.mainWorker;
        }
      }
      
      // Create worker using the official DuckDB worker file
      this.worker = new Worker(workerUrl);
      
      // Create logger - use ERROR level to reduce console noise
      const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR);
      
      // Initialize AsyncDuckDB with the worker (proper pattern)
      this.db = new duckdb.AsyncDuckDB(logger, this.worker);
      await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      
      // Clean up the blob URL if we created one
      if (needsCleanup && typeof URL !== 'undefined' && URL.revokeObjectURL) {
        URL.revokeObjectURL(workerUrl);
      }
      
      // Create a connection
      this.conn = await this.db.connect();
      
      // Configure DuckDB for optimal performance with adaptive memory
      const config = this.getOptimalDuckDBConfig();
      
      for (const [key, value] of Object.entries(config)) {
        try {
          await this.conn.query(`SET ${key}='${value}'`);
        } catch (e) {
          this.log.warn(`Failed to set ${key}=${value}:`, e.message);
        }
      }
      
      // Get DuckDB version for progress tracking
      try {
        const versionResult = await this.conn.query('SELECT version() as version');
        const versionData = versionResult.toArray();
        this.performance.duckdbVersion = versionData[0]?.version || 'unknown';
      } catch (e) {
        this.performance.duckdbVersion = 'unknown';
      }
      
      // Set bundle type for progress tracking
      this.performance.bundleType = 'worker';
      
      // Setup Mosaic connector with the DuckDB connection
      const baseConnector = wasmConnector({ 
        duckdb: this.db,
        connection: this.conn 
      });
      
      // Optionally wrap with batching connector for performance
      if (this.options.enableQueryBatching !== false) {
        this.connector = new BatchingConnector(baseConnector, {
          batchWindow: this.options.batchWindow || 10,
          maxBatchSize: this.options.maxBatchSize || 10,
          enabled: true
        });
      } else {
        this.connector = baseConnector;
      }
      
      this.coordinator.databaseConnector(this.connector);
      
      // Capture memory usage if available
      if (performance.memory) {
        this.performance.memoryUsage = {
          used: Math.round(performance.memory.usedJSHeapSize / 1024 / 1024) + 'MB',
          total: Math.round(performance.memory.totalJSHeapSize / 1024 / 1024) + 'MB'
        };
      }
      
      this.log.debug('DuckDB Worker initialization completed successfully');
      
    } catch (error) {
      this.log.warn('Worker initialization failed, falling back to direct mode:', error.message);
      this.options.useWorker = false;
      this.performance.mode = 'Direct (fallback)';
      
      // Cleanup failed worker
      if (this.worker) {
        this.worker.terminate();
        this.worker = null;
      }
      
      if (this.db) {
        try {
          if (typeof this.db.terminate === 'function') {
            await this.db.terminate();
          }
        } catch (error) {
          this.log.warn('Error terminating DuckDB in fallback:', error);
        }
        this.db = null;
      }
      
      // Fallback to direct mode
      await this.initializeDirect();
    }
  }
  
  
  
  async initializePersistence() {
    this.log.debug('Initializing persistence...');
    
    this.persistenceManager = new PersistenceManager('datatable-session');
    await this.persistenceManager.initialize();
    
    // Try to restore previous session
    const savedData = await this.persistenceManager.loadTable();
    if (savedData) {
      this.log.info('Restoring previous session...');
      // TODO: Restore table data
    }
    
    this.log.debug('Persistence initialized');
  }
  
  createContainer() {
    // Clear any existing content (e.g., empty state)
    this.options.container.innerHTML = '';
    
    this.container = document.createElement('div');
    this.container.className = 'datatable-container';
    this.container.style.height = `${this.options.height}px`;
    this.container.style.border = '1px solid #ddd';
    this.container.style.borderRadius = '4px';
    this.container.style.overflow = 'hidden';
    
    this.options.container.appendChild(this.container);
  }
  
  async loadData(source, options = {}) {
    try {
      // Track data loading
      const loadStartTime = Date.now();
      const fileName = source instanceof File ? source.name : 'data';
      const fileSize = source instanceof File ? source.size : 0;
      
      this.log.info('Loading data...');
      
      // Setup progress tracking
      const progressOptions = {
        ...options,
        onProgress: (progress) => {
          // Emit progress events
          this.emitProgress({
            stage: progress.stage || 'loading',
            percent: progress.percent || 0,
            loaded: progress.loaded || 0,
            total: progress.total || 1,
            fileName,
            fileSize
          });
          
          // Call user-provided progress callback if available
          if (options.onProgress) {
            options.onProgress(progress);
          }
        }
      };
      
      const result = await this.dataLoader.load(source, progressOptions);
      
      // Update table name and schema
      this.tableName.value = result.tableName;
      this.schema.value = result.schema || {};
      
      // Track table creation
      
      // Ensure container is still in DOM (may have been removed by external code)
      if (this.container && typeof document !== 'undefined' && document.contains && !document.contains(this.container)) {
        this.container = null; // Clear stale reference
      }
      
      // Create container if needed
      if (!this.container) {
        this.createContainer();
      }
      
      // Create or update table renderer
      if (this.container) {
        if (this.tableRenderer) {
          // Destroy existing renderer before creating new one
          this.tableRenderer.destroy();
          this.tableRenderer = null;
        }
        
        // Clear coordinator cache before creating new renderer to prevent stale data
        if (this.coordinator && this.coordinator.cache) {
          try {
            if (typeof this.coordinator.cache.clear === 'function') {
              this.coordinator.cache.clear();
              this.log.debug('Coordinator cache cleared before loading new data');
            }
          } catch (error) {
            this.log.warn('Error clearing coordinator cache before loading:', error);
          }
        }
        
        // Small delay to ensure previous cleanup operations complete
        await new Promise(resolve => setTimeout(resolve, 25));
        
        // Create new table renderer
        this.tableRenderer = new TableRenderer({
          table: this.tableName.value,
          schema: this.schema.value,
          container: this.container,
          coordinator: this.coordinator,
          connection: this.conn // Pass DuckDB connection for direct queries
        });
        
        // Connect TableRenderer to coordinator (this will trigger initialization)
        if (this.coordinator) {
          this.coordinator.connect(this.tableRenderer);
        } else {
          // If no coordinator, initialize directly
          await this.tableRenderer.initialize();
        }
      }
      
      // Save to persistence if enabled
      if (this.persistenceManager) {
        await this.persistenceManager.saveTable({
          tableName: this.tableName.value,
          schema: this.schema.value,
          timestamp: Date.now()
        });
      }
      
      // Complete data loading tracking
      const loadTime = Date.now() - loadStartTime;
      
      this.log.info(`Data loaded successfully: ${this.tableName.value}`);
      
      // Validate that the data was actually loaded correctly
      try {
        const sampleQuery = `SELECT * FROM ${this.tableName.value} LIMIT 3`;
        let sampleData = await this.executeSQL(sampleQuery);
        
        // Handle Apache Arrow Table format (same as TableRenderer.queryResult())
        if (sampleData && typeof sampleData === 'object' && !Array.isArray(sampleData) && typeof sampleData.toArray === 'function') {
          this.log.debug('Converting Arrow Table for validation');
          try {
            sampleData = sampleData.toArray();
          } catch (error) {
            this.log.warn('Failed to convert Arrow Table for validation:', error);
            sampleData = [];
          }
        }
        
        this.log.debug(`Sample data from new table ${this.tableName.value} (${sampleData?.length || 0} rows):`, sampleData);
        
        // Log first row for debugging
        if (sampleData && sampleData.length > 0) {
          this.log.debug('First row of new data:', sampleData[0]);
        } else {
          this.log.debug('Empty result from sample query (table may be empty or query failed)');
        }
      } catch (error) {
        this.log.warn('Could not validate loaded data:', error.message);
      }
      
      // Log data loading success
      this.log.info(`Data loaded successfully: ${fileName}`, {
        table: this.tableName.value,
        rows: result.rowCount || 'unknown',
        columns: Object.keys(this.schema.value).length,
        loadTime: `${loadTime}ms`,
        mode: this.performance.mode
      });
      
      return result;
    } catch (error) {
      this.log.error('Failed to load data:', error);
      throw error;
    }
  }
  
  async executeSQL(sql, options = {}) {
    try {
      this.log.debug('Executing SQL:', sql);
      
      // Check memory pressure before executing queries
      const memoryStatus = this.checkMemoryPressure();
      if (memoryStatus && memoryStatus.level === 'critical') {
        this.log.warn('Skipping query due to critical memory pressure');
        throw new Error('Query cancelled due to memory pressure');
      }
      
      // Both Worker and Direct modes use the same connection approach now
      if (!this.conn) {
        throw new Error('DuckDB connection not available');
      }
      
      // Check cache first for SELECT queries
      const sqlLower = sql.trim().toLowerCase();
      const isReadOnlyQuery = sqlLower.startsWith('select') || sqlLower.startsWith('with');
      let result = null;
      let fromCache = false;
      
      if (isReadOnlyQuery && this.queryCache.enabled) {
        result = this.queryCache.get(sql, options);
        if (result) {
          fromCache = true;
          this.log.debug('Query result served from cache');
        }
      }
      
      if (!result) {
        // For large queries, use streaming when available
        const useStreaming = options.streaming && this.conn.send;
        
        if (useStreaming) {
          // Use streaming for large result sets
          this.log.debug('Using streaming query execution');
          result = await this.conn.send(sql);
        } else {
          // Standard query execution
          result = await this.conn.query(sql);
        }
        
        // Cache the result if it's a read-only query
        if (isReadOnlyQuery) {
          this.queryCache.set(sql, result, options);
        }
      }
      
      // Update current SQL
      this.currentSQL.value = sql;
      
      // Add to history with result size tracking
      this.queryHistory.push({
        sql,
        timestamp: Date.now(),
        resultSize: this.getResultSize(result),
        streaming: options.streaming || false,
        cached: fromCache
      });
      
      // Record command for version control (only for non-SELECT queries)
      if (this.versionControl && !isReadOnlyQuery) {
        await this.versionControl.recordCommand(sql, {
          tableName: this.tableName.value
        });
      }
      
      this.log.debug(`SQL executed successfully ${fromCache ? '(cached)' : ''}`);
      return result;
    } catch (error) {
      this.log.error('Failed to execute SQL:', error);
      throw error;
    }
  }
  
  getResultSize(result) {
    if (!result) return 0;
    if (typeof result.length === 'number') return result.length;
    if (typeof result.numRows === 'number') return result.numRows;
    if (result.toArray) {
      try {
        return result.toArray().length;
      } catch (e) {
        return 'unknown';
      }
    }
    return 'unknown';
  }
  
  getCurrentSQL() {
    return this.currentSQL.value;
  }
  
  getQueryHistory() {
    return this.queryHistory;
  }
  
  getSchema() {
    return {
      tables: [this.tableName.value].filter(Boolean),
      columns: Object.keys(this.schema.value)
    };
  }
  
  hasPersistedData() {
    return this.persistenceManager !== null;
  }
  
  /**
   * Get query cache statistics
   */
  getCacheStats() {
    return this.queryCache ? this.queryCache.getStats() : null;
  }
  
  /**
   * Enable or disable query caching
   */
  setCacheEnabled(enabled) {
    if (this.queryCache) {
      this.queryCache.setEnabled(enabled);
    }
  }
  
  /**
   * Clear query cache manually
   */
  clearCache() {
    if (this.queryCache) {
      this.queryCache.clear();
      this.log.info('Query cache cleared manually');
    }
  }
  
  /**
   * Check memory usage and apply adaptive limits
   */
  checkMemoryPressure() {
    if (!this.memoryMonitor.enabled) return null;
    
    const now = Date.now();
    if (now - this.memoryMonitor.lastCheck < this.memoryMonitor.checkInterval) {
      return null; // Too soon since last check
    }
    
    this.memoryMonitor.lastCheck = now;
    
    const memory = performance.memory;
    const usageRatio = memory.usedJSHeapSize / memory.jsHeapSizeLimit;
    
    const status = {
      used: Math.round(memory.usedJSHeapSize / 1024 / 1024),
      total: Math.round(memory.totalJSHeapSize / 1024 / 1024), 
      limit: Math.round(memory.jsHeapSizeLimit / 1024 / 1024),
      usageRatio,
      level: 'normal'
    };
    
    // Determine memory pressure level
    if (usageRatio >= this.memoryMonitor.thresholds.critical) {
      status.level = 'critical';
      this.handleCriticalMemoryPressure();
    } else if (usageRatio >= this.memoryMonitor.thresholds.warning) {
      status.level = 'warning';
      this.handleWarningMemoryPressure();
    }
    
    // Notify callbacks
    this.memoryMonitor.callbacks.forEach(callback => {
      try {
        callback(status);
      } catch (e) {
        this.log.warn('Memory monitor callback error:', e);
      }
    });
    
    return status;
  }
  
  /**
   * Handle warning level memory pressure
   */
  handleWarningMemoryPressure() {
    this.log.warn('Memory pressure detected - applying optimizations');
    
    // Reduce cache size and TTL
    if (this.queryCache) {
      this.queryCache.maxSize = Math.max(20, Math.floor(this.queryCache.maxSize * 0.7));
      this.queryCache.ttl = Math.max(15000, Math.floor(this.queryCache.ttl * 0.7));
      this.queryCache.cleanup(); // Force cleanup
    }
    
    // Clear coordinator cache
    if (this.coordinator && this.coordinator.cache && this.coordinator.cache.clear) {
      this.coordinator.cache.clear();
    }
  }
  
  /**
   * Handle critical level memory pressure
   */
  handleCriticalMemoryPressure() {
    this.log.error('Critical memory pressure - taking emergency measures');
    
    // Disable caching temporarily
    if (this.queryCache) {
      this.queryCache.clear();
      this.queryCache.setEnabled(false);
    }
    
    // Force garbage collection if available
    if (typeof gc === 'function') {
      gc();
    }
    
    // Try to reduce DuckDB memory limit
    if (this.conn) {
      try {
        this.conn.query("SET max_memory='256MB'");
        this.log.info('Reduced DuckDB memory limit due to pressure');
      } catch (e) {
        this.log.warn('Failed to reduce DuckDB memory limit:', e);
      }
    }
  }
  
  /**
   * Add callback for memory pressure events
   */
  onMemoryPressure(callback) {
    if (typeof callback === 'function') {
      this.memoryMonitor.callbacks.add(callback);
    }
  }
  
  /**
   * Remove memory pressure callback
   */
  offMemoryPressure(callback) {
    this.memoryMonitor.callbacks.delete(callback);
  }
  
  /**
   * Get current memory status
   */
  getMemoryStatus() {
    return this.checkMemoryPressure();
  }
  
  /**
   * Add progress callback for long operations
   */
  onProgress(callback) {
    if (typeof callback === 'function') {
      this.progressCallbacks.add(callback);
    }
  }
  
  /**
   * Remove progress callback
   */
  offProgress(callback) {
    this.progressCallbacks.delete(callback);
  }
  
  /**
   * Emit progress event to all callbacks
   */
  emitProgress(progress) {
    this.progressCallbacks.forEach(callback => {
      try {
        callback(progress);
      } catch (e) {
        this.log.warn('Progress callback error:', e);
      }
    });
  }
  
  /**
   * Get query batching statistics
   */
  getBatchingStats() {
    return this.connector && this.connector.getStats ? 
      this.connector.getStats() : null;
  }
  
  /**
   * Enable or disable query batching
   */
  setBatchingEnabled(enabled) {
    if (this.connector && this.connector.setEnabled) {
      this.connector.setEnabled(enabled);
    }
  }
  
  async clearData() {
    try {
      this.log.info('Clearing data...');
      
      // Clear UI first - properly destroy table renderer before clearing data
      if (this.tableRenderer) {
        this.tableRenderer.destroy();
        this.tableRenderer = null;
      }
      this.visualizations.clear();
      
      // Restore empty state in container after clearing renderer
      if (this.container && this.container.parentElement) {
        this.container.parentElement.innerHTML = `
          <div class="empty-state">
            <div class="empty-icon">📊</div>
            <h3>No Data Loaded</h3>
            <p>Upload a file or load data from a URL to get started</p>
          </div>
        `;
        this.container = null; // Clear reference since container is gone
      }
      
      // Clear query cache and coordinator cache to prevent stale data references
      if (this.queryCache) {
        this.queryCache.clear();
        this.log.debug('Query cache cleared');
      }
      
      if (this.coordinator) {
        try {
          // Clear main cache
          if (this.coordinator.cache) {
            if (typeof this.coordinator.cache.clear === 'function') {
              this.coordinator.cache.clear();
              this.log.debug('Coordinator cache cleared');
            } else if (this.coordinator.cache instanceof Map) {
              this.coordinator.cache.clear();
              this.log.debug('Coordinator Map cache cleared');
            }
          }
          
          // Clear query cache if it exists separately
          if (this.coordinator.queryCache && typeof this.coordinator.queryCache.clear === 'function') {
            this.coordinator.queryCache.clear();
            this.log.debug('Coordinator query cache cleared');
          }
          
          // Clear client-specific caches
          if (this.coordinator.clients) {
            this.coordinator.clients.forEach(client => {
              if (client && typeof client.clearCache === 'function') {
                client.clearCache();
              }
            });
          }
          
          // Force invalidate all cached queries by incrementing version if available
          if (this.coordinator.version !== undefined) {
            this.coordinator.version++;
            this.log.debug('Coordinator version incremented to force cache invalidation');
          }
          
        } catch (error) {
          this.log.warn('Error clearing coordinator cache:', error);
        }
      }
      
      // Clear table from database
      if (this.tableName.value) {
        this.log.info(`Dropping table: ${this.tableName.value}`);
        await this.executeSQL(`DROP TABLE IF EXISTS ${this.tableName.value}`);
        
        // Verify table was actually dropped
        try {
          let tables = await this.executeSQL("SHOW TABLES");
          
          // Handle Apache Arrow format if needed
          if (tables && typeof tables === 'object' && !Array.isArray(tables) && typeof tables.toArray === 'function') {
            try {
              tables = tables.toArray();
            } catch (error) {
              this.log.warn('Failed to convert Arrow Table for table verification:', error);
              tables = [];
            }
          }
          
          // Extract table names from the result
          const remainingTables = Array.isArray(tables) ? 
            tables.map(row => typeof row === 'string' ? row : (row.name || row.table_name || Object.values(row)[0])) :
            [];
          
          this.log.debug('Remaining tables after DROP:', remainingTables);
          
          if (remainingTables.includes(this.tableName.value)) {
            this.log.warn(`Table ${this.tableName.value} still exists after DROP command`);
          } else {
            this.log.debug(`Table ${this.tableName.value} successfully dropped`);
          }
        } catch (error) {
          this.log.debug('Could not verify table drop (this is normal):', error.message);
        }
        
        // Add a small delay to ensure DuckDB completes the operation
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      
      // Reset state
      this.tableName.value = null;
      this.schema.value = {};
      this.currentSQL.value = '';
      this.queryHistory = [];
      
      // Clear persistence
      if (this.persistenceManager) {
        await this.persistenceManager.clearTable();
      }
      
      // Clear version control
      if (this.versionControl) {
        await this.versionControl.clear();
      }
      
      this.log.info('Data cleared successfully');
    } catch (error) {
      this.log.error('Failed to clear data:', error);
      throw error;
    }
  }
  
  getOptimalDuckDBConfig() {
    // Determine optimal memory configuration based on available memory
    let maxMemory = '512MB'; // Default
    let tempDirectorySize = '128MB'; // Default
    
    if (typeof performance !== 'undefined' && performance.memory) {
      // Use browser memory information if available
      const totalMemory = performance.memory.totalJSHeapSize;
      const usedMemory = performance.memory.usedJSHeapSize;
      const availableMemory = totalMemory - usedMemory;
      
      // Use 25% of available memory, with limits
      const targetMemory = Math.floor(availableMemory * 0.25);
      const memoryMB = Math.max(128, Math.min(1024, Math.floor(targetMemory / 1024 / 1024)));
      maxMemory = `${memoryMB}MB`;
      
      // Temp directory should be 25% of max memory
      const tempMB = Math.max(32, Math.floor(memoryMB * 0.25));
      tempDirectorySize = `${tempMB}MB`;
      
      this.log.debug(`Adaptive memory configuration: ${maxMemory}, temp: ${tempDirectorySize} (available: ${Math.floor(availableMemory/1024/1024)}MB)`);
    }
    
    return {
      // Memory management
      'max_memory': maxMemory,
      'max_temp_directory_size': tempDirectorySize,
      'temp_directory': '', // Use memory for temp storage (faster)
      
      // Performance optimizations for WASM
      'threads': '1', // Single-threaded for WebAssembly
      'enable_object_cache': 'true',
      'preserve_insertion_order': 'false', // Better performance for analytics
      
      // Query execution optimizations
      'enable_progress_bar': 'false', // Disable for better performance
      'checkpoint_threshold': '1GB', // Optimize WAL checkpointing
      
      // I/O optimizations
      'enable_http_metadata_cache': 'true',
      'http_timeout': '30000', // 30 second timeout for HTTP requests
      
      // Analytics optimizations
      'default_order': 'DESC', // Better for most analytics queries
      'enable_profiling': this.options.logLevel === 'debug' ? 'query_tree' : 'no_output'
    };
  }
  
  async cleanupConnection() {
    try {
      // Proper connection cleanup following DuckDB best practices
      if (this.conn) {
        this.log.debug('Closing DuckDB connection...');
        await this.conn.close();
        this.conn = null;
      }
    } catch (error) {
      this.log.warn('Error closing connection:', error);
    }
  }
  
  async destroy() {
    try {
      this.log.info('Destroying DataTable...');
      
      // Cleanup UI first
      if (this.tableRenderer) {
        this.tableRenderer.destroy();
        this.tableRenderer = null;
      }
      
      // Cleanup connection
      await this.cleanupConnection();
      
      // Cleanup DuckDB instance
      if (this.db) {
        try {
          if (typeof this.db.terminate === 'function') {
            await this.db.terminate();
          }
          this.db = null;
        } catch (error) {
          this.log.warn('Error terminating DuckDB:', error);
        }
      }
      
      // Cleanup worker
      if (this.worker) {
        this.worker.terminate();
        this.worker = null;
      }
      
      // Cleanup persistence
      if (this.persistenceManager) {
        this.persistenceManager.close?.();
      }
      
      // Cleanup query cache
      if (this.queryCache) {
        this.queryCache.destroy();
        this.queryCache = null;
      }
      
      // Cleanup UI
      if (this.container && this.container.parentNode) {
        this.container.parentNode.removeChild(this.container);
      }
      
      // Cleanup connector
      if (this.connector && this.connector.destroy) {
        this.connector.destroy();
      }
      
      // Clear references
      this.coordinator = null;
      this.connector = null;
      this.visualizations.clear();
      this.progressCallbacks.clear();
      this.memoryMonitor.callbacks.clear();
      
      this.log.info('DataTable destroyed');
    } catch (error) {
      this.log.error('Error destroying DataTable:', error);
    }
  }
}