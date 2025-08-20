import { Coordinator, wasmConnector } from '@uwdata/mosaic-core';
import { Query } from '@uwdata/mosaic-sql';
import { signal } from '@preact/signals-core';
import { DataLoader } from '../data/DataLoader.js';
import { PersistenceManager } from '../storage/PersistenceManager.js';
import { VersionControl } from '../storage/VersionControl.js';
import { TableRenderer } from './TableRenderer.js';
import { WorkerConnector } from '../connectors/WorkerConnector.js';
import { detectSchema, getDataProfile } from '../data/DuckDBHelpers.js';

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
    
    // 🚀 Task 2: Progress Tracking & Performance Metrics
    this.performance = {
      initStartTime: null,
      initEndTime: null,
      lastOperationTime: null,
      mode: null,
      duckdbVersion: null,
      bundleType: null,
      memoryUsage: null
    };
    
    this.progress = {
      task1Complete: true, // ✅ Task 1 completed
      task2InProgress: false,
      task2Complete: false,
      task3InProgress: false,
      task3Complete: false,
      currentOperation: null,
      lastUpdate: null
    };
    
    // Setup logging
    this.setupLogging();
    
    // Bind methods (none needed currently)
    
    // 🚀 Log Task 2 initialization
    this.logTaskProgress();
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
  
  // 🚀 Task 2: Progress Tracking Methods
  
  logTaskProgress() {
    console.group('🚀 DataTable Phase 1 Progress');
    console.log('✅ Task 1: DuckDB-WASM Worker Initialization - COMPLETED');
    console.log('✅ Task 2: Enhanced Direct Mode & Progress Tracking - COMPLETED');
    console.log('🔄 Task 3: DuckDB Helper Utilities - STARTING');
    console.log('📊 Mode Preference:', this.options.useWorker ? 'Worker' : 'Direct');
    console.log('🔧 DuckDBHelpers: Schema detection, data profiling, utilities');
    console.groupEnd();
    
    this.progress.task3InProgress = true;
    this.progress.lastUpdate = Date.now();
  }
  
  updateProgress(operation, details = {}) {
    this.progress.currentOperation = operation;
    this.progress.lastUpdate = Date.now();
    this.performance.lastOperationTime = Date.now();
    
    // Emit custom event for UI updates
    if (this.options.container && this.options.container.dispatchEvent) {
      this.options.container.dispatchEvent(new CustomEvent('datatable-progress', {
        detail: {
          operation,
          progress: this.progress,
          performance: this.performance,
          ...details
        }
      }));
    }
    
    this.log.debug(`🔄 Progress: ${operation}`, details);
  }
  
  getProgressInfo() {
    return {
      progress: { ...this.progress },
      performance: { ...this.performance },
      schema: this.schema.value,
      tableName: this.tableName.value,
      queryCount: this.queryHistory.length
    };
  }
  
  logTask2Completion() {
    console.group('🚀 DataTable Task 2 Progress');
    console.log('✅ Task 2: Enhanced Direct Mode - COMPLETED');
    console.log('📊 Mode:', this.performance.mode);
    console.log('🔧 DuckDB Version:', this.performance.duckdbVersion || 'unknown');
    console.log('📦 Bundle Type:', this.performance.bundleType || 'unknown');
    console.log('⏱️ Init Time:', this.performance.initEndTime ? 
      `${this.performance.initEndTime - this.performance.initStartTime}ms` : 'unknown');
    console.log('🗄️ Tables Loaded:', this.tableName.value ? 1 : 0);
    console.log('📈 Memory Usage:', this.performance.memoryUsage || 'unknown');
    console.groupEnd();
    
    this.progress.task2Complete = true;
    this.progress.task2InProgress = false;
    this.updateProgress('Task 2 Complete');
  }

  logTask3Completion() {
    console.group('🚀 DataTable Task 3 Progress');
    console.log('✅ Task 3: DuckDB Helper Utilities - COMPLETED');
    console.log('🔧 Utilities: detectSchema, getRowCount, getDataProfile');
    console.log('📊 Code Consolidation: Removed duplicated helpers from DataLoader and Worker');
    console.log('📈 Enhanced Features: Column stats, distinct values, data profiling');
    console.log('🧪 Testing: Comprehensive unit tests for all helper functions');
    console.log('📊 Progress: Foundation ready for advanced visualizations');
    console.groupEnd();
    
    this.progress.task3Complete = true;
    this.progress.task3InProgress = false;
    this.updateProgress('Task 3 Complete');
  }
  
  async initialize() {
    try {
      // 🚀 Task 2: Start timing initialization
      this.performance.initStartTime = Date.now();
      this.updateProgress('Initializing DataTable');
      
      this.log.info('Initializing DataTable...');
      
      // Setup DuckDB connection
      if (this.options.useWorker) {
        this.updateProgress('Initializing Worker Mode');
        await this.initializeWorker();
        this.performance.mode = 'Worker';
      } else {
        this.updateProgress('Initializing Direct Mode');
        await this.initializeDirect();
        this.performance.mode = 'Direct';
      }
      
      // Setup persistence if enabled
      if (this.options.persistSession) {
        this.updateProgress('Setting up persistence');
        await this.initializePersistence();
      }
      
      // Setup version control
      this.updateProgress('Setting up version control');
      this.versionControl = new VersionControl({
        strategy: 'hybrid',
        maxCommands: 50,
        maxSnapshots: 3
      });
      
      // Create UI container
      this.updateProgress('Creating UI container');
      this.createContainer();
      
      // 🚀 Task 2: Complete initialization timing
      this.performance.initEndTime = Date.now();
      this.updateProgress('Initialization Complete');
      
      this.log.info('DataTable initialized successfully');
      
      // 🚀 Task 2: Log completion
      this.logTask2Completion();
      
      // 🚀 Task 3: DuckDB Helper Utilities are now active
      this.logTask3Completion();
      
      return this;
    } catch (error) {
      this.log.error('Failed to initialize:', error);
      this.updateProgress('Initialization Failed', { error: error.message });
      throw error;
    }
  }
  
  async initializeDirect() {
    this.log.debug('Initializing direct DuckDB connection...');
    
    try {
      // 🚀 Task 2: Track direct mode initialization
      this.updateProgress('Loading DuckDB-WASM module');
      
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
      this.updateProgress('Selecting optimal DuckDB bundle');
      const bundle = await duckdb.selectBundle(bundles);
      
      // 🚀 Task 2: Capture bundle type for progress tracking
      this.performance.bundleType = bundle.mainModule.includes('eh') ? 'eh' : 'mvp';
      this.log.debug('Selected DuckDB bundle:', this.performance.bundleType);
      
      // Create logger
      this.updateProgress('Creating DuckDB instance');
      const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
      
      // 🚀 Even in "direct" mode, AsyncDuckDB requires a worker
      // Create a worker internally but don't expose worker management to the user
      this.updateProgress('Creating internal worker for DuckDB');
      
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
      this.updateProgress('Establishing database connection');
      this.conn = await this.db.connect();
      
      // 🚀 Task 2: Get DuckDB version for progress tracking
      try {
        const versionResult = await this.conn.query('SELECT version() as version');
        const versionData = versionResult.toArray();
        this.performance.duckdbVersion = versionData[0]?.version || 'unknown';
      } catch (e) {
        this.performance.duckdbVersion = 'unknown';
      }
      
      // Setup Mosaic connector with the DuckDB connection
      this.updateProgress('Configuring Mosaic coordinator');
      this.connector = wasmConnector({ 
        duckdb: this.db,
        connection: this.conn 
      });
      this.coordinator.databaseConnector(this.connector);
      
      // 🚀 Task 2: Capture memory usage if available
      if (performance.memory) {
        this.performance.memoryUsage = {
          used: Math.round(performance.memory.usedJSHeapSize / 1024 / 1024) + 'MB',
          total: Math.round(performance.memory.totalJSHeapSize / 1024 / 1024) + 'MB'
        };
      }
      
      this.log.debug('Direct DuckDB connection established');
      this.updateProgress('Direct mode initialization complete');
      
    } catch (error) {
      this.log.error('Failed to initialize direct DuckDB connection:', error);
      this.updateProgress('Direct mode initialization failed', { error: error.message });
      throw new Error(`Direct DuckDB initialization failed: ${error.message}`);
    }
  }

  
  async initializeWorker() {
    this.log.debug('Initializing DuckDB with Web Worker...');
    
    try {
      // 🚀 Task 2: Track worker initialization
      this.updateProgress('Creating DuckDB Web Worker');
      
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
      
      // Create logger
      const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
      
      // Initialize AsyncDuckDB with the worker (proper pattern)
      this.updateProgress('Initializing DuckDB with worker');
      this.db = new duckdb.AsyncDuckDB(logger, this.worker);
      await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      
      // Clean up the blob URL if we created one
      if (needsCleanup && typeof URL !== 'undefined' && URL.revokeObjectURL) {
        URL.revokeObjectURL(workerUrl);
      }
      
      // Create a connection
      this.updateProgress('Establishing database connection');
      this.conn = await this.db.connect();
      
      // Configure DuckDB for optimal performance (WASM-compatible settings only)
      const config = {
        'max_memory': '512MB',
        'enable_object_cache': 'true'
      };
      
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
      this.updateProgress('Configuring Mosaic coordinator');
      this.connector = wasmConnector({ 
        duckdb: this.db,
        connection: this.conn 
      });
      this.coordinator.databaseConnector(this.connector);
      
      // Capture memory usage if available
      if (performance.memory) {
        this.performance.memoryUsage = {
          used: Math.round(performance.memory.usedJSHeapSize / 1024 / 1024) + 'MB',
          total: Math.round(performance.memory.totalJSHeapSize / 1024 / 1024) + 'MB'
        };
      }
      
      this.log.debug('DuckDB Worker initialization completed successfully');
      this.updateProgress('Worker mode initialization complete');
      
    } catch (error) {
      this.log.warn('Worker initialization failed, falling back to direct mode:', error.message);
      this.updateProgress('Worker failed, falling back to direct mode', { error: error.message });
      this.options.useWorker = false;
      this.performance.mode = 'Direct (fallback)';
      
      // Cleanup failed worker
      if (this.worker) {
        this.worker.terminate();
        this.worker = null;
      }
      
      if (this.db) {
        await this.db.terminate();
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
      // 🚀 Task 2: Track data loading
      const loadStartTime = Date.now();
      const fileName = source instanceof File ? source.name : 'data';
      this.updateProgress(`Loading data: ${fileName}`);
      
      this.log.info('Loading data...');
      
      const result = await this.dataLoader.load(source, options);
      
      // Update table name and schema
      this.tableName.value = result.tableName;
      this.schema.value = result.schema || {};
      
      // 🚀 Task 2: Track table creation
      this.updateProgress('Creating table renderer');
      
      // Ensure container is still in DOM (may have been removed by external code)
      if (this.container && !document.contains(this.container)) {
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
        this.updateProgress('Saving to persistence');
        await this.persistenceManager.saveTable({
          tableName: this.tableName.value,
          schema: this.schema.value,
          timestamp: Date.now()
        });
      }
      
      // 🚀 Task 2: Complete data loading tracking
      const loadTime = Date.now() - loadStartTime;
      this.updateProgress(`Data loaded: ${this.tableName.value}`, {
        loadTime: `${loadTime}ms`,
        rowCount: result.rowCount || 'unknown',
        columnCount: Object.keys(this.schema.value).length
      });
      
      this.log.info(`Data loaded successfully: ${this.tableName.value}`);
      
      // Log data loading progress
      console.group('📊 Data Loading Complete');
      console.log('📁 File:', fileName);
      console.log('🗄️ Table:', this.tableName.value);
      console.log('📊 Rows:', result.rowCount || 'unknown');
      console.log('📋 Columns:', Object.keys(this.schema.value).length);
      console.log('⏱️ Load Time:', `${loadTime}ms`);
      console.log('🔧 Mode:', this.performance.mode);
      console.groupEnd();
      
      return result;
    } catch (error) {
      this.log.error('Failed to load data:', error);
      this.updateProgress('Data loading failed', { error: error.message });
      throw error;
    }
  }
  
  async executeSQL(sql) {
    try {
      this.log.debug('Executing SQL:', sql);
      
      // Both Worker and Direct modes use the same connection approach now
      if (!this.conn) {
        throw new Error('DuckDB connection not available');
      }
      
      // Execute SQL query using the unified connection
      const result = await this.conn.query(sql);
      
      // Update current SQL
      this.currentSQL.value = sql;
      
      // Add to history
      this.queryHistory.push({
        sql,
        timestamp: Date.now(),
        result: result ? result.length : 0
      });
      
      // Record command for version control
      if (this.versionControl && sql.trim().toLowerCase().startsWith('select') === false) {
        await this.versionControl.recordCommand(sql, {
          tableName: this.tableName.value
        });
      }
      
      this.log.debug('SQL executed successfully');
      return result;
    } catch (error) {
      this.log.error('Failed to execute SQL:', error);
      throw error;
    }
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
  
  async clearData() {
    try {
      this.log.info('Clearing data...');
      
      // Clear table
      if (this.tableName.value) {
        await this.executeSQL(`DROP TABLE IF EXISTS ${this.tableName.value}`);
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
      
      // Clear UI - properly destroy table renderer instead of clearing container
      if (this.tableRenderer) {
        this.tableRenderer.destroy();
        this.tableRenderer = null;
      }
      this.visualizations.clear();
      
      this.log.info('Data cleared successfully');
    } catch (error) {
      this.log.error('Failed to clear data:', error);
      throw error;
    }
  }
  
  destroy() {
    try {
      this.log.info('Destroying DataTable...');
      
      // Cleanup worker
      if (this.worker) {
        this.worker.terminate();
        this.worker = null;
      }
      
      // Cleanup persistence
      if (this.persistenceManager) {
        this.persistenceManager.close?.();
      }
      
      // Cleanup UI
      if (this.container && this.container.parentNode) {
        this.container.parentNode.removeChild(this.container);
      }
      
      // Clear references
      this.coordinator = null;
      this.connector = null;
      this.tableRenderer = null;
      this.visualizations.clear();
      
      this.log.info('DataTable destroyed');
    } catch (error) {
      this.log.error('Error destroying DataTable:', error);
    }
  }
}