import { Coordinator, wasmConnector } from '@uwdata/mosaic-core';
import { Query } from '@uwdata/mosaic-sql';
import { signal } from '@preact/signals-core';
import { DataLoader } from '../data/DataLoader.js';
import { PersistenceManager } from '../storage/PersistenceManager.js';
import { VersionControl } from '../storage/VersionControl.js';
import { TableRenderer } from './TableRenderer.js';

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
    
    // Setup logging
    this.setupLogging();
    
    // Bind methods
    this.handleWorkerMessage = this.handleWorkerMessage.bind(this);
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
      this.log.info('Initializing DataTable...');
      
      // Setup DuckDB connection
      if (this.options.useWorker) {
        await this.initializeWorker();
      } else {
        await this.initializeDirect();
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
      
      this.log.info('DataTable initialized successfully');
      return this;
    } catch (error) {
      this.log.error('Failed to initialize:', error);
      throw error;
    }
  }
  
  async initializeDirect() {
    this.log.debug('Initializing direct DuckDB connection...');
    
    // Direct DuckDB initialization (no worker)
    this.connector = wasmConnector();
    await this.connector.getDuckDB();
    this.coordinator.databaseConnector(this.connector);
    
    this.log.debug('Direct DuckDB connection established');
  }
  
  async initializeWorker() {
    this.log.debug('Initializing DuckDB Web Worker...');
    
    try {
      // Initialize with Web Worker
      this.worker = new Worker(
        new URL('../workers/duckdb.worker.js', import.meta.url),
        { type: 'module' }
      );
      
      // Setup worker message handling
      this.worker.onmessage = this.handleWorkerMessage;
      this.worker.onerror = (error) => {
        this.log.error('Worker error:', error);
        // Fallback to direct initialization
        this.log.warn('Worker failed, falling back to direct DuckDB initialization');
        this.options.useWorker = false;
        return this.initializeDirect();
      };
      
      // Initialize DuckDB in worker with shorter timeout for development
      await this.sendToWorker('init', {
        config: {
          'max_memory': '512MB',
          'threads': '4'
        }
      });
      
      this.log.debug('DuckDB Web Worker initialized');
    } catch (error) {
      this.log.warn('Worker initialization failed, falling back to direct mode:', error.message);
      this.options.useWorker = false;
      return this.initializeDirect();
    }
  }
  
  handleWorkerMessage(event) {
    const { id, success, result, error } = event.data;
    
    if (this.workerPromises && this.workerPromises.has(id)) {
      const { resolve, reject } = this.workerPromises.get(id);
      this.workerPromises.delete(id);
      
      if (success) {
        resolve(result);
      } else {
        reject(new Error(error));
      }
    }
  }
  
  sendToWorker(type, payload) {
    if (!this.worker) {
      throw new Error('Worker not initialized');
    }
    
    return new Promise((resolve, reject) => {
      const id = crypto.randomUUID();
      
      // Initialize worker promises map if needed
      if (!this.workerPromises) {
        this.workerPromises = new Map();
      }
      
      this.workerPromises.set(id, { resolve, reject });
      
      // Send message to worker
      this.worker.postMessage({ id, type, payload });
      
      // Set timeout for worker operations (shorter for development)
      setTimeout(() => {
        if (this.workerPromises.has(id)) {
          this.workerPromises.delete(id);
          reject(new Error('Worker operation timeout'));
        }
      }, 5000); // 5 second timeout
    });
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
      this.log.info('Loading data...');
      
      const result = await this.dataLoader.load(source, options);
      
      // Update table name and schema
      this.tableName.value = result.tableName;
      this.schema.value = result.schema || {};
      
      // Create table renderer if not exists
      if (!this.tableRenderer && this.container) {
        this.tableRenderer = new TableRenderer({
          table: this.tableName.value,
          schema: this.schema.value,
          container: this.container,
          coordinator: this.coordinator
        });
        
        await this.tableRenderer.initialize();
      }
      
      // Save to persistence if enabled
      if (this.persistenceManager) {
        await this.persistenceManager.saveTable({
          tableName: this.tableName.value,
          schema: this.schema.value,
          timestamp: Date.now()
        });
      }
      
      this.log.info(`Data loaded successfully: ${this.tableName.value}`);
      return result;
    } catch (error) {
      this.log.error('Failed to load data:', error);
      throw error;
    }
  }
  
  async executeSQL(sql) {
    try {
      this.log.debug('Executing SQL:', sql);
      
      let result;
      if (this.options.useWorker) {
        result = await this.sendToWorker('exec', { sql });
      } else {
        const query = this.coordinator.query(Query.sql(sql));
        result = await query;
      }
      
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
      
      // Clear UI
      if (this.container) {
        this.container.innerHTML = '';
      }
      
      this.tableRenderer = null;
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