/**
 * Batching Connector for Mosaic
 * 
 * Optimizes performance by batching multiple queries together
 * and managing query execution efficiently for visualizations.
 */
export class BatchingConnector {
  constructor(baseConnector, options = {}) {
    this.baseConnector = baseConnector;
    this.batchWindow = options.batchWindow || 10; // ms to wait for more queries
    this.maxBatchSize = options.maxBatchSize || 10;
    this.enabled = options.enabled !== false;
    
    // Batch management
    this.pendingQueries = [];
    this.batchTimer = null;
    this.executing = false;
    
    // Statistics
    this.stats = {
      queriesProcessed: 0,
      batchesExecuted: 0,
      totalBatchTime: 0,
      avgBatchSize: 0
    };
  }
  
  async query(request) {
    if (!this.enabled || this.shouldNotBatch(request)) {
      // Execute immediately for certain query types
      return this.baseConnector.query(request);
    }
    
    return new Promise((resolve, reject) => {
      // Add to pending batch
      this.pendingQueries.push({
        request,
        resolve,
        reject,
        timestamp: Date.now()
      });
      
      // Start batch timer if not already running
      if (!this.batchTimer) {
        this.scheduleBatch();
      }
      
      // Execute immediately if batch is full
      if (this.pendingQueries.length >= this.maxBatchSize) {
        this.executeBatch();
      }
    });
  }
  
  shouldNotBatch(request) {
    const sql = request.sql?.toLowerCase() || '';
    
    // Don't batch DDL operations
    if (sql.includes('create') || sql.includes('drop') || sql.includes('alter')) {
      return true;
    }
    
    // Don't batch SET commands
    if (sql.includes('set ') || sql.includes('pragma')) {
      return true;
    }
    
    // Don't batch very large queries (>1000 chars)
    if (sql.length > 1000) {
      return true;
    }
    
    return false;
  }
  
  scheduleBatch() {
    this.batchTimer = setTimeout(() => {
      this.executeBatch();
    }, this.batchWindow);
  }
  
  async executeBatch() {
    if (this.executing || this.pendingQueries.length === 0) {
      return;
    }
    
    // Clear timer
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    
    this.executing = true;
    const batch = this.pendingQueries.splice(0); // Take all pending queries
    const startTime = Date.now();
    
    try {
      // Group similar queries for optimization
      const groups = this.groupSimilarQueries(batch);
      
      // Execute each group
      for (const group of groups) {
        await this.executeQueryGroup(group);
      }
      
      // Update statistics
      this.updateStats(batch.length, Date.now() - startTime);
      
    } catch (error) {
      // If batch execution fails, reject all queries
      batch.forEach(({ reject }) => reject(error));
    } finally {
      this.executing = false;
      
      // If more queries arrived while executing, schedule another batch
      if (this.pendingQueries.length > 0) {
        this.scheduleBatch();
      }
    }
  }
  
  groupSimilarQueries(batch) {
    const groups = new Map();
    
    batch.forEach(item => {
      const key = this.getQueryGroupKey(item.request);
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(item);
    });
    
    return Array.from(groups.values());
  }
  
  getQueryGroupKey(request) {
    const sql = request.sql || '';
    const type = request.type || 'json';
    
    // Group by query pattern
    const normalized = sql
      .replace(/LIMIT \d+/gi, 'LIMIT ?')
      .replace(/OFFSET \d+/gi, 'OFFSET ?')
      .replace(/= \d+/g, '= ?')
      .replace(/= '[^']*'/g, "= '?'")
      .replace(/\s+/g, ' ')
      .trim();
    
    return `${type}:${normalized}`;
  }
  
  async executeQueryGroup(group) {
    if (group.length === 1) {
      // Single query - execute directly
      const { request, resolve, reject } = group[0];
      try {
        const result = await this.baseConnector.query(request);
        resolve(result);
      } catch (error) {
        reject(error);
      }
      return;
    }
    
    // Multiple similar queries - try to optimize
    await this.executeParallelQueries(group);
  }
  
  async executeParallelQueries(group) {
    // Execute queries in parallel for better performance
    const promises = group.map(async ({ request, resolve, reject }) => {
      try {
        const result = await this.baseConnector.query(request);
        resolve(result);
      } catch (error) {
        reject(error);
      }
    });
    
    await Promise.allSettled(promises);
  }
  
  updateStats(batchSize, executionTime) {
    this.stats.queriesProcessed += batchSize;
    this.stats.batchesExecuted++;
    this.stats.totalBatchTime += executionTime;
    this.stats.avgBatchSize = this.stats.queriesProcessed / this.stats.batchesExecuted;
  }
  
  getStats() {
    return {
      ...this.stats,
      avgExecutionTime: this.stats.batchesExecuted > 0 ? 
        this.stats.totalBatchTime / this.stats.batchesExecuted : 0,
      enabled: this.enabled,
      pendingQueries: this.pendingQueries.length
    };
  }
  
  setEnabled(enabled) {
    this.enabled = enabled;
    
    if (!enabled && this.pendingQueries.length > 0) {
      // Execute remaining queries immediately
      this.executeBatch();
    }
  }
  
  // Delegate other methods to base connector
  async getDuckDB() {
    return this.baseConnector.getDuckDB();
  }
  
  async getConnection() {
    return this.baseConnector.getConnection();
  }
  
  destroy() {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    
    // Reject any pending queries
    this.pendingQueries.forEach(({ reject }) => {
      reject(new Error('Connector destroyed'));
    });
    this.pendingQueries = [];
  }
}