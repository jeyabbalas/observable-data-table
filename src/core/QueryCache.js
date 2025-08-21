/**
 * Query Result Cache with TTL for performance optimization
 * 
 * Caches query results to avoid repeated DuckDB operations
 * for the same query within a time window.
 */
export class QueryCache {
  constructor(options = {}) {
    this.cache = new Map();
    this.ttl = options.ttl || 60000; // Default 1 minute TTL
    this.maxSize = options.maxSize || 100; // Max cache entries
    this.enabled = options.enabled !== false;
    
    // Track cache statistics
    this.stats = {
      hits: 0,
      misses: 0,
      evictions: 0
    };
    
    // Periodic cleanup of expired entries
    this.cleanupInterval = setInterval(() => {
      this.cleanup();
    }, this.ttl / 2);
  }
  
  /**
   * Generate cache key from SQL query
   */
  generateKey(sql, options = {}) {
    // Normalize SQL for better cache hits
    const normalizedSQL = sql
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
    
    // Include relevant options in key
    const keyParts = [normalizedSQL];
    if (options.streaming) keyParts.push('streaming');
    if (options.limit) keyParts.push(`limit:${options.limit}`);
    if (options.offset) keyParts.push(`offset:${options.offset}`);
    
    return keyParts.join('|');
  }
  
  /**
   * Get cached result if available and not expired
   */
  get(sql, options = {}) {
    if (!this.enabled) return null;
    
    const key = this.generateKey(sql, options);
    const entry = this.cache.get(key);
    
    if (!entry) {
      this.stats.misses++;
      return null;
    }
    
    // Check if expired
    if (Date.now() > entry.expires) {
      this.cache.delete(key);
      this.stats.misses++;
      return null;
    }
    
    // Move to end (LRU)
    this.cache.delete(key);
    this.cache.set(key, entry);
    
    this.stats.hits++;
    return entry.result;
  }
  
  /**
   * Cache query result with TTL
   */
  set(sql, result, options = {}) {
    if (!this.enabled) return;
    
    // Don't cache certain types of queries
    const sqlLower = sql.toLowerCase().trim();
    if (this.shouldNotCache(sqlLower)) {
      return;
    }
    
    // Don't cache very large results
    const resultSize = this.estimateSize(result);
    if (resultSize > 10 * 1024 * 1024) { // 10MB limit
      return;
    }
    
    const key = this.generateKey(sql, options);
    const expires = Date.now() + this.ttl;
    
    // Enforce max size (LRU eviction)
    if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
      this.stats.evictions++;
    }
    
    this.cache.set(key, {
      result: this.cloneResult(result),
      expires,
      size: resultSize,
      cached: Date.now()
    });
  }
  
  /**
   * Determine if query should not be cached
   */
  shouldNotCache(sqlLower) {
    // Don't cache DDL operations
    if (sqlLower.startsWith('create') || 
        sqlLower.startsWith('drop') || 
        sqlLower.startsWith('alter') ||
        sqlLower.startsWith('insert') ||
        sqlLower.startsWith('update') ||
        sqlLower.startsWith('delete')) {
      return true;
    }
    
    // Don't cache PRAGMA or SET commands
    if (sqlLower.startsWith('pragma') || sqlLower.startsWith('set')) {
      return true;
    }
    
    // Don't cache queries with random functions
    if (sqlLower.includes('random()') || 
        sqlLower.includes('uuid()') ||
        sqlLower.includes('now()') ||
        sqlLower.includes('current_timestamp')) {
      return true;
    }
    
    return false;
  }
  
  /**
   * Estimate result size in bytes
   */
  estimateSize(result) {
    if (!result) return 0;
    
    try {
      // For Arrow tables, estimate based on row count and column count
      if (result.numRows !== undefined && result.schema) {
        const avgRowSize = result.schema.fields.length * 20; // Rough estimate
        return result.numRows * avgRowSize;
      }
      
      // For arrays, use JSON stringify as rough estimate
      if (Array.isArray(result)) {
        if (result.length === 0) return 0;
        const sampleSize = JSON.stringify(result[0] || {}).length;
        return result.length * sampleSize;
      }
      
      // Fallback to JSON stringify
      return JSON.stringify(result).length * 2; // UTF-16 chars
    } catch (e) {
      return 1024; // 1KB fallback
    }
  }
  
  /**
   * Clone result for caching (avoid reference issues)
   */
  cloneResult(result) {
    // For Arrow tables, we can cache the reference since they're immutable
    if (result && typeof result === 'object' && result.toArray) {
      return result;
    }
    
    // For arrays and objects, create a deep copy
    try {
      return JSON.parse(JSON.stringify(result));
    } catch (e) {
      // If can't serialize, return original (risky but better than crash)
      return result;
    }
  }
  
  /**
   * Clean up expired entries
   */
  cleanup() {
    const now = Date.now();
    let removed = 0;
    
    for (const [key, entry] of this.cache.entries()) {
      if (now > entry.expires) {
        this.cache.delete(key);
        removed++;
      }
    }
    
    if (removed > 0) {
      this.stats.evictions += removed;
    }
  }
  
  /**
   * Clear all cached entries
   */
  clear() {
    this.cache.clear();
    this.stats = { hits: 0, misses: 0, evictions: 0 };
  }
  
  /**
   * Get cache statistics
   */
  getStats() {
    const total = this.stats.hits + this.stats.misses;
    const hitRate = total > 0 ? (this.stats.hits / total * 100).toFixed(1) : 0;
    
    return {
      ...this.stats,
      total,
      hitRate: `${hitRate}%`,
      size: this.cache.size,
      enabled: this.enabled
    };
  }
  
  /**
   * Enable or disable caching
   */
  setEnabled(enabled) {
    this.enabled = enabled;
    if (!enabled) {
      this.clear();
    }
  }
  
  /**
   * Destroy cache and cleanup intervals
   */
  destroy() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    this.clear();
  }
}