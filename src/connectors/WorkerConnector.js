/**
 * Mosaic Connector for DuckDB Web Worker
 * 
 * This connector implements the Mosaic Connector interface
 * and proxies all queries to the DuckDB Web Worker.
 */
export class WorkerConnector {
  constructor(dataTable) {
    this.dataTable = dataTable;
  }

  /**
   * Execute a query through the Web Worker
   * @param {Object} request Query request object
   * @returns {Promise} Query result
   */
  async query(request) {
    const { type, sql } = request;
    
    try {
      // Validate that DataTable and worker are available
      if (!this.dataTable) {
        throw new Error('DataTable not available in WorkerConnector');
      }
      
      if (!this.dataTable.worker) {
        throw new Error('Worker not initialized - cannot execute query');
      }
      
      if (type === 'exec') {
        // Execute query without expecting results
        await this.dataTable.sendToWorker('exec', { sql });
        return undefined;
      } else {
        // Execute query and return results
        const result = await this.dataTable.sendToWorker('exec', { sql });
        
        if (type === 'arrow') {
          // For arrow format, we would need to handle Arrow IPC
          // For now, return JSON format which is more compatible
          return result;
        } else {
          // JSON format (default)
          return result;
        }
      }
    } catch (error) {
      console.error('WorkerConnector query failed:', error);
      console.error('Query details:', { type, sql: sql?.substring(0, 100) + '...' });
      throw new Error(`Worker query failed: ${error.message}`);
    }
  }

  /**
   * Get the DuckDB instance (not available in worker mode)
   */
  async getDuckDB() {
    throw new Error('DuckDB instance not available in worker mode');
  }

  /**
   * Get the DuckDB connection (not available in worker mode)
   */
  async getConnection() {
    throw new Error('DuckDB connection not available in worker mode');
  }
}