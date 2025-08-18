// PersistenceManager for IndexedDB operations
export class PersistenceManager {
  constructor(tableName) {
    this.tableName = tableName;
    this.dbName = 'DataTableJS';
    this.db = null;
  }

  async initialize() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 1);
      
      request.onerror = () => reject(request.error);
      request.onsuccess = () => {
        this.db = request.result;
        resolve();
      };
      
      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        
        if (!db.objectStoreNames.contains('tables')) {
          db.createObjectStore('tables', { keyPath: 'name' });
        }
        
        if (!db.objectStoreNames.contains('snapshots')) {
          db.createObjectStore('snapshots', { keyPath: 'id' });
        }
        
        if (!db.objectStoreNames.contains('commands')) {
          db.createObjectStore('commands', { keyPath: 'id' });
        }
      };
    });
  }

  async saveTable(data) {
    const transaction = this.db.transaction(['tables'], 'readwrite');
    const store = transaction.objectStore('tables');
    
    return store.put({
      name: this.tableName,
      data: data,
      timestamp: Date.now()
    });
  }

  async loadTable() {
    const transaction = this.db.transaction(['tables'], 'readonly');
    const store = transaction.objectStore('tables');
    
    return new Promise((resolve, reject) => {
      const request = store.get(this.tableName);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  async clearTable() {
    const transaction = this.db.transaction(['tables'], 'readwrite');
    const store = transaction.objectStore('tables');
    return store.delete(this.tableName);
  }
  
  close() {
    if (this.db) {
      this.db.close();
    }
  }
}