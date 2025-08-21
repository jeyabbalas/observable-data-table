import { DataTable } from '@datatable/core';

class DataTableApp {
  constructor() {
    this.dataTable = null;
    this.isLoading = false;
    
    // State
    this.currentFile = null;
    this.rowCount = 0;
    this.columnCount = 0;
    
    // Bind methods
    this.handleFileInput = this.handleFileInput.bind(this);
    this.handleURLLoad = this.handleURLLoad.bind(this);
    this.handleSampleLoad = this.handleSampleLoad.bind(this);
    this.handleClearData = this.handleClearData.bind(this);
    this.handleCopySQL = this.handleCopySQL.bind(this);
    this.handleExecuteSQL = this.handleExecuteSQL.bind(this);
    this.handleClearFilters = this.handleClearFilters.bind(this);
    this.handleUndo = this.handleUndo.bind(this);
    this.handleRedo = this.handleRedo.bind(this);
  }
  
  async initialize() {
    try {
      // Just setup event listeners and UI - don't initialize DataTable until needed
      this.setupEventListeners();
      
      // Update UI
      this.updateStatus('Ready - Load data to begin');
      this.updateRowColumnCount(0, 0);
      
      this.showNotification('Welcome! Load some data to get started.', 'info');
      
    } catch (error) {
      this.showNotification(`Failed to initialize UI: ${error.message}`, 'error');
      console.error('UI initialization error:', error);
    }
  }

  async initializeDataTable() {
    if (this.dataTable) return; // Already initialized
    
    try {
      this.showLoading('Initializing DataTable...');
      
      // Get options from UI
      const persistSession = document.getElementById('persistSession').checked;
      const useWorker = document.getElementById('useWorker').checked;
      
      // Initialize DataTable
      this.dataTable = new DataTable({
        container: document.getElementById('tableContainer'),
        height: 400,
        persistSession,
        useWorker,
        logLevel: 'info'
      });
      
      // 🚀 Task 2: Setup debug panel event listener
      this.setupDebugPanel();
      
      await this.dataTable.initialize();
      
      this.hideLoading();
      this.showNotification('DataTable initialized successfully!', 'success');
      
    } catch (error) {
      this.hideLoading();
      this.showNotification(`Failed to initialize DataTable: ${error.message}`, 'error');
      console.error('DataTable initialization error:', error);
      throw error;
    }
  }
  
  setupEventListeners() {
    // File input
    document.getElementById('fileInput').addEventListener('change', this.handleFileInput);
    
    // URL loading
    document.getElementById('loadUrl').addEventListener('click', this.handleURLLoad);
    
    // Sample data
    document.getElementById('loadSample').addEventListener('click', this.handleSampleLoad);
    
    // Table controls
    document.getElementById('clearData').addEventListener('click', this.handleClearData);
    document.getElementById('exportData').addEventListener('click', this.handleExportData);
    
    // SQL controls
    document.getElementById('copySql').addEventListener('click', this.handleCopySQL);
    document.getElementById('executeSql').addEventListener('click', this.handleExecuteSQL);
    
    // Filter controls
    document.getElementById('clearFilters').addEventListener('click', this.handleClearFilters);
    
    // Version control
    document.getElementById('undoBtn').addEventListener('click', this.handleUndo);
    document.getElementById('redoBtn').addEventListener('click', this.handleRedo);
    
    // Options change
    document.getElementById('persistSession').addEventListener('change', this.handleOptionsChange);
    document.getElementById('useWorker').addEventListener('change', this.handleOptionsChange);
  }
  
  async handleFileInput(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    this.currentFile = file;
    
    try {
      // Initialize DataTable if needed
      await this.initializeDataTable();
      
      this.showLoading(`Loading ${file.name}...`);
      
      await this.dataTable.loadData(file);
      
      // Update UI
      document.getElementById('fileName').textContent = file.name;
      this.updateDataLoadedState();
      
      this.hideLoading();
      this.showNotification(`Successfully loaded ${file.name}`, 'success');
      
    } catch (error) {
      this.hideLoading();
      this.showNotification(`Failed to load file: ${error.message}`, 'error');
      console.error('File loading error:', error);
    }
  }
  
  async handleURLLoad() {
    const url = document.getElementById('urlInput').value.trim();
    if (!url) {
      this.showNotification('Please enter a valid URL', 'warning');
      return;
    }
    
    try {
      // Initialize DataTable if needed
      await this.initializeDataTable();
      
      this.showLoading(`Loading data from URL...`);
      
      await this.dataTable.loadData(url);
      
      // Update UI
      this.updateDataLoadedState();
      
      this.hideLoading();
      this.showNotification('Successfully loaded data from URL', 'success');
      
    } catch (error) {
      this.hideLoading();
      this.showNotification(`Failed to load URL: ${error.message}`, 'error');
      console.error('URL loading error:', error);
    }
  }
  
  async handleSampleLoad() {
    try {
      // Initialize DataTable if needed
      await this.initializeDataTable();
      
      this.showLoading('Loading sample dataset...');
      
      // Create sample CSV data
      const sampleData = this.generateSampleData();
      const blob = new Blob([sampleData], { type: 'text/csv' });
      const file = new File([blob], 'sample-data.csv', { type: 'text/csv' });
      
      await this.dataTable.loadData(file);
      
      // Update UI
      document.getElementById('fileName').textContent = 'sample-data.csv';
      this.updateDataLoadedState();
      
      this.hideLoading();
      this.showNotification('Sample dataset loaded successfully', 'success');
      
    } catch (error) {
      this.hideLoading();
      this.showNotification(`Failed to load sample data: ${error.message}`, 'error');
      console.error('Sample loading error:', error);
    }
  }
  
  generateSampleData() {
    const headers = ['id', 'name', 'age', 'city', 'salary', 'department', 'hire_date'];
    const departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'];
    const cities = ['New York', 'San Francisco', 'Chicago', 'Boston', 'Seattle', 'Austin'];
    const names = ['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson', 'Emma Brown', 'Frank Miller', 'Grace Lee', 'Henry Chen', 'Ivy Rodriguez', 'Jack Thompson'];
    
    let csv = headers.join(',') + '\n';
    
    for (let i = 1; i <= 1000; i++) {
      const name = names[Math.floor(Math.random() * names.length)];
      const age = Math.floor(Math.random() * 40) + 25;
      const city = cities[Math.floor(Math.random() * cities.length)];
      const salary = Math.floor(Math.random() * 100000) + 50000;
      const department = departments[Math.floor(Math.random() * departments.length)];
      const hireDate = new Date(2020 + Math.floor(Math.random() * 5), Math.floor(Math.random() * 12), Math.floor(Math.random() * 28) + 1).toISOString().split('T')[0];
      
      csv += `${i},"${name}",${age},"${city}",${salary},"${department}","${hireDate}"\n`;
    }
    
    return csv;
  }
  
  async handleClearData() {
    if (!this.dataTable) {
      this.showNotification('No data to clear', 'warning');
      return;
    }
    
    try {
      this.showLoading('Clearing data...');
      
      await this.dataTable.clearData();
      
      // Reset UI state but don't interfere with DataTable's container management
      this.resetDataLoadedStateWithoutContainer();
      
      this.hideLoading();
      this.showNotification('Data cleared successfully', 'success');
      
    } catch (error) {
      this.hideLoading();
      this.showNotification(`Failed to clear data: ${error.message}`, 'error');
      console.error('Clear data error:', error);
    }
  }
  
  handleExportData() {
    if (!this.dataTable || !this.dataTable.tableName.value) {
      this.showNotification('No data to export', 'warning');
      return;
    }
    
    // TODO: Implement data export functionality
    this.showNotification('Export functionality coming soon!', 'info');
  }
  
  handleCopySQL() {
    if (!this.dataTable) return;
    
    const sql = this.dataTable.getCurrentSQL();
    if (!sql) {
      this.showNotification('No SQL to copy', 'warning');
      return;
    }
    
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(sql).then(() => {
        this.showNotification('SQL copied to clipboard', 'success');
      }).catch(() => {
        this.fallbackCopySQL(sql);
      });
    } else {
      this.fallbackCopySQL(sql);
    }
  }
  
  fallbackCopySQL(sql) {
    const textArea = document.createElement('textarea');
    textArea.value = sql;
    textArea.style.position = 'fixed';
    textArea.style.opacity = '0';
    document.body.appendChild(textArea);
    textArea.select();
    
    try {
      document.execCommand('copy');
      this.showNotification('SQL copied to clipboard', 'success');
    } catch (err) {
      this.showNotification('Failed to copy SQL', 'error');
    }
    
    document.body.removeChild(textArea);
  }
  
  async handleExecuteSQL() {
    // TODO: Implement SQL execution when SQL editor is ready
    this.showNotification('SQL editor coming soon!', 'info');
  }
  
  handleClearFilters() {
    // TODO: Implement filter clearing when filters are ready
    this.showNotification('Filter functionality coming soon!', 'info');
  }
  
  async handleUndo() {
    if (!this.dataTable || !this.dataTable.versionControl) return;
    
    try {
      await this.dataTable.versionControl.undo();
      this.showNotification('Undone successfully', 'success');
    } catch (error) {
      this.showNotification(`Undo failed: ${error.message}`, 'error');
    }
  }
  
  async handleRedo() {
    if (!this.dataTable || !this.dataTable.versionControl) return;
    
    try {
      await this.dataTable.versionControl.redo();
      this.showNotification('Redone successfully', 'success');
    } catch (error) {
      this.showNotification(`Redo failed: ${error.message}`, 'error');
    }
  }
  
  handleOptionsChange() {
    // Options changes require reinitialization
    if (this.dataTable) {
      this.showNotification('Please reload the page to apply option changes', 'info');
    }
  }
  
  updateDataLoadedState() {
    // Enable controls
    document.getElementById('clearData').disabled = false;
    document.getElementById('exportData').disabled = false;
    document.getElementById('copySql').disabled = false;
    document.getElementById('executeSql').disabled = false;
    
    // Update row/column count
    // TODO: Get actual counts from DataTable
    this.updateRowColumnCount(1000, 7);
    
    // Update status
    this.updateStatus('Data loaded');
  }
  
  resetDataLoadedState() {
    // Disable controls
    document.getElementById('clearData').disabled = true;
    document.getElementById('exportData').disabled = true;
    document.getElementById('copySql').disabled = true;
    document.getElementById('executeSql').disabled = true;
    
    // Clear file name
    document.getElementById('fileName').textContent = '';
    document.getElementById('urlInput').value = '';
    
    // Reset counts
    this.updateRowColumnCount(0, 0);
    
    // Update status
    this.updateStatus('Ready');
    
    // Restore empty state in table container
    const tableContainer = document.getElementById('tableContainer');
    tableContainer.innerHTML = `
      <div class="empty-state">
        <div class="empty-icon">📊</div>
        <h3>No Data Loaded</h3>
        <p>Upload a file or load data from a URL to get started</p>
      </div>
    `;
  }
  
  resetDataLoadedStateWithoutContainer() {
    // Disable controls
    document.getElementById('clearData').disabled = true;
    document.getElementById('exportData').disabled = true;
    document.getElementById('copySql').disabled = true;
    document.getElementById('executeSql').disabled = true;
    
    // Clear file name
    document.getElementById('fileName').textContent = '';
    document.getElementById('urlInput').value = '';
    
    // Reset counts
    this.updateRowColumnCount(0, 0);
    
    // Update status
    this.updateStatus('Ready');
    
    // Don't touch the table container - let DataTable manage it
  }
  
  updateStatus(text) {
    document.getElementById('statusText').textContent = text;
  }
  
  updateRowColumnCount(rows, columns) {
    this.rowCount = rows;
    this.columnCount = columns;
    document.getElementById('rowCount').textContent = `${rows.toLocaleString()} rows`;
    document.getElementById('columnCount').textContent = `${columns} columns`;
  }
  
  showLoading(text = 'Loading...') {
    this.isLoading = true;
    document.getElementById('loadingText').textContent = text;
    document.getElementById('loadingOverlay').style.display = 'flex';
  }
  
  hideLoading() {
    this.isLoading = false;
    document.getElementById('loadingOverlay').style.display = 'none';
  }
  
  showNotification(message, type = 'info') {
    const container = document.getElementById('notifications');
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.textContent = message;
    
    container.appendChild(notification);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
      if (notification.parentNode) {
        notification.parentNode.removeChild(notification);
      }
    }, 5000);
  }
  
  // 🚀 Task 2: Debug Panel Methods
  
  setupDebugPanel() {
    if (!this.dataTable) return;
    
    // Listen for progress events from DataTable
    document.getElementById('tableContainer').addEventListener('datatable-progress', (event) => {
      this.updateDebugPanel(event.detail);
    });
    
    // Initial debug panel setup
    this.updateDebugPanel({
      progress: this.dataTable.progress,
      performance: this.dataTable.performance
    });
  }
  
  updateDebugPanel(detail) {
    const { progress, performance, operation } = detail;
    
    // Update task status
    if (progress) {
      const task2Badge = document.getElementById('task2Badge');
      if (task2Badge) {
        if (progress.task2Complete) {
          task2Badge.textContent = '✅';
          task2Badge.className = 'task-badge complete';
        } else if (progress.task2InProgress) {
          task2Badge.textContent = '🔄';
          task2Badge.className = 'task-badge in-progress';
        }
      }
      
      // 🚀 Task 3: Update Task 3 badge
      const task3Badge = document.getElementById('task3Badge');
      if (task3Badge) {
        if (progress.task3Complete) {
          task3Badge.textContent = '✅';
          task3Badge.className = 'task-badge complete';
        } else if (progress.task3InProgress) {
          task3Badge.textContent = '🔄';
          task3Badge.className = 'task-badge in-progress';
        }
      }
    }
    
    // Update system info
    if (performance) {
      document.getElementById('debugMode').textContent = performance.mode || 'Not initialized';
      
      // Format DuckDB version
      const version = performance.duckdbVersion || 'Unknown';
      document.getElementById('debugVersion').textContent = version.length > 20 ? 
        version.substring(0, 20) + '...' : version;
      
      document.getElementById('debugBundle').textContent = performance.bundleType || 'Unknown';
      
      // Calculate and display init time
      if (performance.initStartTime && performance.initEndTime) {
        const initTime = performance.initEndTime - performance.initStartTime;
        document.getElementById('debugInitTime').textContent = `${initTime}ms`;
      }
      
      // Update memory info
      if (performance.memoryUsage) {
        if (typeof performance.memoryUsage === 'object') {
          document.getElementById('debugMemory').textContent = 
            `${performance.memoryUsage.used}/${performance.memoryUsage.total}`;
        } else {
          document.getElementById('debugMemory').textContent = performance.memoryUsage;
        }
      }
    }
    
    // Update last operation
    if (operation || (progress && progress.currentOperation)) {
      const lastOp = operation || progress.currentOperation;
      document.getElementById('debugLastOp').textContent = lastOp.length > 25 ? 
        lastOp.substring(0, 25) + '...' : lastOp;
    }
    
    // Update data status if DataTable exists
    if (this.dataTable) {
      const tableName = this.dataTable.tableName?.value || 'None';
      document.getElementById('debugTable').textContent = tableName;
      
      const schema = this.dataTable.schema?.value || {};
      document.getElementById('debugColumns').textContent = Object.keys(schema).length;
      
      // Try to get row count from latest operation
      if (detail.rowCount) {
        document.getElementById('debugRows').textContent = detail.rowCount;
      }
    }
  }
}

// Initialize app when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  const app = new DataTableApp();
  app.initialize();
  
  // Make app globally accessible for debugging
  window.dataTableApp = app;
});