// DataTable.js - Main Library Entry Point
// ES6 client-side JavaScript library for creating ObservableHQ-like interactive data tables

export { DataTable } from './core/DataTable.js';
export { TableRenderer } from './core/TableRenderer.js';
export { InteractionManager } from './core/InteractionManager.js';
export { QueryCache } from './core/QueryCache.js';

export { DataLoader } from './data/DataLoader.js';
export { CloudStorage } from './data/CloudStorage.js';
export { DataConnector } from './data/DataConnector.js';

export { WorkerConnector } from './connectors/WorkerConnector.js';
export { BatchingConnector } from './connectors/BatchingConnector.js';

export { Histogram } from './visualizations/Histogram.js';
export { ValueCounts } from './visualizations/ValueCounts.js';
export { DateHistogram } from './visualizations/DateHistogram.js';

export { SQLEditor } from './sql/SQLEditor.js';
export { QueryBuilder } from './sql/QueryBuilder.js';
export { SchemaProvider } from './sql/SchemaProvider.js';

export { PersistenceManager } from './storage/PersistenceManager.js';
export { VersionControl } from './storage/VersionControl.js';
export { FileSystemAPI } from './storage/FileSystemAPI.js';

// Version info
export const VERSION = '0.1.0';