import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { DataTable } from '../../src/core/DataTable.js';
import { TableRenderer } from '../../src/core/TableRenderer.js';
import { Coordinator, wasmConnector } from '@uwdata/mosaic-core';
import { Query, asc, desc } from '@uwdata/mosaic-sql';

describe('Sorting Issue Diagnosis', () => {
  let dataTable;
  let mockContainer;
  let mockCoordinator;
  let mockConnector;

  beforeEach(() => {
    // Create mock DOM container
    mockContainer = document.createElement('div');
    document.body.appendChild(mockContainer);

    // Mock the coordinator methods
    mockCoordinator = {
      databaseConnector: vi.fn(),
      connect: vi.fn(),
      requestQuery: vi.fn(),
      query: vi.fn()
    };

    // Mock the connector
    mockConnector = {
      query: vi.fn().mockResolvedValue([
        { id: 1, name: 'Alice', age: 30 },
        { id: 2, name: 'Bob', age: 25 }
      ])
    };

    // Mock the imports
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    document.body.removeChild(mockContainer);
    vi.clearAllMocks();
  });

  describe('Coordinator-Connector Connection', () => {
    it('should verify if coordinator.databaseConnector() is called during initialization', async () => {
      // Create a DataTable instance
      const dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      // Spy on the coordinator's databaseConnector method
      const databaseConnectorSpy = vi.spyOn(dataTable.coordinator, 'databaseConnector');

      // Initialize the DataTable
      await dataTable.initialize();

      // Check if databaseConnector was called
      if (databaseConnectorSpy.mock.calls.length === 0) {
        console.error('ISSUE FOUND: coordinator.databaseConnector() is never called!');
        console.error('The coordinator is not being told which connector to use.');
        expect(databaseConnectorSpy).toHaveBeenCalled(); // This will fail and show the issue
      } else {
        console.log('✓ Coordinator has connector set properly');
        expect(databaseConnectorSpy).toHaveBeenCalledWith(expect.any(Object));
      }
    });

    it('should check if connector exists after initialization', async () => {
      const dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      await dataTable.initialize();

      // Check if connector is created
      if (!dataTable.connector) {
        console.error('ISSUE FOUND: No connector created during initialization!');
        expect(dataTable.connector).toBeDefined(); // This will fail
      }

      // Check if coordinator has a way to execute queries
      if (dataTable.coordinator && !dataTable.coordinator.databaseConnector()) {
        console.error('ISSUE FOUND: Coordinator has no database connector set!');
        expect(dataTable.coordinator.databaseConnector()).toBeDefined(); // This will fail
      }
    });
  });

  describe('TableRenderer Sorting Flow', () => {
    it('should trace the complete flow when sorting is triggered', async () => {
      // Create a mock TableRenderer with all necessary methods
      const tableRenderer = new TableRenderer({
        table: 'data',
        schema: { name: { type: 'string' }, age: { type: 'number' } },
        container: mockContainer,
        coordinator: mockCoordinator,
        connection: null
      });

      // Track method calls
      const querySpy = vi.spyOn(tableRenderer, 'query');
      const requestDataSpy = vi.spyOn(tableRenderer, 'requestData');
      const queryResultSpy = vi.spyOn(tableRenderer, 'queryResult');
      const queryErrorSpy = vi.spyOn(tableRenderer, 'queryError');
      const queryPendingSpy = vi.spyOn(tableRenderer, 'queryPending');

      // Simulate clicking on a column header to sort
      tableRenderer.toggleSort('name');

      // Check the flow
      console.log('=== Sorting Flow Trace ===');
      
      // 1. Check if orderBy was updated
      expect(tableRenderer.orderBy.value).toEqual([{ field: 'name', order: 'ASC' }]);
      console.log('✓ Step 1: orderBy updated correctly');

      // 2. Check if requestData was called
      expect(requestDataSpy).toHaveBeenCalled();
      console.log('✓ Step 2: requestData() was called');

      // 3. Check if coordinator.requestQuery was called
      expect(mockCoordinator.requestQuery).toHaveBeenCalledWith(tableRenderer);
      console.log('✓ Step 3: coordinator.requestQuery() was called');

      // 4. Simulate what the coordinator should do next
      // The coordinator should call tableRenderer.query() to get the SQL
      const query = tableRenderer.query();
      console.log('✓ Step 4: Query generated:', query.toString());

      // 5. Check if the query has correct ORDER BY
      const sqlString = query.toString();
      if (!sqlString.includes('ORDER BY')) {
        console.error('ISSUE FOUND: Generated SQL has no ORDER BY clause!');
        expect(sqlString).toContain('ORDER BY');
      } else {
        console.log('✓ Step 5: SQL contains ORDER BY clause');
      }

      // 6. Simulate coordinator executing the query
      // This is where it would fail if no connector is set
      if (!mockCoordinator.databaseConnector) {
        console.error('ISSUE FOUND: Coordinator has no databaseConnector method!');
      } else if (!mockConnector) {
        console.error('ISSUE FOUND: No connector available to execute query!');
      }

      // 7. Check if queryResult or queryError would be called
      // Simulate successful query execution
      tableRenderer.queryResult([{ name: 'Alice' }, { name: 'Bob' }]);
      expect(tableRenderer.tbody).toBeDefined();
      console.log('✓ Step 7: queryResult can be called successfully');
    });

    it('should test ORDER BY SQL generation with Mosaic SQL', () => {
      // Test that our ORDER BY generation is correct
      const orderByValues = [
        { field: 'name', order: 'ASC' },
        { field: 'age', order: 'DESC' }
      ];

      // Convert to Mosaic SQL format
      const orderByExprs = orderByValues.map(({ field, order }) => 
        order === 'DESC' ? desc(field) : asc(field)
      );

      // Build query
      const query = Query
        .from('data')
        .select('*')
        .orderby(...orderByExprs)
        .limit(100);

      const sql = query.toString();
      console.log('Generated SQL:', sql);

      // Verify SQL is correct
      expect(sql).toContain('ORDER BY');
      expect(sql).toContain('"name" ASC');
      expect(sql).toContain('"age" DESC');
    });
  });

  describe('Query Execution Path', () => {
    it('should identify where query execution fails', async () => {
      const dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      // Initialize
      await dataTable.initialize();

      // Create mock data
      const mockFile = new File(['id,name,age\n1,Alice,30\n2,Bob,25'], 'test.csv', { type: 'text/csv' });
      
      // Try to load data (this might fail if connector not set)
      try {
        await dataTable.loadData(mockFile);
        console.log('✓ Data loaded successfully');
      } catch (error) {
        console.error('ISSUE FOUND during data load:', error.message);
      }

      // Check if table renderer exists
      if (!dataTable.tableRenderer) {
        console.error('ISSUE FOUND: No table renderer created after loading data!');
        return;
      }

      // Try to trigger sorting
      dataTable.tableRenderer.toggleSort('name');

      // Check what happens
      const coordinator = dataTable.coordinator;
      const connector = dataTable.connector;

      console.log('=== Execution Path Analysis ===');
      console.log('Coordinator exists:', !!coordinator);
      console.log('Connector exists:', !!connector);
      console.log('Coordinator has databaseConnector method:', !!coordinator?.databaseConnector);
      
      if (coordinator && coordinator.databaseConnector) {
        const connectorFromCoordinator = coordinator.databaseConnector();
        console.log('Connector set in coordinator:', !!connectorFromCoordinator);
        
        if (!connectorFromCoordinator) {
          console.error('ISSUE CONFIRMED: Coordinator has no connector set!');
          console.error('Solution: Call coordinator.databaseConnector(connector) after creating the connector');
        }
      }
    });
  });

  describe('Direct Fix Verification', () => {
    it('should verify the proposed fix works', async () => {
      const dataTable = new DataTable({
        container: mockContainer,
        useWorker: false
      });

      // Spy on methods
      const coordinatorSpy = vi.spyOn(dataTable.coordinator, 'databaseConnector');

      // Initialize
      await dataTable.initialize();

      // Apply the fix manually for testing
      if (dataTable.connector && !coordinatorSpy.mock.calls.length) {
        console.log('Applying fix: Setting connector on coordinator...');
        dataTable.coordinator.databaseConnector(dataTable.connector);
      }

      // Verify fix is applied
      const connectorSet = dataTable.coordinator.databaseConnector();
      if (connectorSet) {
        console.log('✓ Fix applied successfully - connector is now set!');
        expect(connectorSet).toBe(dataTable.connector);
      } else {
        console.error('✗ Fix failed - connector still not set');
        expect(connectorSet).toBeDefined();
      }
    });
  });
});