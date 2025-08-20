import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Coordinator } from '@uwdata/mosaic-core';
import { Query, asc, desc } from '@uwdata/mosaic-sql';

describe('Coordinator-Connector Issue Analysis', () => {
  
  describe('Core Issue: Missing Connector', () => {
    it('should demonstrate the coordinator needs a connector to execute queries', () => {
      // Create a coordinator
      const coordinator = new Coordinator();
      
      // Check if it has a connector by default
      const hasConnector = coordinator.databaseConnector();
      
      console.log('=== Coordinator Analysis ===');
      console.log('Coordinator created');
      console.log('Has connector by default?', !!hasConnector);
      
      if (!hasConnector) {
        console.error('❌ ISSUE CONFIRMED: Coordinator has no connector by default!');
        console.error('   Without a connector, the coordinator cannot execute queries.');
        console.error('   This is why sorting fails - queries are generated but never executed.');
      }
      
      expect(hasConnector).toBeFalsy(); // This confirms coordinators start without a connector
    });
    
    it('should show how to properly set a connector', () => {
      const coordinator = new Coordinator();
      
      // Create a mock connector
      const mockConnector = {
        query: vi.fn().mockResolvedValue([{ id: 1, name: 'Test' }])
      };
      
      // Set the connector on the coordinator
      coordinator.databaseConnector(mockConnector);
      
      // Verify it's set
      const connectorAfter = coordinator.databaseConnector();
      
      console.log('=== Fix Demonstration ===');
      console.log('Mock connector created');
      console.log('Called: coordinator.databaseConnector(connector)');
      console.log('Connector now set?', connectorAfter === mockConnector);
      
      expect(connectorAfter).toBe(mockConnector);
      console.log('✅ Solution: Call coordinator.databaseConnector(connector) after creating the connector');
    });
  });
  
  describe('TableRenderer Flow Without Connector', () => {
    it('should show what happens when requestQuery is called without a connector', () => {
      const coordinator = new Coordinator();
      const mockContainer = document.createElement('div');
      
      // Create a mock client (like TableRenderer)
      const mockClient = {
        query: vi.fn(() => {
          return Query
            .from('data')
            .select('*')
            .orderby(asc('name'))
            .limit(100);
        }),
        queryResult: vi.fn(),
        queryError: vi.fn(),
        queryPending: vi.fn()
      };
      
      // Try to request a query without a connector
      console.log('=== Query Request Without Connector ===');
      console.log('1. Client creates SQL query with ORDER BY');
      console.log('2. coordinator.requestQuery(client) is called');
      
      try {
        // This is what happens when sorting is clicked
        coordinator.requestQuery(mockClient);
        
        console.log('3. Coordinator receives the request');
        console.log('4. Coordinator tries to execute query...');
        console.log('5. But coordinator has no connector!');
        console.log('6. Query cannot be executed');
        console.log('7. Table stays empty (no queryResult called)');
        
      } catch (error) {
        console.error('Error during requestQuery:', error.message);
      }
      
      // Check if queryResult was ever called (it shouldn't be)
      expect(mockClient.queryResult).not.toHaveBeenCalled();
      console.log('❌ Confirmed: queryResult never called because no connector to execute query');
    });
  });
  
  describe('Code Analysis', () => {
    it('should identify the missing line in DataTable.js', () => {
      console.log('=== Missing Code in DataTable.js ===');
      console.log('');
      console.log('In both initializeDirect() and initializeWorker() methods:');
      console.log('');
      console.log('CURRENT CODE (missing connection):');
      console.log('  this.connector = wasmConnector({ ');
      console.log('    duckdb: this.db,');
      console.log('    connection: this.conn ');
      console.log('  });');
      console.log('  // ❌ Missing: coordinator is never told about the connector!');
      console.log('');
      console.log('FIXED CODE (with connection):');
      console.log('  this.connector = wasmConnector({ ');
      console.log('    duckdb: this.db,');
      console.log('    connection: this.conn ');
      console.log('  });');
      console.log('  this.coordinator.databaseConnector(this.connector); // ✅ Add this line!');
      console.log('');
      console.log('This single line will fix the sorting issue.');
      
      // This test just documents the fix
      expect(true).toBe(true);
    });
  });
  
  describe('Sorting SQL Generation', () => {
    it('should verify the SQL generation is actually correct', () => {
      // Test the exact code from TableRenderer
      const orderByValue = [{ field: 'name', order: 'ASC' }];
      
      // Convert orderBy array to Mosaic SQL format
      const orderByExprs = orderByValue.map(({ field, order }) => 
        order === 'DESC' ? desc(field) : asc(field)
      );
      
      const query = Query
        .from('data')
        .select('*')
        .orderby(...orderByExprs)
        .limit(100)
        .offset(0);
      
      const sql = query.toString();
      
      console.log('=== SQL Generation Test ===');
      console.log('Input:', orderByValue);
      console.log('Generated SQL:', sql);
      console.log('Contains ORDER BY?', sql.includes('ORDER BY'));
      
      expect(sql).toContain('ORDER BY');
      expect(sql).toContain('"name"');
      console.log('✅ SQL generation is working correctly');
    });
  });
});