import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Coordinator, wasmConnector } from '@uwdata/mosaic-core';

describe('Connector Verification', () => {
  let coordinator;
  let mockDuckDB;
  let mockConnection;

  beforeEach(() => {
    coordinator = new Coordinator();
    
    // Mock DuckDB and connection
    mockDuckDB = {
      query: vi.fn().mockResolvedValue([{ id: 1, name: 'test' }])
    };
    
    mockConnection = {
      query: vi.fn().mockResolvedValue([{ id: 1, name: 'test' }])
    };
  });

  it('should verify coordinator starts with a SocketConnector', () => {
    const initialConnector = coordinator.databaseConnector();
    
    console.log('=== Initial State ===');
    console.log('Coordinator has connector:', !!initialConnector);
    console.log('Connector type:', initialConnector?.constructor?.name);
    console.log('Is SocketConnector?', initialConnector?.constructor?.name === 'SocketConnector');
    
    expect(initialConnector).toBeDefined();
    expect(initialConnector.constructor.name).toBe('SocketConnector');
  });

  it('should verify we can replace the connector with wasmConnector', () => {
    const initialConnector = coordinator.databaseConnector();
    console.log('Initial connector type:', initialConnector.constructor.name);
    
    // Create wasmConnector (this is what DataTable does)
    const connector = wasmConnector({
      duckdb: mockDuckDB,
      connection: mockConnection
    });
    
    // Set it on the coordinator (this is the fix)
    coordinator.databaseConnector(connector);
    
    // Verify it worked
    const newConnector = coordinator.databaseConnector();
    
    console.log('=== After Setting wasmConnector ===');
    console.log('New connector type:', newConnector.constructor.name);
    console.log('Is WasmConnector?', newConnector.constructor.name.includes('Wasm'));
    console.log('Same instance?', newConnector === connector);
    
    expect(newConnector).toBe(connector);
    expect(newConnector.constructor.name).not.toBe('SocketConnector');
  });

  it('should test what happens when a query is executed', async () => {
    // Start with default SocketConnector
    console.log('=== Testing Query Execution ===');
    
    const mockClient = {
      query: () => ({ toString: () => 'SELECT * FROM data ORDER BY name ASC' }),
      queryResult: vi.fn(),
      queryError: vi.fn(),
      queryPending: vi.fn()
    };
    
    console.log('1. Client query:', mockClient.query().toString());
    
    // Try with default SocketConnector
    try {
      console.log('2. Trying with default SocketConnector...');
      coordinator.requestQuery(mockClient);
      // Wait a bit for async operations
      await new Promise(resolve => setTimeout(resolve, 100));
      
      if (mockClient.queryResult.mock.calls.length > 0) {
        console.log('✅ SocketConnector worked');
      } else if (mockClient.queryError.mock.calls.length > 0) {
        console.log('❌ SocketConnector failed with error');
      } else {
        console.log('❌ SocketConnector: no result, no error - likely connection failed');
      }
    } catch (error) {
      console.log('❌ SocketConnector threw error:', error.message);
    }
    
    // Reset mocks
    mockClient.queryResult.mockClear();
    mockClient.queryError.mockClear();
    
    // Now try with wasmConnector
    const wasmConn = wasmConnector({
      duckdb: mockDuckDB,
      connection: mockConnection
    });
    
    coordinator.databaseConnector(wasmConn);
    
    try {
      console.log('3. Trying with wasmConnector...');
      coordinator.requestQuery(mockClient);
      // Wait a bit for async operations
      await new Promise(resolve => setTimeout(resolve, 100));
      
      if (mockClient.queryResult.mock.calls.length > 0) {
        console.log('✅ wasmConnector worked - got result!');
      } else if (mockClient.queryError.mock.calls.length > 0) {
        console.log('❌ wasmConnector failed with error');
      } else {
        console.log('❌ wasmConnector: no result, no error');
      }
    } catch (error) {
      console.log('❌ wasmConnector threw error:', error.message);
    }
  });
});