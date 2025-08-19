import { describe, it, expect, beforeEach, vi } from 'vitest';

// Mock the real Mosaic components
vi.mock('@uwdata/mosaic-core', () => ({
  MosaicClient: class MockMosaicClient {
    constructor(filterSelection) {
      this._filterBy = filterSelection;
      this._requestUpdate = vi.fn();
      this._coordinator = null;
      this._pending = Promise.resolve();
      this._enabled = true;
      this._initialized = false;
      this._request = null;
      
      console.log('🏗️ MosaicClient constructor called');
    }
    
    get coordinator() { return this._coordinator; }
    set coordinator(coordinator) { 
      console.log('🔗 Setting coordinator:', !!coordinator);
      this._coordinator = coordinator; 
    }
    
    async prepare() { 
      console.log('🛠️ MosaicClient.prepare() called');
      return Promise.resolve(); 
    }
    
    query(filter) { 
      console.log('🔍 MosaicClient.query() called with filter:', filter);
      return null; 
    }
    
    requestQuery(query) {
      console.log('📨 MosaicClient.requestQuery() called with query:', !!query);
      if (this._enabled && this._coordinator) {
        const q = query || this.query(this.filterBy?.predicate?.(this));
        console.log('📨 Forwarding to coordinator.requestQuery with:', !!q);
        return this._coordinator.requestQuery(this, q);
      } else {
        console.log('❌ Cannot request query - enabled:', this._enabled, 'coordinator:', !!this._coordinator);
        return null;
      }
    }
    
    initialize() {
      console.log('🚀 MosaicClient.initialize() called');
      if (!this._enabled) {
        console.log('❌ Not enabled, skipping initialization');
        this._initialized = false;
      } else if (this._coordinator) {
        console.log('✅ Enabled with coordinator, proceeding with initialization');
        this._initialized = true;
        this._pending = this.prepare().then(() => {
          console.log('🛠️ Prepare complete, calling requestQuery');
          return this.requestQuery();
        });
      } else {
        console.log('❌ No coordinator available for initialization');
      }
    }
    
    queryResult(data) { 
      console.log('📊 MosaicClient.queryResult() called with:', Array.isArray(data) ? `${data.length} rows` : typeof data);
      return this; 
    }
  },
  
  Selection: {
    crossfilter: vi.fn(() => ({}))
  },
  
  Coordinator: vi.fn().mockImplementation(() => ({
    connect: vi.fn().mockImplementation((client) => {
      console.log('🔌 Coordinator.connect() called with client');
      client.coordinator = this;
      console.log('🚀 Coordinator calling client.initialize()');
      client.initialize();
    }),
    requestQuery: vi.fn().mockImplementation((client, query) => {
      console.log('📨 Coordinator.requestQuery() called with query:', !!query);
      return new Promise((resolve) => {
        setTimeout(() => {
          console.log('📊 Coordinator responding with mock data');
          const mockData = [{ name: 'Alice', age: 30 }];
          client.queryResult(mockData);
          resolve(mockData);
        }, 10);
      });
    })
  }))
}));

// Mock other dependencies with minimal logging
vi.mock('@uwdata/mosaic-sql', () => ({
  Query: {
    from: vi.fn(() => ({
      select: vi.fn(() => ({
        where: vi.fn(() => ({
          orderby: vi.fn(() => ({
            limit: vi.fn(() => ({
              offset: vi.fn(() => ({
                toString: vi.fn(() => 'SELECT * FROM data LIMIT 100 OFFSET 0')
              }))
            }))
          }))
        }))
      }))
    }))
  }
}));

vi.mock('@preact/signals-core', () => ({
  signal: vi.fn((initial) => ({ value: initial }))
}));

import { TableRenderer } from '../../src/core/TableRenderer.js';

describe('🔍 DIAGNOSTIC: TableRenderer MosaicClient Flow', () => {
  let tableRenderer;
  let container;
  let mockCoordinator;

  beforeEach(async () => {
    console.log('\n🧪 === NEW TEST STARTING ===');
    
    container = document.createElement('div');
    document.body.appendChild(container);
    
    const { Coordinator } = await import('@uwdata/mosaic-core');
    mockCoordinator = new Coordinator();
    
    vi.clearAllMocks();
  });

  afterEach(() => {
    if (tableRenderer) {
      tableRenderer.destroy();
    }
    if (container.parentNode) {
      container.parentNode.removeChild(container);
    }
    console.log('🧹 Test cleanup complete\n');
  });

  it('🔍 DIAGNOSTIC: Trace the complete initialization flow', async () => {
    console.log('🏗️ Creating TableRenderer...');
    
    tableRenderer = new TableRenderer({
      table: 'test_table',
      schema: { name: { type: 'string' } },
      container,
      coordinator: mockCoordinator
    });

    console.log('📋 TableRenderer created, checking initial state:');
    console.log('   - coordinator:', !!tableRenderer.coordinator);
    console.log('   - enabled:', tableRenderer.enabled);
    console.log('   - _initialized:', tableRenderer._initialized);

    console.log('\n🚀 Calling tableRenderer.initialize()...');
    const result = await tableRenderer.initialize();

    console.log('\n📋 After initialize(), checking state:');
    console.log('   - coordinator:', !!tableRenderer.coordinator);
    console.log('   - enabled:', tableRenderer.enabled);
    console.log('   - _initialized:', tableRenderer._initialized);
    console.log('   - connected:', tableRenderer.connected);

    console.log('\n⏳ Waiting for async operations to complete...');
    await new Promise(resolve => setTimeout(resolve, 100));

    console.log('\n📊 Final state:');
    console.log('   - data.length:', tableRenderer.data.length);
    console.log('   - mockCoordinator.connect calls:', mockCoordinator.connect.mock.calls.length);
    console.log('   - mockCoordinator.requestQuery calls:', mockCoordinator.requestQuery.mock.calls.length);

    // Basic assertions
    expect(result).toBe(tableRenderer);
    expect(tableRenderer.coordinator).toBe(mockCoordinator);
  });

  it('🔍 DIAGNOSTIC: Check why coordinator.connect is not called', async () => {
    console.log('🏗️ Creating TableRenderer...');
    
    tableRenderer = new TableRenderer({
      table: 'test_table',
      schema: { name: { type: 'string' } },
      container,
      coordinator: mockCoordinator
    });

    // Spy on the actual coordinator.connect method
    const connectSpy = vi.spyOn(mockCoordinator, 'connect');
    
    console.log('\n🚀 Calling tableRenderer.initialize()...');
    
    // Let's manually trace what TableRenderer.initialize() does
    const originalInitialize = tableRenderer.initialize.bind(tableRenderer);
    tableRenderer.initialize = vi.fn().mockImplementation(async function() {
      console.log('📍 Inside TableRenderer.initialize()');
      
      try {
        console.log('🔗 About to check coordinator and call connect...');
        console.log('   - this.coordinator:', !!this.coordinator);
        console.log('   - this.connected:', this.connected);
        
        if (this.coordinator) {
          try {
            console.log('📞 Calling this.coordinator.connect(this)...');
            this.coordinator.connect(this);
            this.connected = true;
            console.log('✅ coordinator.connect successful');
          } catch (error) {
            console.log('❌ coordinator.connect failed:', error.message);
            if (error.message && error.message.includes('already connected')) {
              console.log('⚠️ Already connected, continuing...');
              this.connected = true;
            } else {
              throw error;
            }
          }
        } else {
          console.log('❌ No coordinator available');
        }

        console.log('🛠️ Calling prepare...');
        await this.prepare();
        
        console.log('📨 Calling requestData...');
        this.requestData();
        
        console.log('⏰ Setting up fallback timeout...');
        setTimeout(async () => {
          if (this.data.length === 0) {
            console.log('⚠️ No data received via coordinator, attempting direct query...');
            await this.fallbackDataLoad();
          }
        }, 1000);
        
        return this;
      } catch (error) {
        console.error('💥 TableRenderer.initialize failed:', error);
        throw error;
      }
    });

    await tableRenderer.initialize();

    console.log('\n📊 Checking spy results:');
    console.log('   - connectSpy.mock.calls.length:', connectSpy.mock.calls.length);
    
    if (connectSpy.mock.calls.length > 0) {
      console.log('✅ coordinator.connect WAS called');
    } else {
      console.log('❌ coordinator.connect was NOT called - this is the problem!');
    }
  });

  it('🔍 DIAGNOSTIC: Manual test of Mosaic flow', async () => {
    console.log('🧪 Testing Mosaic flow manually...');
    
    tableRenderer = new TableRenderer({
      table: 'test_table',
      schema: { name: { type: 'string' } },
      container,
      coordinator: mockCoordinator
    });

    console.log('\n1️⃣ Manual step: coordinator.connect()');
    mockCoordinator.connect(tableRenderer);
    
    console.log('\n2️⃣ Checking state after connect:');
    console.log('   - tableRenderer.coordinator:', !!tableRenderer.coordinator);
    console.log('   - tableRenderer._initialized:', tableRenderer._initialized);
    
    console.log('\n3️⃣ Waiting for async operations...');
    await new Promise(resolve => setTimeout(resolve, 50));
    
    console.log('\n4️⃣ Final check:');
    console.log('   - data.length:', tableRenderer.data.length);
    console.log('   - requestQuery calls:', mockCoordinator.requestQuery.mock.calls.length);
    
    // This should show us what the correct flow looks like
    expect(tableRenderer.coordinator).toBe(mockCoordinator);
  });
});