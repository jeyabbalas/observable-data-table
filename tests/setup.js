// Vitest setup file
import { vi } from 'vitest';

// Mock Web APIs that may not be available in test environment
if (!global.crypto) {
  global.crypto = {};
}
if (!global.crypto.randomUUID) {
  global.crypto.randomUUID = () => Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

// Mock IndexedDB
const mockIDBRequest = {
  onsuccess: null,
  onerror: null,
  result: null,
  error: null
};

const mockIDBTransaction = {
  objectStore: vi.fn(() => ({
    put: vi.fn(() => mockIDBRequest),
    get: vi.fn(() => mockIDBRequest),
    delete: vi.fn(() => mockIDBRequest),
    clear: vi.fn(() => mockIDBRequest),
    getAll: vi.fn(() => mockIDBRequest)
  }))
};

const mockIDBDatabase = {
  transaction: vi.fn(() => mockIDBTransaction),
  objectStoreNames: {
    contains: vi.fn(() => false)
  },
  createObjectStore: vi.fn()
};

global.indexedDB = {
  open: vi.fn(() => {
    const request = { ...mockIDBRequest };
    setTimeout(() => {
      request.result = mockIDBDatabase;
      if (request.onsuccess) request.onsuccess({ target: request });
    }, 0);
    return request;
  })
};

// Mock Worker
global.Worker = vi.fn(() => ({
  postMessage: vi.fn(),
  terminate: vi.fn(),
  addEventListener: vi.fn(),
  removeEventListener: vi.fn(),
  onmessage: null,
  onerror: null
}));

// Mock URL.createObjectURL for worker URLs
global.URL.createObjectURL = vi.fn(() => 'mock-blob-url');

// Mock File and Blob
global.File = class File extends Blob {
  constructor(fileBits, fileName, options = {}) {
    super(fileBits, options);
    this.name = fileName;
    this.lastModified = Date.now();
    this.webkitRelativePath = '';
    this._fileBits = fileBits; // Store fileBits for arrayBuffer method
  }
  
  async arrayBuffer() {
    // Convert the file bits to ArrayBuffer
    if (this._fileBits && this._fileBits.length > 0) {
      const firstBit = this._fileBits[0];
      if (firstBit instanceof ArrayBuffer) {
        return firstBit;
      } else if (firstBit instanceof Uint8Array) {
        return firstBit.buffer;
      } else if (typeof firstBit === 'string') {
        return new TextEncoder().encode(firstBit).buffer;
      }
    }
    // Fallback: create a new ArrayBuffer
    return new ArrayBuffer(0);
  }
};

// Mock FileReader
global.FileReader = class FileReader {
  constructor() {
    this.result = null;
    this.error = null;
    this.readyState = 0;
    this.onload = null;
    this.onerror = null;
    this.onloadend = null;
  }

  readAsArrayBuffer(file) {
    setTimeout(() => {
      this.result = new ArrayBuffer(8);
      this.readyState = 2;
      if (this.onload) this.onload({ target: this });
      if (this.onloadend) this.onloadend({ target: this });
    }, 0);
  }

  readAsText(file) {
    setTimeout(() => {
      this.result = 'mock file content';
      this.readyState = 2;
      if (this.onload) this.onload({ target: this });
      if (this.onloadend) this.onloadend({ target: this });
    }, 0);
  }
};

// Mock clipboard API
global.navigator.clipboard = {
  writeText: vi.fn(() => Promise.resolve())
};

// Mock document.execCommand
global.document.execCommand = vi.fn(() => true);

// Mock showSaveFilePicker (File System Access API)
global.showSaveFilePicker = vi.fn(() => Promise.resolve({
  createWritable: () => Promise.resolve({
    write: vi.fn(() => Promise.resolve()),
    close: vi.fn(() => Promise.resolve())
  })
}));

// Mock console methods to reduce noise in tests
const originalConsole = { ...console };
global.console = {
  ...originalConsole,
  log: vi.fn(),
  info: vi.fn(),
  warn: vi.fn(),
  debug: vi.fn(),
  error: originalConsole.error // Keep error for debugging
};