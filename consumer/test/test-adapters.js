/**
 * Adapter Unit Tests
 * 
 * Tests for the database adapter system to ensure proper timestamp handling,
 * operation detection, and data transformation for each database type.
 */

const MySQLAdapter = require('../adapters/MySQLAdapter');
const PostgresAdapter = require('../adapters/PostgresAdapter');
const MongoDBAdapter = require('../adapters/MongoDBAdapter');
const SQLServerAdapter = require('../adapters/SQLServerAdapter');
const AdapterFactory = require('../adapters/AdapterFactory');

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    console.log(`✓ ${name}`);
    passed++;
  } catch (error) {
    console.log(`✗ ${name}`);
    console.log(`  Error: ${error.message}`);
    failed++;
  }
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message || 'Assertion failed');
  }
}

function assertEqual(actual, expected, message) {
  if (actual !== expected) {
    throw new Error(message || `Expected ${expected}, got ${actual}`);
  }
}

// Mock app config for testing
const createMockConfig = (type, options = {}) => ({
  name: 'test-app',
  displayName: 'Test App',
  database: {
    type,
    idField: options.idField || 'id',
    timezoneOffsetHours: options.timezoneOffset || 0,
    timestampFields: {
      created: options.createdField || 'created_at',
      modified: options.modifiedField || 'updated_at'
    }
  },
  blockchain: {
    apiEndpoint: 'http://localhost:4001'
  },
  tables: [
    { name: 'users', endpoint: '/users', dataKey: 'userData', fields: ['id', 'name', 'email'] }
  ],
  tableMap: new Map([
    ['users', { name: 'users', endpoint: '/users', dataKey: 'userData', fields: ['id', 'name', 'email'] }]
  ])
});

console.log('\n=== Adapter Factory Tests ===\n');

test('AdapterFactory creates MySQLAdapter for mysql type', () => {
  const config = createMockConfig('mysql');
  const adapter = AdapterFactory.create(config);
  assert(adapter instanceof MySQLAdapter, 'Should be MySQLAdapter instance');
});

test('AdapterFactory creates PostgresAdapter for postgres type', () => {
  const config = createMockConfig('postgres');
  const adapter = AdapterFactory.create(config);
  assert(adapter instanceof PostgresAdapter, 'Should be PostgresAdapter instance');
});

test('AdapterFactory creates MongoDBAdapter for mongodb type', () => {
  const config = createMockConfig('mongodb');
  const adapter = AdapterFactory.create(config);
  assert(adapter instanceof MongoDBAdapter, 'Should be MongoDBAdapter instance');
});

test('AdapterFactory creates SQLServerAdapter for sqlserver type', () => {
  const config = createMockConfig('sqlserver');
  const adapter = AdapterFactory.create(config);
  assert(adapter instanceof SQLServerAdapter, 'Should be SQLServerAdapter instance');
});

test('AdapterFactory.isSupported returns true for supported types', () => {
  assert(AdapterFactory.isSupported('mysql'), 'mysql should be supported');
  assert(AdapterFactory.isSupported('postgres'), 'postgres should be supported');
  assert(AdapterFactory.isSupported('mongodb'), 'mongodb should be supported');
  assert(AdapterFactory.isSupported('sqlserver'), 'sqlserver should be supported');
});

test('AdapterFactory.isSupported returns false for unsupported types', () => {
  assert(!AdapterFactory.isSupported('oracle'), 'oracle should not be supported');
  assert(!AdapterFactory.isSupported('sqlite'), 'sqlite should not be supported');
});

console.log('\n=== MySQL Adapter Tests ===\n');

test('MySQLAdapter normalizes microsecond timestamp (ERPNext format)', () => {
  const config = createMockConfig('mysql', { timezoneOffset: 7 });
  const adapter = new MySQLAdapter(config);

  // ERPNext stores timestamps as microseconds since epoch
  const microTimestamp = 1702627200000000; // Example microseconds
  const result = adapter.normalizeTimestamp(microTimestamp);

  assert(result.includes('T'), 'Should be ISO format');
  assert(result.endsWith('Z'), 'Should end with Z');
});

test('MySQLAdapter normalizes string datetime', () => {
  const config = createMockConfig('mysql');
  const adapter = new MySQLAdapter(config);

  const result = adapter.normalizeTimestamp('2024-01-15 10:30:00');
  assert(result.includes('T'), 'Should be ISO format');
});

test('MySQLAdapter extracts record ID using name field (ERPNext)', () => {
  const config = createMockConfig('mysql', { idField: 'name' });
  const adapter = new MySQLAdapter(config);

  const data = { name: 'EMP-001', employee: 'John Doe' };
  assertEqual(adapter.extractRecordId(data), 'EMP-001', 'Should extract name field');
});

test('MySQLAdapter detects CREATE operation from Debezium event', () => {
  const config = createMockConfig('mysql');
  const adapter = new MySQLAdapter(config);

  const event = { payload: { op: 'c' } };
  assertEqual(adapter.detectOperation(event, {}), 'CREATE');
});

test('MySQLAdapter detects UPDATE operation from Debezium event', () => {
  const config = createMockConfig('mysql');
  const adapter = new MySQLAdapter(config);

  const event = { payload: { op: 'u' } };
  assertEqual(adapter.detectOperation(event, {}), 'UPDATE');
});

console.log('\n=== PostgreSQL Adapter Tests ===\n');

test('PostgresAdapter normalizes ISO timestamp', () => {
  const config = createMockConfig('postgres');
  const adapter = new PostgresAdapter(config);

  const result = adapter.normalizeTimestamp('2024-01-15T10:30:00.000Z');
  assertEqual(result, '2024-01-15T10:30:00.000Z', 'Should preserve ISO format');
});

test('PostgresAdapter handles epoch milliseconds', () => {
  const config = createMockConfig('postgres');
  const adapter = new PostgresAdapter(config);

  const timestamp = 1705315800000; // Example milliseconds
  const result = adapter.normalizeTimestamp(timestamp);

  assert(result.includes('T'), 'Should be ISO format');
});

test('PostgresAdapter filters out system columns', () => {
  const config = createMockConfig('postgres');
  const adapter = new PostgresAdapter(config);

  const data = {
    id: 1,
    name: 'Test',
    xmin: 123,
    xmax: 456,
    ctid: '(0,1)'
  };

  const filtered = adapter.filterGeneric(data);
  assertEqual(filtered.id, 1, 'Should keep id');
  assertEqual(filtered.name, 'Test', 'Should keep name');
  assert(!('xmin' in filtered), 'Should filter xmin');
  assert(!('ctid' in filtered), 'Should filter ctid');
});

console.log('\n=== MongoDB Adapter Tests ===\n');

test('MongoDBAdapter normalizes $date object', () => {
  const config = createMockConfig('mongodb');
  const adapter = new MongoDBAdapter(config);

  const mongoDate = { $date: '2024-01-15T10:30:00.000Z' };
  const result = adapter.normalizeTimestamp(mongoDate);

  assertEqual(result, '2024-01-15T10:30:00.000Z', 'Should extract date from $date');
});

test('MongoDBAdapter normalizes ObjectId', () => {
  const config = createMockConfig('mongodb');
  const adapter = new MongoDBAdapter(config);

  const objectId = { $oid: '507f1f77bcf86cd799439011' };
  const result = adapter.normalizeObjectId(objectId);

  assertEqual(result, '507f1f77bcf86cd799439011', 'Should extract oid string');
});

test('MongoDBAdapter extracts record ID from _id', () => {
  const config = createMockConfig('mongodb');
  const adapter = new MongoDBAdapter(config);

  const data = { _id: { $oid: '507f1f77bcf86cd799439011' }, name: 'Test' };
  const result = adapter.extractRecordId(data);

  assertEqual(result, '507f1f77bcf86cd799439011', 'Should extract ObjectId');
});

test('MongoDBAdapter detects operation from change stream event', () => {
  const config = createMockConfig('mongodb');
  const adapter = new MongoDBAdapter(config);

  const event = { operationType: 'insert' };
  assertEqual(adapter.detectOperation(event, {}), 'CREATE');
});

console.log('\n=== SQL Server Adapter Tests ===\n');

test('SQLServerAdapter normalizes datetime string', () => {
  const config = createMockConfig('sqlserver');
  const adapter = new SQLServerAdapter(config);

  const result = adapter.normalizeTimestamp('2024-01-15 10:30:00.000');
  assert(result.includes('T'), 'Should be ISO format');
});

test('SQLServerAdapter extracts GUID record ID', () => {
  const config = createMockConfig('sqlserver');
  const adapter = new SQLServerAdapter(config);

  const guid = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890';
  const data = { id: guid, name: 'Test' };

  assertEqual(adapter.extractRecordId(data), guid, 'Should extract GUID');
});

test('SQLServerAdapter filters out CDC metadata columns', () => {
  const config = createMockConfig('sqlserver');
  const adapter = new SQLServerAdapter(config);

  const data = {
    id: 1,
    name: 'Test',
    '__$operation': 2,
    '__$start_lsn': '00000000000000000001'
  };

  const filtered = adapter.filterGeneric(data);
  assertEqual(filtered.id, 1, 'Should keep id');
  assert(!('__$operation' in filtered), 'Should filter CDC columns');
});

console.log('\n=== Transform Tests ===\n');

test('Adapter transforms data for blockchain', () => {
  const config = createMockConfig('mysql', { idField: 'id' });
  config.database.timestampFields.created = 'created_at';
  config.database.timestampFields.modified = 'updated_at';

  const adapter = new MySQLAdapter(config);

  const rawData = {
    id: '123',
    name: 'John',
    email: 'john@example.com',
    created_at: '2024-01-15T10:00:00Z',
    updated_at: '2024-01-15T11:00:00Z',
    _internal: 'hidden'
  };

  const result = adapter.transformForBlockchain('users', rawData);

  assert('userData' in result, 'Should have userData key');
  assertEqual(result.userData.recordId, '123', 'Should have recordId');
  assert('optimization' in result, 'Should have optimization stats');
});

// Summary
console.log('\n=== Test Summary ===\n');
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
console.log(`Total: ${passed + failed}`);

if (failed > 0) {
  process.exit(1);
}
