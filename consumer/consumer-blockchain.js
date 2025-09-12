const { Kafka } = require('kafkajs');
const axios = require('axios');
const { optimizeForBlockchain } = require('./data-filter');
require('dotenv').config();

// Configuration
const KAFKA_BROKER = process.env.KAFKA_BROKER || '127.0.0.1:29092';
const API_ENDPOINT = process.env.API_ENDPOINT || 'http://127.0.0.1:4001';
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const TARGET_TABLES = process.env.TARGET_TABLES?.split(',').map(t => t.trim()) || ['tabEmployee', 'tabAttendance'];
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'erpnext';
const DB_NAME = process.env.DB_NAME || 'erpnext_db';

// Deduplication settings
const DEDUP_WINDOW_MS = 10000; // 10 seconds window for deduplication
const recentRecords = new Map(); // Store recent records with timestamps

// Statistics
let eventCounter = 0;
let processed = 0;
let errors = 0;
let skipped = 0;
let connected = false;

// Kafka setup
const kafka = new Kafka({
  clientId: 'blockchain-consumer',
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 1000, retries: 8 }
});

const consumer = kafka.consumer({
  groupId: 'blockchain-consumer-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000
});

// API endpoints mapping
const ENDPOINTS = {
  tabEmployee: '/employees',
  tabAttendance: '/attendances',
  tabUser: '/users',
  tabTask: '/tasks',
  tabCompany: '/companies'
};

// Convert ERPNext timestamp to ISO string
function convertTimestamp(timestamp) {
  if (!timestamp) return new Date().toISOString();

  // ERPNext uses microseconds, JavaScript uses milliseconds
  const milliseconds = Math.floor(timestamp / 1000);
  return new Date(milliseconds).toISOString();
}

// Detect CDC operation type
function detectOperation(changeEvent, changeData) {
  // First check Debezium operation codes
  if (changeEvent.payload?.op) {
    const opMap = {
      'c': 'CREATE',
      'u': 'UPDATE',
      'd': 'DELETE',
      'r': 'READ'
    };
    return opMap[changeEvent.payload.op] || changeEvent.payload.op;
  }

  // Check if it's a delete
  if (changeData.__deleted === 'true' || changeData.__deleted === true) {
    return 'DELETE';
  }

  // Try to determine from timestamps
  if (changeData.creation && changeData.modified) {
    const createdTime = typeof changeData.creation === 'number' ?
      changeData.creation : Date.parse(changeData.creation);
    const modifiedTime = typeof changeData.modified === 'number' ?
      changeData.modified : Date.parse(changeData.modified);

    // If creation and modified are very close (within 5 seconds), it's likely a CREATE
    const timeDiff = Math.abs(modifiedTime - createdTime);
    if (timeDiff < 5000000) { // 5 seconds in microseconds
      return 'CREATE';
    } else {
      return 'UPDATE';
    }
  }

  // Default assumption
  return 'CREATE';
}

// Check if record should be deduplicated
function shouldSkipDuplicate(recordId, modifiedTimestamp) {
  const now = Date.now();
  const recordKey = recordId;

  // Clean old entries first
  for (const [key, data] of recentRecords.entries()) {
    if (now - data.timestamp > DEDUP_WINDOW_MS) {
      recentRecords.delete(key);
    }
  }

  // Check if we've seen this record recently
  if (recentRecords.has(recordKey)) {
    const existing = recentRecords.get(recordKey);

    // If the modified timestamp hasn't changed significantly, skip
    const timeDiff = Math.abs(new Date(modifiedTimestamp).getTime() - new Date(existing.modifiedTimestamp).getTime());
    if (timeDiff < 5000) { // Less than 5 seconds difference
      return { shouldSkip: true, timeDiff };
    }
  }

  // Record this event
  recentRecords.set(recordKey, {
    timestamp: now,
    modifiedTimestamp: modifiedTimestamp
  });

  return { shouldSkip: false, timeDiff: 0 };
}

// Transform data for blockchain API
function transformData(table, data) {
  // First, optimize the data for blockchain
  const optimization = optimizeForBlockchain(table, data);

  const baseData = {
    recordId: data.name || data.id || `${table}_${Date.now()}`,
    createdTimestamp: convertTimestamp(data.creation),
    modifiedTimestamp: convertTimestamp(data.modified),
    modifiedBy: data.modified_by || 'system',
    allData: optimization.filteredData // Use filtered data instead of raw data
  };

  // Map table to expected API payload structure
  const dataKeyMap = {
    tabEmployee: 'employeeData',
    tabAttendance: 'attendanceData',
    tabUser: 'userData',
    tabTask: 'taskData',
    tabCompany: 'companyData'
  };

  const dataKey = dataKeyMap[table] || 'data';

  return {
    [dataKey]: baseData,
    optimization: optimization.stats
  };
}

// Send to blockchain
async function sendToBlockchain(table, transformedData) {
  const endpoint = ENDPOINTS[table];
  if (!endpoint) {
    console.log(`WARNING: No endpoint for ${table}`);
    return false;
  }

  // Remove optimization stats from payload
  const { optimization, ...payload } = transformedData;

  const fullPayload = {
    privateKey: PRIVATE_KEY,
    ...payload
  };

  try {
    const recordId = Object.values(payload)[0]?.recordId;
    console.log(`Sending ${table} (${optimization.reductionPercent}% optimized: ${optimization.originalSize} -> ${optimization.filteredSize} chars)`);

    const response = await axios.post(`${API_ENDPOINT}${endpoint}`, fullPayload, {
      timeout: 15000,
      headers: { 'Content-Type': 'application/json' }
    });

    if (response.data.success) {
      const blockNumber = response.data.blockchain?.blockNumber || response.data.blockNumber;
      const txHash = response.data.blockchain?.transactionHash || response.data.transactionHash;

      console.log(`SUCCESS: ${table} ${recordId} -> Block ${blockNumber} (${txHash?.slice(0, 10)}...)`);
      return true;
    } else {
      console.log(`ERROR: API failed for ${table} - ${JSON.stringify(response.data)}`);
      return false;
    }
  } catch (error) {
    const errorMsg = error.response?.data?.error || error.message;
    console.log(`ERROR: ${table} blockchain call failed - ${errorMsg}`);
    return false;
  }
}

// Process CDC message
async function processMessage(topic, message) {
  try {
    const messageValue = message.value?.toString();
    if (!messageValue) return;

    let changeEvent;
    try {
      changeEvent = JSON.parse(messageValue);
    } catch (parseError) {
      errors++;
      console.log(`ERROR: JSON parse failed for topic ${topic}`);
      return;
    }

    // Extract table name from topic: erpnext.erpnext_db.tabEmployee -> tabEmployee
    const topicParts = topic.split('.');
    const tableName = topicParts[topicParts.length - 1];

    if (!TARGET_TABLES.includes(tableName)) {
      return; // Not a target table
    }

    // Handle Debezium CDC event structure
    let changeData;
    if (changeEvent.payload) {
      // Debezium format
      changeData = changeEvent.payload.after || changeEvent.payload.before;
      if (changeEvent.payload.op === 'd') {
        eventCounter++;
        console.log(`\nEvent #${eventCounter}`);
        console.log(`${tableName} ${changeData?.name || 'unknown'} DELETE`);
        console.log(`SKIP: Delete operations not sent to blockchain\n`);
        return; // Skip delete operations
      }
    } else {
      // Direct format or snapshot
      changeData = changeEvent;
    }

    if (!changeData?.name) {
      console.log(`SKIP: No record name for ${tableName}`);
      return;
    }

    // Skip deleted records
    if (changeData.__deleted === 'true' || changeData.__deleted === true) {
      eventCounter++;
      console.log(`\nEvent #${eventCounter}`);
      console.log(`${tableName} ${changeData.name} DELETE`);
      console.log(`SKIP: Deleted records not sent to blockchain\n`);
      return;
    }

    const operation = detectOperation(changeEvent, changeData);
    const modifiedTimestamp = convertTimestamp(changeData.modified);

    // Check for duplicates
    const dupCheck = shouldSkipDuplicate(changeData.name, modifiedTimestamp);
    if (dupCheck.shouldSkip) {
      eventCounter++;
      console.log(`\nEvent #${eventCounter}`);
      console.log(`${tableName} ${changeData.name} ${operation}`);
      console.log(`SKIP: Duplicate event (within ${dupCheck.timeDiff}ms of previous)\n`);
      skipped++;
      return;
    }

    eventCounter++;
    console.log(`\nEvent #${eventCounter}`);
    console.log(`${tableName} ${changeData.name} ${operation}`);

    const transformedData = transformData(tableName, changeData);
    const success = await sendToBlockchain(tableName, transformedData);

    if (success) {
      processed++;
    } else {
      errors++;
    }

    console.log(''); // Empty line for readability

  } catch (error) {
    console.log(`ERROR: Processing failed for topic ${topic} - ${error.message}`);
    errors++;
  }
}

// Discover existing topics
async function discoverTopics() {
  try {
    const admin = kafka.admin();
    await admin.connect();

    const allTopics = await admin.listTopics();

    // Find topics matching our pattern: {topicPrefix}.{database}.{table}
    const relevantTopics = allTopics.filter(topic => {
      const parts = topic.split('.');
      if (parts.length !== 3) return false;

      const [prefix, database, table] = parts;
      return prefix === TOPIC_PREFIX &&
        TARGET_TABLES.includes(table) &&
        !topic.includes('schema-changes');
    });

    await admin.disconnect();

    console.log(`Discovered topics: ${relevantTopics.length > 0 ? relevantTopics.join(', ') : 'none'}`);
    return relevantTopics;

  } catch (error) {
    console.log(`ERROR: Topic discovery failed - ${error.message}`);
    return [];
  }
}

// Main function
async function start() {
  console.log('Starting Blockchain Consumer with Deduplication...');
  console.log(`Kafka: ${KAFKA_BROKER}`);
  console.log(`API: ${API_ENDPOINT}`);
  console.log(`Database: ${DB_NAME}`);
  console.log(`Tables: ${TARGET_TABLES.join(', ')}`);
  console.log(`Deduplication Window: ${DEDUP_WINDOW_MS}ms`);

  // Test API connection
  try {
    const response = await axios.get(API_ENDPOINT, { timeout: 5000 });
    console.log(`API connection: OK (${response.data.name || 'API Server'})`);
  } catch (error) {
    console.log(`API connection: FAILED - ${error.message}`);
  }

  // Connect to Kafka
  try {
    await consumer.connect();
    console.log('Kafka connection: OK');
    connected = true;
  } catch (error) {
    console.log(`Kafka connection: FAILED - ${error.message}`);
    process.exit(1);
  }

  // Discover topics
  const availableTopics = await discoverTopics();

  if (availableTopics.length === 0) {
    console.log('WARNING: No CDC topics found!');
    const expectedTopics = TARGET_TABLES.map(table => `${TOPIC_PREFIX}.${DB_NAME}.${table}`);
    await consumer.subscribe({
      topics: expectedTopics,
      fromBeginning: false
    });
  } else {
    await consumer.subscribe({
      topics: availableTopics,
      fromBeginning: false
    });
    console.log(`Subscribed to: ${availableTopics.join(', ')}`);
  }

  // Start consuming
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await processMessage(topic, message);
    },
  });

  console.log('Consumer ready - listening for CDC events...\n');

  // Stats every 60 seconds
  setInterval(() => {
    if (processed > 0 || errors > 0 || skipped > 0) {
      console.log(`\n--- Stats ---`);
      console.log(`Events: ${eventCounter} total, ${processed} processed, ${errors} errors, ${skipped} skipped`);
      console.log(`Blockchain success rate: ${processed > 0 ? Math.round((processed / (processed + errors)) * 100) : 0}%`);
      console.log(''); // Empty line
    }
  }, 60000);
}

// Shutdown handler
async function shutdown() {
  console.log('\nShutting down...');
  if (connected) {
    try {
      await consumer.disconnect();
      console.log('Disconnected from Kafka');
    } catch (error) {
      console.log(`Disconnect error: ${error.message}`);
    }
  }
  console.log(`Final stats: ${eventCounter} events, ${processed} processed, ${errors} errors, ${skipped} skipped`);
  process.exit(0);
}

// Error handlers
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Start consumer
start().catch(error => {
  console.log(`Startup failed: ${error.message}`);
  process.exit(1);
});