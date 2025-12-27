/**
 * Blockchain CDC Consumer
 * 
 * Consumes Kafka CDC events from Debezium and sends to blockchain API.
 * Focused on employee and attendance data synchronization.
 */

const { Kafka } = require('kafkajs');
const axios = require('axios');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env.local') });

// Configuration
const KAFKA_BROKER = process.env.KAFKA_BROKER || '127.0.0.1:29092';
const API_ENDPOINT = process.env.API_ENDPOINT || 'http://127.0.0.1:4000';
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'erpnext';
const TARGET_TABLES = (process.env.TARGET_TABLES || 'tabEmployee,tabAttendance').split(',').map(t => t.trim());

// Batch processing settings
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 10;
const BATCH_TIMEOUT = parseInt(process.env.BATCH_TIMEOUT) || 100;
const MAX_CONCURRENT_REQUESTS = parseInt(process.env.MAX_CONCURRENT_REQUESTS) || 5;

// Deduplication settings
const DEDUP_WINDOW_MS = parseInt(process.env.DEDUP_WINDOW_MS) || 10000;
const recentRecords = new Map();

// Statistics
let eventCounter = 0;
let processed = 0;
let errors = 0;
let skipped = 0;
let connected = false;
const startTime = Date.now();

// Kafka setup
const kafka = new Kafka({
  clientId: 'blockchain-consumer',
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 100, retries: 3, maxRetryTime: 30000 },
  connectionTimeout: 3000,
  requestTimeout: 30000
});

const consumer = kafka.consumer({
  groupId: 'blockchain-consumer-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxWaitTimeInMs: 1000
});

// Concurrency limiter
class ConcurrencyLimiter {
  constructor(maxConcurrent) {
    this.maxConcurrent = maxConcurrent;
    this.running = 0;
    this.queue = [];
  }

  async execute(fn) {
    return new Promise((resolve, reject) => {
      this.queue.push({ fn, resolve, reject });
      this.tryNext();
    });
  }

  tryNext() {
    if (this.running >= this.maxConcurrent || this.queue.length === 0) return;
    const { fn, resolve, reject } = this.queue.shift();
    this.running++;
    fn().then(resolve).catch(reject).finally(() => {
      this.running--;
      this.tryNext();
    });
  }
}

const concurrencyLimiter = new ConcurrencyLimiter(MAX_CONCURRENT_REQUESTS);

/**
 * Check for duplicate records
 */
function shouldSkipDuplicate(recordId, modifiedTimestamp) {
  const now = Date.now();

  // Clean old entries
  for (const [key, data] of recentRecords.entries()) {
    if (now - data.timestamp > DEDUP_WINDOW_MS) {
      recentRecords.delete(key);
    }
  }

  if (recentRecords.has(recordId)) {
    const existing = recentRecords.get(recordId);
    const timeDiff = Math.abs(new Date(modifiedTimestamp).getTime() - new Date(existing.modifiedTimestamp).getTime());
    if (timeDiff < 5000) {
      return true;
    }
  }

  recentRecords.set(recordId, { timestamp: now, modifiedTimestamp });
  return false;
}

/**
 * Get endpoint for table
 */
function getEndpoint(tableName) {
  const endpoints = {
    'tabEmployee': '/employees',
    'tabAttendance': '/attendances'
  };
  return endpoints[tableName] || `/${tableName.toLowerCase()}`;
}

/**
 * Transform data for blockchain API
 * Maps CDC fields to contract expected format:
 * - recordId = data.name
 * - createdTimestamp = data.creation (unix microseconds from MariaDB)
 * - modifiedTimestamp = data.modified (unix microseconds from MariaDB)
 * - modifiedBy = data.modified_by
 * - allData = all remaining fields as JSON
 */
function transformForBlockchain(tableName, data) {
  const recordId = data.name || `${tableName}-${Date.now()}`;
  const createdTimestamp = data.creation || Date.now() * 1000; // microseconds
  const modifiedTimestamp = data.modified || Date.now() * 1000;
  const modifiedBy = data.modified_by || 'cdc-consumer';

  // Create allData by excluding the mapped fields
  const { name, creation, modified, modified_by, ...restData } = data;
  const allData = restData;

  if (tableName === 'tabEmployee') {
    return {
      employeeData: {
        recordId,
        createdTimestamp,
        modifiedTimestamp,
        modifiedBy,
        allData
      }
    };
  } else if (tableName === 'tabAttendance') {
    return {
      attendanceData: {
        recordId,
        createdTimestamp,
        modifiedTimestamp,
        modifiedBy,
        allData
      }
    };
  } else {
    return {
      documentData: {
        recordId,
        createdTimestamp,
        modifiedTimestamp,
        modifiedBy,
        allData
      }
    };
  }
}

/**
 * Send data to blockchain API
 */
async function sendToBlockchain(endpoint, transformedData) {
  return concurrencyLimiter.execute(async () => {
    const fullPayload = { privateKey: PRIVATE_KEY, ...transformedData };

    try {
      const dataKey = Object.keys(transformedData)[0];
      const recordId = transformedData[dataKey]?.recordId;

      const response = await axios.post(`${API_ENDPOINT}${endpoint}`, fullPayload, {
        timeout: 30000,
        headers: { 'Content-Type': 'application/json' }
      });

      if (response.data.success) {
        const blockNumber = response.data.blockchain?.blockNumber || response.data.blockNumber;
        const txHash = response.data.blockchain?.transactionHash || response.data.transactionHash;
        console.log(`[OK] ${endpoint} ${recordId} -> Block ${blockNumber} (${txHash?.slice(0, 10)}...)`);
        return true;
      } else {
        console.log(`[X] API failed for ${endpoint}: ${JSON.stringify(response.data)}`);
        return false;
      }
    } catch (error) {
      const errorMsg = error.response?.data?.error || error.message;
      console.log(`[X] ${endpoint} blockchain call failed: ${errorMsg}`);
      return false;
    }
  });
}

// Batch processing
const messageQueue = [];
let batchTimeout = null;

async function processBatch() {
  if (messageQueue.length === 0) return;

  const batch = messageQueue.splice(0, BATCH_SIZE);
  console.log(`\n--- Processing batch of ${batch.length} messages ---`);

  const promises = batch.map(({ topic, message }) => processMessage(topic, message));
  await Promise.allSettled(promises);

  console.log(`--- Batch completed ---\n`);
}

async function queueMessage(topic, message) {
  messageQueue.push({ topic, message });

  if (batchTimeout) clearTimeout(batchTimeout);

  if (messageQueue.length >= BATCH_SIZE) {
    await processBatch();
  } else {
    batchTimeout = setTimeout(processBatch, BATCH_TIMEOUT);
  }
}

/**
 * Process a CDC message
 */
async function processMessage(topic, message) {
  try {
    const messageValue = message.value?.toString();
    if (!messageValue) return;

    let changeEvent;
    try {
      changeEvent = JSON.parse(messageValue);
    } catch (parseError) {
      errors++;
      console.log(`[X] JSON parse failed for topic ${topic}`);
      return;
    }

    // Extract table name from topic
    const topicParts = topic.split('.');
    const tableName = topicParts[topicParts.length - 1];

    // Check if table is in target list
    if (!TARGET_TABLES.includes(tableName)) {
      return;
    }

    // Handle Debezium CDC event structure
    let changeData;
    if (changeEvent.payload) {
      changeData = changeEvent.payload.after || changeEvent.payload.before;
      if (changeEvent.payload.op === 'd') {
        eventCounter++;
        console.log(`Event #${eventCounter}: ${tableName} DELETE - SKIPPED`);
        return;
      }
    } else {
      changeData = changeEvent;
    }

    if (!changeData) {
      return;
    }

    // Extract record ID
    const recordId = changeData.name || changeData.id;
    if (!recordId) {
      console.log(`[WARNING] No record ID for ${tableName}`);
      return;
    }

    // Check for delete marker
    if (changeData.__deleted === 'true' || changeData.__deleted === true) {
      eventCounter++;
      console.log(`Event #${eventCounter}: ${tableName} ${recordId} DELETE - SKIPPED`);
      return;
    }

    // Detect operation
    let operation = 'UPDATE';
    if (changeEvent.payload?.op === 'c') {
      operation = 'CREATE';
    } else if (changeEvent.payload?.op === 'u') {
      operation = 'UPDATE';
    }

    // Get modified timestamp for deduplication
    const modifiedTimestamp = changeData.modified || changeData.modification || new Date().toISOString();

    // Deduplication check
    if (shouldSkipDuplicate(recordId, modifiedTimestamp)) {
      eventCounter++;
      console.log(`Event #${eventCounter}: ${tableName} ${recordId} ${operation} - DUPLICATE`);
      skipped++;
      return;
    }

    eventCounter++;
    console.log(`Event #${eventCounter}: ${tableName} ${recordId} ${operation}`);

    // Transform data for blockchain
    const transformedData = transformForBlockchain(tableName, changeData);

    // Get table endpoint
    const tableEndpoint = getEndpoint(tableName);

    // Send to blockchain
    const success = await sendToBlockchain(tableEndpoint, transformedData);

    if (success) {
      processed++;
    } else {
      errors++;
    }

  } catch (error) {
    console.log(`[X] Processing failed for topic ${topic}: ${error.message}`);
    errors++;
  }
}

/**
 * Discover topics
 */
async function discoverTopics() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    const allTopics = await admin.listTopics();

    const relevantTopics = allTopics.filter(topic => {
      if (topic.includes('schema-changes')) return false;
      if (!topic.startsWith(TOPIC_PREFIX)) return false;
      const tableName = topic.split('.').pop();
      return TARGET_TABLES.includes(tableName);
    });

    await admin.disconnect();
    return relevantTopics;
  } catch (error) {
    console.log(`[X] Topic discovery failed: ${error.message}`);
    return [];
  }
}

/**
 * Main startup
 */
async function start() {
  console.log('='.repeat(60));
  console.log('Blockchain CDC Consumer');
  console.log('='.repeat(60));
  console.log(`Kafka: ${KAFKA_BROKER}`);
  console.log(`API: ${API_ENDPOINT}`);
  console.log(`Topic Prefix: ${TOPIC_PREFIX}`);
  console.log(`Tables: ${TARGET_TABLES.join(', ')}`);
  console.log(`Batch Size: ${BATCH_SIZE}, Max Concurrent: ${MAX_CONCURRENT_REQUESTS}`);
  console.log('');

  // Test API connection
  console.log('Testing API connection...');
  try {
    const response = await axios.get(API_ENDPOINT, { timeout: 5000 });
    console.log(`[OK] API connected: ${response.data.name || 'Blockchain API'}`);
  } catch (error) {
    console.log(`[WARNING] API not reachable: ${error.message}`);
    console.log('  Consumer will still run, but blockchain sync will fail.');
  }

  // Connect to Kafka
  try {
    await consumer.connect();
    console.log('[OK] Kafka connected');
    connected = true;
  } catch (error) {
    console.log(`[X] Kafka connection failed: ${error.message}`);
    process.exit(1);
  }

  // Discover and subscribe to topics
  const availableTopics = await discoverTopics();

  if (availableTopics.length === 0) {
    console.log('[WARNING] No CDC topics found yet.');
    console.log('  Run: node utils/add-erp-connector.js to create the connector');
    console.log('  Subscribing to topic pattern...');
    await consumer.subscribe({ topics: new RegExp(`^${TOPIC_PREFIX}\\..*\\.(${TARGET_TABLES.join('|')})$`), fromBeginning: false });
  } else {
    console.log(`[OK] Found ${availableTopics.length} topic(s): ${availableTopics.join(', ')}`);
    await consumer.subscribe({ topics: availableTopics, fromBeginning: false });
  }

  // Start consumer
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await queueMessage(topic, message);
    }
  });

  console.log('\n[OK] Consumer ready. Waiting for CDC events...\n');

  // Status report every 30 seconds (simple stats only)
  setInterval(() => {
    if (processed > 0 || errors > 0 || skipped > 0) {
      const runtime = Math.round((Date.now() - startTime) / 1000);
      const rate = processed > 0 ? (processed / runtime).toFixed(2) : 0;
      console.log(`[STATUS] Events: ${eventCounter} | Processed: ${processed} | Errors: ${errors} | Skipped: ${skipped} | Rate: ${rate}/s`);
    }
  }, 30000);
}

/**
 * Graceful shutdown
 */
async function shutdown() {
  console.log('\nShutting down...');

  if (messageQueue.length > 0) {
    console.log(`Processing ${messageQueue.length} remaining messages...`);
    await processBatch();
  }

  if (connected) {
    try {
      await consumer.disconnect();
      console.log('Disconnected from Kafka');
    } catch (error) {
      console.log(`Disconnect error: ${error.message}`);
    }
  }

  const runtime = Math.round((Date.now() - startTime) / 1000);
  console.log(`\n===== FINAL REPORT =====`);
  console.log(`Total Events: ${eventCounter}`);
  console.log(`Processed: ${processed}`);
  console.log(`Errors: ${errors}`);
  console.log(`Skipped: ${skipped}`);
  console.log(`Success Rate: ${processed > 0 ? Math.round((processed / (processed + errors)) * 100) : 0}%`);
  console.log(`Runtime: ${runtime}s`);
  console.log('=========================');
  process.exit(0);
}

// Error handlers
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  shutdown();
});

// Start consumer
start().catch(error => {
  console.error(`Startup failed: ${error.message}`);
  process.exit(1);
});