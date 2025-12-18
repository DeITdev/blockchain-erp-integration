/**
 * Generic Blockchain-ERP Integration Consumer
 * 
 * Database-agnostic CDC consumer that uses the App Registry and Adapter pattern
 * to support multiple database types (MySQL, PostgreSQL, MongoDB, SQL Server).
 * 
 * Maintains backward compatibility with ERPNext while supporting any database.
 */

const { Kafka } = require('kafkajs');
const axios = require('axios');
require('dotenv').config();

// Import registry and adapters
const { getRegistry } = require('./config/registry');
const AdapterFactory = require('./adapters/AdapterFactory');

// Configuration
const KAFKA_BROKER = process.env.KAFKA_BROKER || '127.0.0.1:29092';
const PRIVATE_KEY = process.env.PRIVATE_KEY;

// Legacy environment variables (for backward compatibility)
const LEGACY_API_ENDPOINT = process.env.API_ENDPOINT || 'http://127.0.0.1:4001';
const LEGACY_TARGET_TABLES = process.env.TARGET_TABLES?.split(',').map(t => t.trim()) || [];
const LEGACY_TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'erpnext';
const LEGACY_DB_NAME = process.env.DB_NAME || '';

// Deduplication settings
const DEDUP_WINDOW_MS = parseInt(process.env.DEDUP_WINDOW_MS) || 10000;
const recentRecords = new Map();

// Batch processing settings
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 10;
const BATCH_TIMEOUT = parseInt(process.env.BATCH_TIMEOUT) || 100;
const MAX_CONCURRENT_REQUESTS = parseInt(process.env.MAX_CONCURRENT_REQUESTS) || 5;

// Statistics
let eventCounter = 0;
let processed = 0;
let errors = 0;
let skipped = 0;
let connected = false;
let batchesProcessed = 0;
let totalBatchTime = 0;
const startTime = Date.now();

// Latency tracking
let latencyStats = {
  totalEvents: 0,
  dbToKafkaSum: 0,
  kafkaToConsumerSum: 0,
  consumerProcessingSum: 0,
  consumerToBlockchainSum: 0,
  totalCDCSum: 0,
  totalSystemSum: 0,
  minDbToKafka: Infinity,
  maxDbToKafka: 0,
  minKafkaToConsumer: Infinity,
  maxKafkaToConsumer: 0,
  minConsumerProcessing: Infinity,
  maxConsumerProcessing: 0,
  minConsumerToBlockchain: Infinity,
  maxConsumerToBlockchain: 0,
  minTotalCDC: Infinity,
  maxTotalCDC: 0,
  minTotalSystem: Infinity,
  maxTotalSystem: 0
};

// Kafka setup
const kafka = new Kafka({
  clientId: 'blockchain-consumer',
  brokers: [KAFKA_BROKER],
  retry: {
    initialRetryTime: 100,
    retries: 3,
    maxRetryTime: 30000
  },
  connectionTimeout: 3000,
  requestTimeout: 30000
});

const consumer = kafka.consumer({
  groupId: 'blockchain-consumer-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxBytesPerPartition: 10485760,
  minBytes: 1024,
  maxBytes: 52428800,
  maxWaitTimeInMs: 1000,
  autoCommitInterval: 5000
});

// Concurrency limiter for blockchain requests
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
    if (this.running >= this.maxConcurrent || this.queue.length === 0) {
      return;
    }

    const { fn, resolve, reject } = this.queue.shift();
    this.running++;

    fn()
      .then(resolve)
      .catch(reject)
      .finally(() => {
        this.running--;
        this.tryNext();
      });
  }
}

const concurrencyLimiter = new ConcurrencyLimiter(MAX_CONCURRENT_REQUESTS);

// Adapter cache (per app)
const adapterCache = new Map();

/**
 * Get or create adapter for an app
 */
function getAdapter(appConfig) {
  if (!appConfig) return null;

  if (!adapterCache.has(appConfig.name)) {
    const adapter = AdapterFactory.create(appConfig);
    adapterCache.set(appConfig.name, adapter);
  }

  return adapterCache.get(appConfig.name);
}

/**
 * Update latency statistics
 */
function updateLatencyStats(dbToKafka, kafkaToConsumer, consumerProcessing, consumerToBlockchain) {
  latencyStats.totalEvents++;
  latencyStats.dbToKafkaSum += dbToKafka;
  latencyStats.kafkaToConsumerSum += kafkaToConsumer;
  latencyStats.consumerProcessingSum += consumerProcessing;
  latencyStats.consumerToBlockchainSum += consumerToBlockchain;

  const totalCDC = dbToKafka + kafkaToConsumer + consumerProcessing;
  const totalSystem = totalCDC + consumerToBlockchain;

  latencyStats.totalCDCSum += totalCDC;
  latencyStats.totalSystemSum += totalSystem;

  latencyStats.minDbToKafka = Math.min(latencyStats.minDbToKafka, dbToKafka);
  latencyStats.maxDbToKafka = Math.max(latencyStats.maxDbToKafka, dbToKafka);
  latencyStats.minKafkaToConsumer = Math.min(latencyStats.minKafkaToConsumer, kafkaToConsumer);
  latencyStats.maxKafkaToConsumer = Math.max(latencyStats.maxKafkaToConsumer, kafkaToConsumer);
  latencyStats.minConsumerProcessing = Math.min(latencyStats.minConsumerProcessing, consumerProcessing);
  latencyStats.maxConsumerProcessing = Math.max(latencyStats.maxConsumerProcessing, consumerProcessing);
  latencyStats.minConsumerToBlockchain = Math.min(latencyStats.minConsumerToBlockchain, consumerToBlockchain);
  latencyStats.maxConsumerToBlockchain = Math.max(latencyStats.maxConsumerToBlockchain, consumerToBlockchain);
  latencyStats.minTotalCDC = Math.min(latencyStats.minTotalCDC, totalCDC);
  latencyStats.maxTotalCDC = Math.max(latencyStats.maxTotalCDC, totalCDC);
  latencyStats.minTotalSystem = Math.min(latencyStats.minTotalSystem, totalSystem);
  latencyStats.maxTotalSystem = Math.max(latencyStats.maxTotalSystem, totalSystem);
}

/**
 * Get latency averages for reporting
 */
function getLatencyAverages() {
  const count = latencyStats.totalEvents;
  if (count === 0) return null;

  return {
    avgDbToKafka: Math.round(latencyStats.dbToKafkaSum / count),
    avgKafkaToConsumer: Math.round(latencyStats.kafkaToConsumerSum / count),
    avgConsumerProcessing: Math.round(latencyStats.consumerProcessingSum / count),
    avgConsumerToBlockchain: Math.round(latencyStats.consumerToBlockchainSum / count),
    avgTotalCDC: Math.round(latencyStats.totalCDCSum / count),
    avgTotalSystem: Math.round(latencyStats.totalSystemSum / count),
    minDbToKafka: latencyStats.minDbToKafka === Infinity ? 0 : latencyStats.minDbToKafka,
    maxDbToKafka: latencyStats.maxDbToKafka,
    minKafkaToConsumer: latencyStats.minKafkaToConsumer === Infinity ? 0 : latencyStats.minKafkaToConsumer,
    maxKafkaToConsumer: latencyStats.maxKafkaToConsumer,
    minConsumerProcessing: latencyStats.minConsumerProcessing === Infinity ? 0 : latencyStats.minConsumerProcessing,
    maxConsumerProcessing: latencyStats.maxConsumerProcessing,
    minConsumerToBlockchain: latencyStats.minConsumerToBlockchain === Infinity ? 0 : latencyStats.minConsumerToBlockchain,
    maxConsumerToBlockchain: latencyStats.maxConsumerToBlockchain,
    minTotalCDC: latencyStats.minTotalCDC === Infinity ? 0 : latencyStats.minTotalCDC,
    maxTotalCDC: latencyStats.maxTotalCDC,
    minTotalSystem: latencyStats.minTotalSystem === Infinity ? 0 : latencyStats.minTotalSystem,
    maxTotalSystem: latencyStats.maxTotalSystem,
    totalEvents: count,
    avgBatchTime: batchesProcessed > 0 ? Math.round(totalBatchTime / batchesProcessed) : 0,
    totalBatches: batchesProcessed
  };
}

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
      return { shouldSkip: true, timeDiff };
    }
  }

  recentRecords.set(recordId, {
    timestamp: now,
    modifiedTimestamp: modifiedTimestamp
  });

  return { shouldSkip: false, timeDiff: 0 };
}

/**
 * Send data to blockchain API
 */
async function sendToBlockchain(apiEndpoint, endpoint, transformedData) {
  return concurrencyLimiter.execute(async () => {
    const blockchainStartTime = Date.now();

    const { optimization, ...payload } = transformedData;
    const fullPayload = { privateKey: PRIVATE_KEY, ...payload };

    try {
      const dataKey = Object.keys(payload)[0];
      const recordId = payload[dataKey]?.recordId;

      const response = await axios.post(`${apiEndpoint}${endpoint}`, fullPayload, {
        timeout: 10000,
        headers: { 'Content-Type': 'application/json' }
      });

      const blockchainLatency = Date.now() - blockchainStartTime;

      if (response.data.success) {
        const blockNumber = response.data.blockchain?.blockNumber || response.data.blockNumber;
        const txHash = response.data.blockchain?.transactionHash || response.data.transactionHash;
        console.log(`SUCCESS: ${endpoint} ${recordId} -> Block ${blockNumber} (${txHash?.slice(0, 10)}...) [${blockchainLatency}ms]`);
        return { success: true, latency: blockchainLatency };
      } else {
        console.log(`ERROR: API failed for ${endpoint} - ${JSON.stringify(response.data)}`);
        return { success: false, latency: blockchainLatency };
      }
    } catch (error) {
      const blockchainLatency = Date.now() - blockchainStartTime;
      const errorMsg = error.response?.data?.error || error.message;
      console.log(`ERROR: ${endpoint} blockchain call failed - ${errorMsg}`);
      return { success: false, latency: blockchainLatency };
    }
  });
}

// Batch processing
const messageQueue = [];
let batchTimeout = null;

async function processBatch() {
  if (messageQueue.length === 0) return;

  const batchStartTime = Date.now();
  const batch = messageQueue.splice(0, BATCH_SIZE);

  console.log(`\n=== Processing batch of ${batch.length} messages ===`);

  const promises = batch.map(({ topic, message, consumerReceiveTime }) =>
    processMessage(topic, message, consumerReceiveTime)
  );

  await Promise.allSettled(promises);

  const batchTime = Date.now() - batchStartTime;
  batchesProcessed++;
  totalBatchTime += batchTime;

  console.log(`=== Batch completed in ${batchTime}ms (avg: ${Math.round(batchTime / batch.length)}ms/message) ===\n`);
}

async function queueMessage(topic, message) {
  const consumerReceiveTime = Date.now();
  messageQueue.push({ topic, message, consumerReceiveTime });

  if (batchTimeout) {
    clearTimeout(batchTimeout);
  }

  if (messageQueue.length >= BATCH_SIZE) {
    await processBatch();
  } else {
    batchTimeout = setTimeout(processBatch, BATCH_TIMEOUT);
  }
}

/**
 * Process a CDC message using the appropriate adapter
 */
async function processMessage(topic, message, consumerReceiveTime) {
  const kafkaMessageTimestamp = parseInt(message.timestamp);
  const registry = await getRegistry();

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

    // Get app configuration from registry
    const appConfig = registry.getAppByTopic(topic);
    if (!appConfig) {
      console.log(`WARNING: No app config found for topic ${topic}`);
      return;
    }

    // Get adapter for this app
    const adapter = getAdapter(appConfig);
    if (!adapter) {
      console.log(`WARNING: No adapter for app ${appConfig.name}`);
      return;
    }

    // Extract table name from topic
    const topicParts = topic.split('.');
    const tableName = topicParts[topicParts.length - 1];

    // Check if table is in target list (from config or env)
    const targetTables = appConfig.tables?.map(t => t.name) || LEGACY_TARGET_TABLES;
    if (targetTables.length > 0 && !targetTables.includes(tableName)) {
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

    // Use adapter to extract record ID
    const recordId = adapter.extractRecordId(changeData);
    if (!recordId) {
      console.log(`SKIP: No record ID for ${tableName}`);
      return;
    }

    // Check for delete marker
    if (changeData.__deleted === 'true' || changeData.__deleted === true) {
      eventCounter++;
      console.log(`Event #${eventCounter}: ${tableName} ${recordId} DELETE - SKIPPED`);
      return;
    }

    // Use adapter to detect operation
    const operation = adapter.detectOperation(changeEvent, changeData);

    // Use adapter to get modified timestamp
    const modifiedTimestamp = adapter.getModifiedTimestamp(changeData);

    // Deduplication check
    const dupCheck = shouldSkipDuplicate(recordId, modifiedTimestamp);
    if (dupCheck.shouldSkip) {
      eventCounter++;
      console.log(`Event #${eventCounter}: ${tableName} ${recordId} ${operation} - DUPLICATE (${dupCheck.timeDiff}ms)`);
      skipped++;
      return;
    }

    eventCounter++;
    console.log(`Event #${eventCounter}: [${appConfig.name}] ${tableName} ${recordId} ${operation}`);

    // Start consumer processing timer
    const processingStartTime = Date.now();

    // Use adapter to get event timestamp for latency calculation
    const dbEventTime = adapter.getEventTimestamp(changeEvent, changeData, kafkaMessageTimestamp);

    // Calculate latencies
    const dbToKafkaLatency = Math.max(0, kafkaMessageTimestamp - dbEventTime);
    const kafkaToConsumerLatency = consumerReceiveTime - kafkaMessageTimestamp;

    // Use adapter to transform data for blockchain
    const transformedData = adapter.transformForBlockchain(tableName, changeData);
    const consumerProcessingLatency = Date.now() - processingStartTime;

    // Get API endpoint and table endpoint from adapter/config
    const apiEndpoint = adapter.getApiEndpoint();
    const tableEndpoint = adapter.getEndpoint(tableName);

    // Send to blockchain
    const blockchainResult = await sendToBlockchain(apiEndpoint, tableEndpoint, transformedData);
    const consumerToBlockchainLatency = blockchainResult.latency;

    // Calculate aggregated metrics
    const totalCDCLatency = dbToKafkaLatency + kafkaToConsumerLatency + consumerProcessingLatency;
    const totalSystemLatency = totalCDCLatency + consumerToBlockchainLatency;

    // Compact latency display
    console.log(`  Latency: DB→Kafka ${dbToKafkaLatency}ms | Kafka→Consumer ${kafkaToConsumerLatency}ms | Processing ${consumerProcessingLatency}ms | Blockchain ${consumerToBlockchainLatency}ms | Total ${totalSystemLatency}ms`);

    // Update statistics
    updateLatencyStats(dbToKafkaLatency, kafkaToConsumerLatency, consumerProcessingLatency, consumerToBlockchainLatency);

    if (blockchainResult.success) {
      processed++;
    } else {
      errors++;
    }

  } catch (error) {
    console.log(`ERROR: Processing failed for topic ${topic} - ${error.message}`);
    errors++;
  }
}

/**
 * Discover topics based on registered apps
 */
async function discoverTopics(registry) {
  try {
    const admin = kafka.admin();
    await admin.connect();
    const allTopics = await admin.listTopics();

    // Get all registered apps
    const apps = registry.getAllApps();
    const relevantTopics = [];

    for (const topic of allTopics) {
      // Skip schema change topics
      if (topic.includes('schema-changes')) continue;

      const parts = topic.split('.');
      if (parts.length < 3) continue;

      const prefix = parts[0];
      const tableName = parts[parts.length - 1];

      // Check if topic matches any registered app
      for (const app of apps) {
        const appPrefix = app.kafka?.topicPrefix || app.name;
        if (prefix === appPrefix) {
          // Check if table is in app's target list
          const targetTables = app.tables?.map(t => t.name) || [];
          if (targetTables.length === 0 || targetTables.includes(tableName)) {
            relevantTopics.push(topic);
            break;
          }
        }
      }
    }

    await admin.disconnect();
    console.log(`Discovered topics: ${relevantTopics.length > 0 ? relevantTopics.join(', ') : 'none'}`);
    return relevantTopics;
  } catch (error) {
    console.log(`ERROR: Topic discovery failed - ${error.message}`);
    return [];
  }
}

/**
 * Build expected topics from configuration
 */
function buildExpectedTopics(registry) {
  const topics = [];
  const apps = registry.getAllApps();

  for (const app of apps) {
    const prefix = app.kafka?.topicPrefix || app.name;
    const dbName = LEGACY_DB_NAME || 'database';
    const tables = app.tables?.map(t => t.name) || LEGACY_TARGET_TABLES;

    for (const table of tables) {
      topics.push(`${prefix}.${dbName}.${table}`);
    }
  }

  return topics;
}

/**
 * Main startup function
 */
async function start() {
  console.log('Starting Generic Blockchain-ERP Integration Consumer...');
  console.log(`Kafka: ${KAFKA_BROKER}`);
  console.log(`Deduplication Window: ${DEDUP_WINDOW_MS}ms`);
  console.log(`Batch Size: ${BATCH_SIZE}, Max Concurrent: ${MAX_CONCURRENT_REQUESTS}`);

  // Initialize registry
  const registry = await getRegistry();
  const apps = registry.getAllApps();

  console.log(`\nLoaded ${apps.length} app configuration(s):`);
  for (const app of apps) {
    console.log(`  - ${app.displayName || app.name} (${app.database.type})`);
    console.log(`    API: ${app.blockchain?.apiEndpoint || LEGACY_API_ENDPOINT}`);
    console.log(`    Tables: ${app.tables?.map(t => t.name).join(', ') || 'all'}`);
  }

  // Test API connections
  console.log('\nTesting API connections...');
  for (const app of apps) {
    const apiEndpoint = app.blockchain?.apiEndpoint || LEGACY_API_ENDPOINT;
    try {
      const response = await axios.get(apiEndpoint, { timeout: 5000 });
      console.log(`  ${app.name}: OK (${response.data.name || 'API Server'})`);
    } catch (error) {
      console.log(`  ${app.name}: FAILED - ${error.message}`);
    }
  }

  // Connect to Kafka
  try {
    await consumer.connect();
    console.log('\nKafka connection: OK');
    connected = true;
  } catch (error) {
    console.log(`Kafka connection: FAILED - ${error.message}`);
    process.exit(1);
  }

  // Discover and subscribe to topics
  const availableTopics = await discoverTopics(registry);

  if (availableTopics.length === 0) {
    console.log('WARNING: No CDC topics found!');
    const expectedTopics = buildExpectedTopics(registry);
    console.log(`Expected topics: ${expectedTopics.join(', ')}`);
    await consumer.subscribe({ topics: expectedTopics, fromBeginning: false });
  } else {
    await consumer.subscribe({ topics: availableTopics, fromBeginning: false });
    console.log(`Subscribed to: ${availableTopics.join(', ')}`);
  }

  // Start consumer
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await queueMessage(topic, message);
    },
  });

  console.log('\nGeneric Consumer ready with multi-database support...\n');

  // Performance report every 30 seconds
  setInterval(() => {
    if (processed > 0 || errors > 0 || skipped > 0) {
      console.log(`\n===== PERFORMANCE REPORT =====`);
      console.log(`Events: ${eventCounter} total, ${processed} processed, ${errors} errors, ${skipped} skipped`);
      console.log(`Success Rate: ${processed > 0 ? Math.round((processed / (processed + errors)) * 100) : 0}%`);
      console.log(`Queue Size: ${messageQueue.length} pending messages`);

      const avgLatency = getLatencyAverages();
      if (avgLatency) {
        console.log(`\n--- Latency Stats (${avgLatency.totalEvents} events) ---`);
        console.log(`1. DB to Kafka:          avg ${avgLatency.avgDbToKafka}ms (${avgLatency.minDbToKafka}-${avgLatency.maxDbToKafka}ms)`);
        console.log(`2. Kafka to Consumer:    avg ${avgLatency.avgKafkaToConsumer}ms (${avgLatency.minKafkaToConsumer}-${avgLatency.maxKafkaToConsumer}ms)`);
        console.log(`3. Consumer Processing:  avg ${avgLatency.avgConsumerProcessing}ms (${avgLatency.minConsumerProcessing}-${avgLatency.maxConsumerProcessing}ms)`);
        console.log(`4. Consumer to Blockchain: avg ${avgLatency.avgConsumerToBlockchain}ms (${avgLatency.minConsumerToBlockchain}-${avgLatency.maxConsumerToBlockchain}ms)`);
        console.log(`\nTotal CDC (1+2+3):       avg ${avgLatency.avgTotalCDC}ms`);
        console.log(`Total System (1+2+3+4):  avg ${avgLatency.avgTotalSystem}ms`);
        console.log(`Throughput: ${Math.round(avgLatency.totalEvents / (Date.now() - startTime) * 1000)} events/sec`);
      }
      console.log('==============================\n');
    }
  }, 30000);
}

/**
 * Graceful shutdown
 */
async function shutdown() {
  console.log('\nShutting down consumer...');

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

  console.log(`\n===== FINAL REPORT =====`);
  console.log(`Total Events: ${eventCounter}`);
  console.log(`Processed: ${processed}`);
  console.log(`Errors: ${errors}`);
  console.log(`Skipped: ${skipped}`);
  console.log(`Success Rate: ${processed > 0 ? Math.round((processed / (processed + errors)) * 100) : 0}%`);
  console.log(`Runtime: ${Math.round((Date.now() - startTime) / 1000)}s`);

  const avgLatency = getLatencyAverages();
  if (avgLatency) {
    console.log(`\nFinal Latency Summary:`);
    console.log(`  Total CDC: avg ${avgLatency.avgTotalCDC}ms`);
    console.log(`  Total System: avg ${avgLatency.avgTotalSystem}ms`);
    console.log(`  Throughput: ${Math.round(avgLatency.totalEvents / (Date.now() - startTime) * 1000)} events/sec`);
  }

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
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  shutdown();
});

// Start consumer
start().catch(error => {
  console.error(`Startup failed: ${error.message}`);
  console.error(error.stack);
  process.exit(1);
});