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

// Timezone offset configuration
const ERPNEXT_TIMEZONE_OFFSET_MS = 7 * 60 * 60 * 1000; // 7 hours in milliseconds

// Deduplication settings
const DEDUP_WINDOW_MS = 10000; // 10 seconds window for deduplication
const recentRecords = new Map(); // Store recent records with timestamps

// Statistics
let eventCounter = 0;
let processed = 0;
let errors = 0;
let skipped = 0;
let connected = false;

// Enhanced Latency tracking for all components
let latencyStats = {
  totalEvents: 0,
  erpnextToKafkaSum: 0,
  kafkaToConsumerSum: 0,
  consumerProcessingSum: 0,
  consumerToBlockchainSum: 0,
  totalCDCSum: 0,
  totalSystemSum: 0,
  minERPNextToKafka: Infinity,
  maxERPNextToKafka: 0,
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
  retry: { initialRetryTime: 1000, retries: 8 },
  connectionTimeout: 3000,
  requestTimeout: 30000
});

const consumer = kafka.consumer({
  groupId: 'blockchain-consumer-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxBytesPerPartition: 1048576,
  minBytes: 1,
  maxBytes: 10485760,
  maxWaitTimeInMs: 5000
});

// API endpoints mapping
const ENDPOINTS = {
  tabEmployee: '/employees',
  tabAttendance: '/attendances',
  tabUser: '/users',
  tabTask: '/tasks',
  tabCompany: '/companies'
};

// Update comprehensive latency statistics
function updateLatencyStats(erpnextToKafka, kafkaToConsumer, consumerProcessing, consumerToBlockchain) {
  latencyStats.totalEvents++;

  latencyStats.erpnextToKafkaSum += erpnextToKafka;
  latencyStats.kafkaToConsumerSum += kafkaToConsumer;
  latencyStats.consumerProcessingSum += consumerProcessing;
  latencyStats.consumerToBlockchainSum += consumerToBlockchain;

  const totalCDC = erpnextToKafka + kafkaToConsumer + consumerProcessing;
  const totalSystem = totalCDC + consumerToBlockchain;

  latencyStats.totalCDCSum += totalCDC;
  latencyStats.totalSystemSum += totalSystem;

  // Update min/max
  latencyStats.minERPNextToKafka = Math.min(latencyStats.minERPNextToKafka, erpnextToKafka);
  latencyStats.maxERPNextToKafka = Math.max(latencyStats.maxERPNextToKafka, erpnextToKafka);
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

// Get comprehensive latency averages
function getLatencyAverages() {
  const count = latencyStats.totalEvents;
  if (count === 0) return null;

  return {
    avgERPNextToKafka: Math.round(latencyStats.erpnextToKafkaSum / count),
    avgKafkaToConsumer: Math.round(latencyStats.kafkaToConsumerSum / count),
    avgConsumerProcessing: Math.round(latencyStats.consumerProcessingSum / count),
    avgConsumerToBlockchain: Math.round(latencyStats.consumerToBlockchainSum / count),
    avgTotalCDC: Math.round(latencyStats.totalCDCSum / count),
    avgTotalSystem: Math.round(latencyStats.totalSystemSum / count),
    minERPNextToKafka: latencyStats.minERPNextToKafka === Infinity ? 0 : latencyStats.minERPNextToKafka,
    maxERPNextToKafka: latencyStats.maxERPNextToKafka,
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
    totalEvents: count
  };
}

// Convert ERPNext timestamp to ISO string with timezone correction
function convertTimestamp(timestamp) {
  if (!timestamp) return new Date().toISOString();
  const milliseconds = Math.floor(timestamp / 1000);
  const correctedMilliseconds = milliseconds - ERPNEXT_TIMEZONE_OFFSET_MS;
  return new Date(correctedMilliseconds).toISOString();
}

// Get ERPNext event timestamp with timezone correction
function getERPNextEventTime(changeEvent, changeData, kafkaTimestamp) {
  // Use Debezium source timestamp if available
  if (changeEvent.payload?.source?.ts_ms) {
    return changeEvent.payload.source.ts_ms;
  }

  if (changeEvent.payload?.ts_ms) {
    return changeEvent.payload.ts_ms;
  }

  if (changeEvent.connector_processing_time) {
    return changeEvent.connector_processing_time;
  }

  // Use ERPNext modified timestamp with timezone correction
  if (changeData.modified) {
    const erpnextTimeMs = Math.floor(changeData.modified / 1000);
    return erpnextTimeMs - ERPNEXT_TIMEZONE_OFFSET_MS;
  }

  // Use ERPNext creation timestamp with timezone correction
  if (changeData.creation) {
    const erpnextTimeMs = Math.floor(changeData.creation / 1000);
    return erpnextTimeMs - ERPNEXT_TIMEZONE_OFFSET_MS;
  }

  return kafkaTimestamp;
}

// Detect CDC operation type
function detectOperation(changeEvent, changeData) {
  if (changeEvent.payload?.op) {
    const opMap = { 'c': 'CREATE', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ' };
    return opMap[changeEvent.payload.op] || changeEvent.payload.op;
  }

  if (changeData.__deleted === 'true' || changeData.__deleted === true) {
    return 'DELETE';
  }

  if (changeData.creation && changeData.modified) {
    const timeDiff = Math.abs(changeData.modified - changeData.creation);
    return timeDiff < 5000000 ? 'CREATE' : 'UPDATE';
  }

  return 'CREATE';
}

// Check if record should be deduplicated
function shouldSkipDuplicate(recordId, modifiedTimestamp) {
  const now = Date.now();
  const recordKey = recordId;

  // Clean old entries
  for (const [key, data] of recentRecords.entries()) {
    if (now - data.timestamp > DEDUP_WINDOW_MS) {
      recentRecords.delete(key);
    }
  }

  if (recentRecords.has(recordKey)) {
    const existing = recentRecords.get(recordKey);
    const timeDiff = Math.abs(new Date(modifiedTimestamp).getTime() - new Date(existing.modifiedTimestamp).getTime());
    if (timeDiff < 5000) {
      return { shouldSkip: true, timeDiff };
    }
  }

  recentRecords.set(recordKey, {
    timestamp: now,
    modifiedTimestamp: modifiedTimestamp
  });

  return { shouldSkip: false, timeDiff: 0 };
}

// Transform data for blockchain API
function transformData(table, data) {
  const optimization = optimizeForBlockchain(table, data);

  const baseData = {
    recordId: data.name || data.id || `${table}_${Date.now()}`,
    createdTimestamp: convertTimestamp(data.creation),
    modifiedTimestamp: convertTimestamp(data.modified),
    modifiedBy: data.modified_by || 'system',
    allData: optimization.filteredData
  };

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
  const blockchainStartTime = Date.now();

  const endpoint = ENDPOINTS[table];
  if (!endpoint) {
    console.log(`WARNING: No endpoint for ${table}`);
    return { success: false, latency: 0 };
  }

  const { optimization, ...payload } = transformedData;
  const fullPayload = { privateKey: PRIVATE_KEY, ...payload };

  try {
    const recordId = Object.values(payload)[0]?.recordId;
    console.log(`Sending ${table} (${optimization.reductionPercent}% optimized: ${optimization.originalSize} -> ${optimization.filteredSize} chars)`);

    const response = await axios.post(`${API_ENDPOINT}${endpoint}`, fullPayload, {
      timeout: 15000,
      headers: { 'Content-Type': 'application/json' }
    });

    const blockchainLatency = Date.now() - blockchainStartTime;

    if (response.data.success) {
      const blockNumber = response.data.blockchain?.blockNumber || response.data.blockNumber;
      const txHash = response.data.blockchain?.transactionHash || response.data.transactionHash;
      console.log(`SUCCESS: ${table} ${recordId} -> Block ${blockNumber} (${txHash?.slice(0, 10)}...)`);
      return { success: true, latency: blockchainLatency };
    } else {
      console.log(`ERROR: API failed for ${table} - ${JSON.stringify(response.data)}`);
      return { success: false, latency: blockchainLatency };
    }
  } catch (error) {
    const blockchainLatency = Date.now() - blockchainStartTime;
    const errorMsg = error.response?.data?.error || error.message;
    console.log(`ERROR: ${table} blockchain call failed - ${errorMsg}`);
    return { success: false, latency: blockchainLatency };
  }
}

// Process CDC message
async function processMessage(topic, message) {
  const consumerReceiveTime = Date.now();
  const kafkaMessageTimestamp = parseInt(message.timestamp);

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

    const topicParts = topic.split('.');
    const tableName = topicParts[topicParts.length - 1];

    if (!TARGET_TABLES.includes(tableName)) {
      return;
    }

    // Handle Debezium CDC event structure
    let changeData;
    if (changeEvent.payload) {
      changeData = changeEvent.payload.after || changeEvent.payload.before;
      if (changeEvent.payload.op === 'd') {
        eventCounter++;
        console.log(`\nEvent #${eventCounter}`);
        console.log(`${tableName} ${changeData?.name || 'unknown'} DELETE`);
        console.log(`SKIP: Delete operations not sent to blockchain\n`);
        return;
      }
    } else {
      changeData = changeEvent;
    }

    if (!changeData?.name) {
      console.log(`SKIP: No record name for ${tableName}`);
      return;
    }

    if (changeData.__deleted === 'true' || changeData.__deleted === true) {
      eventCounter++;
      console.log(`\nEvent #${eventCounter}`);
      console.log(`${tableName} ${changeData.name} DELETE`);
      console.log(`SKIP: Deleted records not sent to blockchain\n`);
      return;
    }

    const operation = detectOperation(changeEvent, changeData);
    const modifiedTimestamp = convertTimestamp(changeData.modified);

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

    // Start consumer processing timer HERE - before any processing logic
    const processingStartTime = Date.now();

    // Get ERPNext event time with timezone correction
    let erpnextEventTime;
    if (operation === 'UPDATE' && changeData.modified) {
      const modifiedTimeMs = Math.floor(changeData.modified / 1000);
      erpnextEventTime = modifiedTimeMs - ERPNEXT_TIMEZONE_OFFSET_MS;
    } else {
      erpnextEventTime = getERPNextEventTime(changeEvent, changeData, kafkaMessageTimestamp);
    }

    // Calculate latencies
    const erpnextToKafkaLatency = Math.max(0, kafkaMessageTimestamp - erpnextEventTime);
    const kafkaToConsumerLatency = consumerReceiveTime - kafkaMessageTimestamp;

    // Transform data (part of consumer processing)
    const transformedData = transformData(tableName, changeData);

    // End consumer processing timer HERE - after all processing is done
    const consumerProcessingLatency = Date.now() - processingStartTime;

    // Send to blockchain
    const blockchainResult = await sendToBlockchain(tableName, transformedData);
    const consumerToBlockchainLatency = blockchainResult.latency;

    // Calculate aggregated metrics
    const totalCDCLatency = erpnextToKafkaLatency + kafkaToConsumerLatency + consumerProcessingLatency;
    const totalSystemLatency = totalCDCLatency + consumerToBlockchainLatency;

    // Display latency breakdown
    console.log(`\n--- Enhanced Latency Breakdown ---`);
    console.log(`1. ERPNext DB to Kafka:      ${erpnextToKafkaLatency}ms`);
    console.log(`2. Kafka to Consumer:        ${kafkaToConsumerLatency}ms`);
    console.log(`3. Consumer Processing:      ${consumerProcessingLatency}ms`);
    console.log(`4. Consumer to Blockchain:   ${consumerToBlockchainLatency}ms`);
    console.log(`--- Aggregated Metrics ---`);
    console.log(`Total CDC Latency:           ${totalCDCLatency}ms (1+2+3)`);
    console.log(`Total System Latency:       ${totalSystemLatency}ms (1+2+3+4)`);

    // Update statistics
    if (blockchainResult.success) {
      processed++;
      updateLatencyStats(erpnextToKafkaLatency, kafkaToConsumerLatency, consumerProcessingLatency, consumerToBlockchainLatency);
    } else {
      errors++;
      updateLatencyStats(erpnextToKafkaLatency, kafkaToConsumerLatency, consumerProcessingLatency, consumerToBlockchainLatency);
    }

    console.log('');

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
  console.log('Starting Enhanced Blockchain-ERP Integration Consumer...');
  console.log(`Kafka: ${KAFKA_BROKER}`);
  console.log(`API: ${API_ENDPOINT}`);
  console.log(`Database: ${DB_NAME}`);
  console.log(`Tables: ${TARGET_TABLES.join(', ')}`);
  console.log(`ERPNext Timezone Offset: +${ERPNEXT_TIMEZONE_OFFSET_MS / 1000 / 60 / 60} hours`);
  console.log(`Deduplication Window: ${DEDUP_WINDOW_MS}ms`);

  try {
    const response = await axios.get(API_ENDPOINT, { timeout: 5000 });
    console.log(`API connection: OK (${response.data.name || 'API Server'})`);
  } catch (error) {
    console.log(`API connection: FAILED - ${error.message}`);
  }

  try {
    await consumer.connect();
    console.log('Kafka connection: OK');
    connected = true;
  } catch (error) {
    console.log(`Kafka connection: FAILED - ${error.message}`);
    process.exit(1);
  }

  const availableTopics = await discoverTopics();

  if (availableTopics.length === 0) {
    console.log('WARNING: No CDC topics found!');
    const expectedTopics = TARGET_TABLES.map(table => `${TOPIC_PREFIX}.${DB_NAME}.${table}`);
    console.log(`Expected topics: ${expectedTopics.join(', ')}`);
    await consumer.subscribe({ topics: expectedTopics, fromBeginning: false });
  } else {
    await consumer.subscribe({ topics: availableTopics, fromBeginning: false });
    console.log(`Subscribed to: ${availableTopics.join(', ')}`);
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await processMessage(topic, message);
    },
  });

  console.log('Consumer ready with enhanced latency tracking...\n');

  // Enhanced stats every 60 seconds
  setInterval(() => {
    if (processed > 0 || errors > 0 || skipped > 0) {
      console.log(`\n===== COMPREHENSIVE PERFORMANCE REPORT =====`);
      console.log(`Events: ${eventCounter} total, ${processed} processed, ${errors} errors, ${skipped} skipped`);
      console.log(`Success Rate: ${processed > 0 ? Math.round((processed / (processed + errors)) * 100) : 0}%`);

      const avgLatency = getLatencyAverages();
      if (avgLatency) {
        console.log(`\n--- Enhanced Latency Stats (${avgLatency.totalEvents} events) ---`);
        console.log(`1. ERPNext DB to Kafka:     avg ${avgLatency.avgERPNextToKafka}ms (${avgLatency.minERPNextToKafka}-${avgLatency.maxERPNextToKafka}ms)`);
        console.log(`2. Kafka to Consumer:       avg ${avgLatency.avgKafkaToConsumer}ms (${avgLatency.minKafkaToConsumer}-${avgLatency.maxKafkaToConsumer}ms)`);
        console.log(`3. Consumer Processing:     avg ${avgLatency.avgConsumerProcessing}ms (${avgLatency.minConsumerProcessing}-${avgLatency.maxConsumerProcessing}ms)`);
        console.log(`4. Consumer to Blockchain:  avg ${avgLatency.avgConsumerToBlockchain}ms (${avgLatency.minConsumerToBlockchain}-${avgLatency.maxConsumerToBlockchain}ms)`);

        console.log(`\n--- Aggregated Metrics ---`);
        console.log(`Total CDC (1+2+3):          avg ${avgLatency.avgTotalCDC}ms (${avgLatency.minTotalCDC}-${avgLatency.maxTotalCDC}ms)`);
        console.log(`Total System (1+2+3+4):     avg ${avgLatency.avgTotalSystem}ms (${avgLatency.minTotalSystem}-${avgLatency.maxTotalSystem}ms)`);

        const bottleneck = [
          { name: 'ERPNext to Kafka', value: avgLatency.avgERPNextToKafka },
          { name: 'Kafka to Consumer', value: avgLatency.avgKafkaToConsumer },
          { name: 'Consumer Processing', value: avgLatency.avgConsumerProcessing },
          { name: 'Consumer to Blockchain', value: avgLatency.avgConsumerToBlockchain }
        ].sort((a, b) => b.value - a.value)[0];

        console.log(`\n--- Performance Insights ---`);
        console.log(`Primary Bottleneck: ${bottleneck.name} (${bottleneck.value}ms avg)`);
        console.log(`CDC Efficiency: ${Math.round((avgLatency.avgTotalCDC / avgLatency.avgTotalSystem) * 100)}% of total time`);
        console.log(`Blockchain Impact: ${Math.round((avgLatency.avgConsumerToBlockchain / avgLatency.avgTotalSystem) * 100)}% of total time`);
      }
      console.log('=============================================\n');
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

  console.log(`\n===== FINAL COMPREHENSIVE REPORT =====`);
  console.log(`Total Events: ${eventCounter}`);
  console.log(`Processed: ${processed}`);
  console.log(`Errors: ${errors}`);
  console.log(`Skipped: ${skipped}`);
  console.log(`Final Success Rate: ${processed > 0 ? Math.round((processed / (processed + errors)) * 100) : 0}%`);

  const avgLatency = getLatencyAverages();
  if (avgLatency) {
    console.log(`\n--- Final Latency Summary (${avgLatency.totalEvents} events) ---`);
    console.log(`1. ERPNext DB to Kafka:     avg ${avgLatency.avgERPNextToKafka}ms (${avgLatency.minERPNextToKafka}-${avgLatency.maxERPNextToKafka}ms)`);
    console.log(`2. Kafka to Consumer:       avg ${avgLatency.avgKafkaToConsumer}ms (${avgLatency.minKafkaToConsumer}-${avgLatency.maxKafkaToConsumer}ms)`);
    console.log(`3. Consumer Processing:     avg ${avgLatency.avgConsumerProcessing}ms (${avgLatency.minConsumerProcessing}-${avgLatency.maxConsumerProcessing}ms)`);
    console.log(`4. Consumer to Blockchain:  avg ${avgLatency.avgConsumerToBlockchain}ms (${avgLatency.minConsumerToBlockchain}-${avgLatency.maxConsumerToBlockchain}ms)`);

    console.log(`\n--- Final Aggregated Metrics ---`);
    console.log(`Total CDC (1+2+3):          avg ${avgLatency.avgTotalCDC}ms (${avgLatency.minTotalCDC}-${avgLatency.maxTotalCDC}ms)`);
    console.log(`Total System (1+2+3+4):     avg ${avgLatency.avgTotalSystem}ms (${avgLatency.minTotalSystem}-${avgLatency.maxTotalSystem}ms)`);

    const bottleneck = [
      { name: 'ERPNext to Kafka', value: avgLatency.avgERPNextToKafka },
      { name: 'Kafka to Consumer', value: avgLatency.avgKafkaToConsumer },
      { name: 'Consumer Processing', value: avgLatency.avgConsumerProcessing },
      { name: 'Consumer to Blockchain', value: avgLatency.avgConsumerToBlockchain }
    ].sort((a, b) => b.value - a.value)[0];

    console.log(`\n--- Final Performance Analysis ---`);
    console.log(`Primary Bottleneck: ${bottleneck.name} (${bottleneck.value}ms avg)`);
    console.log(`CDC Efficiency: ${Math.round((avgLatency.avgTotalCDC / avgLatency.avgTotalSystem) * 100)}% of total time`);
    console.log(`Blockchain Impact: ${Math.round((avgLatency.avgConsumerToBlockchain / avgLatency.avgTotalSystem) * 100)}% of total time`);
  }

  console.log('=======================================');
  console.log('Consumer shutdown complete');
  console.log('=======================================');

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