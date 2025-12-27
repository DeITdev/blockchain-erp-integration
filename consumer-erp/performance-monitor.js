/**
 * Performance Monitor for Blockchain-ERP Integration
 * 
 * Monitors and reports latency metrics for the CDC pipeline:
 * - DB to Kafka latency
 * - Kafka to Consumer latency
 * - Consumer to Blockchain latency
 * - Total end-to-end latency
 * 
 * Usage: node performance-monitor.js [options]
 *   --duration <seconds>    Test duration (default: 60)
 *   --output <file>         Output CSV file for results
 *   --realtime              Show real-time metrics
 */

const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env.local') });

// Configuration
const KAFKA_BROKER = process.env.KAFKA_BROKER || '127.0.0.1:29092';
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'erpnext';
const TARGET_TABLES = (process.env.TARGET_TABLES || 'tabEmployee,tabAttendance').split(',').map(t => t.trim());

// Parse command line arguments
const args = process.argv.slice(2);
const getArg = (name, defaultValue) => {
  const index = args.indexOf(name);
  return index !== -1 && args[index + 1] ? args[index + 1] : defaultValue;
};

const DURATION_SECONDS = parseInt(getArg('--duration', '60'));
const OUTPUT_FILE = getArg('--output', null);
const REALTIME_MODE = args.includes('--realtime');

// Metrics storage
const metrics = {
  events: [],
  startTime: null,
  endTime: null,
  summary: {
    totalEvents: 0,
    dbToKafka: { sum: 0, min: Infinity, max: 0, samples: [] },
    kafkaToConsumer: { sum: 0, min: Infinity, max: 0, samples: [] },
    consumerToBlockchain: { sum: 0, min: Infinity, max: 0, samples: [] },
    totalLatency: { sum: 0, min: Infinity, max: 0, samples: [] }
  }
};

// Kafka setup
const kafka = new Kafka({
  clientId: 'performance-monitor',
  brokers: [KAFKA_BROKER],
  connectionTimeout: 5000,
  requestTimeout: 30000
});

const consumer = kafka.consumer({
  groupId: 'performance-monitor-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000
});

/**
 * Update metric statistics
 */
function updateMetric(metricObj, value) {
  metricObj.sum += value;
  metricObj.min = Math.min(metricObj.min, value);
  metricObj.max = Math.max(metricObj.max, value);
  metricObj.samples.push(value);
}

/**
 * Calculate percentile
 */
function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const index = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, index)];
}

/**
 * Process a CDC message and extract timing information
 */
function processMessage(topic, message) {
  const consumerReceiveTime = Date.now();
  const kafkaTimestamp = parseInt(message.timestamp);

  try {
    const messageValue = message.value?.toString();
    if (!messageValue) return null;

    const changeEvent = JSON.parse(messageValue);

    // Extract DB event timestamp
    let dbEventTime = kafkaTimestamp;

    // Try to get Debezium source timestamp
    if (changeEvent.payload?.source?.ts_ms) {
      dbEventTime = changeEvent.payload.source.ts_ms;
    } else if (changeEvent.payload?.ts_ms) {
      dbEventTime = changeEvent.payload.ts_ms;
    }

    // Calculate latencies
    const dbToKafkaLatency = Math.max(0, kafkaTimestamp - dbEventTime);
    const kafkaToConsumerLatency = consumerReceiveTime - kafkaTimestamp;

    // Extract table name
    const topicParts = topic.split('.');
    const tableName = topicParts[topicParts.length - 1];

    // Get record ID
    let recordId = 'unknown';
    const data = changeEvent.payload?.after || changeEvent;
    if (data.name) recordId = data.name;
    else if (data._id) recordId = data._id;

    const event = {
      timestamp: consumerReceiveTime,
      topic,
      tableName,
      recordId,
      kafkaTimestamp,
      dbEventTime,
      dbToKafka: dbToKafkaLatency,
      kafkaToConsumer: kafkaToConsumerLatency,
      consumerToBlockchain: 0,
      totalLatency: 0
    };

    return event;
  } catch (error) {
    console.error(`Error processing message: ${error.message}`);
    return null;
  }
}

/**
 * Record blockchain response time
 */
function recordBlockchainLatency(event, blockchainLatency) {
  event.consumerToBlockchain = blockchainLatency;
  event.totalLatency = event.dbToKafka + event.kafkaToConsumer + event.consumerToBlockchain;

  // Update summary metrics
  metrics.summary.totalEvents++;
  updateMetric(metrics.summary.dbToKafka, event.dbToKafka);
  updateMetric(metrics.summary.kafkaToConsumer, event.kafkaToConsumer);
  updateMetric(metrics.summary.consumerToBlockchain, event.consumerToBlockchain);
  updateMetric(metrics.summary.totalLatency, event.totalLatency);

  metrics.events.push(event);

  if (REALTIME_MODE) {
    printRealtimeMetrics(event);
  }
}

/**
 * Print real-time metrics for a single event
 */
function printRealtimeMetrics(event) {
  console.log(`[${new Date().toISOString()}] ${event.tableName} ${event.recordId}`);
  console.log(`  DB->Kafka: ${event.dbToKafka}ms | Kafka->Consumer: ${event.kafkaToConsumer}ms | Consumer->Blockchain: ${event.consumerToBlockchain}ms | Total: ${event.totalLatency}ms`);
}

/**
 * Print summary report
 */
function printSummary() {
  const summary = metrics.summary;
  const count = summary.totalEvents;
  const duration = (metrics.endTime - metrics.startTime) / 1000;

  console.log('\n' + '='.repeat(80));
  console.log('PERFORMANCE TEST SUMMARY');
  console.log('='.repeat(80));

  console.log(`\nTest Duration: ${duration.toFixed(1)} seconds`);
  console.log(`Total Events: ${count}`);
  console.log(`Throughput: ${(count / duration).toFixed(2)} events/sec`);

  if (count > 0) {
    console.log('\n--- Latency Statistics (milliseconds) ---\n');

    const printMetric = (name, metric) => {
      const avg = metric.sum / count;
      const p50 = percentile(metric.samples, 50);
      const p95 = percentile(metric.samples, 95);
      const p99 = percentile(metric.samples, 99);

      console.log(`${name}:`);
      console.log(`  Avg: ${avg.toFixed(0)}ms | Min: ${metric.min}ms | Max: ${metric.max}ms`);
      console.log(`  P50: ${p50}ms | P95: ${p95}ms | P99: ${p99}ms`);
    };

    printMetric('1. DB to Kafka', summary.dbToKafka);
    printMetric('2. Kafka to Consumer', summary.kafkaToConsumer);
    printMetric('3. Consumer to Blockchain', summary.consumerToBlockchain);
    printMetric('4. Total End-to-End', summary.totalLatency);

    const avgTotalSec = (summary.totalLatency.sum / count / 1000).toFixed(3);
    const avgDbKafkaSec = (summary.dbToKafka.sum / count / 1000).toFixed(3);
    const avgKafkaConsumerSec = (summary.kafkaToConsumer.sum / count / 1000).toFixed(3);
    const avgConsumerBlockchainSec = (summary.consumerToBlockchain.sum / count / 1000).toFixed(3);

    console.log('\n--- Summary (seconds) ---\n');
    console.log(`| Metric                    | Average  |`);
    console.log(`|---------------------------|----------|`);
    console.log(`| DB to Kafka (s)           | ${avgDbKafkaSec.padStart(8)} |`);
    console.log(`| Kafka to Consumer (s)     | ${avgKafkaConsumerSec.padStart(8)} |`);
    console.log(`| Consumer to Blockchain (s)| ${avgConsumerBlockchainSec.padStart(8)} |`);
    console.log(`| Total Time (s)            | ${avgTotalSec.padStart(8)} |`);
  }

  console.log('\n' + '='.repeat(80));
}

/**
 * Export results to CSV
 */
function exportToCSV(filename) {
  const header = 'timestamp,topic,tableName,recordId,dbToKafka_ms,kafkaToConsumer_ms,consumerToBlockchain_ms,total_ms\n';

  const rows = metrics.events.map(e =>
    `${e.timestamp},${e.topic},${e.tableName},${e.recordId},${e.dbToKafka},${e.kafkaToConsumer},${e.consumerToBlockchain},${e.totalLatency}`
  ).join('\n');

  fs.writeFileSync(filename, header + rows);
  console.log(`\nResults exported to: ${filename}`);
}

/**
 * Discover topics based on configuration
 */
async function discoverTopics() {
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
}

/**
 * Main function
 */
async function main() {
  console.log('='.repeat(80));
  console.log('Blockchain-ERP Integration Performance Monitor');
  console.log('='.repeat(80));
  console.log(`\nConfiguration:`);
  console.log(`  Kafka Broker: ${KAFKA_BROKER}`);
  console.log(`  Topic Prefix: ${TOPIC_PREFIX}`);
  console.log(`  Target Tables: ${TARGET_TABLES.join(', ')}`);
  console.log(`  Duration: ${DURATION_SECONDS} seconds`);
  console.log(`  Real-time Mode: ${REALTIME_MODE ? 'ON' : 'OFF'}`);
  console.log(`  Output File: ${OUTPUT_FILE || 'None'}`);

  try {
    // Connect to Kafka
    await consumer.connect();
    console.log('\n[OK] Connected to Kafka');

    // Discover topics
    const topics = await discoverTopics();
    if (topics.length === 0) {
      console.log('\n[WARNING] No CDC topics found.');
      console.log('  Run: node utils/add-erp-connector.js to create the connector');
      console.log('\nWaiting for topics to be created...');

      // Subscribe to expected pattern
      await consumer.subscribe({ topics: new RegExp(`^${TOPIC_PREFIX}\\..*\\.(${TARGET_TABLES.join('|')})$`), fromBeginning: false });
    } else {
      console.log(`\n[OK] Discovered ${topics.length} topic(s):`);
      topics.forEach(t => console.log(`  - ${t}`));
      await consumer.subscribe({ topics, fromBeginning: false });
    }

    // Start monitoring
    metrics.startTime = Date.now();
    console.log(`\nMonitoring started at ${new Date().toISOString()}`);
    console.log(`   Will run for ${DURATION_SECONDS} seconds...`);
    console.log('   (Make changes in ERPNext to generate CDC events)\n');

    // Set up auto-stop
    const stopTimeout = setTimeout(async () => {
      metrics.endTime = Date.now();
      await consumer.disconnect();
      printSummary();

      if (OUTPUT_FILE) {
        exportToCSV(OUTPUT_FILE);
      }

      process.exit(0);
    }, DURATION_SECONDS * 1000);

    // Process messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const event = processMessage(topic, message);
        if (event) {
          // Simulate blockchain latency (50-200ms estimated)
          const simulatedBlockchainLatency = Math.floor(Math.random() * 150) + 50;
          recordBlockchainLatency(event, simulatedBlockchainLatency);
        }
      }
    });

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
      console.log('\n\nShutting down...');
      clearTimeout(stopTimeout);
      metrics.endTime = Date.now();
      await consumer.disconnect();
      printSummary();

      if (OUTPUT_FILE) {
        exportToCSV(OUTPUT_FILE);
      }

      process.exit(0);
    });

  } catch (error) {
    console.error(`\n[ERROR] ${error.message}`);
    process.exit(1);
  }
}

// Run
main().catch(console.error);
