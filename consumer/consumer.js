// consumer.js - ERPNext CDC Consumer with Blockchain API Integration
// Load environment variables
require('dotenv').config();

const { Kafka } = require('kafkajs');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');

// Import data filter
const { optimizeForBlockchain } = require('./data-filter');

// Get environment variables with fallbacks
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const apiEndpoint = process.env.API_ENDPOINT || 'http://localhost:4001';
const privateKey = process.env.PRIVATE_KEY || '8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63';

// Target table names from environment variable (only supported contracts)
const targetTableNames = process.env.TARGET_TABLES ?
  process.env.TARGET_TABLES.split(',').map(name => name.trim()).filter(name =>
    ['tabUser', 'tabEmployee', 'tabTask', 'tabCompany', 'tabAttendance'].includes(name)
  ) :
  ['tabUser', 'tabEmployee', 'tabTask', 'tabCompany', 'tabAttendance'];

// Create processed records tracker
const PROCESSED_RECORDS_FILE = path.join(__dirname, 'blockchain_integration_log.json');
let processedRecords = [];
let latencyData = [];

// Configure Kafka client
const kafka = new Kafka({
  brokers: [kafkaBroker],
  clientId: 'erpnext-cdc-blockchain-consumer',
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Create consumer instance
const consumer = kafka.consumer({
  groupId: 'erpnext-cdc-blockchain-group',
  sessionTimeout: 60000,
  heartbeatInterval: 10000,
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Load previously processed records
async function loadProcessedRecords() {
  try {
    const data = await fs.readFile(PROCESSED_RECORDS_FILE, 'utf8');
    processedRecords = JSON.parse(data);
    console.log(`Loaded ${processedRecords.length} previously processed records`);
  } catch (error) {
    console.log('No previous processing log found, starting fresh');
    processedRecords = [];
  }
}

// Save processing log
async function saveProcessingLog(eventData) {
  try {
    processedRecords.push(eventData);
    // Keep only the last 1000 events to prevent file from growing too large
    if (processedRecords.length > 1000) {
      processedRecords = processedRecords.slice(-1000);
    }
    await fs.writeFile(PROCESSED_RECORDS_FILE, JSON.stringify(processedRecords, null, 2));
  } catch (error) {
    console.error('ERROR: Error saving processing log:', error);
  }
}

// Save latency data
async function saveLatencyData() {
  try {
    const latencyFile = path.join(__dirname, 'latency_data.json');
    await fs.writeFile(latencyFile, JSON.stringify(latencyData, null, 2));
  } catch (error) {
    console.error('ERROR: Error saving latency data:', error);
  }
}

// Function to check if Kafka topics exist
async function checkTopics() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    const topics = await admin.listTopics();

    // Find ERPNext topics dynamically
    const erpnextTopics = topics.filter(topic =>
      topic.startsWith('erpnext.') &&
      topic.includes('.tab') &&
      !topic.includes('schema-changes')
    );

    // Group topics by database hash
    const topicsByDatabase = {};
    erpnextTopics.forEach(topic => {
      const parts = topic.split('.');
      if (parts.length >= 3) {
        const database = parts[1];
        const tableName = parts[2];

        if (!topicsByDatabase[database]) {
          topicsByDatabase[database] = [];
        }
        topicsByDatabase[database].push({ topic, tableName });
      }
    });

    // Find matching topics for target tables
    const foundTopics = {};
    const missingTables = [];

    targetTableNames.forEach(tableName => {
      let found = false;
      Object.entries(topicsByDatabase).forEach(([database, topics]) => {
        const match = topics.find(t => t.tableName === tableName);
        if (match && !found) {
          foundTopics[tableName] = match.topic;
          found = true;
        }
      });
      if (!found) {
        missingTables.push(tableName);
      }
    });

    console.log(`\nTopic Discovery Results:`);
    targetTableNames.forEach(tableName => {
      const exists = foundTopics[tableName];
      console.log(`  ${tableName}: ${exists ? 'FOUND' : 'NOT FOUND'}`);
      if (exists) {
        console.log(`    Topic: ${exists}`);
      }
    });

    await admin.disconnect();
    return { foundTopics, missingTables, totalFound: Object.keys(foundTopics).length };
  } catch (error) {
    console.error('ERROR: Error checking topics:', error.message);
    return { foundTopics: {}, missingTables: targetTableNames, totalFound: 0 };
  }
}

// Detect operation type from ERPNext CDC events
function detectOperationType(event) {
  if (event.op) {
    return event.op;
  }

  const isDeleted = event.__deleted === true || event.__deleted === "true";
  if (isDeleted) {
    return 'd'; // DELETE
  }

  if (event.creation && !event.modified) {
    return 'r'; // READ (snapshot)
  }

  if (event.creation && event.modified) {
    const creationTime = typeof event.creation === 'number' ? event.creation : Date.parse(event.creation);
    const modifiedTime = typeof event.modified === 'number' ? event.modified : Date.parse(event.modified);

    if (Math.abs(modifiedTime - creationTime) > 1000000) {
      return 'u'; // UPDATE
    }
  }

  return 'c'; // CREATE (default assumption)
}

// Format CDC event data
function formatCDCEvent(topic, message, event) {
  const timestamp = new Date(parseInt(message.timestamp)).toISOString();
  const operation = detectOperationType(event);
  const tableName = topic.split('.').pop();

  const operationMap = {
    'c': 'CREATE',
    'u': 'UPDATE',
    'd': 'DELETE',
    'r': 'READ (Snapshot)'
  };

  return {
    timestamp,
    topic,
    offset: message.offset,
    partition: message.partition,
    operation: operationMap[operation] || operation,
    tableName,
    database: topic.split('.')[1],
    data: event,
    recordId: event.name || event.id || `${tableName}_${Date.now()}`
  };
}

// Contract deployment addresses (from API deployment files)
const CONTRACT_ADDRESSES = {
  'tabUser': '0x2114De86c8Ea1FD8144C2f1e1e94C74E498afB1b',
  'tabEmployee': '0xa9ECbe3F9600f9bF3ec88a428387316714ac95a0',
  'tabTask': '0xF216B6b2D9E76F94f97bE597e2Cec81730520585',
  'tabCompany': '0x0F095aeA9540468B19829d02cC811Ebe5173D615',
  'tabAttendance': '0xc83003B2AD5C3EF3e93Cc3Ef0a48E84dc8DBD718'
};

// Map table names to API endpoints and contract addresses
function getAPIEndpoint(tableName) {
  const endpointMap = {
    'tabUser': '/users',
    'tabEmployee': '/employees',
    'tabTask': '/tasks',
    'tabCompany': '/companies',
    'tabAttendance': '/attendances'
  };

  return endpointMap[tableName] || null;
}

// Get contract address for table
function getContractAddress(tableName) {
  return CONTRACT_ADDRESSES[tableName] || null;
}

// Validate that all contract addresses are loaded
function validateContractAddresses() {
  const missingAddresses = [];
  Object.entries(CONTRACT_ADDRESSES).forEach(([tableName, address]) => {
    if (!address) {
      missingAddresses.push(tableName);
    }
  });

  if (missingAddresses.length > 0) {
    console.error(`ERROR: Missing contract addresses in .env file for: ${missingAddresses.join(', ')}`);
    console.error('Please add the following to your .env file:');
    missingAddresses.forEach(tableName => {
      const envVar = tableName.replace('tab', '').toUpperCase() + '_CONTRACT_ADDRESS';
      console.error(`${envVar}=0x...`);
    });
    process.exit(1);
  }

  console.log('SUCCESS: All contract addresses loaded from environment variables');
}

// Send data to blockchain via API
async function sendToBlockchain(formattedEvent) {
  const startTime = Date.now();

  try {
    const endpoint = getAPIEndpoint(formattedEvent.tableName);
    const contractAddress = getContractAddress(formattedEvent.tableName);

    if (!endpoint) {
      console.log(`WARNING: No blockchain endpoint configured for table: ${formattedEvent.tableName}`);
      return null;
    }

    if (!contractAddress) {
      console.log(`WARNING: No contract address found for table: ${formattedEvent.tableName}`);
      return null;
    }

    // Optimize data before sending to blockchain
    console.log(`INFO: Optimizing data for blockchain storage...`);
    const optimization = optimizeForBlockchain(formattedEvent.tableName, formattedEvent.data);

    // Use filtered data instead of all data
    const optimizedPayload = {
      privateKey: privateKey,
      [`${formattedEvent.tableName.replace('tab', '').toLowerCase()}Data`]: {
        recordId: formattedEvent.recordId,
        createdTimestamp: formattedEvent.data.creation || new Date().toISOString(),
        modifiedTimestamp: formattedEvent.data.modified || new Date().toISOString(),
        modifiedBy: formattedEvent.data.modified_by || 'system',
        allData: optimization.filteredData // Use filtered data instead of raw data
      }
    };

    const url = `${apiEndpoint}${endpoint}`;
    console.log(`INFO: Sending optimized data to blockchain API: ${url}`);
    console.log(`INFO: Record ID: ${formattedEvent.recordId}`);
    console.log(`INFO: Contract Address: ${contractAddress}`);
    console.log(`INFO: Contract Type: ${formattedEvent.tableName.replace('tab', '')}Storage`);
    console.log(`INFO: Data reduced by ${optimization.stats.reductionPercent}% (${optimization.stats.reduction} chars)`);

    const response = await axios.post(url, optimizedPayload, {
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json'
      }
    });

    const endTime = Date.now();
    const blockchainLatency = endTime - startTime;

    console.log(`SUCCESS: Successfully stored optimized data on blockchain!`);
    console.log(`INFO: Blockchain API latency: ${blockchainLatency}ms`);
    console.log(`INFO: Transaction Hash: ${response.data.blockchain?.transactionHash || 'N/A'}`);
    console.log(`INFO: Block Number: ${response.data.blockchain?.blockNumber || 'N/A'}`);
    console.log(`INFO: Used Contract: ${contractAddress}`);
    console.log(`INFO: Final data size: ${optimization.stats.filteredSize} characters`);

    return {
      success: true,
      response: response.data,
      latency: blockchainLatency,
      endpoint: url,
      contractAddress: contractAddress,
      contractType: `${formattedEvent.tableName.replace('tab', '')}Storage`,
      optimization: optimization.stats
    };

  } catch (error) {
    const endTime = Date.now();
    const blockchainLatency = endTime - startTime;

    console.error(`ERROR: Blockchain API error:`, error.response?.data || error.message);
    return {
      success: false,
      error: error.response?.data || error.message,
      latency: blockchainLatency,
      contractAddress: getContractAddress(formattedEvent.tableName)
    };
  }
}

// Process CDC message and send to blockchain
const processCDCMessage = async (topic, message) => {
  const kafkaReceiveTime = Date.now();
  const kafkaTimestamp = parseInt(message.timestamp);
  const kafkaLatency = kafkaReceiveTime - kafkaTimestamp;

  try {
    // Parse message value
    const messageValue = message.value.toString();
    let event;

    try {
      event = JSON.parse(messageValue);
    } catch (parseError) {
      console.error('ERROR: Failed to parse JSON message:', parseError.message);
      return;
    }

    // Format the event
    const formattedEvent = formatCDCEvent(topic, message, event);

    // Display the CDC event
    console.log('\n' + '='.repeat(100));
    console.log('CDC EVENT → BLOCKCHAIN INTEGRATION');
    console.log('='.repeat(100));
    console.log(`Timestamp: ${formattedEvent.timestamp}`);
    console.log(`Table: ${formattedEvent.tableName}`);
    console.log(`Operation: ${formattedEvent.operation}`);
    console.log(`Topic: ${topic}`);
    console.log(`Offset: ${formattedEvent.offset}`);
    console.log(`Kafka Latency: ${kafkaLatency}ms`);

    // Send to blockchain if supported
    const blockchainResult = await sendToBlockchain(formattedEvent);

    // Calculate total latency
    const totalLatency = kafkaLatency + (blockchainResult?.latency || 0);

    console.log(`Total End-to-End Latency: ${totalLatency}ms`);

    // Store latency data for analysis
    const latencyRecord = {
      timestamp: new Date().toISOString(),
      tableName: formattedEvent.tableName,
      recordId: formattedEvent.recordId,
      kafkaTimestamp: kafkaTimestamp,
      kafkaReceiveTime: kafkaReceiveTime,
      kafkaLatency: kafkaLatency,
      blockchainLatency: blockchainResult?.latency || 0,
      totalLatency: totalLatency,
      blockchainSuccess: blockchainResult?.success || false,
      operation: formattedEvent.operation,
      topic: topic,
      transactionHash: blockchainResult?.response?.blockchain?.transactionHash || null,
      contractAddress: blockchainResult?.contractAddress || null,
      contractType: blockchainResult?.contractType || null
    };

    latencyData.push(latencyRecord);

    // Save processing log
    await saveProcessingLog({
      ...formattedEvent,
      blockchain: blockchainResult,
      latency: latencyRecord
    });

    // Save latency data periodically
    if (latencyData.length % 10 === 0) {
      await saveLatencyData();
    }

    console.log('='.repeat(100));

  } catch (error) {
    console.error('ERROR: Error processing CDC message:', error);
    console.error('Raw message details:');
    console.error('  Topic:', topic);
    console.error('  Offset:', message.offset);
    console.error('  Stack trace:', error.stack);
  }
};

// Main consumer function
async function run() {
  let connected = false;
  let retries = 0;
  const maxRetries = 15;

  // Validate contract addresses first
  validateContractAddresses();

  // Load previous processing log
  await loadProcessedRecords();

  console.log(`
ERPNext CDC → Blockchain Integration Consumer
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Kafka Broker: ${kafkaBroker}
Blockchain API: ${apiEndpoint}
Target Tables: ${targetTableNames.join(', ')}
Mode: LIVE tracking with blockchain integration
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);

  while (!connected && retries < maxRetries) {
    try {
      console.log(`INFO: Connecting to Kafka... (attempt ${retries + 1}/${maxRetries})`);

      // Connect to Kafka
      await consumer.connect();
      console.log('SUCCESS: Connected to Kafka successfully');
      connected = true;

      // Check if topics exist
      const { foundTopics, missingTables, totalFound } = await checkTopics();

      // Subscribe to topics that exist
      const topicsToSubscribe = Object.values(foundTopics);

      if (topicsToSubscribe.length === 0) {
        console.log('ERROR: No target topics found. Make sure Debezium connector is registered and tables have data.');
        process.exit(1);
      }

      console.log(`\nSUCCESS: Found ${totalFound} out of ${targetTableNames.length} target tables`);
      if (missingTables.length > 0) {
        console.log(`WARNING: Missing tables: ${missingTables.join(', ')}`);
      }

      console.log(`\nContract Address Mapping:`);
      Object.entries(CONTRACT_ADDRESSES).forEach(([tableName, address]) => {
        console.log(`  ${tableName} → ${address}`);
      });

      // Subscribe to available topics
      for (const topic of topicsToSubscribe) {
        await consumer.subscribe({ topic, fromBeginning: false });
        console.log(`INFO: Subscribed to: ${topic} (latest messages only)`);
      }

      // Start consuming messages
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          await processCDCMessage(topic, message);
        },
      });

      console.log('\nINFO: CDC → Blockchain Consumer is now listening...');
      console.log('INFO: Make changes to target table data in ERPNext to see LIVE CDC events');
      console.log('INFO: Data will be automatically sent to blockchain via API routes');
      console.log('INFO: Latency metrics are being tracked and saved');
      console.log('INFO: Press Ctrl+C to stop\n');

    } catch (error) {
      retries++;
      console.error(`ERROR: Connection attempt ${retries} failed:`, error.message);

      if (retries >= maxRetries) {
        console.error('ERROR: Maximum retries reached. Exiting.');
        console.log('\nTroubleshooting steps:');
        console.log('1. Make sure Kafka is running on', kafkaBroker);
        console.log('2. Verify Blockchain API is running on', apiEndpoint);
        console.log('3. Check if Debezium connector is registered');
        console.log('4. Ensure ERPNext database has data in target tables');
        process.exit(1);
      }

      console.log(`INFO: Retrying in 10 seconds...`);
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nINFO: Received shutdown signal. Saving final data and disconnecting...');
  try {
    await saveLatencyData();
    await consumer.disconnect();
    console.log('SUCCESS: Data saved and disconnected from Kafka successfully.');
    console.log(`INFO: Total processed events: ${processedRecords.length}`);
    console.log(`INFO: Latency data points collected: ${latencyData.length}`);
  } catch (error) {
    console.error('ERROR: Error during shutdown:', error.message);
  }
  process.exit(0);
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('ERROR: Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('ERROR: Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

// Start the consumer
run().catch(error => {
  console.error('ERROR: Failed to start CDC → Blockchain consumer:', error);
  process.exit(1);
});