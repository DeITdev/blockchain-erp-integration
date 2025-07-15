// Load environment variables from consumer folder
try {
  require('dotenv').config();
} catch (error) {
  console.log('No .env file found, using environment variables');
}

const { Kafka } = require('kafkajs');
const fs = require('fs').promises;
const path = require('path');

// Get environment variables with fallbacks
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';

// Create a processed records tracker for debugging (optional)
const PROCESSED_RECORDS_FILE = path.join(__dirname, 'cdc_events_log.json');
let processedRecords = [];

// Load previously processed records for reference
async function loadProcessedRecords() {
  try {
    const data = await fs.readFile(PROCESSED_RECORDS_FILE, 'utf8');
    processedRecords = JSON.parse(data);
    console.log(`Loaded ${processedRecords.length} previously logged CDC events for reference`);
  } catch (error) {
    console.log('No previous CDC events log found, starting fresh');
    processedRecords = [];
  }
}

// Save CDC events log
async function saveCDCEventLog(eventData) {
  try {
    processedRecords.push(eventData);
    // Keep only the last 1000 events to prevent file from growing too large
    if (processedRecords.length > 1000) {
      processedRecords = processedRecords.slice(-1000);
    }
    await fs.writeFile(PROCESSED_RECORDS_FILE, JSON.stringify(processedRecords, null, 2));
  } catch (error) {
    console.error('Error saving CDC event log:', error);
  }
}

// Configure Kafka client
const kafka = new Kafka({
  brokers: [kafkaBroker],
  clientId: 'erpnext-cdc-display-tracker',
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Create consumer instance
const consumer = kafka.consumer({
  groupId: 'erpnext-cdc-display-group',
  sessionTimeout: 60000,
  heartbeatInterval: 10000,
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Function to dynamically find ERPNext topics
function findERPNextTopics(allTopics) {
  const erpnextTopics = allTopics.filter(topic =>
    topic.startsWith('erpnext.') &&
    topic.includes('.tab') &&
    !topic.includes('schema-changes')
  );

  // Group topics by database hash
  const topicsByDatabase = {};
  erpnextTopics.forEach(topic => {
    const parts = topic.split('.');
    if (parts.length >= 3) {
      const database = parts[1]; // The hash part
      const tableName = parts[2]; // The table name

      if (!topicsByDatabase[database]) {
        topicsByDatabase[database] = [];
      }
      topicsByDatabase[database].push({ topic, tableName });
    }
  });

  return topicsByDatabase;
}

// Target table names from environment variable - ALL TABLES NOW SUPPORTED
const targetTableNames = process.env.TARGET_TABLES ?
  process.env.TARGET_TABLES.split(',').map(name => name.trim()) :
  [
    'tabEmployee', 'tabUser', 'tabCompany', 'tabCustomer', 'tabAccount',
    'tabAsset', 'tabLead', 'tabOpportunity', 'tabAttendance',
    'tabLeave Application', 'tabSalary Slip', 'tabProject', 'tabTask',
    'tabTimesheet', 'tabItem', 'tabStock Entry'
  ];

let topicMapping = {}; // Store topic mappings dynamically

// Function to detect operation type from ERPNext CDC events
function detectOperationType(event) {
  // Check for standard Debezium operation field
  if (event.op) {
    return event.op;
  }

  // For ERPNext CDC events, we need to analyze the data
  const isDeleted = event.__deleted === true || event.__deleted === "true";

  if (isDeleted) {
    return 'd'; // DELETE
  }

  // Check if this looks like a snapshot/initial read
  if (event.creation && !event.modified) {
    return 'r'; // READ (snapshot)
  }

  // If we have both creation and modified timestamps, check which is more recent
  if (event.creation && event.modified) {
    const creationTime = typeof event.creation === 'number' ? event.creation : Date.parse(event.creation);
    const modifiedTime = typeof event.modified === 'number' ? event.modified : Date.parse(event.modified);

    // If modified time is significantly different from creation time, it's likely an update
    if (Math.abs(modifiedTime - creationTime) > 1000000) { // 1 second in microseconds
      return 'u'; // UPDATE
    }
  }

  // If we can't determine, assume it's a CREATE/READ operation
  return 'c'; // CREATE (default assumption)
}

// Function to format and display CDC event data
function formatCDCEvent(topic, message, event) {
  const timestamp = new Date(parseInt(message.timestamp)).toISOString();
  const operation = detectOperationType(event);

  const operationMap = {
    'c': 'CREATE',
    'u': 'UPDATE',
    'd': 'DELETE',
    'r': 'READ (Snapshot)',
    'read': 'READ (Snapshot)',
    'create': 'CREATE',
    'update': 'UPDATE',
    'delete': 'DELETE'
  };

  const formattedEvent = {
    timestamp,
    topic,
    offset: message.offset,
    partition: message.partition,
    operation: operationMap[operation] || operation,
    tableName: topic.split('.').pop(),
    database: topic.split('.')[1],
    rawEvent: event,
    detectedOperation: operation
  };

  // Clean data by removing special ERPNext/Debezium fields
  const specialFields = ['__deleted', '_assign', '_comments', '_liked_by', '_user_tags'];
  const cleanData = { ...event };
  specialFields.forEach(field => delete cleanData[field]);

  if (operation === 'd') {
    formattedEvent.deletedData = cleanData;
  } else {
    formattedEvent.data = cleanData;
    if (operation === 'u') {
      formattedEvent.dataAfter = cleanData;
      formattedEvent.dataBefore = null; // Not available in this CDC format
    }
  }

  return formattedEvent;
}

// Function to display all data fields in a formatted way
function formatAllDataFields(data, label = "Data") {
  if (!data) return "No data";

  const lines = [`\n${label}:`];
  const sortedKeys = Object.keys(data).sort();

  sortedKeys.forEach(key => {
    const value = data[key];
    let displayValue;

    if (value === null) {
      displayValue = "NULL";
    } else if (value === undefined) {
      displayValue = "UNDEFINED";
    } else if (value === "") {
      displayValue = "EMPTY STRING";
    } else if (typeof value === 'string' && value.trim() === "") {
      displayValue = "WHITESPACE ONLY";
    } else {
      displayValue = String(value);
    }

    lines.push(`   ${key}: ${displayValue}`);
  });

  return lines.join('\n');
}

// Function to check if Kafka topics exist and find ERPNext topics dynamically
async function checkTopics() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    console.log('Connected to Kafka admin client');

    const topics = await admin.listTopics();
    console.log('Available Kafka topics:', topics);

    // Find ERPNext topics dynamically
    const topicsByDatabase = findERPNextTopics(topics);

    console.log(`\nFound ERPNext databases:`);
    Object.keys(topicsByDatabase).forEach(db => {
      console.log(`   Database: ${db}`);
      topicsByDatabase[db].forEach(({ topic, tableName }) => {
        console.log(`     - ${tableName}: ${topic}`);
      });
    });

    // Find our target topics - NOW CHECKS ALL TARGET TABLES
    const foundTopics = {};
    const missingTables = [...targetTableNames];

    Object.values(topicsByDatabase).forEach(topics => {
      topics.forEach(({ topic, tableName }) => {
        if (targetTableNames.includes(tableName)) {
          foundTopics[tableName] = topic;
          topicMapping[tableName] = topic;
          // Remove from missing list
          const index = missingTables.indexOf(tableName);
          if (index > -1) {
            missingTables.splice(index, 1);
          }
        }
      });
    });

    console.log(`\nTarget Topics Status:`);
    targetTableNames.forEach(tableName => {
      const exists = foundTopics[tableName];
      console.log(`- ${tableName}: ${exists ? 'EXISTS' : 'NOT FOUND'}`);
      if (exists) {
        console.log(`  Topic: ${exists}`);
      }
    });

    if (missingTables.length > 0) {
      console.log(`\nMissing tables: ${missingTables.join(', ')}`);
    }

    await admin.disconnect();
    return { foundTopics, missingTables, totalFound: Object.keys(foundTopics).length };
  } catch (error) {
    console.error('Error checking topics:', error.message);
    return { foundTopics: {}, missingTables: targetTableNames, totalFound: 0 };
  }
}

// Process CDC message with enhanced display
const processCDCMessage = async (topic, message) => {
  try {
    // Parse message value
    const messageValue = message.value.toString();

    let event;
    try {
      event = JSON.parse(messageValue);
    } catch (parseError) {
      console.error('❌ Failed to parse JSON message:', parseError.message);
      console.log('Raw message content:', messageValue.substring(0, 500) + '...');
      return;
    }

    // Format the event for display
    const formattedEvent = formatCDCEvent(topic, message, event);

    // Display the CDC event header
    console.log('\n' + '='.repeat(100));
    console.log('📊 CDC EVENT DETECTED');
    console.log('='.repeat(100));
    console.log(`🕒 Timestamp: ${formattedEvent.timestamp}`);
    console.log(`📋 Table: ${formattedEvent.tableName}`);
    console.log(`🗃️  Database: ${formattedEvent.database}`);
    console.log(`⚡ Operation: ${formattedEvent.operation}`);
    console.log(`📍 Topic: ${topic}`);
    console.log(`🔢 Offset: ${formattedEvent.offset} | Partition: ${formattedEvent.partition || 'N/A'}`);

    // Show the actual data with all fields
    if (formattedEvent.operation === 'DELETE') {
      console.log(formatAllDataFields(formattedEvent.deletedData, "🗑️  Deleted Data"));
    } else if (formattedEvent.operation === 'UPDATE') {
      console.log(formatAllDataFields(formattedEvent.dataAfter, "📝 Updated Data"));
    } else {
      console.log(formatAllDataFields(formattedEvent.data, "📄 Data"));
    }

    // Log to file for reference
    await saveCDCEventLog(formattedEvent);

    console.log('='.repeat(100));

  } catch (error) {
    console.error('Error processing CDC message:', error);
    console.log('Raw message details:');
    console.log('  Topic:', topic);
    console.log('  Offset:', message.offset);
    console.log('  Value length:', message.value ? message.value.toString().length : 'null');
    console.log('  First 200 chars:', message.value ? message.value.toString().substring(0, 200) : 'null');
  }
};

// Main consumer function
async function run() {
  let connected = false;
  let retries = 0;
  const maxRetries = 15;

  // Load previous events log
  await loadProcessedRecords();

  console.log(`
ERPNext CDC Event Display Tracker - ALL TARGET TABLES
================================================================
Kafka Broker: ${kafkaBroker}
Target Tables: ${targetTableNames.join(', ')}
Mode: Dynamic topic detection
Functionality: DISPLAY ONLY - No blockchain integration
Auto-detects ERPNext database hash from available topics
================================================================
`);

  while (!connected && retries < maxRetries) {
    try {
      console.log(`Connecting to Kafka... (attempt ${retries + 1}/${maxRetries})`);

      // Connect to Kafka
      await consumer.connect();
      console.log('Connected to Kafka successfully');
      connected = true;

      // Check if topics exist
      const { foundTopics, missingTables, totalFound } = await checkTopics();

      // Subscribe to topics that exist
      const topicsToSubscribe = Object.values(foundTopics);

      if (topicsToSubscribe.length === 0) {
        console.log('\n❌ No target topics found.');
        console.log('Available ERPNext topics:');

        // Show what topics ARE available
        const admin = kafka.admin();
        await admin.connect();
        const allTopics = await admin.listTopics();
        await admin.disconnect();

        const allErpTopics = allTopics.filter(topic => topic.startsWith('erpnext.') && topic.includes('.tab'));
        if (allErpTopics.length > 0) {
          allErpTopics.forEach(topic => {
            console.log(`  ${topic}`);
          });
          console.log(`\nThe tracker will look for these target tables: ${targetTableNames.join(', ')}`);
        } else {
          console.log('   No ERPNext table topics found at all.');
          console.log('Make sure Debezium connector is registered and tables have data.');
        }

        process.exit(1);
      }

      console.log(`\n✅ Found ${totalFound} out of ${targetTableNames.length} target tables`);
      if (missingTables.length > 0) {
        console.log(`⚠️  Missing tables: ${missingTables.join(', ')}`);
      }

      // Subscribe to available topics - Start from LATEST for live monitoring
      for (const topic of topicsToSubscribe) {
        await consumer.subscribe({ topic, fromBeginning: false });
        console.log(`Subscribed to: ${topic} (latest messages only)`);
      }

      // Start consuming messages
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          await processCDCMessage(topic, message);
        },
      });

      console.log('\n🚀 CDC Event Display Tracker is now listening...');
      console.log('Make changes to target table data in ERPNext to see LIVE CDC events');
      console.log('All data fields will be displayed for each event');
      console.log('Events are logged to cdc_events_log.json for reference');
      console.log('Only displaying events - no blockchain integration');
      console.log('Press Ctrl+C to stop\n');

    } catch (error) {
      retries++;
      console.error(`Connection attempt ${retries} failed:`, error.message);

      if (retries >= maxRetries) {
        console.error('Maximum retries reached. Exiting.');
        console.log('\nTroubleshooting steps:');
        console.log('1. Make sure Kafka is running on', kafkaBroker);
        console.log('2. Verify Debezium connector is registered');
        console.log('3. Check if ERPNext database has data in target tables');
        console.log('4. Ensure CDC is properly configured for your database');
        process.exit(1);
      }

      console.log(`Retrying in 10 seconds...`);
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Received shutdown signal. Disconnecting from Kafka...');
  try {
    await consumer.disconnect();
    console.log('✅ Disconnected from Kafka successfully.');
  } catch (error) {
    console.error('Error during shutdown:', error.message);
  }
  process.exit(0);
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

// Start the consumer
run().catch(error => {
  console.error('Failed to start CDC tracker:', error);
  process.exit(1);
});