// Load environment variables if .env file exists, but don't fail if it doesn't
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
  clientId: 'erpnext-cdc-tracker',
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Create consumer instance
const consumer = kafka.consumer({
  groupId: 'erpnext-cdc-tracking-group',
  sessionTimeout: 60000,
  heartbeatInterval: 10000,
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Topics to listen for (based on the new ERPNext database)
const employeeTopic = 'erpnext._0775ec53bab106f5.tabEmployee';
const userTopic = 'erpnext._0775ec53bab106f5.tabUser';

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
  // Usually these have creation timestamps but no clear operation indicator
  if (event.creation && !event.modified) {
    return 'r'; // READ (snapshot)
  }

  // If we have both creation and modified timestamps, check which is more recent
  if (event.creation && event.modified) {
    // Convert timestamps if they're in microseconds (ERPNext format)
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

  // Detect operation type for ERPNext events
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
    detectedOperation: operation // Keep the detected operation for debugging
  };

  // For ERPNext format, the event itself IS the data
  // Remove special Debezium/ERPNext fields to get clean record data
  const specialFields = ['__deleted', '_assign', '_comments', '_liked_by', '_user_tags'];
  const cleanData = { ...event };
  specialFields.forEach(field => delete cleanData[field]);

  if (operation === 'd') {
    // For deletes, the data represents what was deleted
    formattedEvent.deletedData = cleanData;
  } else {
    // For creates, updates, reads - this is the current/new data
    formattedEvent.data = cleanData;

    // For updates, we don't have "before" data in this format
    // The entire event represents the current state after the change
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

  const lines = [`\n📋 ${label}:`];

  // Sort keys for consistent display
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

// Function to compare and show differences between two objects
function showDataDifferences(beforeData, afterData) {
  if (!beforeData || !afterData) return "";

  const differences = [];
  const allKeys = new Set([...Object.keys(beforeData), ...Object.keys(afterData)]);

  allKeys.forEach(key => {
    const beforeValue = beforeData[key];
    const afterValue = afterData[key];

    if (beforeValue !== afterValue) {
      const formatValue = (val) => {
        if (val === null) return "NULL";
        if (val === undefined) return "UNDEFINED";
        if (val === "") return "EMPTY STRING";
        return String(val);
      };

      differences.push(`   ${key}: "${formatValue(beforeValue)}" → "${formatValue(afterValue)}"`);
    }
  });

  return differences.length > 0 ? `\n🔄 Field Changes:\n${differences.join('\n')}` : "\n🔄 No field changes detected";
}

// Function to check if Kafka topics exist
async function checkTopics() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    console.log('Connected to Kafka admin client');

    const topics = await admin.listTopics();
    console.log('Available Kafka topics:', topics);

    const employeeTopicExists = topics.includes(employeeTopic);
    const userTopicExists = topics.includes(userTopic);

    console.log(`\nTarget Topics Status:`);
    console.log(`- ${employeeTopic}: ${employeeTopicExists ? '✓ EXISTS' : '✗ NOT FOUND'}`);
    console.log(`- ${userTopic}: ${userTopicExists ? '✓ EXISTS' : '✗ NOT FOUND'}`);

    await admin.disconnect();
    return { employeeTopicExists, userTopicExists };
  } catch (error) {
    console.error('Error checking topics:', error.message);
    return { employeeTopicExists: false, userTopicExists: false };
  }
}

// Process CDC message
const processCDCMessage = async (topic, message) => {
  try {
    // Parse message value
    const messageValue = message.value.toString();
    console.log(`\n🔍 Raw message length: ${messageValue.length} characters`);

    let event;
    try {
      event = JSON.parse(messageValue);
    } catch (parseError) {
      console.error('❌ Failed to parse JSON message:', parseError.message);
      console.log('Raw message content:', messageValue.substring(0, 500) + '...');
      return;
    }

    // Debug: Show event structure
    console.log('🔍 Event keys:', Object.keys(event));
    console.log('🔍 Event structure preview:', JSON.stringify(event, null, 2).substring(0, 300) + '...');

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
    console.log(`🔍 Detected Operation: ${formattedEvent.detectedOperation} → ${formattedEvent.operation}`);

    // Show what data we found
    const hasData = formattedEvent.data;
    const hasBefore = formattedEvent.dataBefore;
    const hasAfter = formattedEvent.dataAfter;
    const hasDeleted = formattedEvent.deletedData;

    console.log(`\n🔍 Data Analysis:`);
    console.log(`   Has data: ${hasData ? '✓' : '✗'}`);
    console.log(`   Has before: ${hasBefore ? '✓' : '✗'}`);
    console.log(`   Has after: ${hasAfter ? '✓' : '✗'}`);
    console.log(`   Has deleted: ${hasDeleted ? '✓' : '✗'}`);

    // Show record metadata if available
    if (formattedEvent.data) {
      console.log(`\n📊 Record Metadata:`);
      if (formattedEvent.data.name) console.log(`   Record ID: ${formattedEvent.data.name}`);
      if (formattedEvent.data.creation) {
        const creationDate = new Date(formattedEvent.data.creation / 1000).toISOString();
        console.log(`   Created: ${creationDate}`);
      }
      if (formattedEvent.data.modified) {
        const modifiedDate = new Date(formattedEvent.data.modified / 1000).toISOString();
        console.log(`   Modified: ${modifiedDate}`);
      }
      if (formattedEvent.data.modified_by) console.log(`   Modified by: ${formattedEvent.data.modified_by}`);
    }
    // Display all data based on operation type
    if (formattedEvent.operation === 'CREATE' || formattedEvent.operation === 'READ (Snapshot)') {
      if (formattedEvent.data) {
        console.log(formatAllDataFields(formattedEvent.data, "Record Data"));
      } else {
        console.log('\n⚠️  No data found for CREATE/READ operation');
      }

    } else if (formattedEvent.operation === 'UPDATE') {
      if (formattedEvent.dataAfter) {
        console.log(formatAllDataFields(formattedEvent.dataAfter, "Current Record Data (After Update)"));
        console.log('\n💡 Note: ERPNext CDC format does not provide "before" data for updates');
        console.log('    The data shown represents the current state after the change');
      } else if (formattedEvent.data) {
        console.log(formatAllDataFields(formattedEvent.data, "Current Record Data"));
      } else {
        console.log('\n⚠️  No data found for UPDATE operation');
      }

    } else if (formattedEvent.operation === 'DELETE') {
      if (formattedEvent.deletedData) {
        console.log(formatAllDataFields(formattedEvent.deletedData, "Deleted Record Data"));
      } else {
        console.log('\n⚠️  No deleted data found for DELETE operation');
      }
    } else {
      // Fallback for any unrecognized operations
      console.log('\n⚠️  Unrecognized operation - showing all available data:');
      if (formattedEvent.data) {
        console.log(formatAllDataFields(formattedEvent.data, "Available Data"));
      } else {
        console.log('\n🔍 Raw Event Data:');
        console.log(JSON.stringify(event, null, 2));
      }
    }

    // Always show raw event structure if debug mode is enabled
    if (process.env.DEBUG_RAW_EVENT === 'true') {
      console.log('\n🔍 Complete Raw CDC Event:');
      console.log(JSON.stringify(event, null, 2));
    }

    console.log('='.repeat(100));

    // Save to log file with all data
    await saveCDCEventLog({
      ...formattedEvent,
      allData: {
        before: formattedEvent.dataBefore,
        after: formattedEvent.dataAfter || formattedEvent.data,
        deleted: formattedEvent.deletedData
      },
      rawEventStructure: Object.keys(event)
    });

  } catch (error) {
    console.error('❌ Error processing CDC message:', error);
    console.error('Raw message content:', message.value.toString().substring(0, 1000));
    console.error('Stack trace:', error.stack);
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
🚀 Starting ERPNext CDC Event Tracker
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📡 Kafka Broker: ${kafkaBroker}
🎯 Target Tables: Employee, User  
📊 Database: _0775ec53bab106f5
⚡ Mode: LIVE tracking (new changes only)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);

  while (!connected && retries < maxRetries) {
    try {
      console.log(`🔄 Connecting to Kafka... (attempt ${retries + 1}/${maxRetries})`);

      // Connect to Kafka
      await consumer.connect();
      console.log('✅ Connected to Kafka successfully');
      connected = true;

      // Check if topics exist
      const { employeeTopicExists, userTopicExists } = await checkTopics();

      // Subscribe to topics that exist
      const topicsToSubscribe = [];

      if (employeeTopicExists) {
        topicsToSubscribe.push(employeeTopic);
      } else {
        console.log(`⚠️  Warning: Employee topic ${employeeTopic} not found`);
      }

      if (userTopicExists) {
        topicsToSubscribe.push(userTopic);
      } else {
        console.log(`⚠️  Warning: User topic ${userTopic} not found`);
      }

      if (topicsToSubscribe.length === 0) {
        console.log('❌ No target topics found. Make sure Debezium connector is registered and tables have data.');
        console.log('💡 Try running the register-connector.js script first.');
        process.exit(1);
      }

      // Subscribe to available topics - Start from LATEST, not beginning
      for (const topic of topicsToSubscribe) {
        await consumer.subscribe({ topic, fromBeginning: false }); // Changed to false
        console.log(`📌 Subscribed to: ${topic} (latest messages only)`);
      }

      // Start consuming messages
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          await processCDCMessage(topic, message);
        },
      });

      console.log('\n🎧 CDC Event Tracker is now listening for NEW changes only...');
      console.log('💡 Make changes to Employee or User data in ERPNext to see LIVE CDC events');
      console.log('📋 ALL data fields will be displayed (including NULL values)');
      console.log('🔧 Set DEBUG_RAW_EVENT=true to see raw CDC event structure');
      console.log('⚡ Only NEW changes from this point forward will be shown');
      console.log('🛑 Press Ctrl+C to stop\n');

    } catch (error) {
      retries++;
      console.error(`❌ Connection attempt ${retries} failed:`, error.message);

      if (retries >= maxRetries) {
        console.error('💥 Maximum retries reached. Exiting.');
        console.log('\n🔧 Troubleshooting steps:');
        console.log('1. Make sure Kafka is running on', kafkaBroker);
        console.log('2. Verify Debezium connector is registered');
        console.log('3. Check that ERPNext database is accessible');
        process.exit(1);
      }

      const backoffTime = Math.min(10000, 1000 * Math.pow(2, retries));
      console.log(`⏳ Waiting ${backoffTime / 1000} seconds before retrying...`);
      await new Promise(resolve => setTimeout(resolve, backoffTime));
    }
  }
}

// Start the consumer with auto-restart
function startWithAutoRestart() {
  run().catch(error => {
    console.error('💥 Fatal error in CDC tracker:', error);
    console.log('🔄 Restarting in 10 seconds...');
    setTimeout(startWithAutoRestart, 10000);
  });
}

// Display summary statistics
function displaySummary() {
  console.log(`\n📈 CDC Events Summary: ${processedRecords.length} events tracked`);

  if (processedRecords.length > 0) {
    const operations = processedRecords.reduce((acc, event) => {
      acc[event.operation] = (acc[event.operation] || 0) + 1;
      return acc;
    }, {});

    console.log('📊 Operations breakdown:');
    Object.entries(operations).forEach(([op, count]) => {
      console.log(`   ${op}: ${count}`);
    });
  }
}

// Initial start
startWithAutoRestart();

// Handle termination signals
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down CDC tracker...');
  displaySummary();
  try {
    await consumer.disconnect();
    console.log('✅ Disconnected from Kafka');
  } catch (e) {
    console.error('❌ Error during disconnect:', e);
  }
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n🛑 Shutting down CDC tracker...');
  displaySummary();
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error('❌ Error during disconnect:', e);
  }
  process.exit(0);
});