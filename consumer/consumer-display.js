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

// Target table names from environment variable
const targetTableNames = process.env.TARGET_TABLES ? process.env.TARGET_TABLES.split(',') : ['tabEmployee', 'tabUser'];
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

    // Find our target topics
    let employeeTopicExists = false;
    let userTopicExists = false;

    Object.values(topicsByDatabase).forEach(topics => {
      topics.forEach(({ topic, tableName }) => {
        if (tableName === 'tabEmployee') {
          employeeTopic = topic;
          employeeTopicExists = true;
        } else if (tableName === 'tabUser') {
          userTopic = topic;
          userTopicExists = true;
        }
      });
    });

    console.log(`\nTarget Topics Status:`);
    console.log(`- Employee (tabEmployee): ${employeeTopicExists ? 'EXISTS' : 'NOT FOUND'}`);
    if (employeeTopicExists) console.log(`  Topic: ${employeeTopic}`);

    console.log(`- User (tabUser): ${userTopicExists ? 'EXISTS' : 'NOT FOUND'}`);
    if (userTopicExists) console.log(`  Topic: ${userTopic}`);

    await admin.disconnect();
    return { employeeTopicExists, userTopicExists };
  } catch (error) {
    console.error('Error checking topics:', error.message);
    return { employeeTopicExists: false, userTopicExists: false };
  }
}

// Process CDC message - Display Only
const processCDCMessage = async (topic, message) => {
  try {
    // FIX: Add a check for null message value
    if (message.value === null) {
      console.log('\n' + '='.repeat(80));
      console.log('TOMBSTONE MESSAGE DETECTED (record deletion)');
      console.log('='.repeat(80));
      console.log(`Timestamp: ${new Date(parseInt(message.timestamp)).toISOString()}`);
      console.log(`Topic: ${topic}`);
      console.log(`Offset: ${message.offset} | Partition: ${message.partition || 'N/A'}`);
      console.log('This message indicates that a record was deleted in the source database.');
      console.log('='.repeat(80));

      // Optionally, you can log this to your event log as well
      await saveCDCEventLog({
        timestamp: new Date(parseInt(message.timestamp)).toISOString(),
        topic,
        offset: message.offset,
        partition: message.partition,
        operation: 'DELETE (Tombstone)',
        tableName: topic.split('.').pop(),
        database: topic.split('.')[1],
        processed: true,
        processedAt: new Date().toISOString()
      });
      return; // Stop further processing for this message
    }

    const messageValue = message.value.toString();

    let event;
    try {
      event = JSON.parse(messageValue);
    } catch (parseError) {
      console.error('Failed to parse JSON message:', parseError.message);
      console.log('Raw message content:', messageValue.substring(0, 500) + '...');
      return;
    }

    // Format the event for display
    const formattedEvent = formatCDCEvent(topic, message, event);

    // Display the CDC event
    console.log('\n' + '='.repeat(80));
    console.log('CDC EVENT DETECTED');
    console.log('='.repeat(80));
    console.log(`Timestamp: ${formattedEvent.timestamp}`);
    console.log(`Table: ${formattedEvent.tableName}`);
    console.log(`Database: ${formattedEvent.database}`);
    console.log(`Operation: ${formattedEvent.operation}`);
    console.log(`Topic: ${topic}`);
    console.log(`Offset: ${formattedEvent.offset} | Partition: ${formattedEvent.partition || 'N/A'}`);

    // Show record metadata if available
    if (formattedEvent.data) {
      console.log(`\nRecord Metadata:`);
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

    // Display data based on operation type
    if (formattedEvent.operation === 'CREATE' || formattedEvent.operation === 'READ (Snapshot)') {
      if (formattedEvent.data) {
        console.log(formatAllDataFields(formattedEvent.data, "Record Data"));
      }
    } else if (formattedEvent.operation === 'UPDATE') {
      if (formattedEvent.dataAfter) {
        console.log(formatAllDataFields(formattedEvent.dataAfter, "urrent Record Data (After Update)"));
        console.log('\nNote: ERPNext CDC format does not provide "before" data for updates');
      } else if (formattedEvent.data) {
        console.log(formatAllDataFields(formattedEvent.data, "Current Record Data"));
      }
    } else if (formattedEvent.operation === 'DELETE') {
      if (formattedEvent.deletedData) {
        console.log(formatAllDataFields(formattedEvent.deletedData, "Deleted Record Data"));
      }
    }

    console.log('='.repeat(80));
    console.log('Event displayed successfully\n');

    // Save to log file
    await saveCDCEventLog({
      ...formattedEvent,
      processed: true,
      processedAt: new Date().toISOString()
    });

  } catch (error) {
    console.error('Error processing CDC message:', error);
    console.error('Raw message content:', message.value.toString().substring(0, 1000));
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
ERPNext CDC Event Display Tracker
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
      const { employeeTopicExists, userTopicExists } = await checkTopics();

      // Subscribe to topics that exist
      const topicsToSubscribe = [];

      if (employeeTopicExists) {
        topicsToSubscribe.push(employeeTopic);
      } else {
        console.log(`Warning: Employee topic ${employeeTopic} not found`);
      }

      if (userTopicExists) {
        topicsToSubscribe.push(userTopic);
      } else {
        console.log(`Warning: User topic ${userTopic} not found`);
      }

      if (topicsToSubscribe.length === 0) {
        console.log('No target topics found.');
        console.log('Available ERPNext topics:');

        // Show what topics ARE available
        const allErpTopics = topics.filter(topic => topic.startsWith('erpnext.') && topic.includes('.tab'));
        if (allErpTopics.length > 0) {
          allErpTopics.forEach(topic => {
            console.log(`  ${topic}`);
          });
          console.log('The tracker will look for tabEmployee and tabUser topics from any database.');
        } else {
          console.log('   No ERPNext table topics found at all.');
          console.log('Make sure Debezium connector is registered and tables have data.');
        }

        process.exit(1);
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

      console.log('\nCDC Event Display Tracker is now listening...');
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
        console.log('3. Check that ERPNext database is accessible');
        process.exit(1);
      }

      const backoffTime = Math.min(10000, 1000 * Math.pow(2, retries));
      console.log(`Waiting ${backoffTime / 1000} seconds before retrying...`);
      await new Promise(resolve => setTimeout(resolve, backoffTime));
    }
  }
}

// Start the consumer with auto-restart
function startWithAutoRestart() {
  run().catch(error => {
    console.error('Fatal error in CDC display tracker:', error);
    console.log('Restarting in 10 seconds...');
    setTimeout(startWithAutoRestart, 10000);
  });
}

// Display summary statistics
function displaySummary() {
  console.log(`\nCDC Events Summary: ${processedRecords.length} events displayed`);

  if (processedRecords.length > 0) {
    const operations = processedRecords.reduce((acc, event) => {
      acc[event.operation] = (acc[event.operation] || 0) + 1;
      return acc;
    }, {});

    console.log('Operations breakdown:');
    Object.entries(operations).forEach(([op, count]) => {
      console.log(`   ${op}: ${count} events`);
    });

    // Show recent events
    const recentEvents = processedRecords.slice(-5);
    console.log('\nRecent Events:');
    recentEvents.forEach((event, index) => {
      console.log(`   ${index + 1}. ${event.operation} on ${event.tableName} at ${event.timestamp}`);
    });
  }
}

// Initial start
startWithAutoRestart();

// Handle termination signals
process.on('SIGINT', async () => {
  console.log('\nShutting down CDC Display Tracker...');
  displaySummary();
  try {
    await consumer.disconnect();
    console.log('Disconnected from Kafka');
  } catch (e) {
    console.error('Error during disconnect:', e);
  }
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\nShutting down CDC Display Tracker...');
  displaySummary();
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error('Error during disconnect:', e);
  }
  process.exit(0);
});