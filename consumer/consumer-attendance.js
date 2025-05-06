// consumer-attendance.js
// This file focuses on processing only CDC events for tabAttendance

require('dotenv').config();

const { Kafka } = require('kafkajs');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');

// Get environment variables with fallbacks
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const apiBaseEndpoint = process.env.API_ENDPOINT || 'http://localhost:4001';
const privateKey = process.env.PRIVATE_KEY || '8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63';
const attendanceContractAddress = process.env.CONTRACT_ADDRESS_ATTENDANCE || '0x6486A01e45648B1aDCc51D375Af3a7c0a5e9002a';

// Create a processed records tracker
const PROCESSED_RECORDS_FILE = path.join(__dirname, 'processed_attendance_records.json');
let processedRecords = new Set();

// Load previously processed records
async function loadProcessedRecords() {
  try {
    const data = await fs.readFile(PROCESSED_RECORDS_FILE, 'utf8');
    const records = JSON.parse(data);
    processedRecords = new Set(records);
    console.log(`Loaded ${processedRecords.size} previously processed attendance records`);
  } catch (error) {
    // File might not exist yet, which is fine
    console.log('No processed attendance records file found, starting fresh');
  }
}

// Save processed records
async function saveProcessedRecords() {
  try {
    const recordsArray = Array.from(processedRecords);
    await fs.writeFile(PROCESSED_RECORDS_FILE, JSON.stringify(recordsArray));
  } catch (error) {
    console.error('Error saving processed records:', error);
  }
}

// Configure Kafka client with improved settings
const kafka = new Kafka({
  brokers: [kafkaBroker],
  clientId: 'attendance-blockchain-consumer',
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Create consumer instance with longer session timeouts
const consumer = kafka.consumer({
  groupId: 'attendance-blockchain-group',
  sessionTimeout: 60000, // 60 seconds
  heartbeatInterval: 10000, // 10 seconds
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Attendance topic to listen for
const attendanceTopic = 'erpnext._5e5899d8398b5f7b.tabAttendance';

// Utility function to create record ID from topic and offset
function createRecordId(topic, offset) {
  return `${topic}-${offset}`;
}

// For storing attendance data in blockchain
async function storeAttendanceInBlockchain(attendanceData) {
  try {
    // Check if required environment variables are set
    if (!privateKey || !attendanceContractAddress) {
      console.error('Missing PRIVATE_KEY or CONTRACT_ADDRESS_ATTENDANCE environment variables');
      return false;
    }

    // Extract attendance fields safely
    const id = attendanceData.name ?
      parseInt(attendanceData.name.replace(/\D/g, ''), 10) ||
      Math.floor(Math.random() * 1000000) :
      Math.floor(Math.random() * 1000000);

    const employeeName = attendanceData.employee_name || "";
    const status = attendanceData.status || "";
    const company = attendanceData.company || "";

    // Safely parse attendance date
    let attendanceDate = 0;
    if (attendanceData.attendance_date) {
      try {
        if (typeof attendanceData.attendance_date === 'number') {
          attendanceDate = attendanceData.attendance_date;
        } else {
          const attDate = new Date(attendanceData.attendance_date);
          if (!isNaN(attDate.getTime())) {
            attendanceDate = Math.floor(attDate.getTime() / 1000);
          }
        }
      } catch (error) {
        console.error('Error parsing attendance date:', error.message);
      }
    }

    console.log(`Processing attendance data: ID=${id}, Employee=${employeeName}, Date=${attendanceDate}, Status=${status}`);

    // Retry logic for blockchain API calls
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      try {
        // Send to blockchain
        const response = await axios.post(`${apiBaseEndpoint}/store-attendance`, {
          privateKey: privateKey,
          contractAddress: attendanceContractAddress,
          id: id,
          employeeName: employeeName,
          attendanceDate: attendanceDate,
          status: status,
          company: company
        }, {
          timeout: 15000
        });

        console.log(`Stored attendance in blockchain, response:`, response.data);
        return true;
      } catch (error) {
        attempts++;
        console.error(`Attempt ${attempts}/${maxAttempts} failed:`, error.message);

        if (attempts >= maxAttempts) {
          throw error;
        }

        // Wait before retrying with exponential backoff
        await new Promise(resolve => setTimeout(resolve, 2000 * attempts));
      }
    }

    return false;
  } catch (error) {
    console.error('Error storing attendance data in blockchain:', error.message);
    if (error.response) {
      console.error('API response error:', error.response.status, error.response.data);
    }
    return false;
  }
}

// Function to check if Kafka topics exist
async function checkTopics() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    console.log('Connected to Kafka admin client');

    const topics = await admin.listTopics();
    console.log('Available topics:', topics);

    const attendanceTopicExists = topics.includes(attendanceTopic);
    console.log(`Topic '${attendanceTopic}' exists: ${attendanceTopicExists}`);

    await admin.disconnect();
    return { attendanceTopicExists };
  } catch (error) {
    console.error('Error checking topics:', error.message);
    return { attendanceTopicExists: false };
  }
}

// Process attendance message
const processAttendanceMessage = async (message) => {
  try {
    // Create a unique ID for this record
    const recordId = createRecordId(attendanceTopic, message.offset);

    // Skip if already processed
    if (processedRecords.has(recordId)) {
      console.log(`Attendance record ${recordId} already processed, skipping`);
      return;
    }

    // Parse message value
    const messageValue = message.value.toString();
    const event = JSON.parse(messageValue);

    // Log the received event
    console.log('\n----- Attendance CDC Event Received -----');
    console.log(`Offset: ${message.offset}`);
    console.log(`RecordID: ${recordId}`);
    console.log(`Timestamp: ${new Date(parseInt(message.timestamp)).toISOString()}`);

    // Log the event details
    if (event.op) {
      console.log(`Operation: ${event.op}`); // c=create, u=update, d=delete
    }

    const attendanceData = event.after || event;

    // Skip if the record is marked as deleted
    if (attendanceData.__deleted === "true") {
      console.log('Attendance record marked as deleted, skipping blockchain storage');
      processedRecords.add(recordId);
      await saveProcessedRecords();
      return;
    }

    // Extract more useful fields for logging
    console.log('Attendance Details:');
    console.log(`Name: ${attendanceData.name || 'Not set'}`);
    console.log(`Employee: ${attendanceData.employee || 'Not set'}`);
    console.log(`Employee Name: ${attendanceData.employee_name || 'Not set'}`);
    console.log(`Attendance Date: ${attendanceData.attendance_date || 'Not set'}`);
    console.log(`Status: ${attendanceData.status || 'Not set'}`);
    console.log(`Company: ${attendanceData.company || 'Not set'}`);

    // Store attendance data in blockchain
    const success = await storeAttendanceInBlockchain(attendanceData);

    if (success) {
      // Mark as processed if successful
      processedRecords.add(recordId);
      await saveProcessedRecords();
      console.log(`Record ${recordId} processed successfully and marked as processed`);
    } else {
      console.log(`Record ${recordId} processing failed, will retry on next run`);
    }

    console.log('----- End of Attendance CDC Event -----\n');
  } catch (error) {
    console.error('Error processing attendance message:', error);
    console.error('Message content:', message.value.toString());
  }
};

// Main consumer function
async function run() {
  let connected = false;
  let retries = 0;
  const maxRetries = 15;

  // Load previously processed records
  await loadProcessedRecords();

  while (!connected && retries < maxRetries) {
    try {
      console.log(`Starting attendance consumer with broker: ${kafkaBroker} (attempt ${retries + 1}/${maxRetries})`);

      // Connect to Kafka
      await consumer.connect();
      console.log('Connected to Kafka');
      connected = true;

      // Check if topics exist
      const { attendanceTopicExists } = await checkTopics();

      // Subscribe to attendance topic if it exists
      if (attendanceTopicExists) {
        await consumer.subscribe({ topic: attendanceTopic, fromBeginning: true });
        console.log(`Subscribed to topic: ${attendanceTopic}`);
      } else {
        console.error(`Required topic ${attendanceTopic} does not exist!`);
        console.log('Will retry in 30 seconds...');

        // Disconnect and wait before retrying
        await consumer.disconnect();
        connected = false;
        await new Promise(resolve => setTimeout(resolve, 30000));
        continue;
      }

      // Consume messages
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (topic === attendanceTopic) {
            await processAttendanceMessage(message);
          }
        },
      });

      console.log('Attendance consumer started and waiting for messages...');

    } catch (error) {
      retries++;
      console.error(`Connection attempt ${retries} failed:`, error.message);

      if (retries >= maxRetries) {
        console.error('Maximum retries reached. Exiting.');
        process.exit(1);
      }

      // Exponential backoff for retries
      const backoffTime = Math.min(10000, 1000 * Math.pow(2, retries));
      console.log(`Waiting ${backoffTime / 1000} seconds before retrying...`);
      await new Promise(resolve => setTimeout(resolve, backoffTime));
    }
  }
}

// Start the consumer with auto-restart
function startWithAutoRestart() {
  run().catch(error => {
    console.error('Fatal error in attendance consumer:', error);
    console.log('Restarting consumer in 10 seconds...');
    setTimeout(startWithAutoRestart, 10000);
  });
}

// Display startup information
console.log('Starting ERP-Blockchain Attendance Consumer Service...');
console.log(`Using Kafka broker: ${kafkaBroker}`);
console.log(`API endpoint: ${apiBaseEndpoint}`);
console.log(`Attendance contract: ${attendanceContractAddress || 'Not set'}`);

// Initial start
startWithAutoRestart();

// Handle termination signals
process.on('SIGINT', async () => {
  console.log('Disconnecting attendance consumer...');
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error('Error during disconnect:', e);
  }
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Disconnecting attendance consumer...');
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error('Error during disconnect:', e);
  }
  process.exit(0);
});