// Load environment variables if .env file exists, but don't fail if it doesn't
try {
  require('dotenv').config();
} catch (error) {
  console.log('No .env file found, using environment variables');
}

const { Kafka } = require('kafkajs');
const axios = require('axios');

// Get environment variables with fallbacks
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const apiEndpoint = process.env.API_ENDPOINT || 'http://localhost:4001/store';
const apiEndpointEmployee = process.env.API_ENDPOINT_EMPLOYEE || 'http://localhost:4001/store-employee';
const apiEndpointAttendance = process.env.API_ENDPOINT_ATTENDANCE || 'http://localhost:4001/store-attendance';
const privateKey = process.env.PRIVATE_KEY;
const contractAddress = process.env.CONTRACT_ADDRESS;
const contractAddressEmployee = process.env.CONTRACT_ADDRESS_EMPLOYEE;
const contractAddressAttendance = process.env.CONTRACT_ADDRESS_ATTENDANCE;

// Configure Kafka client with retry settings
const kafka = new Kafka({
  brokers: [kafkaBroker],
  clientId: 'erp-blockchain-consumer',
  retry: {
    initialRetryTime: 5000, // 5 seconds
    retries: 15            // More retries
  }
});

// Create consumer instance
const consumer = kafka.consumer({
  groupId: 'erpnext-blockchain-group',
  // Added retry configuration for consumer
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Topics to listen for
const userTopic = 'erpnext._5e5899d8398b5f7b.tabUser';
const employeeTopic = 'erpnext._5e5899d8398b5f7b.tabEmployee';
const attendanceTopic = 'erpnext._5e5899d8398b5f7b.tabAttendance';

// For storing user data in blockchain
const storeUserInBlockchain = async (userData) => {
  try {
    // Check if required environment variables are set
    if (!privateKey || !contractAddress) {
      console.error('Missing PRIVATE_KEY or CONTRACT_ADDRESS environment variables');
      return;
    }

    // Extract phone field and convert to integer
    const phoneValue = userData.phone ? parseInt(userData.phone, 10) : 0;

    console.log(`Extracted phone value: ${phoneValue} (type: ${typeof phoneValue})`);
    console.log(`Sending to API endpoint: ${apiEndpoint}`);

    // Retry logic for blockchain API calls
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      try {
        // Send to blockchain
        const response = await axios.post(apiEndpoint, {
          privateKey: privateKey,
          contractAddress: contractAddress,
          value: phoneValue
        });

        console.log(`Stored user in blockchain, response:`, response.data);
        return; // Success, exit the function
      } catch (error) {
        attempts++;
        console.error(`Attempt ${attempts}/${maxAttempts} failed:`, error.message);

        if (attempts >= maxAttempts) {
          throw error; // Re-throw if all attempts failed
        }

        // Wait before retrying (exponential backoff)
        await new Promise(resolve => setTimeout(resolve, 2000 * attempts));
      }
    }
  } catch (error) {
    console.error('Error storing user data in blockchain:', error.message);
    // If axios error, log more details
    if (error.response) {
      console.error('API response error:', error.response.status, error.response.data);
    }
  }
};

// For storing employee data in blockchain
const storeEmployeeInBlockchain = async (employeeData) => {
  try {
    // Check if required environment variables are set
    if (!privateKey || !contractAddressEmployee) {
      console.error('Missing PRIVATE_KEY or CONTRACT_ADDRESS_EMPLOYEE environment variables');
      return;
    }

    // Log the raw employee data for debugging
    console.log('Raw employee data:', JSON.stringify(employeeData));

    // Extract employee fields safely
    const id = employeeData.name ? employeeData.name.replace(/\D/g, '') : "0";
    const firstName = employeeData.first_name || "";
    const gender = employeeData.gender || "";

    // Safely parse date of birth
    let dateOfBirth = 0;
    if (employeeData.date_of_birth) {
      try {
        // Try to parse as ISO string or various formats
        const dobDate = new Date(employeeData.date_of_birth);
        if (!isNaN(dobDate.getTime())) {
          dateOfBirth = Math.floor(dobDate.getTime() / 1000);
        }
      } catch (error) {
        console.error('Error parsing date of birth:', error.message);
      }
    }

    // Safely parse date of joining
    let dateOfJoining = 0;
    if (employeeData.date_of_joining) {
      try {
        // Try to parse as ISO string or various formats
        const dojDate = new Date(employeeData.date_of_joining);
        if (!isNaN(dojDate.getTime())) {
          dateOfJoining = Math.floor(dojDate.getTime() / 1000);
        }
      } catch (error) {
        console.error('Error parsing date of joining:', error.message);
      }
    }

    const company = employeeData.company || "";

    console.log(`Processing employee data: ID=${id}, Name=${firstName}, Company=${company}`);
    console.log(`Date values (Unix timestamps): Birth=${dateOfBirth}, Joining=${dateOfJoining}`);
    console.log(`Sending to API endpoint: ${apiEndpointEmployee}`);

    // Retry logic for blockchain API calls
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      try {
        // Send to blockchain
        const response = await axios.post(apiEndpointEmployee, {
          privateKey: privateKey,
          contractAddress: contractAddressEmployee,
          id: parseInt(id, 10) || 0,
          firstName: firstName,
          gender: gender,
          dateOfBirth: dateOfBirth,
          dateOfJoining: dateOfJoining,
          company: company
        });

        console.log(`Stored employee in blockchain, response:`, response.data);
        return; // Success, exit the function
      } catch (error) {
        attempts++;
        console.error(`Attempt ${attempts}/${maxAttempts} failed:`, error.message);

        if (attempts >= maxAttempts) {
          throw error; // Re-throw if all attempts failed
        }

        // Wait before retrying (exponential backoff)
        await new Promise(resolve => setTimeout(resolve, 2000 * attempts));
      }
    }
  } catch (error) {
    console.error('Error storing employee data in blockchain:', error.message);
    // If axios error, log more details
    if (error.response) {
      console.error('API response error:', error.response.status, error.response.data);
    }
  }
};

// For storing attendance data in blockchain
const storeAttendanceInBlockchain = async (attendanceData) => {
  try {
    // Check if required environment variables are set
    if (!privateKey || !contractAddressAttendance) {
      console.error('Missing PRIVATE_KEY or CONTRACT_ADDRESS_ATTENDANCE environment variables');
      return;
    }

    // Log the raw attendance data for debugging
    console.log('Raw attendance data:', JSON.stringify(attendanceData));

    // Extract attendance fields safely
    // Extract a numeric ID from the attendance name (if possible)
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
        // Try to parse as ISO string or various formats
        const attDate = new Date(attendanceData.attendance_date);
        if (!isNaN(attDate.getTime())) {
          attendanceDate = Math.floor(attDate.getTime() / 1000);
        }
      } catch (error) {
        console.error('Error parsing attendance date:', error.message);
      }
    }

    console.log(`Processing attendance data: ID=${id}, Employee=${employeeName}, Date=${attendanceDate}, Status=${status}`);
    console.log(`Sending to API endpoint: ${apiEndpointAttendance}`);

    // Retry logic for blockchain API calls
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      try {
        // Send to blockchain
        const response = await axios.post(apiEndpointAttendance, {
          privateKey: privateKey,
          contractAddress: contractAddressAttendance,
          id: id,
          employeeName: employeeName,
          attendanceDate: attendanceDate,
          status: status,
          company: company
        });

        console.log(`Stored attendance in blockchain, response:`, response.data);
        return; // Success, exit the function
      } catch (error) {
        attempts++;
        console.error(`Attempt ${attempts}/${maxAttempts} failed:`, error.message);

        if (attempts >= maxAttempts) {
          throw error; // Re-throw if all attempts failed
        }

        // Wait before retrying (exponential backoff)
        await new Promise(resolve => setTimeout(resolve, 2000 * attempts));
      }
    }
  } catch (error) {
    console.error('Error storing attendance data in blockchain:', error.message);
    // If axios error, log more details
    if (error.response) {
      console.error('API response error:', error.response.status, error.response.data);
    }
  }
};

// Function to check if Kafka topics exist
async function checkTopics() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    console.log('Connected to Kafka admin client');

    const topics = await admin.listTopics();
    console.log('Available topics:', topics);

    const userTopicExists = topics.includes(userTopic);
    const employeeTopicExists = topics.includes(employeeTopic);
    const attendanceTopicExists = topics.includes(attendanceTopic);

    console.log(`Topic '${userTopic}' exists: ${userTopicExists}`);
    console.log(`Topic '${employeeTopic}' exists: ${employeeTopicExists}`);
    console.log(`Topic '${attendanceTopic}' exists: ${attendanceTopicExists}`);

    await admin.disconnect();
    return { userTopicExists, employeeTopicExists, attendanceTopicExists };
  } catch (error) {
    console.error('Error checking topics:', error.message);
    return { userTopicExists: false, employeeTopicExists: false, attendanceTopicExists: false };
  }
}

// Process message based on topic
const processMessage = async (topic, message) => {
  try {
    // Parse message value
    const messageValue = message.value.toString();
    const event = JSON.parse(messageValue);

    // Log the received event
    console.log('\n----- CDC Event Received -----');
    console.log(`Topic: ${topic}`);
    console.log(`Offset: ${message.offset}`);
    console.log(`Timestamp: ${new Date(parseInt(message.timestamp)).toISOString()}`);

    // Log the event details
    if (event.op) {
      console.log(`Operation: ${event.op}`); // c=create, u=update, d=delete
    }

    // Different processing based on topic
    if (topic === userTopic) {
      console.log('User data:');
      console.log(JSON.stringify(event.after || event, null, 2));

      // Store user data in blockchain
      if (event.after) {
        await storeUserInBlockchain(event.after);
      } else if (event) {
        await storeUserInBlockchain(event);
      }
    } else if (topic === employeeTopic) {
      console.log('Employee data:');
      console.log(JSON.stringify(event.after || event, null, 2));

      // Store employee data in blockchain
      if (event.after) {
        await storeEmployeeInBlockchain(event.after);
      } else if (event) {
        await storeEmployeeInBlockchain(event);
      }
    } else if (topic === attendanceTopic) {
      console.log('Attendance data:');
      console.log(JSON.stringify(event.after || event, null, 2));

      // Store attendance data in blockchain
      if (event.after) {
        await storeAttendanceInBlockchain(event.after);
      } else if (event) {
        await storeAttendanceInBlockchain(event);
      }
    }

    console.log('----- End of CDC Event -----\n');
  } catch (error) {
    console.error('Error processing message:', error);
    console.error('Message content:', message.value.toString());
  }
};

// Main consumer function
async function run() {
  let connected = false;
  let retries = 0;
  const maxRetries = 15;

  while (!connected && retries < maxRetries) {
    try {
      console.log(`Starting consumer with broker: ${kafkaBroker} (attempt ${retries + 1}/${maxRetries})`);

      // Connect to Kafka
      await consumer.connect();
      console.log('Connected to Kafka');
      connected = true;

      // Check if topics exist
      const { userTopicExists, employeeTopicExists, attendanceTopicExists } = await checkTopics();

      // Subscribe to topics that exist
      if (userTopicExists) {
        await consumer.subscribe({ topic: userTopic, fromBeginning: true });
        console.log(`Subscribed to topic: ${userTopic}`);
      }

      if (employeeTopicExists) {
        await consumer.subscribe({ topic: employeeTopic, fromBeginning: true });
        console.log(`Subscribed to topic: ${employeeTopic}`);
      }

      if (attendanceTopicExists) {
        await consumer.subscribe({ topic: attendanceTopic, fromBeginning: true });
        console.log(`Subscribed to topic: ${attendanceTopic}`);
      }

      // If no topics exist, still subscribe so we're ready when they appear
      if (!userTopicExists && !employeeTopicExists && !attendanceTopicExists) {
        await consumer.subscribe({ topic: userTopic, fromBeginning: true });
        await consumer.subscribe({ topic: employeeTopic, fromBeginning: true });
        await consumer.subscribe({ topic: attendanceTopic, fromBeginning: true });
        console.log(`Pre-subscribed to topics that don't exist yet`);
      }

      // Consume messages
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          await processMessage(topic, message);
        },
      });

      console.log('Consumer started and waiting for messages...');

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
    console.error('Fatal error in consumer:', error);
    console.log('Restarting consumer in 10 seconds...');
    setTimeout(startWithAutoRestart, 10000);
  });
}

// Initial start
startWithAutoRestart();

// Handle termination signals
process.on('SIGINT', async () => {
  console.log('Disconnecting consumer...');
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error('Error during disconnect:', e);
  }
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Disconnecting consumer...');
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error('Error during disconnect:', e);
  }
  process.exit(0);
});