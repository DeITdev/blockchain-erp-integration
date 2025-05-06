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
const privateKey = process.env.PRIVATE_KEY;

// Define contract addresses from environment variables
const registryAddress = process.env.REGISTRY_CONTRACT_ADDRESS;
const basicInfoAddress = process.env.BASIC_INFO_CONTRACT_ADDRESS;
const datesAddress = process.env.DATES_CONTRACT_ADDRESS;
const contactInfoAddress = process.env.CONTACT_INFO_CONTRACT_ADDRESS;
const basicEmploymentAddress = process.env.BASIC_EMPLOYMENT_CONTRACT_ADDRESS;
const careerAddress = process.env.CAREER_CONTRACT_ADDRESS;
const approvalAddress = process.env.APPROVAL_CONTRACT_ADDRESS;
const financialAddress = process.env.FINANCIAL_CONTRACT_ADDRESS;
const personalAddress = process.env.PERSONAL_CONTRACT_ADDRESS;

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

// For storing user data in blockchain (simple storage)
const storeUserInBlockchain = async (userData) => {
  try {
    // Check if required environment variables are set
    if (!privateKey || !process.env.CONTRACT_ADDRESS) {
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
          contractAddress: process.env.CONTRACT_ADDRESS,
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

// For storing employee data in blockchain using multiple contracts
// For storing employee data in blockchain using multiple contracts
const storeEmployeeInBlockchain = async (employeeData) => {
  try {
    // Check if required environment variables are set
    if (!privateKey) {
      console.error('Missing PRIVATE_KEY environment variable');
      return;
    }

    // Log the raw employee data for debugging
    console.log('Raw employee data:', JSON.stringify(employeeData));

    // Generate an employee ID from the name or create a random one
    const employeeId = employeeData.name ?
      parseInt(employeeData.name.replace(/\D/g, ''), 10) ||
      Math.floor(Math.random() * 1000000) :
      Math.floor(Math.random() * 1000000);

    // Extract employee name
    const employeeName = employeeData.employee_name ||
      `${employeeData.first_name || ''} ${employeeData.last_name || ''}`.trim() ||
      `Employee ${employeeId}`;

    console.log(`Processing employee with ID: ${employeeId}, Name: ${employeeName}`);

    // Prepare basic employee data with correct field names
    const basicEmployeeData = {
      id: employeeId,
      name: employeeId.toString(),
      first_name: employeeData.first_name || '',
      middle_name: employeeData.middle_name || '',
      last_name: employeeData.last_name || '',
      employee_name: employeeName,
      gender: employeeData.gender || '',
      company: employeeData.company || '',
      department: employeeData.department || '',
      designation: employeeData.designation || '',

      // Format dates correctly
      date_of_birth: employeeData.date_of_birth ?
        Math.floor(new Date(employeeData.date_of_birth).getTime() / 1000) : 0,
      date_of_joining: employeeData.date_of_joining ?
        Math.floor(new Date(employeeData.date_of_joining).getTime() / 1000) : 0,

      phone: employeeData.phone || employeeData.cell_number || '',
      cell_number: employeeData.phone || employeeData.cell_number || '',

      status: employeeData.status || 'Active'
    };

    // Store basic data - try with setupEmployeeComplete
    try {
      console.log(`Using complete setup endpoint...`);
      const response = await axios.post('http://localhost:4001/api/v2/employee/setup-employee-complete', {
        privateKey: privateKey,
        employeeData: basicEmployeeData
      });

      console.log(`Employee data stored successfully:`, response.data);
      return true;
    } catch (error) {
      console.error('Error in setup endpoint:', error.message);
      if (error.response) {
        console.error('Response error:', error.response.status, error.response.data);
      }

      // If complete setup fails, fall back to individual endpoints
      console.log('Falling back to individual contract updates...');

      // Store basic info
      try {
        if (basicInfoAddress) {
          console.log(`Storing basic info in contract: ${basicInfoAddress}`);
          await axios.post('http://localhost:4001/api/v2/employee/store-basic-info', {
            privateKey: privateKey,
            contractAddress: basicInfoAddress,
            employeeId: employeeId,
            basicInfo: {
              firstName: employeeData.first_name || '',
              middleName: employeeData.middle_name || '',
              lastName: employeeData.last_name || '',
              fullName: employeeName,
              gender: employeeData.gender || '',
              company: employeeData.company || '',
              department: employeeData.department || '',
              designation: employeeData.designation || '',
              status: employeeData.status || 'Active'
            }
          });
          console.log('Basic info stored successfully');
        }
      } catch (error) {
        console.error('Error storing basic info:', error.message);
      }

      // If all else fails, store in simple contract
      console.log('Storing in simple contract as fallback...');
      await storeSimpleEmployeeData(employeeData);
    }
  } catch (error) {
    console.error('Error storing employee data in blockchain:', error.message);
    if (error.response) {
      console.error('API response error:', error.response.status, error.response.data);
    }
    return false;
  }
};

// Fallback function to store basic employee data in simple storage contract
const storeSimpleEmployeeData = async (employeeData) => {
  try {
    if (!privateKey || !process.env.CONTRACT_ADDRESS) {
      console.error('Missing PRIVATE_KEY or CONTRACT_ADDRESS environment variables');
      return false;
    }

    // Extract a numeric value to store (employee ID or phone number)
    const employeeId = employeeData.name ?
      parseInt(employeeData.name.replace(/\D/g, ''), 10) : null;

    const phoneValue = employeeData.phone ?
      parseInt(employeeData.phone, 10) : null;

    const valueToStore = employeeId || phoneValue || Math.floor(Math.random() * 1000000);

    console.log(`Storing employee data in simple storage: value=${valueToStore}`);

    const response = await axios.post(apiEndpoint, {
      privateKey: privateKey,
      contractAddress: process.env.CONTRACT_ADDRESS,
      value: valueToStore
    });

    console.log(`Stored simple employee data, response:`, response.data);
    return true;
  } catch (error) {
    console.error('Error storing simple employee data:', error.message);
    if (error.response) {
      console.error('API response error:', error.response.status, error.response.data);
    }
    return false;
  }
};

// For storing attendance data in blockchain
const storeAttendanceInBlockchain = async (attendanceData) => {
  try {
    // Check if required environment variables are set
    if (!privateKey || !process.env.CONTRACT_ADDRESS_ATTENDANCE) {
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
    console.log(`Sending to API endpoint: ${process.env.API_ENDPOINT_ATTENDANCE || 'http://localhost:4001/store-attendance'}`);

    // Retry logic for blockchain API calls
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      try {
        // Send to blockchain
        const response = await axios.post(process.env.API_ENDPOINT_ATTENDANCE || 'http://localhost:4001/store-attendance', {
          privateKey: privateKey,
          contractAddress: process.env.CONTRACT_ADDRESS_ATTENDANCE,
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