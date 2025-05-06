// consumer-employee.js
// A specialized consumer for employee data using a single combined smart contract

// Load environment variables if .env file exists, but don't fail if it doesn't
try {
  require('dotenv').config();
} catch (error) {
  console.log('No .env file found, using environment variables');
}

const { Kafka } = require('kafkajs');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');

// Get environment variables with fallbacks
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const apiBaseEndpoint = process.env.API_ENDPOINT || 'http://localhost:4001';
const privateKey = process.env.PRIVATE_KEY || '8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63';

// Use the single contract address from environment variables
const employeeContractAddress = process.env.CONTRACT_ADDRESS_EMPLOYEE;

// Create a processed records tracker
const PROCESSED_RECORDS_FILE = path.join(__dirname, 'processed_employee_records.json');
let processedRecords = new Set();

// Load previously processed records
async function loadProcessedRecords() {
  try {
    const data = await fs.readFile(PROCESSED_RECORDS_FILE, 'utf8');
    const records = JSON.parse(data);
    processedRecords = new Set(records);
    console.log(`Loaded ${processedRecords.size} previously processed employee records`);
  } catch (error) {
    // File might not exist yet, which is fine
    console.log('No processed employee records file found, starting fresh');
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

// Configure Kafka client with retry settings
const kafka = new Kafka({
  brokers: [kafkaBroker],
  clientId: 'employee-blockchain-consumer',
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Create consumer instance with longer session timeouts
const consumer = kafka.consumer({
  groupId: 'employee-blockchain-group',
  sessionTimeout: 60000, // 60 seconds
  heartbeatInterval: 10000, // 10 seconds
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Topic to listen for
const employeeTopic = 'erpnext._5e5899d8398b5f7b.tabEmployee';

// Utility function to create record ID from topic and offset
function createRecordId(topic, offset) {
  return `${topic}-${offset}`;
}

// Function to store employee data in single contract via API
const storeEmployeeData = async (employeeData) => {
  try {
    // Check if required environment variables are set
    if (!privateKey) {
      console.error('Missing PRIVATE_KEY environment variable');
      return false;
    }

    if (!employeeContractAddress) {
      console.error('Missing CONTRACT_ADDRESS_EMPLOYEE environment variable');
      console.error('Make sure to deploy the employee contract first');
      return false;
    }

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

    // Format dates correctly
    const dateOfBirth = employeeData.date_of_birth ?
      (typeof employeeData.date_of_birth === 'number' ? employeeData.date_of_birth :
        Math.floor(new Date(employeeData.date_of_birth).getTime() / 1000)) : 0;

    const dateOfJoining = employeeData.date_of_joining ?
      (typeof employeeData.date_of_joining === 'number' ? employeeData.date_of_joining :
        Math.floor(new Date(employeeData.date_of_joining).getTime() / 1000)) : 0;

    const dateOfRetirement = employeeData.date_of_retirement ?
      (typeof employeeData.date_of_retirement === 'number' ? employeeData.date_of_retirement :
        Math.floor(new Date(employeeData.date_of_retirement).getTime() / 1000)) : 0;

    // Create a single comprehensive object for the employee data
    const employeePayload = {
      basicInfo: {
        firstName: employeeData.first_name || '',
        middleName: employeeData.middle_name || '',
        lastName: employeeData.last_name || '',
        fullName: employeeName,
        gender: employeeData.gender || '',
        salutation: employeeData.salutation || '',
        company: employeeData.company || '',
        department: employeeData.department || '',
        designation: employeeData.designation || '',
        status: employeeData.status || 'Active'
      },
      dates: {
        dateOfBirth: dateOfBirth,
        dateOfJoining: dateOfJoining,
        dateOfRetirement: dateOfRetirement,
        creationDate: employeeData.creation ? Math.floor(employeeData.creation / 1000000) : 0,
        modificationDate: employeeData.modified ? Math.floor(employeeData.modified / 1000000) : 0,
        scheduledConfirmationDate: employeeData.scheduled_confirmation_date || 0,
        finalConfirmationDate: employeeData.final_confirmation_date || 0,
        contractEndDate: employeeData.contract_end_date || 0,
        resignationLetterDate: employeeData.resignation_letter_date || 0,
        relievingDate: employeeData.relieving_date || 0,
        encashmentDate: employeeData.encashment_date || 0,
        heldOnDate: employeeData.held_on || 0
      },
      contactInfo: {
        cellNumber: employeeData.cell_number || '',
        personalEmail: employeeData.personal_email || '',
        companyEmail: employeeData.company_email || '',
        preferredContactEmail: employeeData.prefered_contact_email || '',
        currentAddress: employeeData.current_address || '',
        currentAccommodationType: employeeData.current_accommodation_type || '',
        permanentAddress: employeeData.permanent_address || '',
        permanentAccommodationType: employeeData.permanent_accommodation_type || '',
        personToBeContacted: employeeData.person_to_be_contacted || '',
        emergencyPhoneNumber: employeeData.emergency_phone_number || '',
        relation: employeeData.relation || ''
      },
      basicEmployment: {
        employeeNumber: employeeData.employee_number || '',
        reportsTo: employeeData.reports_to || '',
        branch: employeeData.branch || '',
        noticeNumberOfDays: employeeData.notice_number_of_days || 0,
        newWorkplace: employeeData.new_workplace || '',
        leaveEncashed: employeeData.leave_encashed === "1" ? true : false
      },
      career: {
        reasonForLeaving: employeeData.reason_for_leaving || '',
        feedback: employeeData.feedback || '',
        employmentType: employeeData.employment_type || '',
        grade: employeeData.grade || '',
        jobApplicant: employeeData.job_applicant || '',
        defaultShift: employeeData.default_shift || ''
      },
      approval: {
        expenseApprover: employeeData.expense_approver || '',
        leaveApprover: employeeData.leave_approver || '',
        shiftRequestApprover: employeeData.shift_request_approver || '',
        payrollCostCenter: employeeData.payroll_cost_center || '',
        healthInsuranceProvider: employeeData.health_insurance_provider || '',
        healthInsuranceNo: employeeData.health_insurance_no || ''
      },
      financial: {
        salaryCurrency: employeeData.salary_currency || '',
        salaryMode: employeeData.salary_mode || '',
        bankName: employeeData.bank_name || '',
        bankAccountNo: employeeData.bank_ac_no || '',
        iban: employeeData.iban || ''
      },
      personal: {
        maritalStatus: employeeData.marital_status || '',
        familyBackground: employeeData.family_background || '',
        bloodGroup: employeeData.blood_group || '',
        healthDetails: employeeData.health_details || '',
        passportNumber: employeeData.passport_number || '',
        validUpto: employeeData.valid_upto || '',
        dateOfIssue: employeeData.date_of_issue || '',
        placeOfIssue: employeeData.place_of_issue || '',
        bio: employeeData.bio || '',
        attendanceDeviceId: employeeData.attendance_device_id || '',
        holidayList: employeeData.holiday_list || ''
      }
    };

    // First, register the employee
    console.log(`Registering employee ${employeeId} in contract: ${employeeContractAddress}`);

    try {
      // 1. Register the employee
      await axios.post(`${apiBaseEndpoint}/store-employee`, {
        privateKey: privateKey,
        contractAddress: employeeContractAddress,
        employeeId: employeeId,
        employeeName: employeeName
      }, {
        timeout: 15000
      });
      console.log(`Employee ${employeeId} registered successfully`);

      // 2. Store basic info
      await axios.post(`${apiBaseEndpoint}/store-employee-basic-info`, {
        privateKey: privateKey,
        contractAddress: employeeContractAddress,
        employeeId: employeeId,
        ...employeePayload.basicInfo
      }, {
        timeout: 15000
      });
      console.log('Basic info stored successfully');

      // 3. Store dates
      await axios.post(`${apiBaseEndpoint}/store-employee-dates`, {
        privateKey: privateKey,
        contractAddress: employeeContractAddress,
        employeeId: employeeId,
        ...employeePayload.dates
      }, {
        timeout: 15000
      });
      console.log('Dates stored successfully');

      // 4. Store contact info
      await axios.post(`${apiBaseEndpoint}/store-employee-contact`, {
        privateKey: privateKey,
        contractAddress: employeeContractAddress,
        employeeId: employeeId,
        ...employeePayload.contactInfo
      }, {
        timeout: 15000
      });
      console.log('Contact info stored successfully');

      // Store remaining data sections
      // We'll just call them in sequence and handle any errors

      // 5. Basic employment
      try {
        await axios.post(`${apiBaseEndpoint}/store-employee-employment`, {
          privateKey: privateKey,
          contractAddress: employeeContractAddress,
          employeeId: employeeId,
          ...employeePayload.basicEmployment
        }, {
          timeout: 15000
        });
        console.log('Basic employment stored successfully');
      } catch (error) {
        console.error('Error storing basic employment:', error.message);
      }

      // 6. Career
      try {
        await axios.post(`${apiBaseEndpoint}/store-employee-career`, {
          privateKey: privateKey,
          contractAddress: employeeContractAddress,
          employeeId: employeeId,
          ...employeePayload.career
        }, {
          timeout: 15000
        });
        console.log('Career info stored successfully');
      } catch (error) {
        console.error('Error storing career info:', error.message);
      }

      // 7. Approval
      try {
        await axios.post(`${apiBaseEndpoint}/store-employee-approval`, {
          privateKey: privateKey,
          contractAddress: employeeContractAddress,
          employeeId: employeeId,
          ...employeePayload.approval
        }, {
          timeout: 15000
        });
        console.log('Approval info stored successfully');
      } catch (error) {
        console.error('Error storing approval info:', error.message);
      }

      // 8. Financial
      try {
        await axios.post(`${apiBaseEndpoint}/store-employee-financial`, {
          privateKey: privateKey,
          contractAddress: employeeContractAddress,
          employeeId: employeeId,
          ...employeePayload.financial
        }, {
          timeout: 15000
        });
        console.log('Financial info stored successfully');
      } catch (error) {
        console.error('Error storing financial info:', error.message);
      }

      // 9. Personal
      try {
        await axios.post(`${apiBaseEndpoint}/store-employee-personal`, {
          privateKey: privateKey,
          contractAddress: employeeContractAddress,
          employeeId: employeeId,
          ...employeePayload.personal
        }, {
          timeout: 15000
        });
        console.log('Personal info stored successfully');
      } catch (error) {
        console.error('Error storing personal info:', error.message);
      }

      console.log(`Employee ${employeeId} data successfully stored in blockchain`);
      return true;

    } catch (error) {
      console.error('Error storing employee data in blockchain:', error.message);
      if (error.response) {
        console.error('API response error:', error.response.status, error.response.data);
      }
      return false;
    }
  } catch (error) {
    console.error('Fatal error processing employee data:', error.message);
    return false;
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

    const employeeTopicExists = topics.includes(employeeTopic);
    console.log(`Topic '${employeeTopic}' exists: ${employeeTopicExists}`);

    await admin.disconnect();
    return { employeeTopicExists };
  } catch (error) {
    console.error('Error checking topics:', error.message);
    return { employeeTopicExists: false };
  }
}

// Healthcheck function
async function performHealthCheck() {
  try {
    console.log('\n----- CDC Employee Consumer Health Check -----');
    console.log(`Kafka Broker: ${kafkaBroker}`);
    console.log(`API Endpoint: ${apiBaseEndpoint}`);
    console.log(`Employee Contract: ${employeeContractAddress || 'Not set'}`);
    console.log(`Processed Records: ${processedRecords.size}`);

    // Check if employee contract is set
    if (!employeeContractAddress) {
      console.log('⚠️ WARNING: Employee contract address is not set!');
      console.log('Run the deploy-employee-contract.js script first.');
    }

    // Check API connectivity
    try {
      const response = await axios.get(`${apiBaseEndpoint}/health`, { timeout: 5000 });
      console.log(`API Status: ${response.data?.status || 'Unknown'}`);
    } catch (error) {
      console.log(`API Status: Not available (${error.message})`);
    }

    console.log('----- Health Check Complete -----\n');
  } catch (error) {
    console.error('Error in health check:', error.message);
  }
}

// Process employee message
const processEmployeeMessage = async (message) => {
  try {
    // Create a unique ID for this record
    const recordId = createRecordId(employeeTopic, message.offset);

    // Skip if already processed
    if (processedRecords.has(recordId)) {
      console.log(`Employee record ${recordId} already processed, skipping`);
      return;
    }

    // Parse message value
    const messageValue = message.value.toString();
    const event = JSON.parse(messageValue);

    // Log the received event
    console.log('\n----- Employee CDC Event Received -----');
    console.log(`Offset: ${message.offset}`);
    console.log(`RecordID: ${recordId}`);
    console.log(`Timestamp: ${new Date(parseInt(message.timestamp)).toISOString()}`);

    // Log the event details
    if (event.op) {
      console.log(`Operation: ${event.op}`); // c=create, u=update, d=delete
    }

    const employeeData = event.after || event;

    // Skip if the record is marked as deleted
    if (employeeData.__deleted === "true") {
      console.log('Employee record marked as deleted, skipping blockchain storage');
      processedRecords.add(recordId);
      await saveProcessedRecords();
      return;
    }

    // Extract more useful fields for logging
    console.log('Employee Details:');
    console.log(`Name: ${employeeData.name || 'Not set'}`);
    console.log(`Employee Name: ${employeeData.employee_name || 'Not set'}`);
    console.log(`First Name: ${employeeData.first_name || 'Not set'}`);
    console.log(`Last Name: ${employeeData.last_name || 'Not set'}`);
    console.log(`Gender: ${employeeData.gender || 'Not set'}`);
    console.log(`Company: ${employeeData.company || 'Not set'}`);

    // Store employee data in blockchain using the single contract
    const success = await storeEmployeeData(employeeData);

    if (success) {
      // Mark as processed if successful
      processedRecords.add(recordId);
      await saveProcessedRecords();
      console.log(`Record ${recordId} processed successfully and marked as processed`);
    } else {
      console.log(`Record ${recordId} processing failed, will retry on next run`);
    }

    console.log('----- End of Employee CDC Event -----\n');
  } catch (error) {
    console.error('Error processing employee message:', error);
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

  // Perform initial health check
  await performHealthCheck();

  while (!connected && retries < maxRetries) {
    try {
      console.log(`Starting employee consumer with broker: ${kafkaBroker} (attempt ${retries + 1}/${maxRetries})`);

      // Connect to Kafka
      await consumer.connect();
      console.log('Connected to Kafka');
      connected = true;

      // Check if topics exist
      const { employeeTopicExists } = await checkTopics();

      // Subscribe to employee topic if it exists
      if (employeeTopicExists) {
        await consumer.subscribe({ topic: employeeTopic, fromBeginning: true });
        console.log(`Subscribed to topic: ${employeeTopic}`);
      } else {
        console.error(`Required topic ${employeeTopic} does not exist!`);
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
          if (topic === employeeTopic) {
            await processEmployeeMessage(message);
          }
        },
      });

      console.log('Employee consumer started and waiting for messages...');

    } catch (error) {
      retries++;
      console.error(`Connection attempt ${retries} failed:`, error.message);

      if (retries >= maxRetries) {
        console.error('Maximum retries reached. Exiting.');
        process.exit(1);
      }

      // Exponential backoff for retries
      const backoffTime = Math.min(30000, 1000 * Math.pow(2, retries));
      console.log(`Waiting ${backoffTime / 1000} seconds before retrying...`);
      await new Promise(resolve => setTimeout(resolve, backoffTime));
    }
  }
}

// Start the consumer with auto-restart
function startWithAutoRestart() {
  run().catch(error => {
    console.error('Fatal error in employee consumer:', error);
    console.log('Restarting consumer in 10 seconds...');
    setTimeout(startWithAutoRestart, 10000);
  });
}

// Schedule periodic health checks
setInterval(performHealthCheck, 15 * 60 * 1000); // Every 15 minutes

// Display startup information
console.log('Starting ERP-Blockchain Employee Consumer Service...');
console.log(`Using Kafka broker: ${kafkaBroker}`);
console.log(`API endpoint: ${apiBaseEndpoint}`);
console.log(`Employee contract: ${employeeContractAddress || 'Not set'}`);

// Initial start
startWithAutoRestart();

// Handle termination signals
process.on('SIGINT', async () => {
  console.log('Disconnecting employee consumer...');
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error('Error during disconnect:', e);
  }
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Disconnecting employee consumer...');
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error('Error during disconnect:', e);
  }
  process.exit(0);
});