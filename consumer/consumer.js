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

// Define contract addresses from environment variables
const registryAddress = process.env.REGISTRY_CONTRACT_ADDRESS || '0xDE87AF9156a223404885002669D3bE239313Ae33';
const basicInfoAddress = process.env.BASIC_INFO_CONTRACT_ADDRESS || '0x686AfD6e502A81D2e77f2e038A23C0dEf4949A20';
const datesAddress = process.env.DATES_CONTRACT_ADDRESS || '0x664D6EbAbbD5cf656eD07A509AFfBC81f9615741';
const contactInfoAddress = process.env.CONTACT_INFO_CONTRACT_ADDRESS || '0x37A49B1F380c74e47A1544Ac2BB5404FF159275c';
const basicEmploymentAddress = process.env.BASIC_EMPLOYMENT_CONTRACT_ADDRESS || '0x1Be01cBe5a96FBAc978B3f25C3eB5d541233Ab27';
const careerAddress = process.env.CAREER_CONTRACT_ADDRESS || '0x1024d31846670b356f952F4c002E3758Ab9c4FFC';
const approvalAddress = process.env.APPROVAL_CONTRACT_ADDRESS || '0xE6BAb1eAc80e9d68BD76c3bb61abad86133109DD';
const financialAddress = process.env.FINANCIAL_CONTRACT_ADDRESS || '0x0d8425cEa91B9c8d7Dd2bE278Fb945aF78Aba57b';
const personalAddress = process.env.PERSONAL_CONTRACT_ADDRESS || '0x520F3536Ce622A9C90d9E355b2547D9e5cfb76fE';
const attendanceContractAddress = process.env.CONTRACT_ADDRESS_ATTENDANCE || '0x6486A01e45648B1aDCc51D375Af3a7c0a5e9002a';

// Create a processed records tracker
const PROCESSED_RECORDS_FILE = path.join(__dirname, 'processed_records.json');
let processedRecords = new Set();

// Load previously processed records
async function loadProcessedRecords() {
  try {
    const data = await fs.readFile(PROCESSED_RECORDS_FILE, 'utf8');
    const records = JSON.parse(data);
    processedRecords = new Set(records);
    console.log(`Loaded ${processedRecords.size} previously processed records`);
  } catch (error) {
    // File might not exist yet, which is fine
    console.log('No processed records file found, starting fresh');
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

// Configure Kafka client with retry settings and rebalance settings
const kafka = new Kafka({
  brokers: [kafkaBroker],
  clientId: 'erp-blockchain-consumer',
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Create consumer instance with longer session timeouts to prevent frequent rebalancing
const consumer = kafka.consumer({
  groupId: 'erpnext-blockchain-group',
  sessionTimeout: 60000, // 60 seconds
  heartbeatInterval: 10000, // 10 seconds
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Topics to listen for
const userTopic = 'erpnext._5e5899d8398b5f7b.tabUser';
const employeeTopic = 'erpnext._5e5899d8398b5f7b.tabEmployee';
const attendanceTopic = 'erpnext._5e5899d8398b5f7b.tabAttendance';

// Utility function to create record ID from topic and offset
function createRecordId(topic, offset) {
  return `${topic}-${offset}`;
}

// For storing employee data directly using specific contracts
// For storing employee data directly using specific contracts
const storeEmployeeDataIndividually = async (employeeData) => {
  try {
    // Check if private key is set
    if (!privateKey) {
      console.error('Missing PRIVATE_KEY environment variable');
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

    // Register the employee in the registry first
    if (registryAddress) {
      try {
        await axios.post(`${apiBaseEndpoint}/api/v2/employee/register-employee`, {
          privateKey: privateKey,
          registryAddress: registryAddress,
          employeeId: employeeId,
          employeeName: employeeName
        }, {
          timeout: 15000
        });
        console.log(`Employee ${employeeId} registered in registry`);
      } catch (error) {
        console.log(`Employee ${employeeId} likely already registered in registry`);
      }
    }

    // Store data in all nine contracts - wrap each in try/catch to continue
    // even if some fail
    const results = {
      success: false,
      basicInfo: false,
      dates: false,
      contact: false,
      employment: false,
      career: false,
      approval: false,
      financial: false,
      personal: false
    };

    // 1. Store basic info
    if (basicInfoAddress) {
      try {
        console.log(`Storing basic info in contract: ${basicInfoAddress}`);
        await axios.post(`${apiBaseEndpoint}/api/v2/employee/store-basic-info`, {
          privateKey: privateKey,
          contractAddress: basicInfoAddress,
          employeeId: employeeId,
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
          }
        }, {
          timeout: 15000
        });
        console.log('Basic info stored successfully');
        results.basicInfo = true;
      } catch (error) {
        console.error('Error storing basic info:', error.message);
      }
    }

    // 2. Store dates
    if (datesAddress) {
      try {
        console.log(`Storing dates in contract: ${datesAddress}`);
        await axios.post(`${apiBaseEndpoint}/api/v2/employee/store-dates`, {
          privateKey: privateKey,
          contractAddress: datesAddress,
          employeeId: employeeId,
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
          }
        }, {
          timeout: 15000
        });
        console.log('Dates stored successfully');
        results.dates = true;
      } catch (error) {
        console.error('Error storing dates:', error.message);
      }
    }

    // 3. Store contact info
    if (contactInfoAddress) {
      try {
        console.log(`Storing contact info in contract: ${contactInfoAddress}`);
        await axios.post(`${apiBaseEndpoint}/api/v2/employee/store-contact-info`, {
          privateKey: privateKey,
          contractAddress: contactInfoAddress,
          employeeId: employeeId,
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
          }
        }, {
          timeout: 15000
        });
        console.log('Contact info stored successfully');
        results.contact = true;
      } catch (error) {
        console.error('Error storing contact info:', error.message);
      }
    }

    // 4. Store basic employment info
    if (basicEmploymentAddress) {
      try {
        console.log(`Storing basic employment in contract: ${basicEmploymentAddress}`);
        await axios.post(`${apiBaseEndpoint}/api/v2/employee/store-basic-employment`, {
          privateKey: privateKey,
          contractAddress: basicEmploymentAddress,
          employeeId: employeeId,
          basicEmployment: {
            employeeNumber: employeeData.employee_number || '',
            reportsTo: employeeData.reports_to || '',
            branch: employeeData.branch || '',
            noticeNumberOfDays: employeeData.notice_number_of_days || 0,
            newWorkplace: employeeData.new_workplace || '',
            leaveEncashed: employeeData.leave_encashed === "1" ? true : false
          }
        }, {
          timeout: 15000
        });
        console.log('Basic employment stored successfully');
        results.employment = true;
      } catch (error) {
        console.error('Error storing basic employment:', error.message);
      }
    }

    // 5. Store career info
    if (careerAddress) {
      try {
        console.log(`Storing career info in contract: ${careerAddress}`);
        await axios.post(`${apiBaseEndpoint}/api/v2/employee/store-career`, {
          privateKey: privateKey,
          contractAddress: careerAddress,
          employeeId: employeeId,
          career: {
            reasonForLeaving: employeeData.reason_for_leaving || '',
            feedback: employeeData.feedback || '',
            employmentType: employeeData.employment_type || '',
            grade: employeeData.grade || '',
            jobApplicant: employeeData.job_applicant || '',
            defaultShift: employeeData.default_shift || ''
          }
        }, {
          timeout: 15000
        });
        console.log('Career info stored successfully');
        results.career = true;
      } catch (error) {
        console.error('Error storing career info:', error.message);
      }
    }

    // 6. Store approval info
    if (approvalAddress) {
      try {
        console.log(`Storing approval info in contract: ${approvalAddress}`);
        await axios.post(`${apiBaseEndpoint}/api/v2/employee/store-approval`, {
          privateKey: privateKey,
          contractAddress: approvalAddress,
          employeeId: employeeId,
          approval: {
            expenseApprover: employeeData.expense_approver || '',
            leaveApprover: employeeData.leave_approver || '',
            shiftRequestApprover: employeeData.shift_request_approver || '',
            payrollCostCenter: employeeData.payroll_cost_center || '',
            healthInsuranceProvider: employeeData.health_insurance_provider || '',
            healthInsuranceNo: employeeData.health_insurance_no || ''
          }
        }, {
          timeout: 15000
        });
        console.log('Approval info stored successfully');
        results.approval = true;
      } catch (error) {
        console.error('Error storing approval info:', error.message);
      }
    }

    // 7. Store financial info
    if (financialAddress) {
      try {
        console.log(`Storing financial info in contract: ${financialAddress}`);
        await axios.post(`${apiBaseEndpoint}/api/v2/employee/store-financial`, {
          privateKey: privateKey,
          contractAddress: financialAddress,
          employeeId: employeeId,
          financial: {
            salaryCurrency: employeeData.salary_currency || '',
            salaryMode: employeeData.salary_mode || '',
            bankName: employeeData.bank_name || '',
            bankAccountNo: employeeData.bank_ac_no || '',
            iban: employeeData.iban || ''
          }
        }, {
          timeout: 15000
        });
        console.log('Financial info stored successfully');
        results.financial = true;
      } catch (error) {
        console.error('Error storing financial info:', error.message);
      }
    }

    // 8. Store personal info
    if (personalAddress) {
      try {
        console.log(`Storing personal info in contract: ${personalAddress}`);
        await axios.post(`${apiBaseEndpoint}/api/v2/employee/store-personal`, {
          privateKey: privateKey,
          contractAddress: personalAddress,
          employeeId: employeeId,
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
        }, {
          timeout: 15000
        });
        console.log('Personal info stored successfully');
        results.personal = true;
      } catch (error) {
        console.error('Error storing personal info:', error.message);
      }
    }

    // Register each contract in the registry if needed
    if (registryAddress) {
      try {
        // Only register contracts that were successfully stored and have valid addresses
        const contractsToRegister = [
          { type: 0, address: basicInfoAddress, success: results.basicInfo },
          { type: 1, address: datesAddress, success: results.dates },
          { type: 2, address: contactInfoAddress, success: results.contact },
          { type: 3, address: basicEmploymentAddress, success: results.employment },
          { type: 4, address: careerAddress, success: results.career },
          { type: 5, address: approvalAddress, success: results.approval },
          { type: 6, address: financialAddress, success: results.financial },
          { type: 7, address: personalAddress, success: results.personal }
        ];

        for (const contract of contractsToRegister) {
          if (contract.success && contract.address) {
            try {
              await axios.post(`${apiBaseEndpoint}/api/v2/employee/register-contract-in-registry`, {
                privateKey: privateKey,
                registryAddress: registryAddress,
                employeeId: employeeId,
                contractType: contract.type,
                contractAddress: contract.address
              }, {
                timeout: 15000
              });
              console.log(`Contract type ${contract.type} registered in registry`);
            } catch (error) {
              console.log(`Contract type ${contract.type} likely already registered or error: ${error.message}`);
            }
          }
        }
      } catch (error) {
        console.error('Error registering contracts in registry:', error.message);
      }
    }

    // Return success if at least basic info and dates were stored
    results.success = results.basicInfo || results.dates ||
      results.contact || results.employment ||
      results.career || results.approval ||
      results.financial || results.personal;

    return results;
  } catch (error) {
    console.error('Fatal error storing employee data in blockchain:', error.message);
    if (error.response) {
      console.error('API response error:', error.response.status, error.response.data);
    }
    return { success: false };
  }
};

// For storing attendance data in blockchain
const storeAttendanceInBlockchain = async (attendanceData) => {
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
    // Create a unique ID for this record
    const recordId = createRecordId(topic, message.offset);

    // Skip if already processed
    if (processedRecords.has(recordId)) {
      console.log(`Record ${recordId} already processed, skipping`);
      return;
    }

    // Parse message value
    const messageValue = message.value.toString();
    const event = JSON.parse(messageValue);

    // Log the received event
    console.log('\n----- CDC Event Received -----');
    console.log(`Topic: ${topic}`);
    console.log(`Offset: ${message.offset}`);
    console.log(`RecordID: ${recordId}`);
    console.log(`Timestamp: ${new Date(parseInt(message.timestamp)).toISOString()}`);

    // Log the event details
    if (event.op) {
      console.log(`Operation: ${event.op}`); // c=create, u=update, d=delete
    }

    // Different processing based on topic
    if (topic === employeeTopic) {
      console.log('Employee data detected - processing...');
      const employeeData = event.after || event;

      // Skip if the record is marked as deleted
      if (employeeData.__deleted === "true") {
        console.log('Employee record marked as deleted, skipping blockchain storage');
        processedRecords.add(recordId);
        await saveProcessedRecords();
        return;
      }

      // Store employee data in blockchain directly using individual contracts
      const result = await storeEmployeeDataIndividually(employeeData);

      if (result.success) {
        // Mark as processed if successful
        processedRecords.add(recordId);
        await saveProcessedRecords();
      }
    }
    else if (topic === attendanceTopic) {
      console.log('Attendance data detected - processing...');
      const attendanceData = event.after || event;

      // Skip if the record is marked as deleted
      if (attendanceData.__deleted === "true") {
        console.log('Attendance record marked as deleted, skipping blockchain storage');
        processedRecords.add(recordId);
        await saveProcessedRecords();
        return;
      }

      // Store attendance data in blockchain
      const success = await storeAttendanceInBlockchain(attendanceData);

      if (success) {
        // Mark as processed if successful
        processedRecords.add(recordId);
        await saveProcessedRecords();
      }
    }
    // User topic processing is removed to focus on employee and attendance

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

  // Load previously processed records
  await loadProcessedRecords();

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
      // We'll prioritize employee and attendance topics
      if (employeeTopicExists) {
        await consumer.subscribe({ topic: employeeTopic, fromBeginning: true });
        console.log(`Subscribed to topic: ${employeeTopic}`);
      }

      if (attendanceTopicExists) {
        await consumer.subscribe({ topic: attendanceTopic, fromBeginning: true });
        console.log(`Subscribed to topic: ${attendanceTopic}`);
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

// Display startup information
console.log('Starting ERP-Blockchain Consumer Service...');
console.log(`Using Kafka broker: ${kafkaBroker}`);
console.log(`API endpoint: ${apiBaseEndpoint}`);
console.log(`Registry contract: ${registryAddress || 'Not set'}`);
console.log(`Basic info contract: ${basicInfoAddress || 'Not set'}`);
console.log(`Dates contract: ${datesAddress || 'Not set'}`);
console.log(`Attendance contract: ${attendanceContractAddress || 'Not set'}`);

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