const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Try multiple possible locations for the config files
const userConnectorPath = path.join(__dirname, 'debezium-mariadb-config.json');
const employeeConnectorPath = path.join(__dirname, 'debezium-employee-config.json');
const attendanceConnectorPath = path.join(__dirname, 'debezium-attendance-config.json');

async function registerConnector(configPath, connectorName) {
  try {
    // Check if the config file exists
    if (!fs.existsSync(configPath)) {
      console.error(`Error: Configuration file not found at ${configPath}`);
      return false;
    }

    console.log(`Reading configuration from: ${configPath}`);
    const connectorConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));

    console.log(`Registering ${connectorName} connector...`);

    // Check if connector exists
    const checkResponse = await axios.get('http://localhost:8083/connectors');
    const existingConnectors = checkResponse.data || [];

    if (existingConnectors.includes(connectorName)) {
      console.log(`${connectorName} already exists. Deleting it first...`);
      await axios.delete(`http://localhost:8083/connectors/${connectorName}`);
      console.log(`Existing ${connectorName} deleted.`);
    }

    // Register the connector
    const response = await axios.post(
      'http://localhost:8083/connectors',
      connectorConfig
    );

    console.log(`${connectorName} registration successful!`);
    console.log('Response:', JSON.stringify(response.data, null, 2));
    return true;
  } catch (error) {
    console.error(`Error registering ${connectorName} connector:`);
    if (error.response) {
      console.error('Status:', error.response.status);
      console.error('Response:', JSON.stringify(error.response.data, null, 2));
    } else {
      console.error(error.message);
    }
    return false;
  }
}

async function checkConnectorStatus(connectorName) {
  try {
    console.log(`Checking status of ${connectorName}...`);
    const response = await axios.get(`http://localhost:8083/connectors/${connectorName}/status`);
    console.log(`${connectorName} status:`, JSON.stringify(response.data, null, 2));

    // Check if the connector is running or failed
    const state = response.data?.connector?.state;
    const taskStates = response.data?.tasks?.map(task => task.state) || [];

    if (state === 'RUNNING' && taskStates.every(s => s === 'RUNNING')) {
      console.log(`${connectorName} is running correctly.`);
      return true;
    } else {
      console.log(`${connectorName} has issues. State: ${state}, Tasks: ${taskStates.join(', ')}`);

      // If there are errors, display them
      response.data?.tasks?.forEach(task => {
        if (task.state === 'FAILED') {
          console.error(`Task ${task.id} failure trace:`);
          console.error(task.trace);
        }
      });

      return false;
    }
  } catch (error) {
    console.error(`Error checking ${connectorName} status:`, error.message);
    return false;
  }
}

async function registerAll() {
  console.log('Starting registration of all connectors...');
  let results = {
    user: false,
    employee: false,
    attendance: false
  };

  // Register the user connector
  console.log('\n================ USER CONNECTOR ================');
  results.user = await registerConnector(userConnectorPath, 'erpnext-connector');
  if (results.user) {
    console.log('User connector registration completed.');
    // Wait a bit for the connector to start up
    await new Promise(resolve => setTimeout(resolve, 5000));
    await checkConnectorStatus('erpnext-connector');
  } else {
    console.log('User connector registration failed.');
  }

  // Register the employee connector
  console.log('\n================ EMPLOYEE CONNECTOR ================');
  results.employee = await registerConnector(employeeConnectorPath, 'employee-connector');
  if (results.employee) {
    console.log('Employee connector registration completed.');
    // Wait a bit for the connector to start up
    await new Promise(resolve => setTimeout(resolve, 5000));
    await checkConnectorStatus('employee-connector');
  } else {
    console.log('Employee connector registration failed.');
  }

  // Register the attendance connector
  console.log('\n================ ATTENDANCE CONNECTOR ================');
  results.attendance = await registerConnector(attendanceConnectorPath, 'attendance-connector');
  if (results.attendance) {
    console.log('Attendance connector registration completed.');
    // Wait a bit for the connector to start up
    await new Promise(resolve => setTimeout(resolve, 5000));
    await checkConnectorStatus('attendance-connector');
  } else {
    console.log('Attendance connector registration failed.');
  }

  // Final summary
  console.log('\n================ REGISTRATION SUMMARY ================');
  console.log(`User Connector: ${results.user ? 'SUCCESS' : 'FAILED'}`);
  console.log(`Employee Connector: ${results.employee ? 'SUCCESS' : 'FAILED'}`);
  console.log(`Attendance Connector: ${results.attendance ? 'SUCCESS' : 'FAILED'}`);

  // Overall result
  if (results.user && results.employee && results.attendance) {
    console.log('\nAll connectors registered successfully!');
  } else {
    console.log('\nSome connectors failed to register.');
  }

  console.log('\nYou can now start the consumer to process data from these topics.');
}

// Execute the registration
registerAll().catch(error => {
  console.error('Fatal error during connector registration:', error);
});