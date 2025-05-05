const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Path to the attendance connector config
const configFilePath = path.join(__dirname, 'debezium-attendance-config.json');

// Check if the config file exists
if (!fs.existsSync(configFilePath)) {
  console.error(`Error: Configuration file not found at ${configFilePath}`);
  process.exit(1);
}

// Read connector configuration
console.log(`Reading configuration from: ${configFilePath}`);
try {
  const connectorConfig = JSON.parse(fs.readFileSync(configFilePath, 'utf8'));

  async function registerConnector() {
    console.log('Registering Attendance Debezium connector...');

    try {
      // Check if connector exists
      const checkResponse = await axios.get('http://localhost:8083/connectors');
      const existingConnectors = checkResponse.data || [];

      if (existingConnectors.includes('attendance-connector')) {
        console.log('Attendance Connector already exists. Deleting it first...');
        await axios.delete('http://localhost:8083/connectors/attendance-connector');
        console.log('Existing attendance connector deleted.');
      }

      // Register the connector
      const response = await axios.post(
        'http://localhost:8083/connectors',
        connectorConfig
      );

      console.log('Attendance Connector registration successful!');
      console.log('Response:', JSON.stringify(response.data, null, 2));
      return true;
    } catch (error) {
      console.error('Error registering attendance connector:');
      if (error.response) {
        console.error('Status:', error.response.status);
        console.error('Response:', JSON.stringify(error.response.data, null, 2));
      } else {
        console.error(error.message);
      }
      return false;
    }
  }

  // Execute the registration
  registerConnector().then(success => {
    if (success) {
      console.log('Attendance Connector registration completed.');
    } else {
      console.log('Attendance Connector registration failed.');
    }
  });
} catch (error) {
  console.error(`Error reading config file: ${error.message}`);
}