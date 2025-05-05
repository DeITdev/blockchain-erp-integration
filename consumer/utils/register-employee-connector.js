const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Path to the employee connector config
const configFilePath = path.join(__dirname, 'debezium-employee-config.json');

// Read connector configuration
console.log(`Reading configuration from: ${configFilePath}`);
try {
  const connectorConfig = JSON.parse(fs.readFileSync(configFilePath, 'utf8'));

  async function registerConnector() {
    console.log('Registering Employee Debezium connector...');

    try {
      // Check if connector exists
      const checkResponse = await axios.get('http://localhost:8083/connectors');
      const existingConnectors = checkResponse.data || [];

      if (existingConnectors.includes('employee-connector')) {
        console.log('Employee Connector already exists. Deleting it first...');
        await axios.delete('http://localhost:8083/connectors/employee-connector');
        console.log('Existing employee connector deleted.');
      }

      // Register the connector
      const response = await axios.post(
        'http://localhost:8083/connectors',
        connectorConfig
      );

      console.log('Employee Connector registration successful!');
      console.log('Response:', JSON.stringify(response.data, null, 2));
      return true;
    } catch (error) {
      console.error('Error registering employee connector:');
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
      console.log('Employee Connector registration completed.');
    } else {
      console.log('Employee Connector registration failed.');
    }
  });
} catch (error) {
  console.error(`Error reading config file: ${error.message}`);
}