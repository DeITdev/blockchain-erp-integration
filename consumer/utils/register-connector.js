const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Try multiple possible locations for the config file
let configFilePath;
const possiblePaths = [
  path.join(__dirname, 'debezium-mariadb-config.json'),
  path.join(__dirname, '..', 'debezium-mariadb-config.json'),
  path.join(__dirname, '..', '..', 'debezium-mariadb-config.json'),
  path.join(__dirname, '..', 'kafka-debezium', 'debezium-mariadb-config.json')
];

for (const testPath of possiblePaths) {
  if (fs.existsSync(testPath)) {
    configFilePath = testPath;
    break;
  }
}

if (!configFilePath) {
  console.error('Error: Could not find debezium-mariadb-config.json in any of these locations:');
  possiblePaths.forEach(p => console.error(`- ${p}`));
  console.error('\nPlease create the configuration file in one of these locations or update the script.');
  process.exit(1);
}

// Read connector configuration
console.log(`Reading configuration from: ${configFilePath}`);
const connectorConfig = JSON.parse(fs.readFileSync(configFilePath, 'utf8'));

async function registerConnector() {
  console.log('Registering Debezium connector...');

  try {
    // Check if connector exists
    const checkResponse = await axios.get('http://localhost:8083/connectors');
    const existingConnectors = checkResponse.data || [];

    if (existingConnectors.includes('erpnext-connector')) {
      console.log('Connector already exists. Deleting it first...');
      await axios.delete('http://localhost:8083/connectors/erpnext-connector');
      console.log('Existing connector deleted.');
    }

    // Register the connector
    const response = await axios.post(
      'http://localhost:8083/connectors',
      connectorConfig
    );

    console.log('Connector registration successful!');
    console.log('Response:', JSON.stringify(response.data, null, 2));
    return true;
  } catch (error) {
    console.error('Error registering connector:');
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
    console.log('Connector registration completed.');
  } else {
    console.log('Connector registration failed.');
  }
});