const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Try multiple possible locations for the config file
let configFilePath;
const possiblePaths = [
  path.join(__dirname, 'debezium-erpnext-config.json'),
  path.join(__dirname, '..', 'debezium-erpnext-config.json'),
  path.join(__dirname, '..', '..', 'debezium-erpnext-config.json'),
  path.join(__dirname, '..', 'kafka-debezium', 'debezium-erpnext-config.json'),
  path.join(__dirname, '..', 'consumer', 'utils', 'debezium-erpnext-config.json')
];

for (const testPath of possiblePaths) {
  if (fs.existsSync(testPath)) {
    configFilePath = testPath;
    break;
  }
}

if (!configFilePath) {
  console.error('Error: Could not find debezium-erpnext-config.json in any of these locations:');
  possiblePaths.forEach(p => console.error(`- ${p}`));
  console.error('\nPlease create the configuration file in one of these locations or update the script.');
  process.exit(1);
}

// Read connector configuration
console.log(`Reading configuration from: ${configFilePath}`);
const connectorConfig = JSON.parse(fs.readFileSync(configFilePath, 'utf8'));

async function registerConnector() {
  console.log('Registering ERPNext CDC Connector...');
  console.log('Target Database:', connectorConfig.config['database.include.list']);
  console.log('Target Tables:', connectorConfig.config['table.include.list']);

  try {
    // Check Kafka Connect status first
    const healthResponse = await axios.get('http://localhost:8083/');
    console.log('Kafka Connect is running. Version:', healthResponse.data.version);

    // Check if connector exists
    const checkResponse = await axios.get('http://localhost:8083/connectors');
    const existingConnectors = checkResponse.data || [];
    console.log('Existing connectors:', existingConnectors);

    const connectorName = connectorConfig.name;
    if (existingConnectors.includes(connectorName)) {
      console.log(`Connector '${connectorName}' already exists. Deleting it first...`);
      await axios.delete(`http://localhost:8083/connectors/${connectorName}`);
      console.log('Existing connector deleted.');

      // Wait a bit for cleanup
      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    // Register the connector
    console.log(`Registering new connector '${connectorName}'...`);
    const response = await axios.post(
      'http://localhost:8083/connectors',
      connectorConfig,
      {
        headers: {
          'Content-Type': 'application/json'
        }
      }
    );

    console.log('Connector registration successful!');
    console.log('Response:', JSON.stringify(response.data, null, 2));

    // Check connector status after registration
    await new Promise(resolve => setTimeout(resolve, 3000));
    const statusResponse = await axios.get(`http://localhost:8083/connectors/${connectorName}/status`);
    console.log('\nConnector Status:', JSON.stringify(statusResponse.data, null, 2));

    return true;
  } catch (error) {
    console.error('Error registering connector:');
    if (error.response) {
      console.error('Status:', error.response.status);
      console.error('Response:', JSON.stringify(error.response.data, null, 2));

      // Additional troubleshooting info
      if (error.response.status === 409) {
        console.error('\nThis usually means the connector name already exists.');
      } else if (error.response.status === 400) {
        console.error('\nThis usually means there\'s an issue with the connector configuration.');
      }
    } else if (error.code === 'ECONNREFUSED') {
      console.error('Connection refused. Make sure Kafka Connect is running on http://localhost:8083');
    } else {
      console.error(error.message);
    }
    return false;
  }
}

async function listAvailableTopics() {
  try {
    console.log('\nChecking available Kafka topics...');
    // This is a simple way to check if topics are being created
    // In a real setup, you'd use Kafka admin client
    console.log('Expected topics based on configuration:');
    const database = connectorConfig.config['database.include.list'];
    const tables = connectorConfig.config['table.include.list'].split(',');

    tables.forEach(table => {
      const topicName = `${connectorConfig.config['topic.prefix']}.${table.trim()}`;
      console.log(`- ${topicName}`);
    });
  } catch (error) {
    console.error('Error listing topics:', error.message);
  }
}

// Execute the registration
registerConnector().then(success => {
  if (success) {
    console.log('\nConnector registration completed successfully!');
    listAvailableTopics();
    console.log('\nYou can now start the consumer to see CDC events.');
  } else {
    console.log('\nConnector registration failed.');
  }
}).catch(error => {
  console.error('Unexpected error:', error);
});