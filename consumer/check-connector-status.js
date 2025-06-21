const axios = require('axios');

async function checkConnectorStatus() {
  try {
    console.log('🔍 Checking Debezium Connector Status...');

    // Check if connector exists
    const connectorsResponse = await axios.get('http://localhost:8083/connectors');
    console.log('📋 Available connectors:', connectorsResponse.data);

    // Check for both possible connector names
    const connectorNames = ['erpnext-connector', 'erpnext-cdc-connector'];
    let activeConnector = null;

    for (const name of connectorNames) {
      if (connectorsResponse.data.includes(name)) {
        activeConnector = name;
        break;
      }
    }

    if (!activeConnector) {
      console.log('❌ No ERPNext connector found!');
      console.log('💡 You need to register the Debezium connector first.');
      return;
    }

    console.log(`✅ Found connector: ${activeConnector}`);

    // Get detailed status
    const statusResponse = await axios.get(`http://localhost:8083/connectors/${activeConnector}/status`);
    const status = statusResponse.data;

    console.log('\n📊 Connector Status Details:');
    console.log(`   State: ${status.connector.state}`);
    console.log(`   Worker ID: ${status.connector.worker_id}`);

    // Check tasks
    console.log('\n📋 Task Status:');
    status.tasks.forEach((task, index) => {
      console.log(`   Task ${index}: ${task.state}`);
      if (task.state === 'FAILED') {
        console.log(`   ❌ Error: ${task.trace}`);
      }
    });

    // Get connector configuration
    const configResponse = await axios.get(`http://localhost:8083/connectors/${activeConnector}/config`);
    console.log('\n⚙️ Connector Configuration:');
    console.log(`   Database: ${configResponse.data['database.include.list']}`);
    console.log(`   Tables: ${configResponse.data['table.include.list']}`);
    console.log(`   Topic Prefix: ${configResponse.data['topic.prefix']}`);

    return status;

  } catch (error) {
    console.error('❌ Error checking connector status:');
    if (error.response) {
      console.error(`   HTTP ${error.response.status}: ${error.response.statusText}`);
      console.error(`   Response: ${JSON.stringify(error.response.data, null, 2)}`);
    } else {
      console.error(`   ${error.message}`);
    }
  }
}

// Execute the check
checkConnectorStatus();