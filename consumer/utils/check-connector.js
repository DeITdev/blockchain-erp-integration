const axios = require('axios');

async function checkConnectorStatus() {
  try {
    // Get list of connectors
    const connectorsResponse = await axios.get('http://localhost:8083/connectors');
    console.log('Available connectors:', connectorsResponse.data);

    // If our connector exists, check its status
    if (connectorsResponse.data.includes('erpnext-connector')) {
      const statusResponse = await axios.get('http://localhost:8083/connectors/erpnext-connector/status');
      console.log('\nConnector status:');
      console.log(JSON.stringify(statusResponse.data, null, 2));

      // Check tasks status
      const tasksStatus = statusResponse.data?.tasks || [];

      let hasErrors = false;
      tasksStatus.forEach(task => {
        if (task.state === 'FAILED') {
          hasErrors = true;
          console.error(`\nTask ${task.id} failed with trace:`);
          console.error(task.trace || 'No trace available');
        }
      });

      if (hasErrors) {
        console.log('\nThere are errors in the connector. Check the logs above.');
      } else {
        console.log('\nConnector is running without errors.');
      }
    } else {
      console.log('Connector "erpnext-connector" is not registered.');
    }
  } catch (error) {
    console.error('Error checking connector status:');
    if (error.response) {
      console.error('Status:', error.response.status);
      console.error('Response:', JSON.stringify(error.response.data, null, 2));
    } else {
      console.error(error.message);
    }
  }
}

// Execute the check
checkConnectorStatus();