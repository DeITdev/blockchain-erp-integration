const axios = require('axios');
const { exec } = require('child_process');
const path = require('path');

async function execCommand(command, cwd) {
  return new Promise((resolve, reject) => {
    exec(command, { cwd }, (error, stdout, stderr) => {
      if (error) {
        console.log(`   Error: ${error.message}`);
        resolve({ error, stdout, stderr });
      } else {
        console.log(`   ${stdout.trim()}`);
        resolve({ stdout, stderr });
      }
    });
  });
}

async function kafkaDebeziumReset() {
  console.log('🔄 KAFKA-DEBEZIUM RESET - Targeted cleanup...\n');

  const kafkaDebeziumPath = 'C:\\blockchain-erp-integration\\kafka-debezium';

  try {
    // Step 1: Delete ALL connectors
    console.log('1️⃣ Deleting all connectors...');
    try {
      const connectorsResponse = await axios.get('http://localhost:8083/connectors');
      const connectors = connectorsResponse.data;

      for (const connector of connectors) {
        console.log(`   Deleting: ${connector}`);
        await axios.delete(`http://localhost:8083/connectors/${connector}`);
      }
    } catch (error) {
      console.log('   No connectors to delete or Connect not available');
    }

    // Step 2: Wait for cleanup
    console.log('\n2️⃣ Waiting 10 seconds for connector cleanup...');
    await new Promise(resolve => setTimeout(resolve, 10000));

    // Step 3: Stop Kafka-Debezium services only
    console.log('\n3️⃣ Stopping Kafka-Debezium containers...');
    await execCommand('docker-compose down', kafkaDebeziumPath);

    // Step 4: Remove volumes to reset Kafka topics and Connect state
    console.log('\n4️⃣ Removing Kafka-Debezium volumes...');
    await execCommand('docker-compose down -v', kafkaDebeziumPath);

    // Step 5: Start fresh Kafka-Debezium services
    console.log('\n5️⃣ Starting fresh Kafka-Debezium containers...');
    await execCommand('docker-compose up -d', kafkaDebeziumPath);

    console.log('\n6️⃣ Waiting 45 seconds for Kafka and Connect to start...');
    await new Promise(resolve => setTimeout(resolve, 45000));

    // Step 7: Verify services are running
    console.log('\n7️⃣ Checking service status...');

    // Check Kafka Connect
    try {
      const response = await axios.get('http://localhost:8083/connectors');
      console.log('   ✅ Kafka Connect is running');
    } catch (error) {
      console.log('   ⚠️ Kafka Connect not ready yet, may need more time');
    }

    console.log('\n✅ KAFKA-DEBEZIUM RESET COMPLETE!');
    console.log('\n📋 Next steps:');
    console.log('1. Wait 1-2 more minutes for full initialization');
    console.log('2. Run: node setup-connector.js auto');
    console.log('3. Test with: node ../consumer-display.js');

  } catch (error) {
    console.error('\n💥 Reset failed:', error.message);
  }
}

kafkaDebeziumReset();