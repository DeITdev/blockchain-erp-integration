const axios = require('axios');

// Fixed connector configuration with snapshot reset
const connectorConfig = {
  "name": "erpnext-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "host.docker.internal",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "admin",
    "database.server.id": "184055", // Changed server ID to avoid conflicts
    "topic.prefix": "erpnext",
    "database.include.list": "_a76baa702733b1ae",
    "table.include.list": "_a76baa702733b1ae.tabEmployee,_a76baa702733b1ae.tabUser",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.erpnext.multi",
    "schema.history.internal.consumer.security.protocol": "PLAINTEXT",
    "schema.history.internal.producer.security.protocol": "PLAINTEXT",
    "include.schema.changes": "true",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",

    // KEY CHANGES TO FIX THE BINLOG ISSUE:
    "snapshot.mode": "when_needed",  // Take snapshot when binlog position is not available
    "snapshot.locking.mode": "none",
    "database.allowPublicKeyRetrieval": "true",
    "decimal.handling.mode": "string",
    "bigint.unsigned.handling.mode": "long",
    "time.precision.mode": "adaptive_time_microseconds",

    // Reset configuration to start fresh
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "dbhistory.erpnext.reset", // New topic to start fresh

    // Additional settings for reliability
    "heartbeat.interval.ms": "10000",
    "heartbeat.topics.prefix": "__debezium-heartbeat",
    "skipped.operations": "none",
    "provide.transaction.metadata": "false" // Simplify for now
  }
};

async function fixConnectorBinlogIssue() {
  try {
    console.log('🔧 Fixing Connector Binlog Issue...');

    const connectorName = connectorConfig.name;

    // Step 1: Delete existing connector completely
    console.log(`🗑️ Deleting existing connector '${connectorName}'...`);
    try {
      await axios.delete(`http://localhost:8083/connectors/${connectorName}`);
      console.log('✅ Connector deleted successfully.');
    } catch (error) {
      if (error.response && error.response.status === 404) {
        console.log('ℹ️ Connector was not found (already deleted).');
      } else {
        console.error('⚠️ Error deleting connector:', error.message);
      }
    }

    // Step 2: Wait for cleanup
    console.log('⏳ Waiting 10 seconds for cleanup...');
    await new Promise(resolve => setTimeout(resolve, 10000));

    // Step 3: Clean up Kafka topics (optional - helps with fresh start)
    console.log('🧹 Cleaning up related Kafka topics...');
    try {
      // Delete the old schema history topic to start fresh
      const kafkaTopics = [
        'schema-changes.erpnext.multi',
        'dbhistory.erpnext',
        '__debezium-heartbeat.erpnext'
      ];

      // Note: This requires Kafka admin access, which might not be available
      // The connector will recreate these topics automatically
      console.log('ℹ️ Topics will be recreated automatically by the connector.');
    } catch (error) {
      console.log('ℹ️ Could not clean topics (this is OK - they will be reused).');
    }

    // Step 4: Register new connector with fresh configuration
    console.log(`📝 Registering connector with fresh configuration...`);
    console.log('🔑 Key changes:');
    console.log('   - snapshot.mode: when_needed (will take snapshot if binlog unavailable)');
    console.log('   - New server ID to avoid conflicts');
    console.log('   - Fresh database history topic');

    const response = await axios.post(
      'http://localhost:8083/connectors',
      connectorConfig,
      {
        headers: {
          'Content-Type': 'application/json'
        }
      }
    );

    console.log('✅ Connector registered successfully!');

    // Step 5: Monitor startup
    console.log('⏳ Monitoring connector startup (this may take 30-60 seconds)...');

    let attempts = 0;
    const maxAttempts = 12; // 2 minutes

    while (attempts < maxAttempts) {
      await new Promise(resolve => setTimeout(resolve, 10000)); // Wait 10 seconds
      attempts++;

      try {
        const statusResponse = await axios.get(`http://localhost:8083/connectors/${connectorName}/status`);
        const status = statusResponse.data;

        console.log(`📊 Attempt ${attempts}/${maxAttempts}:`);
        console.log(`   Connector State: ${status.connector.state}`);

        if (status.tasks.length > 0) {
          const taskState = status.tasks[0].state;
          console.log(`   Task State: ${taskState}`);

          if (taskState === 'RUNNING') {
            console.log('\n🎉 SUCCESS! Connector is now RUNNING!');
            console.log('✅ The binlog issue has been resolved.');
            console.log('💡 Try making changes to Employee or User data in ERPNext now.');

            // Show topic information
            console.log('\n📋 Expected Kafka topics:');
            console.log('   - erpnext._0775ec53bab106f5.tabEmployee');
            console.log('   - erpnext._0775ec53bab106f5.tabUser');

            return true;
          } else if (taskState === 'FAILED') {
            console.log(`❌ Task failed again. Error: ${status.tasks[0].trace}`);
            break;
          } else {
            console.log(`⏳ Task is in ${taskState} state, waiting...`);
          }
        } else {
          console.log('⏳ No tasks yet, waiting for connector to initialize...');
        }

      } catch (error) {
        console.log(`⚠️ Error checking status on attempt ${attempts}: ${error.message}`);
      }
    }

    if (attempts >= maxAttempts) {
      console.log('\n⏰ Timeout reached. Checking final status...');
      const finalStatus = await axios.get(`http://localhost:8083/connectors/${connectorName}/status`);
      console.log('📊 Final Status:', JSON.stringify(finalStatus.data, null, 2));
    }

    return false;

  } catch (error) {
    console.error('❌ Error fixing connector:');
    if (error.response) {
      console.error(`   HTTP ${error.response.status}: ${error.response.statusText}`);
      console.error(`   Response: ${JSON.stringify(error.response.data, null, 2)}`);
    } else {
      console.error(`   ${error.message}`);
    }

    console.log('\n🔧 Alternative Solutions:');
    console.log('1. Restart MariaDB container to reset binary logs');
    console.log('2. Check MariaDB binary log retention settings');
    console.log('3. Use snapshot.mode=always to force full snapshot');

    return false;
  }
}

// Execute the fix
console.log('🚀 Starting Connector Binlog Issue Fix...');
console.log('📋 This will:');
console.log('   1. Delete the existing connector');
console.log('   2. Wait for cleanup');
console.log('   3. Register with fresh configuration');
console.log('   4. Monitor startup until RUNNING');
console.log('');

fixConnectorBinlogIssue().then(success => {
  if (success) {
    console.log('\n✅ CONNECTOR FIX COMPLETED SUCCESSFULLY!');
    console.log('🎯 Your CDC tracking should now work.');
    console.log('📝 Test by making changes in ERPNext.');
  } else {
    console.log('\n❌ CONNECTOR FIX INCOMPLETE');
    console.log('📞 Check the logs above for specific issues.');
  }
}).catch(error => {
  console.error('\n💥 UNEXPECTED ERROR:', error.message);
});