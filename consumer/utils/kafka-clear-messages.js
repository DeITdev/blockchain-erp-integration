const { Kafka } = require('kafkajs');
require('dotenv').config();

// Configuration
const KAFKA_BROKER = process.env.KAFKA_BROKER || '127.0.0.1:29092';
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'erpnext';
const DB_NAME = process.env.DB_NAME || 'erpnext_db';

// Kafka setup
const kafka = new Kafka({
  clientId: 'kafka-clear-messages-client',
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 1000, retries: 3 },
  connectionTimeout: 10000,
  requestTimeout: 30000
});

async function resetConsumerOffsets() {
  console.log('KAFKA MESSAGE SKIPPER - Reset consumer to skip old messages\n');
  console.log(`Kafka Broker: ${KAFKA_BROKER}`);
  console.log(`Topic Prefix: ${TOPIC_PREFIX}`);
  console.log(`Database: ${DB_NAME}\n`);

  const admin = kafka.admin();

  try {
    // Step 1: Connect to Kafka
    console.log('1. Connecting to Kafka...');
    await admin.connect();
    console.log('   Connected to Kafka\n');

    // Step 2: List all topics
    console.log('2. Discovering topics...');
    const allTopics = await admin.listTopics();

    // Filter for our ERP topics
    const erpTopics = allTopics.filter(topic => {
      return topic.startsWith(TOPIC_PREFIX) &&
        !topic.includes('schema-changes') &&
        !topic.includes('__consumer_offsets') &&
        !topic.includes('_connect-');
    });

    console.log(`   Found ${allTopics.length} total topics`);
    console.log(`   Found ${erpTopics.length} ERP topics:`);
    erpTopics.forEach(topic => console.log(`     - ${topic}`));

    if (erpTopics.length === 0) {
      console.log('   No ERP topics found');
      return;
    }

    // Step 3: Check current message counts
    console.log('\n3. Checking current message counts...');
    const topicMessageCounts = new Map();

    for (const topic of erpTopics) {
      try {
        const topicOffsets = await admin.fetchTopicOffsets(topic);
        let totalMessages = 0;

        topicOffsets.forEach(partition => {
          const messages = parseInt(partition.high) - parseInt(partition.low);
          totalMessages += messages;
        });

        topicMessageCounts.set(topic, totalMessages);
        console.log(`     - ${topic}: ${totalMessages} messages`);
      } catch (error) {
        console.log(`     - ${topic}: Unable to get count`);
        topicMessageCounts.set(topic, 0);
      }
    }

    const totalMessages = Array.from(topicMessageCounts.values()).reduce((a, b) => a + b, 0);
    console.log(`   Total messages in topics: ${totalMessages}`);

    // Step 4: Delete existing consumer group to reset offsets
    console.log('\n4. Resetting consumer group offsets...');
    try {
      const groups = await admin.listGroups();
      const targetGroup = groups.groups.find(group =>
        group.groupId === 'blockchain-consumer-group'
      );

      if (targetGroup) {
        await admin.deleteGroups(['blockchain-consumer-group']);
        console.log('   Deleted existing consumer group');
      } else {
        console.log('   No existing consumer group found');
      }
    } catch (error) {
      console.log('   Consumer group deletion failed (may not exist)');
    }

    // Step 5: Create new consumer with latest offset
    console.log('\n5. Setting up consumer to start from latest...');
    const consumer = kafka.consumer({
      groupId: 'blockchain-consumer-group',
      // This ensures we start from the latest offset
      fromBeginning: false
    });

    await consumer.connect();

    // Subscribe to all ERP topics starting from latest
    await consumer.subscribe({
      topics: erpTopics,
      fromBeginning: false  // Start from latest messages only
    });

    console.log('   Subscribed to topics starting from latest offset');

    // Run consumer very briefly to establish the latest offsets
    let establishedOffsets = false;
    const timeout = setTimeout(() => {
      establishedOffsets = true;
    }, 3000); // 3 second timeout

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        // We don't need to process messages, just establish offsets
        console.log(`   Established offset for ${topic} partition ${partition}`);
        return; // Continue to establish all partition offsets
      },
    });

    // Wait for timeout or offset establishment
    await new Promise(resolve => {
      const checkInterval = setInterval(() => {
        if (establishedOffsets) {
          clearInterval(checkInterval);
          clearTimeout(timeout);
          resolve();
        }
      }, 100);
    });

    await consumer.stop();
    await consumer.disconnect();

    console.log('   Consumer offsets established at latest position');

    // Step 6: Verify the reset
    console.log('\n6. Verifying offset reset...');

    // Check consumer group offsets
    try {
      const groupOffsets = await admin.fetchOffsets({
        groupId: 'blockchain-consumer-group',
        topics: erpTopics.map(topic => ({ topic }))
      });

      console.log('   Current consumer offsets:');
      groupOffsets.forEach(topicOffset => {
        topicOffset.partitions.forEach(partition => {
          console.log(`     - ${topicOffset.topic} partition ${partition.partition}: offset ${partition.offset}`);
        });
      });
    } catch (error) {
      console.log('   Unable to fetch consumer offsets (may be normal)');
    }

    console.log('\nCONSUMER OFFSET RESET COMPLETE!');
    console.log('\nWhat happened:');
    console.log(`   - Consumer group reset to start from latest position`);
    console.log(`   - ${totalMessages} existing messages will be skipped`);
    console.log(`   - Consumer will only process NEW events from now on`);
    console.log(`   - Topics and messages preserved (not deleted)`);

    console.log('\nNext steps:');
    console.log('1. Start your consumer: node consumer-blockchain.js');
    console.log('2. Consumer will only see NEW events from ERPNext');
    console.log('3. Make changes in ERPNext to test');

  } catch (error) {
    console.error('\nOffset reset failed:', error.message);

    if (error.message.includes('Connection timeout')) {
      console.log('\nTroubleshooting:');
      console.log('1. Check if Kafka is running: docker ps');
      console.log('2. Verify Kafka port: netstat -an | findstr 29092');
      console.log('3. Check Kafka logs: docker logs kafka-debezium-kafka-1');
    }
  } finally {
    await admin.disconnect();
    console.log('\nDisconnected from Kafka');
  }
}

// Simple method that just deletes consumer group
async function quickReset() {
  console.log('QUICK CONSUMER RESET - Delete consumer group only\n');

  const admin = kafka.admin();

  try {
    await admin.connect();

    console.log('Deleting consumer group...');
    await admin.deleteGroups(['blockchain-consumer-group']);
    console.log('Consumer group deleted successfully');

    console.log('\nResult: Next consumer start will begin from latest offset');
    console.log('This skips all existing messages in topics');

  } catch (error) {
    if (error.message.includes('group does not exist')) {
      console.log('Consumer group does not exist (already reset)');
    } else {
      console.error('Reset failed:', error.message);
    }
  } finally {
    await admin.disconnect();
  }
}

// Main execution
async function main() {
  const args = process.argv.slice(2);
  const mode = args[0] || 'reset';

  console.log('Starting Kafka Consumer Reset...\n');

  if (mode === 'quick') {
    await quickReset();
  } else {
    await resetConsumerOffsets();
  }

  console.log('\nConsumer reset complete!');
}

// Handle errors
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled rejection:', reason);
  process.exit(1);
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error);
  process.exit(1);
});

// Run the script
main().catch(error => {
  console.error('Script failed:', error.message);
  process.exit(1);
});