const { Kafka } = require('kafkajs');
const axios = require('axios');
require('dotenv').config();

const kafka = new Kafka({
  clientId: 'blockchain-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:29092'],
  connectionTimeout: 10000, // 10 seconds
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

const consumer = kafka.consumer({
  groupId: 'blockchain-group'
});

async function run() {
  try {
    console.log('Connecting to Kafka broker at:', process.env.KAFKA_BROKER || 'localhost:29092');
    await consumer.connect();

    console.log('Subscribing to topic: erpnext');
    await consumer.subscribe({
      topic: 'erpnext.erpnext_db.tabUser',
      fromBeginning: true
    });

    console.log('Starting consumer...');
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          console.log(`Received message from topic: ${topic}`);
          console.log(`Message key: ${message.key?.toString()}`);
          console.log(`Message value: ${message.value?.toString().substring(0, 200)}...`);

          const event = JSON.parse(message.value.toString());

          // Extract a numeric value from the event
          const numericValue = event.after && event.after.modified ?
            new Date(event.after.modified).getTime() : Date.now();

          console.log(`Extracted value: ${numericValue}`);

          // Send to blockchain API
          const response = await axios.post(process.env.API_ENDPOINT, {
            privateKey: process.env.PRIVATE_KEY,
            contractAddress: process.env.CONTRACT_ADDRESS,
            value: Math.floor(numericValue)
          });

          console.log('Blockchain response:', response.data);
          console.log(`Successfully stored value: ${numericValue} on blockchain`);
        } catch (err) {
          console.error('Error processing message:', err.message);
          console.error(err.stack);
        }
      },
    });
  } catch (err) {
    console.error('Consumer error:', err.message);
    console.error(err.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach(type => {
  process.on(type, async e => {
    console.error(`${type}: ${e.message}`);
    try {
      await consumer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});

console.log('Starting Kafka consumer...');
run().catch(console.error);