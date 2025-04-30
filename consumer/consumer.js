require('dotenv').config();

const { Kafka } = require('kafkajs');
const axios = require('axios');

// Configure Kafka client
const kafka = new Kafka({
  brokers: [process.env.KAFKA_BROKER], // Using the external port mapped in docker-compose
  clientId: 'erp-blockchain-consumer'
});

// Create consumer instance
const consumer = kafka.consumer({ groupId: 'erpnext-blockchain-group' });

// Topic to listen for (will match our configured Debezium connector)
const topic = 'erpnext._5e5899d8398b5f7b.tabUser';

// For storing user data in blockchain (optional, based on your requirements)
const storeUserInBlockchain = async (userData) => {
  try {
    // Extract phone field and convert to integer
    const phoneValue = userData.phone ? parseInt(userData.phone, 10) : 0;

    console.log(`Extracted phone value: ${phoneValue} (type: ${typeof phoneValue})`);

    // Send to blockchain
    const response = await axios.post(process.env.API_ENDPOINT, {
      privateKey: process.env.PRIVATE_KEY,
      contractAddress: process.env.CONTRACT_ADDRESS,
      value: phoneValue
    });

    console.log(`Stored in blockchain, response:`, response.data);
  } catch (error) {
    console.error('Error storing data in blockchain:', error.message);
  }
};

// Main consumer function
async function run() {
  try {
    // Connect to Kafka
    await consumer.connect();
    console.log('Connected to Kafka');

    // Subscribe to the tabUser topic
    await consumer.subscribe({ topic: topic, fromBeginning: true });
    console.log(`Subscribed to topic: ${topic}`);

    // Consume messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          // Parse message value
          const messageValue = message.value.toString();
          const event = JSON.parse(messageValue);

          // Log the received event
          console.log('\n----- CDC Event Received -----');
          console.log(`Topic: ${topic}`);
          console.log(`Partition: ${partition}`);
          console.log(`Offset: ${message.offset}`);
          console.log(`Timestamp: ${new Date(parseInt(message.timestamp)).toISOString()}`);

          // Log the event details
          if (event.op) {
            console.log(`Operation: ${event.op}`); // c=create, u=update, d=delete
          }

          console.log('User data:');
          console.log(JSON.stringify(event.after || event, null, 2));

          // Store in blockchain if needed
          if (event.after) {
            await storeUserInBlockchain(event.after);
          } else if (event) {
            await storeUserInBlockchain(event);
          }

          console.log('----- End of CDC Event -----\n');
        } catch (error) {
          console.error('Error processing message:', error);
          console.error('Message content:', message.value.toString());
        }
      },
    });

    console.log('Consumer started and waiting for messages...');

  } catch (error) {
    console.error('Fatal error in consumer:', error);
    process.exit(1);
  }
}

// Start the consumer
run().catch(console.error);

// Handle termination signals
process.on('SIGINT', async () => {
  console.log('Disconnecting consumer...');
  await consumer.disconnect();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Disconnecting consumer...');
  await consumer.disconnect();
  process.exit(0);
});