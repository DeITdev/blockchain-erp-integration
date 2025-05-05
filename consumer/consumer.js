// Load environment variables if .env file exists, but don't fail if it doesn't
try {
  require('dotenv').config();
} catch (error) {
  console.log('No .env file found, using environment variables');
}

const { Kafka } = require('kafkajs');
const axios = require('axios');

// Get environment variables with fallbacks
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const apiEndpoint = process.env.API_ENDPOINT || 'http://localhost:4001/store';
const privateKey = process.env.PRIVATE_KEY;
const contractAddress = process.env.CONTRACT_ADDRESS;

// Configure Kafka client with retry settings
const kafka = new Kafka({
  brokers: [kafkaBroker],
  clientId: 'erp-blockchain-consumer',
  retry: {
    initialRetryTime: 5000, // 5 seconds
    retries: 15            // More retries
  }
});

// Create consumer instance
const consumer = kafka.consumer({
  groupId: 'erpnext-blockchain-group',
  // Added retry configuration for consumer
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Topic to listen for (will match our configured Debezium connector)
const topic = 'erpnext._5e5899d8398b5f7b.tabUser';

// For storing user data in blockchain (optional, based on your requirements)
const storeUserInBlockchain = async (userData) => {
  try {
    // Check if required environment variables are set
    if (!privateKey || !contractAddress) {
      console.error('Missing PRIVATE_KEY or CONTRACT_ADDRESS environment variables');
      return;
    }

    // Extract phone field and convert to integer
    const phoneValue = userData.phone ? parseInt(userData.phone, 10) : 0;

    console.log(`Extracted phone value: ${phoneValue} (type: ${typeof phoneValue})`);
    console.log(`Sending to API endpoint: ${apiEndpoint}`);

    // Retry logic for blockchain API calls
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      try {
        // Send to blockchain
        const response = await axios.post(apiEndpoint, {
          privateKey: privateKey,
          contractAddress: contractAddress,
          value: phoneValue
        });

        console.log(`Stored in blockchain, response:`, response.data);
        return; // Success, exit the function
      } catch (error) {
        attempts++;
        console.error(`Attempt ${attempts}/${maxAttempts} failed:`, error.message);

        if (attempts >= maxAttempts) {
          throw error; // Re-throw if all attempts failed
        }

        // Wait before retrying (exponential backoff)
        await new Promise(resolve => setTimeout(resolve, 2000 * attempts));
      }
    }
  } catch (error) {
    console.error('Error storing data in blockchain:', error.message);
    // If axios error, log more details
    if (error.response) {
      console.error('API response error:', error.response.status, error.response.data);
    }
  }
};

// Function to check if Kafka topic exists
async function checkTopic() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    console.log('Connected to Kafka admin client');

    const topics = await admin.listTopics();
    console.log('Available topics:', topics);

    const topicExists = topics.includes(topic);
    console.log(`Topic '${topic}' exists: ${topicExists}`);

    if (!topicExists) {
      console.log(`Warning: Topic '${topic}' does not exist yet. Will keep trying.`);
    }

    await admin.disconnect();
    return topicExists;
  } catch (error) {
    console.error('Error checking topic:', error.message);
    return false;
  }
}

// Main consumer function
async function run() {
  let connected = false;
  let retries = 0;
  const maxRetries = 15;

  while (!connected && retries < maxRetries) {
    try {
      console.log(`Starting consumer with broker: ${kafkaBroker} (attempt ${retries + 1}/${maxRetries})`);

      // Connect to Kafka
      await consumer.connect();
      console.log('Connected to Kafka');
      connected = true;

      // Check if topic exists
      const topicExists = await checkTopic();

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
      retries++;
      console.error(`Connection attempt ${retries} failed:`, error.message);

      if (retries >= maxRetries) {
        console.error('Maximum retries reached. Exiting.');
        process.exit(1);
      }

      // Exponential backoff for retries
      const backoffTime = Math.min(10000, 1000 * Math.pow(2, retries));
      console.log(`Waiting ${backoffTime / 1000} seconds before retrying...`);
      await new Promise(resolve => setTimeout(resolve, backoffTime));
    }
  }
}

// Start the consumer with auto-restart
function startWithAutoRestart() {
  run().catch(error => {
    console.error('Fatal error in consumer:', error);
    console.log('Restarting consumer in 10 seconds...');
    setTimeout(startWithAutoRestart, 10000);
  });
}

// Initial start
startWithAutoRestart();

// Handle termination signals
process.on('SIGINT', async () => {
  console.log('Disconnecting consumer...');
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error('Error during disconnect:', e);
  }
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Disconnecting consumer...');
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error('Error during disconnect:', e);
  }
  process.exit(0);
});