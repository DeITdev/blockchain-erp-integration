const { Kafka } = require('kafkajs');
const axios = require('axios');

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'blockchain-group' });

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'erpnext.erpnext_db.tabSalesOrder' });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      const numericValue = event.after.total_amount || 0;

      await axios.post('http://localhost:4001/store', {
        privateKey: Process.env.PRIVATE_KEY,
        contractAddress: Process.env.CONTRACT_ADDRESS,
        value: numericValue
      });
      console.log(`Stored value: ${numericValue}`);
    },
  });
}

run().catch(console.error);