const { Kafka } = require('kafkajs');
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

async function checkTopics() {
  const kafka = new Kafka({
    clientId: 'topic-checker',
    brokers: [process.env.KAFKA_BROKER || '127.0.0.1:29092']
  });

  const admin = kafka.admin();

  try {
    await admin.connect();
    console.log('Connected to Kafka\n');

    const topics = await admin.listTopics();
    console.log(`Total topics: ${topics.length}\n`);

    // Filter for ERPNext topics
    const erpTopics = topics.filter(topic =>
      topic.startsWith(process.env.TOPIC_PREFIX || 'erpnext') &&
      !topic.includes('schema-changes')
    );

    console.log('ERPNext CDC Topics:');
    if (erpTopics.length === 0) {
      console.log('  No ERPNext topics found!');
      console.log('  Run: node utils/setup-connector.js auto');
    } else {
      erpTopics.forEach(topic => {
        const parts = topic.split('.');
        const table = parts[parts.length - 1];
        console.log(`  ${topic} -> ${table}`);
      });
    }

    console.log('\nTarget Tables Status:');
    const targetTables = (process.env.TARGET_TABLES || '').split(',').map(t => t.trim());
    targetTables.forEach(table => {
      const found = erpTopics.some(topic => topic.endsWith(`.${table}`));
      console.log(`  ${table}: ${found ? 'FOUND' : 'MISSING'}`);
    });

    await admin.disconnect();

  } catch (error) {
    console.error('Error:', error.message);
  }
}

if (require.main === module) {
  checkTopics();
}

module.exports = { checkTopics };