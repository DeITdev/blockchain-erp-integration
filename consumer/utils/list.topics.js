const { Kafka } = require('kafkajs');

async function listTopics() {
  const kafka = new Kafka({
    clientId: 'topic-lister',
    brokers: ['localhost:29092']
  });

  const admin = kafka.admin();

  try {
    await admin.connect();
    console.log('Connected to Kafka admin');

    const topics = await admin.listTopics();
    console.log('Available Kafka Topics:');
    topics.forEach(topic => console.log(`- ${topic}`));

    // Get detailed information for each topic
    if (topics.length > 0) {
      console.log('\nTopic Details:');
      const metadata = await admin.fetchTopicMetadata({ topics });

      metadata.topics.forEach(topic => {
        console.log(`\nTopic: ${topic.name}`);
        console.log(`Partitions: ${topic.partitions.length}`);

        topic.partitions.forEach(partition => {
          console.log(`  Partition ${partition.partitionId}:`);
          console.log(`    Leader: ${partition.leader}`);
          console.log(`    Replicas: ${partition.replicas.join(', ')}`);
          console.log(`    ISR: ${partition.isr.join(', ')}`);
        });
      });
    }
  } catch (error) {
    console.error('Error listing topics:', error);
  } finally {
    await admin.disconnect();
    console.log('Disconnected from Kafka admin');
  }
}

listTopics().catch(console.error);