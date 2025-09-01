// latency-monitor.js - Real-time Latency Monitor for ERPNext CDC Events
require('dotenv').config();

const { Kafka } = require('kafkajs');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');

// Import data filter
const { optimizeForBlockchain } = require('./data-filter');

// Configuration from environment
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:29092';
const API_ENDPOINT = process.env.API_ENDPOINT || 'http://localhost:4001';
const PRIVATE_KEY = process.env.PRIVATE_KEY || '8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63';

// Contract addresses from environment variables (NO FALLBACKS - must be in .env)
const CONTRACT_ADDRESSES = {
  'tabUser': process.env.USER_CONTRACT_ADDRESS,
  'tabEmployee': process.env.EMPLOYEE_CONTRACT_ADDRESS,
  'tabTask': process.env.TASK_CONTRACT_ADDRESS,
  'tabCompany': process.env.COMPANY_CONTRACT_ADDRESS,
  'tabAttendance': process.env.ATTENDANCE_CONTRACT_ADDRESS
};

// Validate that all contract addresses are loaded
function validateContractAddresses() {
  const missingAddresses = [];
  Object.entries(CONTRACT_ADDRESSES).forEach(([tableName, address]) => {
    if (!address) {
      missingAddresses.push(tableName);
    }
  });

  if (missingAddresses.length > 0) {
    console.error(`❌ Missing contract addresses in .env file for: ${missingAddresses.join(', ')}`);
    console.error('Please add the following to your .env file:');
    missingAddresses.forEach(tableName => {
      const envVar = tableName.replace('tab', '').toUpperCase() + '_CONTRACT_ADDRESS';
      console.error(`${envVar}=0x...`);
    });
    process.exit(1);
  }

  console.log('✅ All contract addresses loaded from environment variables');
}

// Supported modules (5 smart contracts)
const SUPPORTED_MODULES = ['tabUser', 'tabEmployee', 'tabTask', 'tabCompany', 'tabAttendance'];

// Latency tracking data
let latencyMetrics = {
  tabUser: [],
  tabEmployee: [],
  tabTask: [],
  tabCompany: [],
  tabAttendance: []
};

let totalEvents = 0;
let startTime = Date.now();

// Configure Kafka client
const kafka = new Kafka({
  brokers: [KAFKA_BROKER],
  clientId: 'erpnext-latency-monitor',
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Create consumer instance
const consumer = kafka.consumer({
  groupId: 'erpnext-latency-monitor-group',
  sessionTimeout: 60000,
  heartbeatInterval: 10000,
  retry: {
    initialRetryTime: 5000,
    retries: 15
  }
});

// Function to check if Kafka topics exist
async function checkTopics() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    const topics = await admin.listTopics();

    // Find ERPNext topics dynamically
    const erpnextTopics = topics.filter(topic =>
      topic.startsWith('erpnext.') &&
      topic.includes('.tab') &&
      !topic.includes('schema-changes')
    );

    // Group topics by database hash
    const topicsByDatabase = {};
    erpnextTopics.forEach(topic => {
      const parts = topic.split('.');
      if (parts.length >= 3) {
        const database = parts[1];
        const tableName = parts[2];

        if (!topicsByDatabase[database]) {
          topicsByDatabase[database] = [];
        }
        topicsByDatabase[database].push({ topic, tableName });
      }
    });

    // Find matching topics for supported modules
    const foundTopics = {};
    const missingModules = [];

    SUPPORTED_MODULES.forEach(moduleName => {
      let found = false;
      Object.entries(topicsByDatabase).forEach(([database, topics]) => {
        const match = topics.find(t => t.tableName === moduleName);
        if (match && !found) {
          foundTopics[moduleName] = match.topic;
          found = true;
        }
      });
      if (!found) {
        missingModules.push(moduleName);
      }
    });

    console.log(`\n📊 Module Discovery Results:`);
    SUPPORTED_MODULES.forEach(moduleName => {
      const exists = foundTopics[moduleName];
      const contractAddr = CONTRACT_ADDRESSES[moduleName];
      console.log(`  ${moduleName}: ${exists ? '✅ FOUND' : '❌ NOT FOUND'}`);
      if (exists) {
        console.log(`    Topic: ${exists}`);
        console.log(`    Contract: ${contractAddr}`);
      }
    });

    await admin.disconnect();
    return { foundTopics, missingModules, totalFound: Object.keys(foundTopics).length };
  } catch (error) {
    console.error('❌ Error checking topics:', error.message);
    return { foundTopics: {}, missingModules: SUPPORTED_MODULES, totalFound: 0 };
  }
}

// Detect operation type from ERPNext CDC events
function detectOperationType(event) {
  if (event.op) {
    return event.op;
  }

  const isDeleted = event.__deleted === true || event.__deleted === "true";
  if (isDeleted) {
    return 'd'; // DELETE
  }

  if (event.creation && !event.modified) {
    return 'r'; // READ (snapshot)
  }

  if (event.creation && event.modified) {
    const creationTime = typeof event.creation === 'number' ? event.creation : Date.parse(event.creation);
    const modifiedTime = typeof event.modified === 'number' ? event.modified : Date.parse(event.modified);

    if (Math.abs(modifiedTime - creationTime) > 1000000) {
      return 'u'; // UPDATE
    }
  }

  return 'c'; // CREATE (default assumption)
}

// Format CDC event data
function formatCDCEvent(topic, message, event) {
  const timestamp = new Date(parseInt(message.timestamp)).toISOString();
  const operation = detectOperationType(event);
  const tableName = topic.split('.').pop();

  const operationMap = {
    'c': 'CREATE',
    'u': 'UPDATE',
    'd': 'DELETE',
    'r': 'READ (Snapshot)'
  };

  return {
    timestamp,
    topic,
    offset: message.offset,
    partition: message.partition,
    operation: operationMap[operation] || operation,
    tableName,
    database: topic.split('.')[1],
    data: event,
    recordId: event.name || event.id || event.email || event.employee_number || `${tableName}_${Date.now()}`
  };
}

// Get API endpoint for table
function getAPIEndpoint(tableName) {
  const endpointMap = {
    'tabUser': '/users',
    'tabEmployee': '/employees',
    'tabTask': '/tasks',
    'tabCompany': '/companies',
    'tabAttendance': '/attendances'
  };

  return endpointMap[tableName] || null;
}

// Test blockchain API call (simulate consumer → blockchain latency)
async function testBlockchainAPI(formattedEvent) {
  const startTime = Date.now();

  try {
    const endpoint = getAPIEndpoint(formattedEvent.tableName);
    const contractAddress = CONTRACT_ADDRESSES[formattedEvent.tableName];

    if (!endpoint || !contractAddress) {
      return null;
    }

    // Optimize data before sending to blockchain
    const optimization = optimizeForBlockchain(formattedEvent.tableName, formattedEvent.data);

    const url = `${API_ENDPOINT}${endpoint}`;
    const payload = {
      privateKey: PRIVATE_KEY,
      [`${formattedEvent.tableName.replace('tab', '').toLowerCase()}Data`]: {
        recordId: formattedEvent.recordId,
        createdTimestamp: formattedEvent.data.creation || new Date().toISOString(),
        modifiedTimestamp: formattedEvent.data.modified || new Date().toISOString(),
        modifiedBy: formattedEvent.data.modified_by || 'system',
        allData: optimization.filteredData // Use filtered data
      }
    };

    const response = await axios.post(url, payload, {
      timeout: 30000,
      headers: { 'Content-Type': 'application/json' }
    });

    const endTime = Date.now();
    const latency = endTime - startTime;

    return {
      success: true,
      latency: latency,
      transactionHash: response.data.blockchain?.transactionHash,
      blockNumber: response.data.blockchain?.blockNumber,
      contractAddress: contractAddress,
      optimization: optimization.stats
    };

  } catch (error) {
    const endTime = Date.now();
    const latency = endTime - startTime;

    return {
      success: false,
      latency: latency,
      error: error.response?.data || error.message,
      contractAddress: CONTRACT_ADDRESSES[formattedEvent.tableName]
    };
  }
}

// Process CDC message and measure latency
const processCDCMessage = async (topic, message) => {
  const kafkaReceiveTime = Date.now();
  const kafkaTimestamp = parseInt(message.timestamp);
  const kafkaLatency = kafkaReceiveTime - kafkaTimestamp;

  try {
    // Parse message value
    const messageValue = message.value.toString();
    let event;

    try {
      event = JSON.parse(messageValue);
    } catch (parseError) {
      console.error('❌ Failed to parse JSON message:', parseError.message);
      return;
    }

    // Format the event
    const formattedEvent = formatCDCEvent(topic, message, event);

    // Skip if not a supported module
    if (!SUPPORTED_MODULES.includes(formattedEvent.tableName)) {
      return;
    }

    totalEvents++;

    // Display event notification
    console.log(`\n🔔 Event #${totalEvents}: ${formattedEvent.tableName} - ${formattedEvent.operation}`);
    console.log(`   Record ID: ${formattedEvent.recordId}`);
    console.log(`   Kafka Latency: ${kafkaLatency}ms`);

    // Test blockchain API call
    const blockchainResult = await testBlockchainAPI(formattedEvent);

    if (blockchainResult) {
      const totalLatency = kafkaLatency + blockchainResult.latency;

      console.log(`   Blockchain Latency: ${blockchainResult.latency}ms`);
      console.log(`   Total End-to-End: ${totalLatency}ms`);
      console.log(`   Success: ${blockchainResult.success ? '✅' : '❌'}`);
      console.log(`   Data Reduction: ${blockchainResult.optimization?.reductionPercent || 0}%`);

      if (blockchainResult.success) {
        console.log(`   Transaction: ${blockchainResult.transactionHash}`);
        console.log(`   Contract: ${blockchainResult.contractAddress}`);
      }

      // Store latency metrics
      const latencyRecord = {
        timestamp: new Date().toISOString(),
        recordId: formattedEvent.recordId,
        operation: formattedEvent.operation,
        kafkaLatency: kafkaLatency,
        blockchainLatency: blockchainResult.latency,
        totalLatency: totalLatency,
        success: blockchainResult.success,
        transactionHash: blockchainResult.transactionHash,
        contractAddress: blockchainResult.contractAddress,
        kafkaTimestamp: kafkaTimestamp,
        processedTimestamp: kafkaReceiveTime
      };

      latencyMetrics[formattedEvent.tableName].push(latencyRecord);
    } else {
      console.log(`   ⚠️  Module not supported for blockchain integration`);
    }

  } catch (error) {
    console.error('❌ Error processing CDC message:', error.message);
  }
};

// Generate latency summary report
async function generateLatencySummary() {
  const endTime = Date.now();
  const totalRunTime = endTime - startTime;

  console.log(`\n${'='.repeat(100)}`);
  console.log('📊 LATENCY MONITORING SUMMARY REPORT');
  console.log(`${'='.repeat(100)}`);

  console.log(`\n📈 MONITORING SESSION OVERVIEW:`);
  console.log(`  Start Time:        ${new Date(startTime).toISOString()}`);
  console.log(`  End Time:          ${new Date(endTime).toISOString()}`);
  console.log(`  Total Duration:    ${Math.round(totalRunTime / 1000)}s (${Math.round(totalRunTime / 1000 / 60)}m)`);
  console.log(`  Total Events:      ${totalEvents}`);
  console.log(`  Events per Minute: ${Math.round(totalEvents / (totalRunTime / 1000 / 60))}`);

  // Calculate per-module statistics
  const moduleStats = {};
  let grandTotalEvents = 0;
  let grandTotalSuccessful = 0;

  SUPPORTED_MODULES.forEach(moduleName => {
    const metrics = latencyMetrics[moduleName];
    if (metrics.length > 0) {
      const successful = metrics.filter(m => m.success);
      const kafkaLatencies = metrics.map(m => m.kafkaLatency);
      const blockchainLatencies = metrics.map(m => m.blockchainLatency);
      const totalLatencies = metrics.map(m => m.totalLatency);

      moduleStats[moduleName] = {
        totalEvents: metrics.length,
        successfulEvents: successful.length,
        successRate: Math.round((successful.length / metrics.length) * 100),
        kafkaLatency: {
          avg: Math.round(kafkaLatencies.reduce((a, b) => a + b, 0) / kafkaLatencies.length),
          min: Math.min(...kafkaLatencies),
          max: Math.max(...kafkaLatencies)
        },
        blockchainLatency: {
          avg: Math.round(blockchainLatencies.reduce((a, b) => a + b, 0) / blockchainLatencies.length),
          min: Math.min(...blockchainLatencies),
          max: Math.max(...blockchainLatencies)
        },
        totalLatency: {
          avg: Math.round(totalLatencies.reduce((a, b) => a + b, 0) / totalLatencies.length),
          min: Math.min(...totalLatencies),
          max: Math.max(...totalLatencies)
        }
      };

      grandTotalEvents += metrics.length;
      grandTotalSuccessful += successful.length;
    }
  });

  console.log(`\n⏱️  LATENCY BREAKDOWN BY MODULE:`);
  console.log(`  Module        Events  Success  Kafka(ms)       Blockchain(ms)  Total(ms)`);
  console.log(`  ────────────  ──────  ───────  ──────────────  ──────────────  ──────────────`);

  SUPPORTED_MODULES.forEach(moduleName => {
    const stats = moduleStats[moduleName];
    if (stats) {
      const contractAddr = CONTRACT_ADDRESSES[moduleName].substring(0, 8) + '...';
      console.log(`  ${moduleName.padEnd(12)} ${stats.totalEvents.toString().padStart(6)}  ${stats.successRate.toString().padStart(4)}%   ${stats.kafkaLatency.avg.toString().padStart(4)} (${stats.kafkaLatency.min}-${stats.kafkaLatency.max})   ${stats.blockchainLatency.avg.toString().padStart(4)} (${stats.blockchainLatency.min}-${stats.blockchainLatency.max})   ${stats.totalLatency.avg.toString().padStart(4)} (${stats.totalLatency.min}-${stats.totalLatency.max})`);
      console.log(`                                                                      ${contractAddr}`);
    } else {
      console.log(`  ${moduleName.padEnd(12)}      0       0%   No events recorded`);
    }
  });

  console.log(`  ────────────  ──────  ───────  ──────────────  ──────────────  ──────────────`);
  console.log(`  TOTAL         ${grandTotalEvents.toString().padStart(6)}  ${Math.round((grandTotalSuccessful / grandTotalEvents) * 100).toString().padStart(4)}%`);

  // Performance recommendations
  console.log(`\n💡 PERFORMANCE ANALYSIS:`);
  const recommendations = [];

  if (grandTotalEvents === 0) {
    recommendations.push('No events were detected. Ensure ERPNext is making changes to monitored tables.');
  } else {
    const avgKafkaLatency = Object.values(moduleStats).reduce((sum, stats) => sum + (stats?.kafkaLatency?.avg || 0), 0) / Object.values(moduleStats).filter(s => s).length;
    const avgBlockchainLatency = Object.values(moduleStats).reduce((sum, stats) => sum + (stats?.blockchainLatency?.avg || 0), 0) / Object.values(moduleStats).filter(s => s).length;
    const overallSuccessRate = Math.round((grandTotalSuccessful / grandTotalEvents) * 100);

    if (avgKafkaLatency > 3000) {
      recommendations.push('High Kafka CDC latency detected. Check Debezium and Kafka configuration.');
    }

    if (avgBlockchainLatency > 5000) {
      recommendations.push('High blockchain API latency. Consider optimizing smart contracts or network.');
    }

    if (overallSuccessRate < 95) {
      recommendations.push('Low success rate detected. Check blockchain API error logs.');
    }

    if (recommendations.length === 0) {
      recommendations.push('All metrics are within acceptable ranges. System performance is good.');
    }

    recommendations.push(`Average end-to-end latency: ${Math.round(avgKafkaLatency + avgBlockchainLatency)}ms`);
    recommendations.push(`Overall success rate: ${overallSuccessRate}%`);
  }

  recommendations.forEach(rec => {
    console.log(`  • ${rec}`);
  });

  // Save detailed report
  const reportData = {
    sessionInfo: {
      startTime: new Date(startTime).toISOString(),
      endTime: new Date(endTime).toISOString(),
      duration: totalRunTime,
      totalEvents: totalEvents
    },
    moduleStats: moduleStats,
    contractAddresses: CONTRACT_ADDRESSES,
    rawData: latencyMetrics,
    recommendations: recommendations
  };

  const reportFile = path.join(__dirname, `latency-monitor-report-${Date.now()}.json`);
  await fs.writeFile(reportFile, JSON.stringify(reportData, null, 2));
  console.log(`\n💾 Detailed report saved to: ${reportFile}`);

  console.log(`\n${'='.repeat(100)}`);
}

// Main monitoring function
async function startLatencyMonitoring() {
  // Validate contract addresses first
  validateContractAddresses();

  console.log(`
🔍 ERPNext Real-time Latency Monitor
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📡 Kafka Broker: ${KAFKA_BROKER}
🔗 Blockchain API: ${API_ENDPOINT}
📋 Monitoring Modules: ${SUPPORTED_MODULES.join(', ')}
⏱️  Measuring: DB→Kafka, Consumer→Blockchain latencies
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 Instructions:
1. Make changes to any of the 5 modules in ERPNext
2. Watch real-time latency measurements below
3. Press Ctrl+C to stop monitoring and get summary report
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);

  try {
    // Connect to Kafka
    await consumer.connect();
    console.log('✅ Connected to Kafka successfully');

    // Check if topics exist
    const { foundTopics, missingModules, totalFound } = await checkTopics();

    if (totalFound === 0) {
      console.log('❌ No supported module topics found. Make sure Debezium connector is registered.');
      return;
    }

    console.log(`\n✅ Found ${totalFound} out of ${SUPPORTED_MODULES.length} supported modules`);
    if (missingModules.length > 0) {
      console.log(`⚠️  Missing modules: ${missingModules.join(', ')}`);
    }

    // Subscribe to available topics
    const topicsToSubscribe = Object.values(foundTopics);
    for (const topic of topicsToSubscribe) {
      await consumer.subscribe({ topic, fromBeginning: false });
      console.log(`📌 Subscribed to: ${topic}`);
    }

    // Start consuming messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await processCDCMessage(topic, message);
      },
    });

    console.log('\n🎧 Latency monitor is now active...');
    console.log('💡 Make changes to User, Employee, Task, Company, or Attendance in ERPNext');
    console.log('📊 Real-time latency metrics will appear below:');
    console.log('🛑 Press Ctrl+C to stop monitoring and generate summary report\n');

  } catch (error) {
    console.error('❌ Failed to start latency monitoring:', error.message);
    process.exit(1);
  }
}

// Graceful shutdown with report generation
process.on('SIGINT', async () => {
  console.log('\n🛑 Stopping latency monitor and generating summary report...');
  try {
    await generateLatencySummary();
    await consumer.disconnect();
    console.log('✅ Latency monitor stopped successfully');
  } catch (error) {
    console.error('❌ Error during shutdown:', error.message);
  }
  process.exit(0);
});

// Handle uncaught exceptions
process.on('uncaughtException', async (error) => {
  console.error('❌ Uncaught Exception:', error);
  await generateLatencySummary();
  process.exit(1);
});

process.on('unhandledRejection', async (reason, promise) => {
  console.error('❌ Unhandled Rejection:', reason);
  await generateLatencySummary();
  process.exit(1);
});

// Start the monitor
if (require.main === module) {
  startLatencyMonitoring().catch(async (error) => {
    console.error('❌ Failed to start latency monitor:', error);
    process.exit(1);
  });
}

module.exports = {
  startLatencyMonitoring,
  generateLatencySummary
};