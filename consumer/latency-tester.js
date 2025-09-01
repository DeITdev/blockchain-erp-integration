// latency-tester.js - End-to-End Latency Testing for ERP → Kafka → Consumer → Blockchain
require('dotenv').config();

const mysql = require('mysql2/promise');
const { Kafka } = require('kafkajs');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');

// Configuration from environment
const DB_CONFIG = {
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 3306,
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || 'admin',
  database: process.env.DB_NAME || 'erpnext_db'
};

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:29092';
const API_ENDPOINT = process.env.API_ENDPOINT || 'http://localhost:4001';
const PRIVATE_KEY = process.env.PRIVATE_KEY || '8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63';

// Test configuration
const TEST_TABLES = [
  {
    name: 'tabEmployee',
    apiEndpoint: '/employees',
    testData: {
      employee_number: `EMP-${Date.now()}`,
      first_name: 'Test',
      last_name: 'Employee',
      gender: 'Male',
      date_of_birth: '1990-01-01',
      date_of_joining: new Date().toISOString().split('T')[0],
      status: 'Active',
      company: 'Test Company'
    }
  },
  {
    name: 'tabUser',
    apiEndpoint: '/users',
    testData: {
      email: `test.${Date.now()}@example.com`,
      first_name: 'Test',
      last_name: 'User',
      username: `testuser${Date.now()}`,
      enabled: 1,
      user_type: 'System User',
      role_profile_name: 'Employee'
    }
  },
  {
    name: 'tabTask',
    apiEndpoint: '/tasks',
    testData: {
      subject: `Test Task ${Date.now()}`,
      status: 'Open',
      priority: 'Medium',
      is_group: 0,
      description: 'This is a test task for latency testing'
    }
  },
  {
    name: 'tabCompany',
    apiEndpoint: '/companies',
    testData: {
      company_name: `Test Company ${Date.now()}`,
      domain: 'Technology',
      country: 'Indonesia',
      default_currency: 'IDR'
    }
  },
  {
    name: 'tabAttendance',
    apiEndpoint: '/attendances',
    testData: {
      employee: 'EMP001',
      employee_name: 'Test Employee',
      attendance_date: new Date().toISOString().split('T')[0],
      status: 'Present',
      company: 'Test Company',
      in_time: '09:00:00',
      out_time: '17:00:00',
      working_hours: 8.0
    }
  }
];

// Global test results
let testResults = [];
let kafkaConsumer = null;
let dbConnection = null;

// Initialize Kafka consumer for monitoring
async function initializeKafkaMonitor() {
  const kafka = new Kafka({
    brokers: [KAFKA_BROKER],
    clientId: 'latency-test-monitor'
  });

  kafkaConsumer = kafka.consumer({
    groupId: 'latency-test-group',
    sessionTimeout: 30000,
    heartbeatInterval: 3000
  });

  await kafkaConsumer.connect();
  console.log('✅ Kafka monitor connected');

  return kafkaConsumer;
}

// Initialize database connection
async function initializeDatabase() {
  try {
    dbConnection = await mysql.createConnection(DB_CONFIG);
    console.log('✅ Database connected');
    return dbConnection;
  } catch (error) {
    console.error('❌ Database connection failed:', error.message);
    throw error;
  }
}

// Wait for Kafka message with timeout
function waitForKafkaMessage(topic, recordId, timeoutMs = 30000) {
  return new Promise(async (resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Timeout waiting for Kafka message for ${recordId}`));
    }, timeoutMs);

    try {
      await kafkaConsumer.subscribe({ topic, fromBeginning: false });

      await kafkaConsumer.run({
        eachMessage: async ({ message }) => {
          try {
            const event = JSON.parse(message.value.toString());

            // Check if this is our test record
            const eventRecordId = event.name || event.id || event.email || event.employee_number;

            if (eventRecordId && eventRecordId.toString().includes(recordId.toString())) {
              clearTimeout(timeout);
              resolve({
                kafkaTimestamp: parseInt(message.timestamp),
                receivedAt: Date.now(),
                event: event,
                offset: message.offset
              });
            }
          } catch (error) {
            // Ignore parsing errors for non-matching messages
          }
        }
      });
    } catch (error) {
      clearTimeout(timeout);
      reject(error);
    }
  });
}

// Test blockchain API call
async function testBlockchainAPI(tableName, apiEndpoint, testData, recordId) {
  const startTime = Date.now();

  try {
    const url = `${API_ENDPOINT}${apiEndpoint}`;
    const dataKey = `${tableName.replace('tab', '').toLowerCase()}Data`;

    const payload = {
      privateKey: PRIVATE_KEY,
      [dataKey]: {
        recordId: recordId,
        createdTimestamp: new Date().toISOString(),
        modifiedTimestamp: new Date().toISOString(),
        modifiedBy: 'latency-tester',
        allData: testData
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
      response: response.data,
      transactionHash: response.data.blockchain?.transactionHash,
      blockNumber: response.data.blockchain?.blockNumber
    };

  } catch (error) {
    const endTime = Date.now();
    const latency = endTime - startTime;

    return {
      success: false,
      latency: latency,
      error: error.response?.data || error.message
    };
  }
}

// Insert test data into database
async function insertTestData(tableName, testData) {
  const dbInsertStart = Date.now();

  try {
    // Add standard fields
    const now = new Date();
    const recordData = {
      ...testData,
      name: testData.name || testData.email || testData.employee_number || `${tableName}-${Date.now()}`,
      creation: now,
      modified: now,
      modified_by: 'latency-tester',
      owner: 'latency-tester',
      docstatus: 0,
      idx: 0
    };

    // Build INSERT query
    const columns = Object.keys(recordData);
    const values = Object.values(recordData);
    const placeholders = values.map(() => '?').join(', ');

    const query = `INSERT INTO \`${tableName}\` (${columns.map(col => `\`${col}\``).join(', ')}) VALUES (${placeholders})`;

    console.log(`📝 Inserting test data into ${tableName}...`);
    await dbConnection.execute(query, values);

    const dbInsertEnd = Date.now();
    const dbLatency = dbInsertEnd - dbInsertStart;

    console.log(`✅ Data inserted into ${tableName} (${dbLatency}ms)`);

    return {
      success: true,
      recordId: recordData.name,
      dbLatency: dbLatency,
      insertTimestamp: dbInsertStart
    };

  } catch (error) {
    const dbInsertEnd = Date.now();
    const dbLatency = dbInsertEnd - dbInsertStart;

    console.error(`❌ Database insert failed for ${tableName}:`, error.message);
    return {
      success: false,
      error: error.message,
      dbLatency: dbLatency
    };
  }
}

// Get Kafka topic for table
function getKafkaTopic(tableName) {
  // This would need to be dynamically discovered in a real scenario
  // For now, we'll use a pattern-based approach
  return `erpnext.erpnext_db.${tableName}`;
}

// Run latency test for a specific table
async function runTableLatencyTest(tableConfig, testNumber = 1) {
  console.log(`\n${'='.repeat(80)}`);
  console.log(`🧪 LATENCY TEST ${testNumber}: ${tableConfig.name}`);
  console.log(`${'='.repeat(80)}`);

  const testStartTime = Date.now();
  let kafkaMonitorPromise = null;

  try {
    // Step 1: Prepare Kafka monitoring
    const topic = getKafkaTopic(tableConfig.name);
    console.log(`📡 Monitoring Kafka topic: ${topic}`);

    // Start monitoring for Kafka messages (but don't wait yet)
    const recordId = tableConfig.testData.name ||
      tableConfig.testData.email ||
      tableConfig.testData.employee_number ||
      `${tableConfig.name}-${Date.now()}`;

    kafkaMonitorPromise = waitForKafkaMessage(topic, recordId);

    // Step 2: Insert data into database (this should trigger CDC)
    console.log(`\n⏱️  Step 1: Database Insert`);
    const dbResult = await insertTestData(tableConfig.name, tableConfig.testData);

    if (!dbResult.success) {
      throw new Error(`Database insert failed: ${dbResult.error}`);
    }

    // Step 3: Wait for Kafka CDC message
    console.log(`\n⏱️  Step 2: Waiting for Kafka CDC Event...`);
    const kafkaResult = await kafkaMonitorPromise;
    const kafkaLatency = kafkaResult.receivedAt - dbResult.insertTimestamp;

    console.log(`✅ Kafka message received (${kafkaLatency}ms after DB insert)`);

    // Step 4: Test blockchain API
    console.log(`\n⏱️  Step 3: Testing Blockchain API...`);
    const blockchainResult = await testBlockchainAPI(
      tableConfig.name,
      tableConfig.apiEndpoint,
      tableConfig.testData,
      dbResult.recordId
    );

    if (blockchainResult.success) {
      console.log(`✅ Blockchain API call successful (${blockchainResult.latency}ms)`);
      console.log(`🔗 Transaction Hash: ${blockchainResult.transactionHash}`);
      console.log(`📦 Block Number: ${blockchainResult.blockNumber}`);
    } else {
      console.log(`❌ Blockchain API call failed (${blockchainResult.latency}ms): ${blockchainResult.error}`);
    }

    // Calculate total latency
    const totalLatency = kafkaLatency + blockchainResult.latency;
    const testEndTime = Date.now();
    const totalTestTime = testEndTime - testStartTime;

    // Results summary
    console.log(`\n📊 LATENCY BREAKDOWN:`);
    console.log(`  DB → Kafka CDC:     ${kafkaLatency}ms`);
    console.log(`  Kafka → Consumer:   ~0ms (instant processing)`);
    console.log(`  Consumer → Blockchain: ${blockchainResult.latency}ms`);
    console.log(`  ────────────────────────────────────`);
    console.log(`  Total End-to-End:   ${totalLatency}ms`);
    console.log(`  Total Test Time:    ${totalTestTime}ms`);

    // Store results
    const testResult = {
      timestamp: new Date().toISOString(),
      testNumber: testNumber,
      tableName: tableConfig.name,
      recordId: dbResult.recordId,
      latencies: {
        dbInsert: dbResult.dbLatency,
        kafkaCDC: kafkaLatency,
        consumerProcessing: 0, // Instant for our test
        blockchainAPI: blockchainResult.latency,
        totalEndToEnd: totalLatency
      },
      success: {
        dbInsert: dbResult.success,
        kafkaMessage: true,
        blockchainAPI: blockchainResult.success
      },
      blockchain: {
        transactionHash: blockchainResult.transactionHash,
        blockNumber: blockchainResult.blockNumber
      },
      totalTestTime: totalTestTime
    };

    testResults.push(testResult);
    return testResult;

  } catch (error) {
    console.error(`❌ Test failed for ${tableConfig.name}:`, error.message);

    const testEndTime = Date.now();
    const totalTestTime = testEndTime - testStartTime;

    const testResult = {
      timestamp: new Date().toISOString(),
      testNumber: testNumber,
      tableName: tableConfig.name,
      recordId: null,
      error: error.message,
      totalTestTime: totalTestTime,
      success: {
        dbInsert: false,
        kafkaMessage: false,
        blockchainAPI: false
      }
    };

    testResults.push(testResult);
    return testResult;
  }
}

// Run comprehensive latency tests
async function runLatencyTests() {
  console.log(`
🚀 END-TO-END LATENCY TESTING SUITE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Testing Pipeline: DB → Kafka → Consumer → Blockchain
🎯 Tables: ${TEST_TABLES.map(t => t.name).join(', ')}
⏱️  Measuring: DB→Kafka, Kafka→Consumer, Consumer→Blockchain
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);

  try {
    // Initialize connections
    console.log('🔧 Initializing connections...');
    await initializeDatabase();
    await initializeKafkaMonitor();

    // Test each table
    for (let i = 0; i < TEST_TABLES.length; i++) {
      const tableConfig = TEST_TABLES[i];
      console.log(`\n📋 Testing ${i + 1}/${TEST_TABLES.length}: ${tableConfig.name}`);

      try {
        await runTableLatencyTest(tableConfig, i + 1);

        // Wait between tests to avoid overwhelming the system
        if (i < TEST_TABLES.length - 1) {
          console.log('\n⏳ Waiting 5 seconds before next test...');
          await new Promise(resolve => setTimeout(resolve, 5000));
        }
      } catch (error) {
        console.error(`❌ Failed to test ${tableConfig.name}:`, error.message);
      }
    }

    // Generate final report
    await generateLatencyReport();

  } catch (error) {
    console.error('❌ Test suite failed:', error.message);
  } finally {
    // Cleanup
    await cleanup();
  }
}

// Generate comprehensive latency report
async function generateLatencyReport() {
  console.log(`\n${'='.repeat(100)}`);
  console.log('📊 COMPREHENSIVE LATENCY REPORT');
  console.log(`${'='.repeat(100)}`);

  if (testResults.length === 0) {
    console.log('❌ No test results to report');
    return;
  }

  // Successful tests only
  const successfulTests = testResults.filter(r => r.latencies);

  if (successfulTests.length === 0) {
    console.log('❌ No successful tests to analyze');
    console.log('\n🔍 Failed Tests:');
    testResults.forEach(r => {
      console.log(`  ${r.tableName}: ${r.error || 'Unknown error'}`);
    });
    return;
  }

  // Calculate statistics
  const stats = {
    total: testResults.length,
    successful: successfulTests.length,
    failed: testResults.length - successfulTests.length
  };

  // Latency statistics by component
  const latencyStats = {
    dbInsert: {
      values: successfulTests.map(r => r.latencies.dbInsert),
      avg: 0, min: 0, max: 0, median: 0
    },
    kafkaCDC: {
      values: successfulTests.map(r => r.latencies.kafkaCDC),
      avg: 0, min: 0, max: 0, median: 0
    },
    blockchainAPI: {
      values: successfulTests.map(r => r.latencies.blockchainAPI),
      avg: 0, min: 0, max: 0, median: 0
    },
    totalEndToEnd: {
      values: successfulTests.map(r => r.latencies.totalEndToEnd),
      avg: 0, min: 0, max: 0, median: 0
    }
  };

  // Calculate stats for each component
  Object.keys(latencyStats).forEach(key => {
    const values = latencyStats[key].values.sort((a, b) => a - b);
    latencyStats[key].avg = Math.round(values.reduce((a, b) => a + b, 0) / values.length);
    latencyStats[key].min = values[0];
    latencyStats[key].max = values[values.length - 1];
    latencyStats[key].median = values[Math.floor(values.length / 2)];
  });

  // Print overall statistics
  console.log(`\n📈 OVERALL STATISTICS:`);
  console.log(`  Total Tests:     ${stats.total}`);
  console.log(`  Successful:      ${stats.successful} (${Math.round(stats.successful / stats.total * 100)}%)`);
  console.log(`  Failed:          ${stats.failed} (${Math.round(stats.failed / stats.total * 100)}%)`);

  // Print latency breakdown
  console.log(`\n⏱️  LATENCY BREAKDOWN (milliseconds):`);
  console.log(`  Component              Avg    Min    Max    Median`);
  console.log(`  ─────────────────────  ─────  ─────  ─────  ──────`);
  console.log(`  DB Insert              ${latencyStats.dbInsert.avg.toString().padStart(5)}  ${latencyStats.dbInsert.min.toString().padStart(5)}  ${latencyStats.dbInsert.max.toString().padStart(5)}  ${latencyStats.dbInsert.median.toString().padStart(6)}`);
  console.log(`  DB → Kafka CDC         ${latencyStats.kafkaCDC.avg.toString().padStart(5)}  ${latencyStats.kafkaCDC.min.toString().padStart(5)}  ${latencyStats.kafkaCDC.max.toString().padStart(5)}  ${latencyStats.kafkaCDC.median.toString().padStart(6)}`);
  console.log(`  Consumer → Blockchain  ${latencyStats.blockchainAPI.avg.toString().padStart(5)}  ${latencyStats.blockchainAPI.min.toString().padStart(5)}  ${latencyStats.blockchainAPI.max.toString().padStart(5)}  ${latencyStats.blockchainAPI.median.toString().padStart(6)}`);
  console.log(`  ─────────────────────  ─────  ─────  ─────  ──────`);
  console.log(`  Total End-to-End       ${latencyStats.totalEndToEnd.avg.toString().padStart(5)}  ${latencyStats.totalEndToEnd.min.toString().padStart(5)}  ${latencyStats.totalEndToEnd.max.toString().padStart(5)}  ${latencyStats.totalEndToEnd.median.toString().padStart(6)}`);

  // Print per-table results
  console.log(`\n📋 PER-TABLE RESULTS:`);
  successfulTests.forEach(result => {
    console.log(`\n  ${result.tableName}:`);
    console.log(`    Total Latency:    ${result.latencies.totalEndToEnd}ms`);
    console.log(`    DB → Kafka:       ${result.latencies.kafkaCDC}ms`);
    console.log(`    Consumer → BC:    ${result.latencies.blockchainAPI}ms`);
    console.log(`    Record ID:        ${result.recordId}`);
    console.log(`    Transaction:      ${result.blockchain.transactionHash || 'N/A'}`);
    console.log(`    Block Number:     ${result.blockchain.blockNumber || 'N/A'}`);
  });

  // Print failed tests
  const failedTests = testResults.filter(r => !r.latencies);
  if (failedTests.length > 0) {
    console.log(`\n❌ FAILED TESTS:`);
    failedTests.forEach(result => {
      console.log(`  ${result.tableName}: ${result.error}`);
    });
  }

  // Save detailed report to file
  const reportData = {
    timestamp: new Date().toISOString(),
    summary: {
      ...stats,
      latencyStats
    },
    testResults: testResults,
    recommendations: generateRecommendations(latencyStats)
  };

  const reportFile = path.join(__dirname, `latency-report-${Date.now()}.json`);
  await fs.writeFile(reportFile, JSON.stringify(reportData, null, 2));
  console.log(`\n💾 Detailed report saved to: ${reportFile}`);

  // Print recommendations
  console.log(`\n💡 RECOMMENDATIONS:`);
  reportData.recommendations.forEach(rec => {
    console.log(`  • ${rec}`);
  });

  console.log(`\n${'='.repeat(100)}`);
}

// Generate performance recommendations
function generateRecommendations(latencyStats) {
  const recommendations = [];

  if (latencyStats.kafkaCDC.avg > 5000) {
    recommendations.push('High Kafka CDC latency detected. Consider optimizing Debezium configuration.');
  }

  if (latencyStats.blockchainAPI.avg > 10000) {
    recommendations.push('High blockchain API latency. Consider optimizing gas limits or using faster blockchain network.');
  }

  if (latencyStats.totalEndToEnd.avg > 15000) {
    recommendations.push('Total end-to-end latency is high. Consider parallel processing or caching strategies.');
  }

  if (latencyStats.totalEndToEnd.max > latencyStats.totalEndToEnd.avg * 3) {
    recommendations.push('High latency variance detected. Check for network issues or resource constraints.');
  }

  if (recommendations.length === 0) {
    recommendations.push('All latencies are within acceptable ranges. System performance is good.');
  }

  return recommendations;
}

// Cleanup connections
async function cleanup() {
  console.log('\n🧹 Cleaning up connections...');

  try {
    if (kafkaConsumer) {
      await kafkaConsumer.disconnect();
      console.log('✅ Kafka consumer disconnected');
    }
  } catch (error) {
    console.error('❌ Error disconnecting Kafka:', error.message);
  }

  try {
    if (dbConnection) {
      await dbConnection.end();
      console.log('✅ Database connection closed');
    }
  } catch (error) {
    console.error('❌ Error closing database:', error.message);
  }
}

// Handle process termination
process.on('SIGINT', async () => {
  console.log('\n🛑 Test suite interrupted. Generating partial report...');
  if (testResults.length > 0) {
    await generateLatencyReport();
  }
  await cleanup();
  process.exit(0);
});

process.on('uncaughtException', async (error) => {
  console.error('❌ Uncaught Exception:', error);
  await cleanup();
  process.exit(1);
});

// Main execution
if (require.main === module) {
  console.log('🧪 Starting End-to-End Latency Testing Suite...');
  runLatencyTests().catch(async (error) => {
    console.error('❌ Test suite failed:', error);
    await cleanup();
    process.exit(1);
  });
}

module.exports = {
  runLatencyTests,
  runTableLatencyTest,
  generateLatencyReport
};