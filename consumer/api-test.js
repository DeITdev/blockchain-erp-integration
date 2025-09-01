// api-test.js - Test blockchain API connection and employee endpoint
require('dotenv').config();
const axios = require('axios');

const API_ENDPOINT = process.env.API_ENDPOINT || 'http://localhost:4001';
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const EMPLOYEE_CONTRACT_ADDRESS = process.env.EMPLOYEE_CONTRACT_ADDRESS;

async function testAPI() {
  console.log('🔍 Testing Blockchain API...\n');

  // Test 1: Check if API server is running
  try {
    console.log('1. Testing API server connection...');
    console.log('   URL:', API_ENDPOINT);

    // Force IPv4 by using 127.0.0.1 instead of localhost
    const testUrl = API_ENDPOINT.replace('localhost', '127.0.0.1');
    console.log('   Testing with IPv4:', testUrl);

    const response = await axios.get(`${testUrl}/`, {
      timeout: 10000,
      headers: {
        'Content-Type': 'application/json'
      }
    });
    console.log('✅ API server is running');
    console.log('   Status:', response.data.status);
    console.log('   Version:', response.data.version);
  } catch (error) {
    console.log('❌ API server connection failed:', error.message);
    console.log('   Error code:', error.code);
    console.log('   Make sure API server is running on port 4001');
    return;
  }

  // Test 2: Test employee endpoint with real data
  try {
    console.log('\n2. Testing employee endpoint...');
    const testUrl = API_ENDPOINT.replace('localhost', '127.0.0.1');
    console.log('   API Endpoint:', `${testUrl}/employees`);
    console.log('   Contract Address:', EMPLOYEE_CONTRACT_ADDRESS);

    const testData = {
      privateKey: PRIVATE_KEY,
      employeeData: {
        recordId: 'TEST-EMP-' + Date.now(),
        createdTimestamp: new Date().toISOString(),
        modifiedTimestamp: new Date().toISOString(),
        modifiedBy: 'api-test',
        allData: {
          employee_number: 'TEST-001',
          first_name: 'Test',
          last_name: 'Employee',
          status: 'Active',
          company: 'Test Company'
        }
      }
    };

    const response = await axios.post(`${testUrl}/employees`, testData, {
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json'
      }
    });
    console.log('✅ Employee endpoint working');
    console.log('   Transaction Hash:', response.data.blockchain?.transactionHash);
    console.log('   Contract Address:', response.data.blockchain?.contractAddress);
    console.log('   Block Number:', response.data.blockchain?.blockNumber);

  } catch (error) {
    console.log('❌ Employee endpoint failed:', error.response?.data || error.message);
  }
}

testAPI();