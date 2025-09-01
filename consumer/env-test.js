// env-test.js - Test if environment variables are loaded correctly
require('dotenv').config();

console.log('🔍 Testing Environment Variables...\n');

// Test contract addresses
const contractAddresses = {
  'tabUser': process.env.USER_CONTRACT_ADDRESS,
  'tabEmployee': process.env.EMPLOYEE_CONTRACT_ADDRESS,
  'tabTask': process.env.TASK_CONTRACT_ADDRESS,
  'tabCompany': process.env.COMPANY_CONTRACT_ADDRESS,
  'tabAttendance': process.env.ATTENDANCE_CONTRACT_ADDRESS
};

console.log('📋 Contract Addresses from .env file:');
Object.entries(contractAddresses).forEach(([tableName, address]) => {
  console.log(`  ${tableName}: ${address || '❌ NOT FOUND'}`);
});

console.log('\n🔗 Other Configuration:');
console.log(`  API_ENDPOINT: ${process.env.API_ENDPOINT || '❌ NOT FOUND'}`);
console.log(`  KAFKA_BROKER: ${process.env.KAFKA_BROKER || '❌ NOT FOUND'}`);
console.log(`  PRIVATE_KEY: ${process.env.PRIVATE_KEY ? '✅ FOUND' : '❌ NOT FOUND'}`);

console.log('\n📄 .env file location expected: ' + require('path').resolve('.env'));

// Test if all required variables are present
const missing = [];
Object.entries(contractAddresses).forEach(([tableName, address]) => {
  if (!address) missing.push(tableName);
});

if (missing.length > 0) {
  console.log(`\n❌ Missing contract addresses: ${missing.join(', ')}`);
  console.log('Please add these to your .env file:');
  missing.forEach(tableName => {
    const envVar = tableName.replace('tab', '').toUpperCase() + '_CONTRACT_ADDRESS';
    console.log(`${envVar}=0x...`);
  });
} else {
  console.log('\n✅ All contract addresses found!');
}