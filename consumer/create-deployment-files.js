// create-deployment-files.js - Create deployment files for your contracts
const fs = require('fs');
const path = require('path');

// Your contract addresses from .env
const contracts = {
  user: {
    address: '0x9B8397f1B0FEcD3a1a40CdD5E8221Fa461898517',
    name: 'UserStorage',
    type: 'user'
  },
  employee: {
    address: '0xC9Bc439c8723c5c6fdbBE14E5fF3a1a40CdD5E821',
    name: 'EmployeeStorage',
    type: 'employee'
  },
  task: {
    address: '0x9a3DBCa554e9f6b9257aAa24010DA8377C57c17e',
    name: 'TaskStorage',
    type: 'task'
  },
  company: {
    address: '0x42699A7612A82f1d9C36148af9C77354759b210b',
    name: 'CompanyStorage',
    type: 'company'
  },
  attendance: {
    address: '0xa50a51c09a5c451C52BB714527E1974b686D8e77',
    name: 'AttendanceStorage',
    type: 'attendance'
  }
};

// Create deployment files
Object.entries(contracts).forEach(([key, contract]) => {
  const deploymentData = {
    success: true,
    contractType: contract.type,
    contractName: contract.name,
    contractAddress: contract.address,
    transactionHash: `0x${'0'.repeat(64)}`, // Placeholder
    blockNumber: 1000,
    blockHash: `0x${'0'.repeat(64)}`, // Placeholder
    gasUsed: 1000000,
    gasLimit: 8000000,
    gasPrice: "1000000000",
    deployerAddress: "0xFE3B557E8Fb62b89F4916B721be55cEb828dBd73",
    deploymentTime: new Date().toISOString(),
    constructorArgs: [],
    transactionDetails: {
      nonce: 1,
      cumulativeGasUsed: 1000000,
      effectiveGasPrice: 1000000000,
      status: true,
      chainId: 1337
    }
  };

  // Write to API directory (adjust path as needed)
  const filename = `contract-deployment-${contract.type}.json`;
  const filePath = path.join('../API', filename);

  console.log(`Creating ${filename}...`);
  console.log(`  Contract: ${contract.name}`);
  console.log(`  Address: ${contract.address}`);
  console.log(`  File: ${filePath}`);

  try {
    fs.writeFileSync(filePath, JSON.stringify(deploymentData, null, 2));
    console.log(`✅ Created ${filename}`);
  } catch (error) {
    console.error(`❌ Failed to create ${filename}:`, error.message);

    // Try current directory
    try {
      fs.writeFileSync(filename, JSON.stringify(deploymentData, null, 2));
      console.log(`✅ Created ${filename} in current directory`);
      console.log(`   Please copy this file to your API directory`);
    } catch (error2) {
      console.error(`❌ Failed to create ${filename} in current directory:`, error2.message);
    }
  }

  console.log('');
});

console.log('🎯 Next steps:');
console.log('1. Copy these files to your API directory');
console.log('2. Restart your API server (node app.js)');
console.log('3. Test the API endpoints');
console.log('4. Run the consumer again');