const Web3 = require('web3');
const fs = require('fs');
const path = require('path');

const url = "http://localhost:8545";
const chainId = 1337;
const web3 = new Web3(url);

// Auto-load all contract artifacts from build/contracts/
function loadAllContracts() {
  const buildDir = path.join(__dirname, 'build', 'contracts');
  const contracts = {};

  try {
    const files = fs.readdirSync(buildDir);

    files.forEach(file => {
      if (file.endsWith('.json')) {
        const contractName = file.replace('.json', '');
        const artifactPath = path.join(buildDir, file);

        try {
          contracts[contractName] = require(artifactPath);
          console.log(`[OK] Loaded contract: ${contractName}`);
        } catch (error) {
          console.warn(`[WARNING] Failed to load ${contractName}:`, error.message);
        }
      }
    });

    console.log(`[OK] Total contracts loaded: ${Object.keys(contracts).length}`);
  } catch (error) {
    console.error('[ERROR] Error loading contracts:', error.message);
  }

  return contracts;
}

// Load all contracts on startup
const ALL_CONTRACTS = loadAllContracts();

// Get contract address by name
function getContractAddress(contractName) {
  const contract = ALL_CONTRACTS[contractName];
  if (!contract) {
    throw new Error(`Contract ${contractName} not found in build artifacts`);
  }
  const address = contract.networks?.[chainId]?.address;
  if (!address) {
    throw new Error(`Contract ${contractName} not deployed on network ${chainId}`);
  }
  return address;
}

// Get contract instance
function getContractInstance(contractName, address = null) {
  const contract = ALL_CONTRACTS[contractName];
  if (!contract) {
    throw new Error(`Contract ${contractName} not found`);
  }
  const addr = address || getContractAddress(contractName);
  return new web3.eth.Contract(contract.abi, addr);
}

// Sign and send transaction
async function signAndSendTransaction(privateKey, txData, to) {
  const account = web3.eth.accounts.privateKeyToAccount(`0x${privateKey}`);

  const txCount = await web3.eth.getTransactionCount(account.address, 'pending');
  const gasPrice = await web3.eth.getGasPrice();

  const txObject = {
    nonce: web3.utils.toHex(txCount),
    gasPrice: web3.utils.toHex(gasPrice),
    gasLimit: web3.utils.toHex(4700000),
    to: to,
    data: txData,
    chainId: chainId
  };

  const signedTx = await web3.eth.accounts.signTransaction(txObject, `0x${privateKey}`);

  return web3.eth.sendSignedTransaction(signedTx.rawTransaction);
}

// Store employee data to EmployeeStorage contract
exports.storeEmployee = async (privateKey, employeeData) => {
  try {
    const contractAddress = getContractAddress('EmployeeStorage');
    const contract = getContractInstance('EmployeeStorage', contractAddress);

    const { recordId, createdTimestamp, modifiedTimestamp, modifiedBy, allData } = employeeData;

    // Convert timestamps - MariaDB sends microseconds, contract expects seconds
    // If timestamp is > 10^12, it's microseconds; otherwise treat as-is
    const createdUnix = createdTimestamp > 1e12 ? Math.floor(createdTimestamp / 1000000) : createdTimestamp;
    const modifiedUnix = modifiedTimestamp > 1e12 ? Math.floor(modifiedTimestamp / 1000000) : modifiedTimestamp;

    // allData can be object or string
    const allDataStr = typeof allData === 'string' ? allData : JSON.stringify(allData);

    // Encode function call
    const encoded = contract.methods.storeEmployee(
      recordId,
      createdUnix,
      modifiedUnix,
      modifiedBy || 'cdc-consumer',
      allDataStr
    ).encodeABI();

    console.log(`[OK] Storing employee: ${recordId} to EmployeeStorage at ${contractAddress}`);

    const receipt = await signAndSendTransaction(privateKey, encoded, contractAddress);

    console.log(`[OK] Employee stored: ${receipt.transactionHash}`);
    return receipt;
  } catch (error) {
    console.error('[ERROR] Store employee error:', error.message);
    throw error;
  }
};

// Store attendance data to AttendanceStorage contract
exports.storeAttendance = async (privateKey, attendanceData) => {
  try {
    const contractAddress = getContractAddress('AttendanceStorage');
    const contract = getContractInstance('AttendanceStorage', contractAddress);

    const { recordId, createdTimestamp, modifiedTimestamp, modifiedBy, allData } = attendanceData;

    // Convert timestamps - MariaDB sends microseconds, contract expects seconds
    const createdUnix = createdTimestamp > 1e12 ? Math.floor(createdTimestamp / 1000000) : createdTimestamp;
    const modifiedUnix = modifiedTimestamp > 1e12 ? Math.floor(modifiedTimestamp / 1000000) : modifiedTimestamp;

    // allData can be object or string
    const allDataStr = typeof allData === 'string' ? allData : JSON.stringify(allData);

    // Encode function call
    const encoded = contract.methods.storeAttendance(
      recordId,
      createdUnix,
      modifiedUnix,
      modifiedBy || 'cdc-consumer',
      allDataStr
    ).encodeABI();

    console.log(`[OK] Storing attendance: ${recordId} to AttendanceStorage at ${contractAddress}`);

    const receipt = await signAndSendTransaction(privateKey, encoded, contractAddress);

    console.log(`[OK] Attendance stored: ${receipt.transactionHash}`);
    return receipt;
  } catch (error) {
    console.error('[ERROR] Store attendance error:', error.message);
    throw error;
  }
};

// Write/Store value to SimpleStorage
exports.store = async (privateKey, contractAddress, value) => {
  try {
    const contract = new web3.eth.Contract(ALL_CONTRACTS.SimpleStorage.abi, contractAddress);
    const encoded = contract.methods.store(value).encodeABI();

    console.log(`[OK] Storing value: ${value} to SimpleStorage at ${contractAddress}`);

    const receipt = await signAndSendTransaction(privateKey, encoded, contractAddress);

    console.log('[OK] Value stored:', receipt.transactionHash);
    return receipt;
  } catch (error) {
    console.error('[ERROR] Store error:', error.message);
    throw error;
  }
};

// Read value from smart contract
exports.read = async (contractAddress, methodName = 'retrieve', ...methodArgs) => {
  try {
    // Find contract by address
    let contractAbi = null;
    let contractName = null;

    for (const [name, artifact] of Object.entries(ALL_CONTRACTS)) {
      const addr = artifact.networks?.[chainId]?.address;
      if (addr && addr.toLowerCase() === contractAddress.toLowerCase()) {
        contractAbi = artifact.abi;
        contractName = name;
        break;
      }
    }

    if (!contractAbi) {
      throw new Error(`Contract at ${contractAddress} not found`);
    }

    const contract = new web3.eth.Contract(contractAbi, contractAddress);

    console.log(`[OK] Reading from ${contractName} at ${contractAddress}`);
    console.log(`[OK] Calling method: ${methodName}`);

    if (!contract.methods[methodName]) {
      throw new Error(`Method ${methodName} not found`);
    }

    const value = await contract.methods[methodName](...methodArgs).call();
    console.log(`[OK] Retrieved value`);

    return value;
  } catch (error) {
    console.error('[ERROR] Read error:', error.message);
    throw error;
  }
};

// Helper: Get all deployed contracts
exports.getDeployedContracts = () => {
  const deployed = {};

  Object.entries(ALL_CONTRACTS).forEach(([name, artifact]) => {
    const address = artifact.networks?.[chainId]?.address;
    if (address) {
      deployed[name] = address;
    }
  });

  return deployed;
};

// Get registered contracts from ContractRegistry
exports.getRegisteredContracts = async () => {
  try {
    const registryAddress = getContractAddress('ContractRegistry');
    const registry = getContractInstance('ContractRegistry', registryAddress);

    console.log('[OK] Fetching registered contracts from ContractRegistry...');

    const contractNames = await registry.methods.getAllContracts().call();
    console.log('Contract names:', contractNames);

    const contracts = [];

    for (const name of contractNames) {
      const address = await registry.methods.getContract(name).call();
      const version = await registry.methods.getContractVersion(name).call();

      contracts.push({
        name,
        address,
        version: parseInt(version)
      });
    }

    console.log(`[OK] Found ${contracts.length} registered contract(s)`);

    return {
      registryAddress,
      totalContracts: contracts.length,
      contracts
    };
  } catch (error) {
    console.error('[ERROR] Error fetching registered contracts:', error.message);
    throw error;
  }
};

// Export web3 instance
exports.web3 = web3;