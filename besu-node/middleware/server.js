// server.js - Middleware to connect ERPNext with Hyperledger Besu
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const Web3 = require('web3');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const winston = require('winston');

// Initialize logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} ${level}: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'middleware.log' })
  ]
});

// Initialize Express app
const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));
app.use(bodyParser.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Environment variables (would typically be in .env file)
const BESU_RPC_URL = process.env.BESU_RPC_URL || 'http://localhost:8545';
const PORT = process.env.PORT || 3000;

// Load private key (in production, use secure environment variables)
// This is the address from your genesis.json file
const PRIVATE_KEY = process.env.PRIVATE_KEY || '0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63';

// Initialize Web3 with Besu connection
const web3 = new Web3(BESU_RPC_URL);

// Create account from private key
let account;
try {
  account = web3.eth.accounts.privateKeyToAccount(PRIVATE_KEY);
  web3.eth.accounts.wallet.add(account);
  web3.eth.defaultAccount = account.address;
  logger.info(`Using blockchain account: ${account.address}`);
} catch (error) {
  logger.error(`Failed to load blockchain account: ${error.message}`);
  process.exit(1);
}

// Load contract ABI and address
let contractABI;
let contractAddress;
let contract;

// Function to generate SHA-256 hash of data
function generateHash(data) {
  if (typeof data !== 'string') {
    data = JSON.stringify(data);
  }
  return '0x' + crypto.createHash('sha256').update(data).digest('hex');
}

// Function to deploy the smart contract if needed
async function deployContract() {
  logger.info('Deploying ERPStorage contract...');

  try {
    // Load contract source
    const contractPath = path.join(__dirname, 'contracts', 'ERPStorage.sol');
    const contractSource = fs.readFileSync(contractPath, 'utf8');

    // In a real-world scenario, you would compile this properly with solc
    // This is a simplification assuming you have the compiled ABI and bytecode

    // For demonstration purposes - in production you would use solc to compile
    // Placeholder for compiled contract data
    const compiledContract = {
      abi: [], // Your contract ABI would go here
      bytecode: '0x' // Your contract bytecode would go here
    };

    // Deploy contract
    const Contract = new web3.eth.Contract(compiledContract.abi);
    const deployTx = Contract.deploy({
      data: compiledContract.bytecode
    });

    const gas = await deployTx.estimateGas();
    const deployedContract = await deployTx.send({
      from: account.address,
      gas: gas
    });

    contractAddress = deployedContract.options.address;
    contractABI = compiledContract.abi;
    contract = deployedContract;

    logger.info(`Contract deployed at address: ${contractAddress}`);

    // Save contract address to a file for future use
    fs.writeFileSync(
      path.join(__dirname, 'contract-address.json'),
      JSON.stringify({ address: contractAddress })
    );

    return contractAddress;
  } catch (error) {
    logger.error(`Contract deployment failed: ${error.message}`);
    throw error;
  }
}

// Function to initialize the contract
async function initializeContract() {
  try {
    // Check if contract address exists
    const addressFile = path.join(__dirname, 'contract-address.json');

    if (fs.existsSync(addressFile)) {
      // Load existing contract address
      const addressData = JSON.parse(fs.readFileSync(addressFile, 'utf8'));
      contractAddress = addressData.address;
      logger.info(`Using existing contract at address: ${contractAddress}`);
    } else {
      // Deploy new contract
      contractAddress = await deployContract();
    }

    // Load contract ABI from a file
    // In production, this would be generated from compilation
    const abiFile = path.join(__dirname, 'contract-abi.json');
    if (fs.existsSync(abiFile)) {
      contractABI = JSON.parse(fs.readFileSync(abiFile, 'utf8'));
    } else {
      logger.error('Contract ABI file not found. Please compile the contract first.');
      process.exit(1);
    }

    // Initialize contract instance
    contract = new web3.eth.Contract(contractABI, contractAddress);

    return contract;
  } catch (error) {
    logger.error(`Contract initialization failed: ${error.message}`);
    throw error;
  }
}

// Webhook endpoint to receive ERPNext document events
app.post('/webhook/erp-document', async (req, res) => {
  try {
    const { doctype, name, event, data } = req.body;

    if (!doctype || !name || !event || !data) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields'
      });
    }

    logger.info(`Received webhook for ${doctype} ${name}, event: ${event}`);

    // Generate hash of the document data
    const dataHash = generateHash(data);

    // Record document to blockchain
    const tx = await contract.methods.recordDocument(
      doctype,
      name,
      event,
      dataHash
    ).send({
      from: account.address,
      gas: 500000
    });

    logger.info(`Document recorded to blockchain. Transaction hash: ${tx.transactionHash}`);

    return res.json({
      success: true,
      doctype,
      name,
      event,
      dataHash,
      transaction: tx.transactionHash,
      blockNumber: tx.blockNumber
    });
  } catch (error) {
    logger.error(`Error processing webhook: ${error.message}`);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// API endpoint to verify a document
app.post('/verify', async (req, res) => {
  try {
    const { doctype, name, data } = req.body;

    if (!doctype || !name || !data) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields'
      });
    }

    // Generate hash of the document data
    const dataHash = generateHash(data);

    // Check if document exists in blockchain
    const exists = await contract.methods.documentExists(dataHash).call();

    if (!exists) {
      return res.json({
        success: false,
        verified: false,
        message: 'Document not found in blockchain'
      });
    }

    // Get document details
    const document = await contract.methods.getDocument(dataHash).call();

    return res.json({
      success: true,
      verified: true,
      doctype: document[0],
      name: document[1],
      event: document[2],
      timestamp: new Date(parseInt(document[3]) * 1000).toISOString(),
      recorder: document[4]
    });
  } catch (error) {
    logger.error(`Error verifying document: ${error.message}`);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// API endpoint to get document history
app.get('/history/:doctype/:name', async (req, res) => {
  try {
    const { doctype, name } = req.params;

    // Get document history count
    const count = await contract.methods.getDocumentHistoryCount(doctype, name).call();

    if (count == 0) {
      return res.json({
        success: true,
        doctype,
        name,
        history: []
      });
    }

    // Get all history entries
    const history = [];
    for (let i = 0; i < count; i++) {
      // Get hash for this history entry
      const dataHash = await contract.methods.getDocumentHistoryHash(doctype, name, i).call();

      // Get document details
      const document = await contract.methods.getDocument(dataHash).call();

      history.push({
        doctype: document[0],
        name: document[1],
        event: document[2],
        timestamp: new Date(parseInt(document[3]) * 1000).toISOString(),
        dataHash,
        recorder: document[4]
      });
    }

    return res.json({
      success: true,
      doctype,
      name,
      history
    });
  } catch (error) {
    logger.error(`Error getting document history: ${error.message}`);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// API endpoint to get blockchain stats
app.get('/stats', async (req, res) => {
  try {
    const blockNumber = await web3.eth.getBlockNumber();
    const documentCount = await contract.methods.getDocumentCount().call();

    return res.json({
      success: true,
      blockNumber,
      documentCount: parseInt(documentCount),
      contractAddress
    });
  } catch (error) {
    logger.error(`Error getting stats: ${error.message}`);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Health check endpoint
app.get('/health', async (req, res) => {
  try {
    const blockNumber = await web3.eth.getBlockNumber();

    return res.json({
      success: true,
      status: 'healthy',
      blockchainConnected: true,
      currentBlock: blockNumber,
      account: account.address
    });
  } catch (error) {
    logger.error(`Health check failed: ${error.message}`);
    return res.status(500).json({
      success: false,
      status: 'unhealthy',
      blockchainConnected: false,
      error: error.message
    });
  }
});

// Start the application
async function startApp() {
  try {
    // Check blockchain connection
    const blockNumber = await web3.eth.getBlockNumber();
    logger.info(`Connected to blockchain. Current block number: ${blockNumber}`);

    // Initialize contract
    await initializeContract();

    // Start server
    app.listen(PORT, () => {
      logger.info(`Middleware server running on port ${PORT}`);
    });
  } catch (error) {
    logger.error(`Failed to start middleware: ${error.message}`);
    process.exit(1);
  }
}

// Run the application
startApp();