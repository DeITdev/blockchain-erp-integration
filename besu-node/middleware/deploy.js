// deploy.js - Script to deploy the smart contract
const Web3 = require('web3');
const fs = require('fs');
const path = require('path');

// Connect to Besu node
const BESU_RPC_URL = process.env.BESU_RPC_URL || 'http://localhost:8545';
const web3 = new Web3(BESU_RPC_URL);

// Load the private key (use the one from your genesis.json)
const PRIVATE_KEY = process.env.PRIVATE_KEY || '0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63';

// Load contract ABI and bytecode
const abiPath = path.resolve(__dirname, 'contract-abi.json');
const bytecodePath = path.resolve(__dirname, 'contract-bytecode.json');

if (!fs.existsSync(abiPath) || !fs.existsSync(bytecodePath)) {
  console.error('Contract ABI or bytecode file not found. Run compile.js first.');
  process.exit(1);
}

const contractABI = JSON.parse(fs.readFileSync(abiPath, 'utf8'));
const contractBytecode = JSON.parse(fs.readFileSync(bytecodePath, 'utf8')).bytecode;

async function deployContract() {
  try {
    console.log('Connecting to blockchain...');

    // Check connection
    const blockNumber = await web3.eth.getBlockNumber();
    console.log(`Connected to blockchain. Current block number: ${blockNumber}`);

    // Set up account from private key
    const account = web3.eth.accounts.privateKeyToAccount(PRIVATE_KEY);
    web3.eth.accounts.wallet.add(account);
    const accountAddress = account.address;

    console.log(`Using account: ${accountAddress}`);

    // Get account balance
    const balance = await web3.eth.getBalance(accountAddress);
    console.log(`Account balance: ${web3.utils.fromWei(balance, 'ether')} ETH`);

    // Create contract instance
    const Contract = new web3.eth.Contract(contractABI);

    // Prepare deployment transaction
    console.log('Deploying contract...');
    const deployTx = Contract.deploy({
      data: '0x' + contractBytecode
    });

    // Estimate gas
    const gas = await deployTx.estimateGas({ from: accountAddress });
    console.log(`Estimated gas: ${gas}`);

    // Deploy contract
    const deployedContract = await deployTx.send({
      from: accountAddress,
      gas: Math.floor(gas * 1.1) // Add 10% buffer
    });

    const contractAddress = deployedContract.options.address;
    console.log(`Contract deployed successfully!`);
    console.log(`Contract address: ${contractAddress}`);

    // Save contract address to file
    const addressFilePath = path.resolve(__dirname, 'contract-address.json');
    fs.writeFileSync(
      addressFilePath,
      JSON.stringify({ address: contractAddress })
    );
    console.log(`Contract address saved to ${addressFilePath}`);

    return contractAddress;
  } catch (error) {
    console.error('Error deploying contract:', error.message);
    process.exit(1);
  }
}

deployContract();