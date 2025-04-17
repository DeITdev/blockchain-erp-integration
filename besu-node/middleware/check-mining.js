// check-mining.js
const Web3 = require('web3');
const web3 = new Web3('http://localhost:8545');

async function checkMining() {
  try {
    const initialBlock = await web3.eth.getBlockNumber();
    console.log(`Initial block number: ${initialBlock}`);

    // Wait a minute
    console.log('Waiting 60 seconds to check if new blocks are being mined...');
    await new Promise(resolve => setTimeout(resolve, 60000));

    const newBlock = await web3.eth.getBlockNumber();
    console.log(`New block number: ${newBlock}`);

    if (newBlock > initialBlock) {
      console.log('Mining is working! New blocks are being created.');
    } else {
      console.log('No new blocks were mined in the last minute.');
    }
  } catch (error) {
    console.error('Error checking mining status:', error.message);
  }
}

checkMining();