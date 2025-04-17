// start.js - Script to start the middleware with all dependencies
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

// Check if contract is deployed
const contractAddressPath = path.join(__dirname, 'contract-address.json');
const contractAbiPath = path.join(__dirname, 'contract-abi.json');

async function start() {
  console.log('Starting ERPNext-Blockchain Middleware...');

  // Step 1: Check if the contract is compiled
  if (!fs.existsSync(contractAbiPath)) {
    console.log('Contract ABI not found. Compiling contract...');
    try {
      // Check if solc is installed
      if (!fs.existsSync(path.join(__dirname, 'node_modules', 'solc'))) {
        console.log('Installing solc...');
        await runCommand('npm', ['install', 'solc', '--save-dev']);
      }

      await runCommand('node', ['compile.js']);
    } catch (error) {
      console.error('Error compiling contract:', error);
      process.exit(1);
    }
  }

  // Step 2: Check if contract is deployed
  if (!fs.existsSync(contractAddressPath)) {
    console.log('Contract address not found. Deploying contract...');
    try {
      await runCommand('node', ['deploy.js']);
    } catch (error) {
      console.error('Error deploying contract:', error);
      process.exit(1);
    }
  }

  // Step 3: Start the server
  console.log('Starting middleware server...');
  const server = spawn('node', ['server.js'], {
    stdio: 'inherit'
  });

  server.on('close', (code) => {
    console.log(`Server process exited with code ${code}`);
  });

  console.log('Middleware server started. Press Ctrl+C to stop.');
}

// Helper function to run a command
function runCommand(command, args) {
  return new Promise((resolve, reject) => {
    const process = spawn(command, args, {
      stdio: 'inherit'
    });

    process.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Command failed with exit code ${code}`));
      }
    });

    process.on('error', (err) => {
      reject(err);
    });
  });
}

start();