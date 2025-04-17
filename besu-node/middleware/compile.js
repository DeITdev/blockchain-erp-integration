// compile.js - Script to compile the smart contract
const fs = require('fs');
const path = require('path');
const solc = require('solc');

// Path to the contract
const contractPath = path.resolve(__dirname, 'contracts', 'ERPStorage.sol');
const contractSource = fs.readFileSync(contractPath, 'utf8');

// Prepare input for solc compiler
const input = {
  language: 'Solidity',
  sources: {
    'ERPStorage.sol': {
      content: contractSource
    }
  },
  settings: {
    outputSelection: {
      '*': {
        '*': ['*']
      }
    }
  }
};

// Compile the contract
console.log('Compiling ERPStorage.sol...');
const output = JSON.parse(solc.compile(JSON.stringify(input)));

// Check for errors
if (output.errors) {
  output.errors.forEach(error => {
    console.error(error.formattedMessage);
  });

  // Exit if there are severe errors
  if (output.errors.some(error => error.severity === 'error')) {
    console.error('Compilation failed due to errors.');
    process.exit(1);
  }
}

// Extract compilation results
const contractName = 'ERPStorage';
const compiledContract = output.contracts['ERPStorage.sol'][contractName];

// Save ABI to file
const abiPath = path.resolve(__dirname, 'contract-abi.json');
fs.writeFileSync(abiPath, JSON.stringify(compiledContract.abi));
console.log(`ABI saved to ${abiPath}`);

// Save bytecode to file
const bytecodePath = path.resolve(__dirname, 'contract-bytecode.json');
fs.writeFileSync(
  bytecodePath,
  JSON.stringify({ bytecode: compiledContract.evm.bytecode.object })
);
console.log(`Bytecode saved to ${bytecodePath}`);

// Save full compiled output for reference
const fullOutputPath = path.resolve(__dirname, 'compiled-contract.json');
fs.writeFileSync(fullOutputPath, JSON.stringify(compiledContract, null, 2));
console.log(`Full compilation output saved to ${fullOutputPath}`);

console.log('Compilation completed successfully!');