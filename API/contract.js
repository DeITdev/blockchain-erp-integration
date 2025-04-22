const Web3 = require('web3');
const EEAClient = require("web3-eea");
const url = "http://127.0.0.1:8545"
const chainId = 1981

// Add connection error handling
let web3;
try {
  web3 = new EEAClient(new Web3(url), chainId);

  // Test connection
  web3.eth.net.isListening()
    .then(() => console.log("Connected to Ethereum node at", url))
    .catch(err => console.error("Failed to connect to Ethereum node:", err.message));
} catch (error) {
  console.error("Error initializing Web3:", error.message);
}

const Tx = require("ethereumjs-tx").Transaction
const Common = require('ethereumjs-common');
const contract = require("./contract.json")

exports.deploy = (privateKey) => {
  let besuAccount = web3.eth.accounts.privateKeyToAccount(privateKey)
  console.log(besuAccount)
  return web3.eth.getTransactionCount(besuAccount.address, "pending").then(async (txCount) => {
    let gasPrice = await web3.eth.getGasPrice();
    console.log("tx count", txCount);
    console.log("gasPrice", gasPrice)
    const txObj = {
      nonce: web3.utils.toHex(txCount),
      gasPrice: web3.utils.toHex(gasPrice),
      gasLimit: web3.utils.toHex(210000),
      data: contract.bytecode,
      chainId: chainId
    };
    let custom = Common.default.forCustomChain(
      "mainnet",
      {
        networkId: 123,
        chainId: chainId,
        name: "besu-network"
      },
      "istanbul"
    );

    let tx = new Tx(txObj, { common: custom });
    let private = Buffer.from(privateKey, "hex");
    tx.sign(private);
    let serialized = tx.serialize();
    let rawSerialized = "0x" + serialized.toString("hex");

    return web3.eth.sendSignedTransaction(rawSerialized)
      .on("receipt", receipt => { console.log(receipt) })
      .catch(error => console.log(error));

  });
}

exports.store = async (privateKey, contractAddress, value) => {
  try {
    // Validate private key format (64 hex characters)
    if (!/^[0-9a-fA-F]{64}$/.test(privateKey)) {
      throw new Error("Invalid private key - must be 64 hex characters without 0x prefix");
    }

    // Create account from private key (without 0x prefix)
    const besuAccount = web3.eth.accounts.privateKeyToAccount(privateKey);

    // Create contract instance
    const contractInstance = new web3.eth.Contract(contract.abi, contractAddress);

    // Encode the store transaction
    const encoded = contractInstance.methods.store(value).encodeABI();

    // Get transaction count and gas price
    const [txCount, gasPrice] = await Promise.all([
      web3.eth.getTransactionCount(besuAccount.address, "pending"),
      web3.eth.getGasPrice()
    ]);

    // Build transaction object
    const txObj = {
      nonce: web3.utils.toHex(txCount),
      gasPrice: web3.utils.toHex(gasPrice),
      gasLimit: web3.utils.toHex(210000),
      data: encoded,
      chainId: chainId,
      to: contractAddress
    };

    // Create custom chain configuration
    const custom = Common.default.forCustomChain(
      "mainnet",
      {
        networkId: 123,
        chainId: chainId,
        name: "besu-network"
      },
      "istanbul"
    );

    // Create and sign transaction
    const tx = new Tx(txObj, { common: custom });
    const privateKeyBuffer = Buffer.from(privateKey, "hex");
    tx.sign(privateKeyBuffer);

    // Serialize and send transaction
    const serializedTx = tx.serialize();
    const rawTx = "0x" + serializedTx.toString("hex");

    return web3.eth.sendSignedTransaction(rawTx)
      .on("transactionHash", hash => console.log("Tx hash:", hash))
      .on("receipt", receipt => {
        console.log("Transaction successful:", receipt);
        return receipt;
      })
      .on("error", error => {
        console.error("Transaction error:", error);
        throw error;
      });

  } catch (error) {
    console.error("Store function error:", error);
    throw error;
  }
};

exports.read = async (contractAddress) => {
  let contractInstance = new web3.eth.Contract(contract.abi, contractAddress);
  return contractInstance.methods.retrieve().call().then(value => { return value });
}

exports.deploySimple = async (privateKey) => {
  try {
    // Setup account
    const account = web3.eth.accounts.privateKeyToAccount(`0x${privateKey}`);
    web3.eth.accounts.wallet.add(account);

    // Use a very simple contract bytecode
    const simpleBytecode = "0x6080604052348015600f57600080fd5b5060978061001e6000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c8063771602f714602d575b600080fd5b605660048036036040811015604157600080fd5b8101908080359060200190929190803590602001909291905050506068565b6040518082815260200191505060405180910390f35b600081830190509291505056fea26469706673582212201336f9cc716cd6a09c0a18b96e05cfa155386ea0010ca0e794e4c3fe1322473064736f6c63430006000033";

    console.log("Deploying simple test contract...");

    // Send transaction
    const tx = await web3.eth.sendTransaction({
      from: account.address,
      gas: 2000000,
      data: simpleBytecode
    });

    console.log("Simple contract deployed at:", tx.contractAddress);
    return {
      success: true,
      contractAddress: tx.contractAddress
    };
  } catch (error) {
    console.error("Simple deployment error:", error);
    return {
      success: false,
      error: error.message
    };
  }
};