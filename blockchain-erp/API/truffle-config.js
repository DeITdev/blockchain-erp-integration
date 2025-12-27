const HDWalletProvider = require('@truffle/hdwallet-provider');
const privateKey = '8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63';


module.exports = {
  networks: {
    besu: {
      provider: () => new HDWalletProvider({
        privateKeys: [privateKey],
        providerOrUrl: 'http://localhost:8545', // Sesuaikan dengan URL RPC Besu Anda
        chainId: 1337
      }),
      network_id: 1337,
      gas: 4700000,
      gasPrice: 0
    }
  },

  compilers: {
    solc: {
      version: "0.8.11", // Match your contract version
      settings: {
        optimizer: {
          enabled: true,
          runs: 200
        },
        evmVersion: "istanbul" // Besu compatibility
      }
    }
  }
};