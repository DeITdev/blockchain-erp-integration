const ContractRegistry = artifacts.require("ContractRegistry");
const DocumentCertificate = artifacts.require("DocumentCertificate");
const SimpleStorage = artifacts.require("SimpleStorage");
const AttendanceStorage = artifacts.require("ERPNext/AttendanceStorage");
const EmployeeStorage = artifacts.require("ERPNext/EmployeeStorage");
const CompanyStorage = artifacts.require("ERPNext/CompanyStorage");
const UserStorage = artifacts.require("ERPNext/UserStorage");
// Add more contracts here if needed
// const AnotherContract = artifacts.require("AnotherContract");

module.exports = async function (deployer, network, accounts) {
  const forceRedeploy = process.env.FORCE_REDEPLOY === 'true';
  const redeployOnly = process.env.REDEPLOY_ONLY ? process.env.REDEPLOY_ONLY.split(',') : [];

  let registry;  // Deploy or get existing registry
  if (forceRedeploy) {
    console.log("🔄 Force redeploy mode: Reusing existing ContractRegistry...");
    try {
      registry = await ContractRegistry.deployed();
      console.log("✓ Reusing ContractRegistry at", registry.address);
    } catch (e) {
      console.log("📦 No existing registry found, deploying new one...");
      await deployer.deploy(ContractRegistry);
      registry = await ContractRegistry.deployed();
      console.log("✓ New ContractRegistry deployed at", registry.address);
    }
  } else {
    try {
      registry = await ContractRegistry.deployed();
      console.log("✓ Using existing ContractRegistry at", registry.address);
    } catch (e) {
      console.log("📦 Deploying new ContractRegistry...");
      await deployer.deploy(ContractRegistry);
      registry = await ContractRegistry.deployed();
      console.log("✓ ContractRegistry deployed at", registry.address);
    }
  }

  // Define contracts to deploy
  const contractsToDeployment = [
    { name: "DocumentCertificate", artifact: DocumentCertificate },
    { name: "SimpleStorage", artifact: SimpleStorage },
    { name: "AttendanceStorage", artifact: AttendanceStorage },
    { name: "EmployeeStorage", artifact: EmployeeStorage },
    { name: "CompanyStorage", artifact: CompanyStorage },
    { name: "UserStorage", artifact: UserStorage },
    // { name: "AnotherContract", artifact: AnotherContract },
  ];

  // Deploy only if not already deployed (unless force redeploy)
  for (const contract of contractsToDeployment) {
    try {
      // Check if this specific contract should be redeployed
      const shouldRedeployThis = redeployOnly.length > 0
        ? redeployOnly.includes(contract.name)
        : forceRedeploy;

      const isDeployed = shouldRedeployThis ? false : await registry.isContractDeployed(contract.name);

      if (!isDeployed) {
        console.log(`\n📦 Deploying ${contract.name}...`);
        await deployer.deploy(contract.artifact);
        const instance = await contract.artifact.deployed();

        console.log(`Contract deployed at: ${instance.address}`);

        const oldAddress = await registry.getContract(contract.name);
        console.log(`Old address from registry: ${oldAddress}`);

        // Register contract in registry with proper transaction options
        console.log(`Registering ${contract.name} at ${instance.address} in ContractRegistry...`);

        const tx = await registry.registerContract(contract.name, instance.address, {
          from: accounts[0],
          gas: 4700000
        });

        console.log(`Registration transaction hash: ${tx.tx}`);

        if (oldAddress !== "0x0000000000000000000000000000000000000000") {
          const version = await registry.getContractVersion(contract.name);
          console.log(`✓ ${contract.name} UPDATED (v${version}) at ${instance.address}`);
          console.log(`  Previous address: ${oldAddress}`);
        } else {
          console.log(`✓ ${contract.name} deployed at ${instance.address}`);
        }
      } else {
        const address = await registry.getContract(contract.name);
        const version = await registry.getContractVersion(contract.name);
        console.log(`\n✓ ${contract.name} (v${version}) already deployed at ${address}`);
      }
    } catch (error) {
      console.error(`\n✗ Error deploying ${contract.name}:`, error.message);
      if (error.data) {
        console.error('Error data:', error.data);
      }
      if (error.reason) {
        console.error('Error reason:', error.reason);
      }
      // Continue with next contract instead of stopping
      continue;
    }
  }  // Summary
  const totalContracts = await registry.getContractCount();
  console.log(`\n✓ Total registered contracts: ${totalContracts}`);
};