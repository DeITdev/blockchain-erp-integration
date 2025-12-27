const express = require('express');
const app = express();
const bodyparser = require('body-parser');
const {
  store,
  read,
  storeEmployee,
  storeAttendance,
  getDeployedContracts,
  getRegisteredContracts
} = require('./blockchainService');

app.use(bodyparser.json());

// Default private key (from genesis.json)
const DEFAULT_PRIVATE_KEY = '8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63';

// Root endpoint - API info
app.get("/", (req, res) => {
  const contracts = getDeployedContracts();
  res.json({
    name: "Blockchain ERP Integration API",
    version: "2.1.0",
    blockchain: {
      url: "http://localhost:8545",
      chainId: 1337
    },
    contracts: Object.keys(contracts),
    endpoints: [
      "GET /employees - Get all employees from blockchain",
      "GET /employees/:id - Get specific employee by ID",
      "POST /employees - Store employee data to EmployeeStorage",
      "GET /attendances - Get all attendances from blockchain",
      "GET /attendances/:id - Get specific attendance by ID",
      "POST /attendances - Store attendance data to AttendanceStorage",
      "POST /store - Store value to SimpleStorage",
      "GET /read - Read from smart contract",
      "GET /contracts - Get registered contracts",
      "GET /contracts/deployed - Get deployed contracts"
    ]
  });
});

// GET /employees - Get all employees from blockchain
app.get("/employees", async (req, res) => {
  try {
    const contracts = getDeployedContracts();
    const address = contracts.EmployeeStorage;

    if (!address) {
      return res.status(404).json({ success: false, error: "EmployeeStorage not deployed" });
    }

    // Get all employee IDs
    const ids = await read(address, 'getAllEmployeeIds');

    // Get full data for each employee
    const employees = [];
    for (const id of ids) {
      try {
        const emp = await read(address, 'getEmployee', id);
        employees.push({
          recordId: emp.recordId || emp[0],
          createdTimestamp: emp.createdTimestamp || emp[1],
          modifiedTimestamp: emp.modifiedTimestamp || emp[2],
          modifiedBy: emp.modifiedBy || emp[3],
          allData: JSON.parse(emp.allData || emp[4] || '{}')
        });
      } catch (e) {
        console.error(`Error getting employee ${id}:`, e.message);
      }
    }

    res.json({
      success: true,
      total: employees.length,
      employees
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /employees/:id - Get specific employee by ID
app.get("/employees/:id", async (req, res) => {
  try {
    const contracts = getDeployedContracts();
    const address = contracts.EmployeeStorage;

    if (!address) {
      return res.status(404).json({ success: false, error: "EmployeeStorage not deployed" });
    }

    const emp = await read(address, 'getEmployee', req.params.id);

    res.json({
      success: true,
      recordId: emp.recordId || emp[0],
      createdTimestamp: emp.createdTimestamp || emp[1],
      modifiedTimestamp: emp.modifiedTimestamp || emp[2],
      modifiedBy: emp.modifiedBy || emp[3],
      allData: JSON.parse(emp.allData || emp[4] || '{}')
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /attendances - Get all attendances from blockchain
app.get("/attendances", async (req, res) => {
  try {
    const contracts = getDeployedContracts();
    const address = contracts.AttendanceStorage;

    if (!address) {
      return res.status(404).json({ success: false, error: "AttendanceStorage not deployed" });
    }

    // Get all attendance IDs
    const ids = await read(address, 'getAllAttendanceIds');

    // Get full data for each attendance
    const attendances = [];
    for (const id of ids) {
      try {
        const att = await read(address, 'getAttendance', id);
        attendances.push({
          recordId: att.recordId || att[0],
          createdTimestamp: att.createdTimestamp || att[1],
          modifiedTimestamp: att.modifiedTimestamp || att[2],
          modifiedBy: att.modifiedBy || att[3],
          allData: JSON.parse(att.allData || att[4] || '{}')
        });
      } catch (e) {
        console.error(`Error getting attendance ${id}:`, e.message);
      }
    }

    res.json({
      success: true,
      total: attendances.length,
      attendances
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /attendances/:id - Get specific attendance by ID
app.get("/attendances/:id", async (req, res) => {
  try {
    const contracts = getDeployedContracts();
    const address = contracts.AttendanceStorage;

    if (!address) {
      return res.status(404).json({ success: false, error: "AttendanceStorage not deployed" });
    }

    const att = await read(address, 'getAttendance', req.params.id);

    res.json({
      success: true,
      recordId: att.recordId || att[0],
      createdTimestamp: att.createdTimestamp || att[1],
      modifiedTimestamp: att.modifiedTimestamp || att[2],
      modifiedBy: att.modifiedBy || att[3],
      allData: JSON.parse(att.allData || att[4] || '{}')
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /employees - Store employee data to EmployeeStorage
app.post("/employees", async (req, res) => {
  try {
    const { privateKey, employeeData } = req.body;

    if (!employeeData) {
      return res.status(400).json({
        success: false,
        error: "Missing required field: employeeData"
      });
    }

    if (!employeeData.recordId) {
      return res.status(400).json({
        success: false,
        error: "Missing required field: employeeData.recordId"
      });
    }

    const result = await storeEmployee(
      privateKey || DEFAULT_PRIVATE_KEY,
      employeeData
    );

    res.json({
      success: true,
      recordId: employeeData.recordId,
      transactionHash: result.transactionHash,
      blockNumber: result.blockNumber,
      blockchain: {
        transactionHash: result.transactionHash,
        blockNumber: result.blockNumber,
        contract: 'EmployeeStorage'
      }
    });
  } catch (error) {
    console.error("Employee store error:", error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// POST /attendances - Store attendance data to AttendanceStorage
app.post("/attendances", async (req, res) => {
  try {
    const { privateKey, attendanceData } = req.body;

    if (!attendanceData) {
      return res.status(400).json({
        success: false,
        error: "Missing required field: attendanceData"
      });
    }

    if (!attendanceData.recordId) {
      return res.status(400).json({
        success: false,
        error: "Missing required field: attendanceData.recordId"
      });
    }

    const result = await storeAttendance(
      privateKey || DEFAULT_PRIVATE_KEY,
      attendanceData
    );

    res.json({
      success: true,
      recordId: attendanceData.recordId,
      transactionHash: result.transactionHash,
      blockNumber: result.blockNumber,
      blockchain: {
        transactionHash: result.transactionHash,
        blockNumber: result.blockNumber,
        contract: 'AttendanceStorage'
      }
    });
  } catch (error) {
    console.error("Attendance store error:", error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Write/Store value to SimpleStorage
app.post("/store", async (req, res) => {
  try {
    const { privateKey, contractAddress, value } = req.body;

    if (!privateKey || !contractAddress || value === undefined) {
      return res.status(400).json({
        success: false,
        error: "Missing required fields: privateKey, contractAddress, value"
      });
    }

    let result = await store(privateKey, contractAddress, value);

    res.json({
      success: true,
      transactionHash: result.transactionHash,
      blockNumber: result.blockNumber,
      contractAddress: contractAddress,
      storedValue: value
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Read value from smart contract
app.get("/read", async (req, res) => {
  try {
    const { contractAddress, method, ...args } = req.query;

    if (!contractAddress) {
      return res.status(400).json({
        success: false,
        error: "Missing required parameter: contractAddress"
      });
    }

    // Convert query args to array of values (for method arguments)
    const methodArgs = Object.keys(args)
      .filter(key => key.startsWith('arg'))
      .sort()
      .map(key => args[key]);

    let result = await read(contractAddress, method || 'retrieve', ...methodArgs);

    // Clean up result: remove numeric keys (keep only named parameters)
    let cleanResult = result;
    if (typeof result === 'object' && result !== null) {
      cleanResult = {};
      for (const key in result) {
        if (isNaN(key)) {
          cleanResult[key] = result[key];
        }
      }
    }

    res.json({
      success: true,
      contractAddress: contractAddress,
      method: method || 'retrieve',
      value: cleanResult
    });
  } catch (error) {
    console.error("[ERROR] Read error:", error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Get all registered contracts
app.get("/contracts", async (req, res) => {
  try {
    const result = await getRegisteredContracts();

    res.json({
      success: true,
      ...result
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Get all deployed contracts
app.get("/contracts/deployed", async (req, res) => {
  try {
    const contracts = getDeployedContracts();

    res.json({
      success: true,
      contracts
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.listen(4000, () => {
  console.log("Server started on http://localhost:4000");
});