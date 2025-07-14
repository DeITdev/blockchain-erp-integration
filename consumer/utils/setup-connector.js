#!/usr/bin/env node

const path = require('path');
const dotenvResult = require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const DynamicConnectorSetup = require('./dynamic-connector-setup');

async function listDatabases() {
  console.log('Scanning for ERPNext databases...\n');

  const setup = new DynamicConnectorSetup();

  try {
    const databases = await setup.discoverERPNextDatabases();

    if (databases.length === 0) {
      console.log('No ERPNext databases found!');
      return;
    }

    console.log('Found ERPNext databases:\n');

    for (const db of databases) {
      console.log(`Database: ${db}`);

      try {
        const tables = await setup.getAvailableTables(db);
        const targetTables = (process.env.TARGET_TABLES || 'tabEmployee,tabUser').split(',');
        const availableTargets = tables.filter(table => targetTables.includes(table));

        console.log(`   Total tables: ${tables.length}`);
        console.log(`   Target tables available: ${availableTargets.join(', ') || 'none'}`);

      } catch (error) {
        console.log(`   Error accessing tables: ${error.message}`);
      }

      console.log('');
    }

  } catch (error) {
    console.error('Error listing databases:', error.message);
  }
}

async function main() {
  const command = process.argv[2] || 'help';

  console.log('ERPNext Dynamic Connector Setup\n');

  if (command === 'list') {
    await listDatabases();
  } else if (command === 'auto') {
    try {
      const setup = new DynamicConnectorSetup();
      await setup.autoSetup();
      console.log('\nAuto-setup completed!');
    } catch (error) {
      console.error('Auto-setup failed:', error.message);
      process.exit(1);
    }
  } else {
    console.log('Usage: node setup-connector.js [list|auto]');
  }
}

if (require.main === module) {
  main().catch(error => {
    console.error('\nUnexpected error:', error);
    process.exit(1);
  });
}