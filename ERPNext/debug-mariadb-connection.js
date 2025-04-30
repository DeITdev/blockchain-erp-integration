/**
 * This script tests the connection to MariaDB and checks database/table existence
 * It helps diagnose issues with Debezium connector configuration
 */
const mysql = require('mysql2/promise');

async function testConnection() {
  console.log('Testing MariaDB connection for Debezium...');

  // Connection configuration - update these to match your environment
  const config = {
    host: 'localhost', // or host.docker.internal if running from Docker
    port: 3306,
    user: 'root',
    password: 'admin'
  };

  try {
    // Test basic connection
    console.log('Connecting to MariaDB server...');
    const connection = await mysql.createConnection(config);
    console.log('✅ Connection successful!');

    // Get server information
    const [serverInfoRows] = await connection.query('SELECT @@version as version, @@server_id as server_id');
    console.log(`\nServer version: ${serverInfoRows[0].version}`);
    console.log(`Server ID: ${serverInfoRows[0].server_id}`);

    // Check binary log status
    const [binlogRows] = await connection.query('SHOW VARIABLES LIKE "log_bin"');
    const binlogEnabled = binlogRows[0].Value === 'ON';
    console.log(`Binary logging enabled: ${binlogEnabled ? '✅ Yes' : '❌ No'}`);

    if (!binlogEnabled) {
      console.error('❌ Binary logging must be enabled for Debezium to work!');
    }

    // Check binlog format
    const [formatRows] = await connection.query('SHOW VARIABLES LIKE "binlog_format"');
    const correctFormat = formatRows[0].Value === 'ROW';
    console.log(`Binlog format is ROW: ${correctFormat ? '✅ Yes' : '❌ No'}`);

    if (!correctFormat) {
      console.error('❌ Binary log format must be ROW for Debezium to work!');
    }

    // List databases
    const [databases] = await connection.query('SHOW DATABASES');
    console.log('\nAvailable databases:');
    const dbNames = databases.map(db => db.Database);
    console.log(dbNames.join(', '));

    // Check for the specific database
    const dbName = '_5e5899d8398b5f7b';
    const dbExists = dbNames.includes(dbName);
    console.log(`\nDatabase '${dbName}' exists: ${dbExists ? '✅ Yes' : '❌ No'}`);

    if (dbExists) {
      // List tables in the database
      const [tables] = await connection.query(`SHOW TABLES FROM \`${dbName}\``);
      console.log(`\nTables in ${dbName}:`);

      // Display first 10 tables and total count
      const tableCount = tables.length;
      const displayTables = tables.slice(0, 10).map(t => Object.values(t)[0]);
      console.log(displayTables.join(', ') + (tableCount > 10 ? `, ... and ${tableCount - 10} more` : ''));

      // Check for tabUser table specifically
      const tableColumnName = `Tables_in_${dbName}`;
      const userTableExists = tables.some(t => t[tableColumnName] === 'tabUser');
      console.log(`\nTable 'tabUser' exists: ${userTableExists ? '✅ Yes' : '❌ No'}`);

      if (userTableExists) {
        // Check tabUser structure 
        const [columns] = await connection.query(`DESCRIBE \`${dbName}\`.\`tabUser\``);
        console.log('\nColumns in tabUser table:');
        columns.slice(0, 5).forEach(col => {
          console.log(`- ${col.Field} (${col.Type})`);
        });
        if (columns.length > 5) {
          console.log(`... and ${columns.length - 5} more columns`);
        }

        // Check if there's data in tabUser
        const [countResult] = await connection.query(`SELECT COUNT(*) as count FROM \`${dbName}\`.\`tabUser\``);
        const rowCount = countResult[0].count;
        console.log(`\nRows in tabUser table: ${rowCount}`);
      }
    }

    // Check user permissions
    console.log('\nChecking MySQL user permissions:');
    const [grants] = await connection.query('SHOW GRANTS FOR CURRENT_USER');
    grants.forEach(g => console.log(`- ${Object.values(g)[0]}`));

    await connection.end();

    // Final summary
    console.log('\n=== Connection Test Summary ===');
    if (binlogEnabled && correctFormat && dbExists && dbNames.includes(dbName)) {
      console.log('✅ Database is correctly configured for Debezium CDC!');
    } else {
      console.log('❌ Some requirements for Debezium CDC are not met:');
      if (!binlogEnabled) console.log('  - Binary logging is not enabled');
      if (!correctFormat) console.log('  - Binary log format is not ROW');
      if (!dbExists) console.log(`  - Database '${dbName}' not found`);
    }

  } catch (error) {
    console.error('Error connecting to MariaDB:');
    console.error(error.message);

    if (error.code === 'ECONNREFUSED') {
      console.log('\nTroubleshooting tips:');
      console.log('1. Ensure MariaDB container is running');
      console.log('2. Check if port 3306 is exposed and not blocked by firewall');
      console.log('3. If using host.docker.internal, ensure your Docker setup supports this hostname');
    }
  }
}

testConnection().catch(console.error);