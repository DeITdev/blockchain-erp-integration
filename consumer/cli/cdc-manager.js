#!/usr/bin/env node

/**
 * CDC Manager CLI - Command-line tool for managing CDC sources
 * 
 * Usage:
 *   node cli/cdc-manager.js <command> [options]
 * 
 * Commands:
 *   list-apps      List all registered app configurations
 *   discover       Discover database schema and generate config
 *   add-app        Add a new app from config file
 *   setup          Setup Debezium connector for an app
 *   status         Show connector status
 *   delete         Delete a connector
 */

const { Command } = require('commander');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const { getRegistry } = require('../config/registry');
const SchemaDiscoverer = require('../utils/schema-discoverer');
const DynamicConnectorSetup = require('../utils/dynamic-connector-setup');
const AdapterFactory = require('../adapters/AdapterFactory');

const program = new Command();

program
  .name('cdc-manager')
  .description('CLI tool for managing CDC (Change Data Capture) sources')
  .version('1.0.0');

// ============================================================
// list-apps command
// ============================================================
program
  .command('list-apps')
  .description('List all registered app configurations')
  .action(async () => {
    try {
      const registry = await getRegistry();
      const apps = registry.getAllApps();

      if (apps.length === 0) {
        console.log('No app configurations found.');
        console.log('Use "cdc-manager discover" to create a new configuration.');
        return;
      }

      console.log('\n=== Registered CDC Apps ===\n');

      for (const app of apps) {
        console.log(`📦 ${app.displayName || app.name}`);
        console.log(`   Name: ${app.name}`);
        console.log(`   Database: ${app.database.type}`);
        console.log(`   API Endpoint: ${app.blockchain?.apiEndpoint || 'Not configured'}`);
        console.log(`   Topic Prefix: ${app.kafka?.topicPrefix || app.name}`);
        console.log(`   Tables: ${app.tables?.length || 0}`);
        if (app.tables?.length > 0) {
          console.log(`   - ${app.tables.map(t => t.name).join('\n   - ')}`);
        }
        console.log();
      }
    } catch (error) {
      console.error('Error:', error.message);
      process.exit(1);
    }
  });

// ============================================================
// discover command
// ============================================================
program
  .command('discover')
  .description('Discover database schema and generate configuration')
  .requiredOption('--type <type>', 'Database type (mysql, postgres, mongodb, sqlserver)')
  .requiredOption('--host <host>', 'Database host')
  .requiredOption('--database <database>', 'Database name')
  .option('--port <port>', 'Database port')
  .option('--user <user>', 'Database user', 'root')
  .option('--password <password>', 'Database password', '')
  .option('--name <name>', 'App name for configuration')
  .option('--output <path>', 'Output file path')
  .option('--tables <tables>', 'Comma-separated list of tables to include')
  .action(async (options) => {
    try {
      const discoverer = new SchemaDiscoverer();

      console.log(`\nConnecting to ${options.type} database: ${options.database}...`);

      await discoverer.connect(options.type, {
        host: options.host,
        port: options.port ? parseInt(options.port) : undefined,
        user: options.user,
        password: options.password,
        database: options.database
      });

      console.log('\nDiscovering tables...');
      const tables = await discoverer.discoverTables();

      console.log(`\nFound ${tables.length} tables:`);
      tables.forEach((t, i) => {
        console.log(`  ${i + 1}. ${t.name} (${t.rowCount} rows)`);
      });

      // Determine which tables to include
      let selectedTables;
      if (options.tables) {
        selectedTables = options.tables.split(',').map(t => t.trim());
      } else {
        // Interactive selection or use all
        selectedTables = tables.map(t => t.name);
        console.log('\nUsing all tables. Use --tables to specify specific tables.');
      }

      // Generate config
      const appName = options.name || options.database.replace(/[^a-z0-9]/gi, '-').toLowerCase();
      console.log(`\nGenerating configuration for: ${appName}...`);

      const config = await discoverer.generateConfig(appName, selectedTables, {
        displayName: options.name || options.database
      });

      // Save config
      const outputPath = discoverer.saveConfig(config, options.output);

      await discoverer.close();

      console.log('\n✓ Configuration generated successfully!');
      console.log(`\nNext steps:`);
      console.log(`  1. Review the configuration: ${outputPath}`);
      console.log(`  2. Set up the connector: cdc-manager setup --app ${appName}`);
      console.log(`  3. Start the consumer: npm start`);

    } catch (error) {
      console.error('Error:', error.message);
      process.exit(1);
    }
  });

// ============================================================
// setup command
// ============================================================
program
  .command('setup')
  .description('Setup Debezium connector for an app')
  .requiredOption('--app <name>', 'App name from configuration')
  .requiredOption('--host <host>', 'Database host')
  .requiredOption('--database <database>', 'Database name')
  .option('--port <port>', 'Database port')
  .option('--user <user>', 'Database user', 'root')
  .option('--password <password>', 'Database password', '')
  .option('--kafka-connect <url>', 'Kafka Connect URL', 'http://localhost:8083')
  .action(async (options) => {
    try {
      const registry = await getRegistry();
      const appConfig = registry.getApp(options.app);

      if (!appConfig) {
        console.error(`App configuration not found: ${options.app}`);
        console.log('Available apps:');
        registry.getAllApps().forEach(a => console.log(`  - ${a.name}`));
        process.exit(1);
      }

      const connectorSetup = new DynamicConnectorSetup({
        kafkaConnectUrl: options.kafkaConnect
      });

      console.log(`\nSetting up CDC connector for: ${appConfig.displayName || options.app}`);
      console.log(`Database Type: ${appConfig.database.type}`);

      const success = await connectorSetup.setupFromAppConfig(options.app, {
        host: options.host,
        port: options.port ? parseInt(options.port) : undefined,
        user: options.user,
        password: options.password,
        database: options.database
      });

      if (success) {
        console.log('\n✓ Connector setup complete!');
        console.log('\nThe consumer will now receive CDC events from this database.');
      } else {
        console.log('\n✗ Connector setup failed. Check the error messages above.');
        process.exit(1);
      }

    } catch (error) {
      console.error('Error:', error.message);
      process.exit(1);
    }
  });

// ============================================================
// status command
// ============================================================
program
  .command('status')
  .description('Show status of all connectors')
  .option('--kafka-connect <url>', 'Kafka Connect URL', 'http://localhost:8083')
  .action(async (options) => {
    try {
      const connectorSetup = new DynamicConnectorSetup({
        kafkaConnectUrl: options.kafkaConnect
      });

      // Get cluster info
      const clusterInfo = await connectorSetup.getClusterInfo();
      if (clusterInfo) {
        console.log('\n=== Kafka Connect Cluster ===');
        console.log(`Version: ${clusterInfo.version}`);
        console.log(`Commit: ${clusterInfo.commit}`);
      }

      // List connectors
      const connectors = await connectorSetup.listConnectors();

      console.log('\n=== Connectors ===\n');

      if (connectors.length === 0) {
        console.log('No connectors registered.');
        return;
      }

      for (const name of connectors) {
        const status = await connectorSetup.getConnectorStatus(name);

        const stateEmoji = {
          'RUNNING': '🟢',
          'PAUSED': '🟡',
          'FAILED': '🔴',
          'UNASSIGNED': '⚪'
        };

        const state = status?.connector?.state || 'UNKNOWN';
        console.log(`${stateEmoji[state] || '⚪'} ${name}`);
        console.log(`   State: ${state}`);

        if (status?.tasks) {
          for (const task of status.tasks) {
            console.log(`   Task ${task.id}: ${task.state}`);
            if (task.trace) {
              console.log(`   Error: ${task.trace.substring(0, 100)}...`);
            }
          }
        }
        console.log();
      }

    } catch (error) {
      console.error('Error:', error.message);
      process.exit(1);
    }
  });

// ============================================================
// delete command
// ============================================================
program
  .command('delete')
  .description('Delete a connector')
  .requiredOption('--name <name>', 'Connector name to delete')
  .option('--kafka-connect <url>', 'Kafka Connect URL', 'http://localhost:8083')
  .action(async (options) => {
    try {
      const connectorSetup = new DynamicConnectorSetup({
        kafkaConnectUrl: options.kafkaConnect
      });

      console.log(`Deleting connector: ${options.name}...`);
      const success = await connectorSetup.deleteConnector(options.name);

      if (success) {
        console.log('✓ Connector deleted successfully!');
      } else {
        console.log('✗ Failed to delete connector.');
        process.exit(1);
      }

    } catch (error) {
      console.error('Error:', error.message);
      process.exit(1);
    }
  });

// ============================================================
// supported-dbs command
// ============================================================
program
  .command('supported-dbs')
  .description('List supported database types')
  .action(() => {
    console.log('\n=== Supported Database Types ===\n');

    const dbs = [
      { type: 'mysql', name: 'MySQL / MariaDB', port: 3306 },
      { type: 'postgres', name: 'PostgreSQL', port: 5432 },
      { type: 'mongodb', name: 'MongoDB', port: 27017 },
      { type: 'sqlserver', name: 'Microsoft SQL Server', port: 1433 }
    ];

    for (const db of dbs) {
      console.log(`📊 ${db.name}`);
      console.log(`   Type: ${db.type}`);
      console.log(`   Default Port: ${db.port}`);
      console.log();
    }

    console.log('Use "cdc-manager discover --type <type>" to discover schemas.');
  });

// ============================================================
// generate-template command
// ============================================================
program
  .command('generate-template')
  .description('Generate a template configuration file')
  .requiredOption('--type <type>', 'Database type (mysql, postgres, mongodb, sqlserver)')
  .requiredOption('--name <name>', 'App name')
  .option('--output <path>', 'Output file path')
  .action(async (options) => {
    try {
      const yaml = require('js-yaml');

      const template = {
        name: options.name,
        displayName: options.name.charAt(0).toUpperCase() + options.name.slice(1),
        description: `CDC configuration for ${options.name}`,

        database: {
          type: options.type,
          tablePrefix: '',
          idField: 'id',
          timestampFields: {
            created: 'created_at',
            modified: 'updated_at',
            modifiedBy: 'updated_by'
          },
          timezoneOffsetHours: 0
        },

        blockchain: {
          apiEndpoint: '${API_ENDPOINT:-http://127.0.0.1:4001}'
        },

        kafka: {
          topicPrefix: options.name
        },

        tables: [
          {
            name: 'example_table',
            displayName: 'Example Table',
            endpoint: '/examples',
            dataKey: 'exampleData',
            fields: []
          }
        ]
      };

      const yamlContent = yaml.dump(template, { indent: 2, lineWidth: 120 });

      const outputPath = options.output ||
        path.join(__dirname, '..', 'config', 'apps', `${options.name}.yaml`);

      const dir = path.dirname(outputPath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      fs.writeFileSync(outputPath, yamlContent);

      console.log(`\n✓ Template generated: ${outputPath}`);
      console.log('\nEdit this file to configure your database tables and fields.');

    } catch (error) {
      console.error('Error:', error.message);
      process.exit(1);
    }
  });

// Parse arguments
program.parse();
