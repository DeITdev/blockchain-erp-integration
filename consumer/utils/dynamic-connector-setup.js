const axios = require('axios');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');

class DynamicConnectorSetup {
  constructor() {
    this.dbConfig = {
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 3306,
      user: process.env.DB_USER || 'root',
      password: process.env.DB_PASSWORD || 'admin'
    };

    this.kafkaConnectUrl = process.env.KAFKA_CONNECT_URL;
    this.topicPrefix = process.env.TOPIC_PREFIX;
    this.targetTables = (process.env.TARGET_TABLES).split(',');
  }

  async discoverERPNextDatabases() {
    console.log('Discovering ERPNext databases...');

    try {
      const connection = await mysql.createConnection(this.dbConfig);

      const [databases] = await connection.execute('SHOW DATABASES');
      const erpNextDatabases = [];

      for (const db of databases) {
        const dbName = db.Database;

        if (['information_schema', 'performance_schema', 'mysql', 'sys'].includes(dbName)) {
          continue;
        }

        try {
          await connection.execute(`USE \`${dbName}\``);
          const [tables] = await connection.execute(`
            SELECT TABLE_NAME 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = ? 
            AND TABLE_NAME IN ('tabDocType', 'tabUser', 'tabEmployee', 'tabCompany')
          `, [dbName]);

          if (tables.length >= 2) {
            console.log(`Found ERPNext database: ${dbName}`);
            erpNextDatabases.push(dbName);
          }
        } catch (error) {
          console.log(`Skipped database ${dbName}: ${error.message}`);
        }
      }

      await connection.end();
      return erpNextDatabases;

    } catch (error) {
      console.error('Error discovering databases:', error.message);
      throw error;
    }
  }

  async getAvailableTables(databaseName) {
    try {
      const connection = await mysql.createConnection(this.dbConfig);
      await connection.execute(`USE \`${databaseName}\``);

      const [tables] = await connection.execute(`
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = ? 
        AND TABLE_NAME LIKE 'tab%'
        ORDER BY TABLE_NAME
      `, [databaseName]);

      await connection.end();
      return tables.map(t => t.TABLE_NAME);

    } catch (error) {
      console.error(`Error getting tables for database ${databaseName}:`, error.message);
      return [];
    }
  }

  createConnectorConfig(databaseName, targetTables = null) {
    const tables = targetTables || this.targetTables;
    const tableList = tables.map(table => `${databaseName}.${table}`).join(',');

    const serverIdBase = Math.abs(databaseName.split('').reduce((hash, char) => {
      return ((hash << 5) - hash) + char.charCodeAt(0);
    }, 0));
    const serverId = 184000 + (serverIdBase % 1000);

    // Convert localhost to host.docker.internal for Docker containers
    let dbHost = this.dbConfig.host;
    if (dbHost === 'localhost' || dbHost === '127.0.0.1') {
      dbHost = 'host.docker.internal';
    }

    return {
      name: `erpnext-cdc-${databaseName.replace(/[^a-zA-Z0-9]/g, '-')}`,
      config: {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": dbHost,
        "database.port": this.dbConfig.port.toString(),
        "database.user": this.dbConfig.user,
        "database.password": this.dbConfig.password,
        "database.server.id": serverId.toString(),
        "topic.prefix": this.topicPrefix,
        "database.include.list": databaseName,
        "table.include.list": tableList,
        "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
        "schema.history.internal.kafka.topic": `schema-changes.${this.topicPrefix}.${databaseName}`,
        "schema.history.internal.consumer.security.protocol": "PLAINTEXT",
        "schema.history.internal.producer.security.protocol": "PLAINTEXT",
        "include.schema.changes": "true",

        // === ENHANCED TIMESTAMP TRACKING ===
        // Enable transaction metadata for better timing
        "provide.transaction.metadata": "true",

        // Enhanced timestamp precision
        "source.timestamp.mode": "connector",
        "time.precision.mode": "adaptive_time_microseconds",

        // Better decimal and numeric handling
        "decimal.handling.mode": "string",
        "bigint.unsigned.handling.mode": "long",

        // Enhanced transforms for timestamp tracking
        "transforms": "unwrap,addTimestamp",

        // Original unwrap transform
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",

        // NEW: Add connector processing timestamp
        "transforms.addTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.addTimestamp.timestamp.field": "connector_processing_time",

        // Kafka producer optimizations for latency tracking
        "producer.override.compression.type": "gzip",
        "producer.override.acks": "1",
        "producer.override.batch.size": "1024",
        "producer.override.linger.ms": "5",
        "producer.override.request.timeout.ms": "10000",

        // Binary log optimizations
        "binlog.buffer.size": "32768",
        "max.batch.size": "2048",
        "max.queue.size": "8192",

        // Snapshot and general settings
        "snapshot.mode": "initial",
        "snapshot.locking.mode": "none",
        "database.allowPublicKeyRetrieval": "true",

        // Error handling
        "errors.tolerance": "none",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true"
      }
    };
  }

  async registerConnector(connectorConfig) {
    try {
      console.log(`Registering connector with enhanced timestamps: ${connectorConfig.name}`);
      console.log(`Target Database: ${connectorConfig.config['database.include.list']}`);
      console.log(`Target Tables: ${connectorConfig.config['table.include.list']}`);
      console.log(`Database Hostname: ${connectorConfig.config['database.hostname']}`);
      console.log(`Timestamp Mode: ${connectorConfig.config['source.timestamp.mode']}`);
      console.log(`Time Precision: ${connectorConfig.config['time.precision.mode']}`);

      try {
        await axios.delete(`${this.kafkaConnectUrl}/connectors/${connectorConfig.name}`);
        console.log('Deleted existing connector');
        await new Promise(resolve => setTimeout(resolve, 2000));
      } catch (error) {
        // Connector doesn't exist, which is fine
      }

      const response = await axios.post(
        `${this.kafkaConnectUrl}/connectors`,
        connectorConfig,
        {
          headers: {
            'Content-Type': 'application/json'
          }
        }
      );

      console.log('✓ Connector with enhanced timestamps registered successfully!');

      await new Promise(resolve => setTimeout(resolve, 3000));
      const statusResponse = await axios.get(`${this.kafkaConnectUrl}/connectors/${connectorConfig.name}/status`);

      console.log(`Connector Status: ${statusResponse.data.connector.state}`);
      if (statusResponse.data.tasks.length > 0) {
        console.log(`Task Status: ${statusResponse.data.tasks[0].state}`);
        if (statusResponse.data.tasks[0].trace) {
          console.log(`Task Error: ${statusResponse.data.tasks[0].trace}`);
        }
      }

      return true;

    } catch (error) {
      console.error('Error registering connector:', error.message);
      if (error.response) {
        console.error('Response:', JSON.stringify(error.response.data, null, 2));
      }
      return false;
    }
  }

  async autoSetup() {
    console.log('Auto-detecting ERPNext setup with enhanced timestamp tracking...');

    const databases = await this.discoverERPNextDatabases();

    if (databases.length === 0) {
      throw new Error('No ERPNext databases found');
    }

    const database = databases[0];
    console.log(`Auto-selected database: ${database}`);

    const availableTables = await this.getAvailableTables(database);
    const existingTargetTables = this.targetTables.filter(table =>
      availableTables.includes(table)
    );

    if (existingTargetTables.length === 0) {
      throw new Error(`No target tables found in ${database}. Available tables: ${availableTables.slice(0, 10).join(', ')}`);
    }

    console.log(`Found target tables: ${existingTargetTables.join(', ')}`);

    const connectorConfig = this.createConnectorConfig(database, existingTargetTables);
    const success = await this.registerConnector(connectorConfig);

    if (!success) {
      throw new Error('Failed to register connector');
    }

    console.log('\n=== Enhanced Timestamp Features Enabled ===');
    console.log('✓ Transaction metadata tracking');
    console.log('✓ Connector timestamp insertion');
    console.log('✓ Microsecond precision timing');
    console.log('✓ Optimized producer settings for latency');
    console.log('✓ Enhanced binary log buffering');

    return {
      database,
      tables: existingTargetTables,
      connectorName: connectorConfig.name,
      topics: existingTargetTables.map(table => `${this.topicPrefix}.${database}.${table}`),
      timestampFeatures: {
        transactionMetadata: true,
        connectorTimestamp: true,
        microsecondPrecision: true,
        optimizedLatency: true
      }
    };
  }

  // Additional utility methods for monitoring
  async listConnectors() {
    try {
      const response = await axios.get(`${this.kafkaConnectUrl}/connectors`);
      return response.data;
    } catch (error) {
      console.error('Error listing connectors:', error.message);
      return [];
    }
  }

  async getConnectorStatus(connectorName) {
    try {
      const response = await axios.get(`${this.kafkaConnectUrl}/connectors/${connectorName}/status`);
      return response.data;
    } catch (error) {
      console.error(`Error getting status for ${connectorName}:`, error.message);
      return null;
    }
  }

  async getConnectorConfig(connectorName) {
    try {
      const response = await axios.get(`${this.kafkaConnectUrl}/connectors/${connectorName}/config`);
      return response.data;
    } catch (error) {
      console.error(`Error getting config for ${connectorName}:`, error.message);
      return null;
    }
  }
}

module.exports = DynamicConnectorSetup;