/**
 * Dynamic Connector Setup - Multi-database Debezium connector management
 * 
 * Supports MySQL, PostgreSQL, MongoDB, and SQL Server connectors.
 * Uses app configuration from the registry for connector setup.
 */

const axios = require('axios');
const mysql = require('mysql2/promise');
const { getRegistry } = require('../config/registry');

class DynamicConnectorSetup {
  constructor(options = {}) {
    this.kafkaConnectUrl = options.kafkaConnectUrl || process.env.KAFKA_CONNECT_URL || 'http://localhost:8083';
  }

  /**
   * Debezium connector class mapping
   */
  static CONNECTOR_CLASSES = {
    mysql: 'io.debezium.connector.mysql.MySqlConnector',
    mariadb: 'io.debezium.connector.mysql.MySqlConnector',
    postgres: 'io.debezium.connector.postgresql.PostgresConnector',
    postgresql: 'io.debezium.connector.postgresql.PostgresConnector',
    mongodb: 'io.debezium.connector.mongodb.MongoDbConnector',
    mongo: 'io.debezium.connector.mongodb.MongoDbConnector',
    sqlserver: 'io.debezium.connector.sqlserver.SqlServerConnector',
    mssql: 'io.debezium.connector.sqlserver.SqlServerConnector'
  };

  /**
   * Get connector class for database type
   */
  getConnectorClass(dbType) {
    const connectorClass = DynamicConnectorSetup.CONNECTOR_CLASSES[dbType?.toLowerCase()];
    if (!connectorClass) {
      throw new Error(`Unsupported database type: ${dbType}`);
    }
    return connectorClass;
  }

  /**
   * Create connector configuration from app config
   * @param {object} appConfig - App configuration from registry
   * @param {object} dbCredentials - Database credentials
   * @returns {object} Debezium connector configuration
   */
  createConnectorConfig(appConfig, dbCredentials) {
    const dbType = appConfig.database.type.toLowerCase();
    const connectorClass = this.getConnectorClass(dbType);

    const connectorName = `${appConfig.name}-cdc-connector`;
    const topicPrefix = appConfig.kafka?.topicPrefix || appConfig.name;

    // Build table list
    const tables = appConfig.tables?.map(t => t.name) || [];
    const tableList = tables.map(t => `${dbCredentials.database}.${t}`).join(',');

    // Generate unique server ID
    const serverIdBase = Math.abs(appConfig.name.split('').reduce((hash, char) => {
      return ((hash << 5) - hash) + char.charCodeAt(0);
    }, 0));
    const serverId = 184000 + (serverIdBase % 1000);

    // Convert localhost to host.docker.internal for Docker containers
    let dbHost = dbCredentials.host || 'localhost';
    if (dbHost === 'localhost' || dbHost === '127.0.0.1') {
      dbHost = 'host.docker.internal';
    }

    // Base configuration
    const baseConfig = {
      name: connectorName,
      config: {
        'connector.class': connectorClass,
        'tasks.max': '1',
        'topic.prefix': topicPrefix,

        // Transforms
        'transforms': 'unwrap,addTimestamp',
        'transforms.unwrap.type': 'io.debezium.transforms.ExtractNewRecordState',
        'transforms.unwrap.drop.tombstones': 'false',
        'transforms.unwrap.delete.handling.mode': 'rewrite',
        'transforms.addTimestamp.type': 'org.apache.kafka.connect.transforms.InsertField$Value',
        'transforms.addTimestamp.timestamp.field': 'connector_processing_time',

        // Schema history
        'schema.history.internal.kafka.bootstrap.servers': 'kafka:9092',
        'schema.history.internal.kafka.topic': `schema-changes.${topicPrefix}`,
        'schema.history.internal.consumer.security.protocol': 'PLAINTEXT',
        'schema.history.internal.producer.security.protocol': 'PLAINTEXT',
        'include.schema.changes': 'true',

        // Producer optimizations
        'producer.override.compression.type': 'gzip',
        'producer.override.acks': '1',
        'producer.override.batch.size': '1024',
        'producer.override.linger.ms': '5',

        // Error handling
        'errors.tolerance': 'none',
        'errors.log.enable': 'true',
        'errors.log.include.messages': 'true',

        // Snapshot settings
        'snapshot.mode': 'initial',
        'snapshot.locking.mode': 'none'
      }
    };

    // Add database-specific configuration
    switch (dbType) {
      case 'mysql':
      case 'mariadb':
        return this.addMySQLConfig(baseConfig, dbHost, dbCredentials, tableList, serverId);
      case 'postgres':
      case 'postgresql':
        return this.addPostgresConfig(baseConfig, dbHost, dbCredentials, tableList);
      case 'mongodb':
      case 'mongo':
        return this.addMongoDBConfig(baseConfig, dbHost, dbCredentials, tables);
      case 'sqlserver':
      case 'mssql':
        return this.addSQLServerConfig(baseConfig, dbHost, dbCredentials, tableList);
      default:
        throw new Error(`Unsupported database type: ${dbType}`);
    }
  }

  addMySQLConfig(config, host, credentials, tableList, serverId) {
    Object.assign(config.config, {
      'database.hostname': host,
      'database.port': String(credentials.port || 3306),
      'database.user': credentials.user,
      'database.password': credentials.password,
      'database.server.id': String(serverId),
      'database.include.list': credentials.database,
      'table.include.list': tableList,
      'database.allowPublicKeyRetrieval': 'true',
      'decimal.handling.mode': 'string',
      'bigint.unsigned.handling.mode': 'long',
      'time.precision.mode': 'adaptive_time_microseconds',
      'provide.transaction.metadata': 'true',
      'binlog.buffer.size': '32768',
      'max.batch.size': '2048',
      'max.queue.size': '8192'
    });
    return config;
  }

  addPostgresConfig(config, host, credentials, tableList) {
    Object.assign(config.config, {
      'database.hostname': host,
      'database.port': String(credentials.port || 5432),
      'database.user': credentials.user,
      'database.password': credentials.password,
      'database.dbname': credentials.database,
      'table.include.list': tableList,
      'plugin.name': 'pgoutput',
      'slot.name': `${config.name.replace(/[^a-z0-9]/g, '_')}_slot`,
      'publication.name': 'dbz_publication',
      'decimal.handling.mode': 'string',
      'time.precision.mode': 'adaptive_time_microseconds',
      'provide.transaction.metadata': 'true'
    });
    return config;
  }

  addMongoDBConfig(config, host, credentials, collections) {
    const connectionString = credentials.connectionString ||
      `mongodb://${credentials.user}:${credentials.password}@${host}:${credentials.port || 27017}`;

    Object.assign(config.config, {
      'mongodb.connection.string': connectionString,
      'mongodb.name': credentials.database,
      'collection.include.list': collections.map(c => `${credentials.database}.${c}`).join(','),
      'capture.mode': 'change_streams_update_full',
      'snapshot.mode': 'initial'
    });

    // Remove SQL-specific transforms
    delete config.config['transforms'];
    delete config.config['transforms.unwrap.type'];
    delete config.config['transforms.unwrap.drop.tombstones'];
    delete config.config['transforms.unwrap.delete.handling.mode'];
    delete config.config['transforms.addTimestamp.type'];
    delete config.config['transforms.addTimestamp.timestamp.field'];

    return config;
  }

  addSQLServerConfig(config, host, credentials, tableList) {
    Object.assign(config.config, {
      'database.hostname': host,
      'database.port': String(credentials.port || 1433),
      'database.user': credentials.user,
      'database.password': credentials.password,
      'database.dbname': credentials.database,
      'table.include.list': tableList,
      'database.encrypt': 'false',
      'database.trustServerCertificate': 'true',
      'decimal.handling.mode': 'string',
      'time.precision.mode': 'adaptive_time_microseconds'
    });
    return config;
  }

  /**
   * Register a connector with Kafka Connect
   */
  async registerConnector(connectorConfig) {
    try {
      console.log(`Registering connector: ${connectorConfig.name}`);
      console.log(`Database Type: ${connectorConfig.config['connector.class']}`);

      // Delete existing connector if present
      try {
        await axios.delete(`${this.kafkaConnectUrl}/connectors/${connectorConfig.name}`);
        console.log('Deleted existing connector');
        await new Promise(resolve => setTimeout(resolve, 2000));
      } catch (error) {
        // Connector doesn't exist, which is fine
      }

      // Create new connector
      const response = await axios.post(
        `${this.kafkaConnectUrl}/connectors`,
        connectorConfig,
        { headers: { 'Content-Type': 'application/json' } }
      );

      console.log('✓ Connector registered successfully!');

      // Check status
      await new Promise(resolve => setTimeout(resolve, 3000));
      const status = await this.getConnectorStatus(connectorConfig.name);

      console.log(`Connector Status: ${status?.connector?.state || 'UNKNOWN'}`);
      if (status?.tasks?.[0]) {
        console.log(`Task Status: ${status.tasks[0].state}`);
        if (status.tasks[0].trace) {
          console.log(`Task Error: ${status.tasks[0].trace.substring(0, 200)}...`);
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

  /**
   * Setup connector from app configuration
   */
  async setupFromAppConfig(appName, dbCredentials) {
    const registry = await getRegistry();
    const appConfig = registry.getApp(appName);

    if (!appConfig) {
      throw new Error(`App configuration not found: ${appName}`);
    }

    console.log(`Setting up CDC connector for: ${appConfig.displayName || appName}`);
    console.log(`Database Type: ${appConfig.database.type}`);
    console.log(`Tables: ${appConfig.tables?.map(t => t.name).join(', ') || 'all'}`);

    const connectorConfig = this.createConnectorConfig(appConfig, dbCredentials);
    return await this.registerConnector(connectorConfig);
  }

  /**
   * Auto-setup for ERPNext (legacy support)
   */
  async autoSetupERPNext(dbCredentials) {
    return await this.setupFromAppConfig('erpnext', dbCredentials);
  }

  /**
   * List all connectors
   */
  async listConnectors() {
    try {
      const response = await axios.get(`${this.kafkaConnectUrl}/connectors`);
      return response.data;
    } catch (error) {
      console.error('Error listing connectors:', error.message);
      return [];
    }
  }

  /**
   * Get connector status
   */
  async getConnectorStatus(connectorName) {
    try {
      const response = await axios.get(`${this.kafkaConnectUrl}/connectors/${connectorName}/status`);
      return response.data;
    } catch (error) {
      console.error(`Error getting status for ${connectorName}:`, error.message);
      return null;
    }
  }

  /**
   * Delete a connector
   */
  async deleteConnector(connectorName) {
    try {
      await axios.delete(`${this.kafkaConnectUrl}/connectors/${connectorName}`);
      console.log(`Deleted connector: ${connectorName}`);
      return true;
    } catch (error) {
      console.error(`Error deleting connector ${connectorName}:`, error.message);
      return false;
    }
  }

  /**
   * Restart a connector
   */
  async restartConnector(connectorName) {
    try {
      await axios.post(`${this.kafkaConnectUrl}/connectors/${connectorName}/restart`);
      console.log(`Restarted connector: ${connectorName}`);
      return true;
    } catch (error) {
      console.error(`Error restarting connector ${connectorName}:`, error.message);
      return false;
    }
  }

  /**
   * Get Kafka Connect cluster info
   */
  async getClusterInfo() {
    try {
      const response = await axios.get(this.kafkaConnectUrl);
      return response.data;
    } catch (error) {
      console.error('Error getting cluster info:', error.message);
      return null;
    }
  }

  /**
   * Get available connector plugins
   */
  async getConnectorPlugins() {
    try {
      const response = await axios.get(`${this.kafkaConnectUrl}/connector-plugins`);
      return response.data;
    } catch (error) {
      console.error('Error getting connector plugins:', error.message);
      return [];
    }
  }
}

module.exports = DynamicConnectorSetup;