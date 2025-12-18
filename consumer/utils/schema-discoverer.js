/**
 * Schema Discoverer - Auto-discover database schema for CDC configuration
 * 
 * Connects to databases and discovers tables/columns to auto-generate
 * app configuration files.
 */

const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

class SchemaDiscoverer {
  constructor() {
    this.connection = null;
    this.dbType = null;
    this.dbConfig = null;
  }

  /**
   * Connect to a database
   * @param {string} dbType - Database type (mysql, postgres, mongodb, sqlserver)
   * @param {object} config - Connection configuration
   */
  async connect(dbType, config) {
    this.dbType = dbType.toLowerCase();
    this.dbConfig = config;

    switch (this.dbType) {
      case 'mysql':
      case 'mariadb':
        await this.connectMySQL(config);
        break;
      case 'postgres':
      case 'postgresql':
        await this.connectPostgres(config);
        break;
      case 'mongodb':
      case 'mongo':
        await this.connectMongoDB(config);
        break;
      case 'sqlserver':
      case 'mssql':
        await this.connectSQLServer(config);
        break;
      default:
        throw new Error(`Unsupported database type: ${dbType}`);
    }
  }

  async connectMySQL(config) {
    this.connection = await mysql.createConnection({
      host: config.host || 'localhost',
      port: config.port || 3306,
      user: config.user || 'root',
      password: config.password || '',
      database: config.database
    });
    console.log(`Connected to MySQL: ${config.database}`);
  }

  async connectPostgres(config) {
    // Note: Requires 'pg' package to be installed
    try {
      const { Client } = require('pg');
      this.connection = new Client({
        host: config.host || 'localhost',
        port: config.port || 5432,
        user: config.user || 'postgres',
        password: config.password || '',
        database: config.database
      });
      await this.connection.connect();
      console.log(`Connected to PostgreSQL: ${config.database}`);
    } catch (error) {
      if (error.code === 'MODULE_NOT_FOUND') {
        throw new Error('PostgreSQL support requires the "pg" package. Run: npm install pg');
      }
      throw error;
    }
  }

  async connectMongoDB(config) {
    // Note: Requires 'mongodb' package to be installed
    try {
      const { MongoClient } = require('mongodb');
      const uri = config.connectionString || `mongodb://${config.host || 'localhost'}:${config.port || 27017}`;
      this.connection = new MongoClient(uri);
      await this.connection.connect();
      this.mongoDb = this.connection.db(config.database);
      console.log(`Connected to MongoDB: ${config.database}`);
    } catch (error) {
      if (error.code === 'MODULE_NOT_FOUND') {
        throw new Error('MongoDB support requires the "mongodb" package. Run: npm install mongodb');
      }
      throw error;
    }
  }

  async connectSQLServer(config) {
    // Note: Requires 'mssql' package to be installed
    try {
      const sql = require('mssql');
      this.connection = await sql.connect({
        server: config.host || 'localhost',
        port: config.port || 1433,
        user: config.user,
        password: config.password,
        database: config.database,
        options: {
          trustServerCertificate: true
        }
      });
      console.log(`Connected to SQL Server: ${config.database}`);
    } catch (error) {
      if (error.code === 'MODULE_NOT_FOUND') {
        throw new Error('SQL Server support requires the "mssql" package. Run: npm install mssql');
      }
      throw error;
    }
  }

  /**
   * Discover all tables in the database
   * @returns {Array<{name: string, rowCount: number}>}
   */
  async discoverTables() {
    switch (this.dbType) {
      case 'mysql':
      case 'mariadb':
        return await this.discoverMySQLTables();
      case 'postgres':
      case 'postgresql':
        return await this.discoverPostgresTables();
      case 'mongodb':
      case 'mongo':
        return await this.discoverMongoCollections();
      case 'sqlserver':
      case 'mssql':
        return await this.discoverSQLServerTables();
      default:
        return [];
    }
  }

  async discoverMySQLTables() {
    const [tables] = await this.connection.execute(`
      SELECT TABLE_NAME as name, TABLE_ROWS as rowCount
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = ?
      ORDER BY TABLE_NAME
    `, [this.dbConfig.database]);

    return tables.map(t => ({
      name: t.name,
      rowCount: t.rowCount || 0
    }));
  }

  async discoverPostgresTables() {
    const result = await this.connection.query(`
      SELECT tablename as name, 
             (SELECT count(*) FROM schemaname.tablename) as row_count
      FROM pg_tables
      WHERE schemaname = 'public'
      ORDER BY tablename
    `);

    return result.rows.map(t => ({
      name: t.name,
      rowCount: parseInt(t.row_count) || 0
    }));
  }

  async discoverMongoCollections() {
    const collections = await this.mongoDb.listCollections().toArray();
    const result = [];

    for (const col of collections) {
      const count = await this.mongoDb.collection(col.name).countDocuments();
      result.push({
        name: col.name,
        rowCount: count
      });
    }

    return result.sort((a, b) => a.name.localeCompare(b.name));
  }

  async discoverSQLServerTables() {
    const result = await this.connection.request().query(`
      SELECT t.name, 
             SUM(p.rows) as rowCount
      FROM sys.tables t
      INNER JOIN sys.partitions p ON t.object_id = p.object_id
      WHERE p.index_id IN (0, 1)
      GROUP BY t.name
      ORDER BY t.name
    `);

    return result.recordset.map(t => ({
      name: t.name,
      rowCount: t.rowCount || 0
    }));
  }

  /**
   * Discover columns for a table
   * @param {string} tableName - Table name
   * @returns {Array<{name: string, type: string, nullable: boolean}>}
   */
  async discoverColumns(tableName) {
    switch (this.dbType) {
      case 'mysql':
      case 'mariadb':
        return await this.discoverMySQLColumns(tableName);
      case 'postgres':
      case 'postgresql':
        return await this.discoverPostgresColumns(tableName);
      case 'mongodb':
      case 'mongo':
        return await this.discoverMongoFields(tableName);
      case 'sqlserver':
      case 'mssql':
        return await this.discoverSQLServerColumns(tableName);
      default:
        return [];
    }
  }

  async discoverMySQLColumns(tableName) {
    const [columns] = await this.connection.execute(`
      SELECT COLUMN_NAME as name, 
             DATA_TYPE as type,
             IS_NULLABLE as nullable,
             COLUMN_KEY as keyType
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      ORDER BY ORDINAL_POSITION
    `, [this.dbConfig.database, tableName]);

    return columns.map(c => ({
      name: c.name,
      type: c.type,
      nullable: c.nullable === 'YES',
      isPrimary: c.keyType === 'PRI'
    }));
  }

  async discoverPostgresColumns(tableName) {
    const result = await this.connection.query(`
      SELECT column_name as name,
             data_type as type,
             is_nullable as nullable
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = $1
      ORDER BY ordinal_position
    `, [tableName]);

    return result.rows.map(c => ({
      name: c.name,
      type: c.type,
      nullable: c.nullable === 'YES'
    }));
  }

  async discoverMongoFields(collectionName) {
    // Sample documents to infer schema
    const samples = await this.mongoDb.collection(collectionName)
      .find({})
      .limit(100)
      .toArray();

    const fieldMap = new Map();

    for (const doc of samples) {
      for (const [key, value] of Object.entries(doc)) {
        if (!fieldMap.has(key)) {
          fieldMap.set(key, {
            name: key,
            type: this.getMongoFieldType(value),
            nullable: false
          });
        }
      }
    }

    return Array.from(fieldMap.values());
  }

  getMongoFieldType(value) {
    if (value === null) return 'null';
    if (Array.isArray(value)) return 'array';
    if (value instanceof Date) return 'date';
    if (value && value._bsontype === 'ObjectId') return 'objectId';
    return typeof value;
  }

  async discoverSQLServerColumns(tableName) {
    const result = await this.connection.request().query(`
      SELECT COLUMN_NAME as name,
             DATA_TYPE as type,
             IS_NULLABLE as nullable
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = '${tableName}'
      ORDER BY ORDINAL_POSITION
    `);

    return result.recordset.map(c => ({
      name: c.name,
      type: c.type,
      nullable: c.nullable === 'YES'
    }));
  }

  /**
   * Generate app configuration for selected tables
   * @param {string} appName - App name
   * @param {Array<string>} selectedTables - Tables to include
   * @param {object} options - Additional options
   * @returns {object} Generated configuration
   */
  async generateConfig(appName, selectedTables, options = {}) {
    const config = {
      name: appName,
      displayName: options.displayName || this.titleCase(appName),
      description: options.description || `CDC configuration for ${appName}`,

      database: {
        type: this.normalizeDbType(this.dbType),
        tablePrefix: options.tablePrefix || '',
        idField: options.idField || 'id',
        timestampFields: {
          created: options.createdField || 'created_at',
          modified: options.modifiedField || 'updated_at',
          modifiedBy: options.modifiedByField || 'updated_by'
        },
        timezoneOffsetHours: options.timezoneOffset || 0
      },

      blockchain: {
        apiEndpoint: options.apiEndpoint || '${API_ENDPOINT:-http://127.0.0.1:4001}'
      },

      kafka: {
        topicPrefix: options.topicPrefix || appName
      },

      tables: []
    };

    // Discover schema for each selected table
    for (const tableName of selectedTables) {
      const columns = await this.discoverColumns(tableName);
      const fieldNames = columns
        .filter(c => !c.name.startsWith('_') || c.name === '_id')
        .map(c => c.name);

      // Find ID field
      const primaryCol = columns.find(c => c.isPrimary);
      if (primaryCol && config.database.idField === 'id') {
        config.database.idField = primaryCol.name;
      }

      config.tables.push({
        name: tableName,
        displayName: this.titleCase(tableName.replace(/^tab/, '')),
        endpoint: `/${tableName.toLowerCase().replace(/^tab/, '')}s`,
        dataKey: `${this.camelCase(tableName.replace(/^tab/, ''))}Data`,
        fields: fieldNames
      });
    }

    return config;
  }

  /**
   * Save configuration to file
   * @param {object} config - Configuration object
   * @param {string} outputPath - Output file path (optional)
   * @returns {string} Path to saved file
   */
  saveConfig(config, outputPath = null) {
    const fileName = outputPath || path.join(
      __dirname, '..', 'config', 'apps', `${config.name}.yaml`
    );

    // Ensure directory exists
    const dir = path.dirname(fileName);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    const yamlContent = yaml.dump(config, {
      indent: 2,
      lineWidth: 120,
      noRefs: true
    });

    fs.writeFileSync(fileName, yamlContent);
    console.log(`Configuration saved to: ${fileName}`);

    return fileName;
  }

  /**
   * Close database connection
   */
  async close() {
    if (this.connection) {
      switch (this.dbType) {
        case 'mysql':
        case 'mariadb':
          await this.connection.end();
          break;
        case 'postgres':
        case 'postgresql':
          await this.connection.end();
          break;
        case 'mongodb':
        case 'mongo':
          await this.connection.close();
          break;
        case 'sqlserver':
        case 'mssql':
          await this.connection.close();
          break;
      }
      this.connection = null;
      console.log('Database connection closed');
    }
  }

  // Helper methods
  normalizeDbType(type) {
    const typeMap = {
      'mariadb': 'mysql',
      'postgresql': 'postgres',
      'mongo': 'mongodb',
      'mssql': 'sqlserver'
    };
    return typeMap[type] || type;
  }

  titleCase(str) {
    return str
      .replace(/([A-Z])/g, ' $1')
      .replace(/[_-]/g, ' ')
      .split(' ')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join(' ')
      .trim();
  }

  camelCase(str) {
    return str
      .replace(/[_-](.)/g, (_, c) => c.toUpperCase())
      .replace(/^./, c => c.toLowerCase());
  }
}

module.exports = SchemaDiscoverer;
