/**
 * AdapterFactory - Creates appropriate adapter based on database type
 * 
 * Factory pattern for instantiating database-specific adapters.
 */

const MySQLAdapter = require('./MySQLAdapter');
const PostgresAdapter = require('./PostgresAdapter');
const MongoDBAdapter = require('./MongoDBAdapter');
const SQLServerAdapter = require('./SQLServerAdapter');
const BaseAdapter = require('./BaseAdapter');

class AdapterFactory {
  /**
   * Create an adapter for the given app configuration
   * @param {object} appConfig - App configuration from registry
   * @returns {BaseAdapter} Database-specific adapter
   */
  static create(appConfig) {
    const dbType = appConfig?.database?.type?.toLowerCase();

    switch (dbType) {
      case 'mysql':
      case 'mariadb':
        return new MySQLAdapter(appConfig);

      case 'postgres':
      case 'postgresql':
        return new PostgresAdapter(appConfig);

      case 'mongodb':
      case 'mongo':
        return new MongoDBAdapter(appConfig);

      case 'sqlserver':
      case 'mssql':
        return new SQLServerAdapter(appConfig);

      default:
        console.warn(`Unknown database type: ${dbType}, using BaseAdapter`);
        return new BaseAdapter(appConfig);
    }
  }

  /**
   * Get adapter class by database type (without instantiation)
   * @param {string} dbType - Database type
   * @returns {class} Adapter class
   */
  static getAdapterClass(dbType) {
    const type = dbType?.toLowerCase();

    switch (type) {
      case 'mysql':
      case 'mariadb':
        return MySQLAdapter;

      case 'postgres':
      case 'postgresql':
        return PostgresAdapter;

      case 'mongodb':
      case 'mongo':
        return MongoDBAdapter;

      case 'sqlserver':
      case 'mssql':
        return SQLServerAdapter;

      default:
        return BaseAdapter;
    }
  }

  /**
   * Check if a database type is supported
   * @param {string} dbType - Database type
   * @returns {boolean}
   */
  static isSupported(dbType) {
    const supportedTypes = [
      'mysql', 'mariadb',
      'postgres', 'postgresql',
      'mongodb', 'mongo',
      'sqlserver', 'mssql'
    ];
    return supportedTypes.includes(dbType?.toLowerCase());
  }

  /**
   * Get list of supported database types
   * @returns {Array<string>}
   */
  static getSupportedTypes() {
    return ['mysql', 'postgres', 'mongodb', 'sqlserver'];
  }
}

module.exports = AdapterFactory;
