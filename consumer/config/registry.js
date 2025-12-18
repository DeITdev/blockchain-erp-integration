/**
 * App Registry - Central configuration management for CDC apps
 * 
 * Loads YAML configurations and provides runtime access to app settings.
 * Supports multiple database types and configurable blockchain endpoints.
 */

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

class AppRegistry {
  constructor() {
    this.apps = new Map();
    this.topicToApp = new Map();
    this.configDir = path.join(__dirname, 'apps');
  }

  /**
   * Initialize registry by loading all app configs from the apps directory
   */
  async init() {
    await this.loadAllConfigs();
    return this;
  }

  /**
   * Load all YAML configs from the apps directory
   */
  async loadAllConfigs() {
    if (!fs.existsSync(this.configDir)) {
      fs.mkdirSync(this.configDir, { recursive: true });
      console.log(`Created config directory: ${this.configDir}`);
      return;
    }

    const files = fs.readdirSync(this.configDir);

    for (const file of files) {
      if (file.endsWith('.yaml') || file.endsWith('.yml')) {
        // Skip example template
        if (file === 'example.yaml' || file === 'example.yml') {
          continue;
        }

        try {
          const filePath = path.join(this.configDir, file);
          await this.loadConfig(filePath);
        } catch (error) {
          console.error(`Failed to load config ${file}:`, error.message);
        }
      }
    }

    console.log(`Loaded ${this.apps.size} app configuration(s)`);
  }

  /**
   * Load a single config file
   * @param {string} filePath - Path to YAML config file
   */
  async loadConfig(filePath) {
    const content = fs.readFileSync(filePath, 'utf8');
    let config = yaml.load(content);

    // Resolve environment variables in config
    config = this.resolveEnvVariables(config);

    // Validate required fields
    this.validateConfig(config);

    // Build table lookup map
    config.tableMap = new Map();
    if (config.tables) {
      for (const table of config.tables) {
        config.tableMap.set(table.name, table);
      }
    }

    // Register the app
    this.apps.set(config.name, config);

    // Map topic prefix to app
    const topicPrefix = config.kafka?.topicPrefix || config.name;
    this.topicToApp.set(topicPrefix, config.name);

    console.log(`Loaded app config: ${config.displayName || config.name} (${config.database.type})`);

    return config;
  }

  /**
   * Resolve ${VAR:-default} patterns in config values
   */
  resolveEnvVariables(obj) {
    if (typeof obj === 'string') {
      return obj.replace(/\$\{([^}]+)\}/g, (match, expr) => {
        const [varName, defaultValue] = expr.split(':-');
        return process.env[varName] || defaultValue || '';
      });
    }

    if (Array.isArray(obj)) {
      return obj.map(item => this.resolveEnvVariables(item));
    }

    if (obj && typeof obj === 'object') {
      const resolved = {};
      for (const [key, value] of Object.entries(obj)) {
        resolved[key] = this.resolveEnvVariables(value);
      }
      return resolved;
    }

    return obj;
  }

  /**
   * Validate config has required fields
   */
  validateConfig(config) {
    if (!config.name) {
      throw new Error('Config must have a "name" field');
    }

    if (!config.database) {
      throw new Error('Config must have a "database" section');
    }

    if (!config.database.type) {
      throw new Error('Config must specify database.type');
    }

    const validTypes = ['mysql', 'postgres', 'mongodb', 'sqlserver'];
    if (!validTypes.includes(config.database.type)) {
      throw new Error(`Invalid database.type: ${config.database.type}. Must be one of: ${validTypes.join(', ')}`);
    }
  }

  /**
   * Get app config by name
   * @param {string} appName 
   * @returns {object|null}
   */
  getApp(appName) {
    return this.apps.get(appName) || null;
  }

  /**
   * Get app config by Kafka topic
   * @param {string} topic - Full topic name (e.g., "erpnext.dbname.tabEmployee")
   * @returns {object|null}
   */
  getAppByTopic(topic) {
    const parts = topic.split('.');
    if (parts.length < 1) return null;

    const prefix = parts[0];
    const appName = this.topicToApp.get(prefix);

    if (appName) {
      return this.apps.get(appName);
    }

    // Fallback to generic config if no match
    return this.apps.get('generic') || null;
  }

  /**
   * Get table configuration from an app
   * @param {string} appName 
   * @param {string} tableName 
   * @returns {object|null}
   */
  getTableConfig(appName, tableName) {
    const app = this.apps.get(appName);
    if (!app) return null;

    return app.tableMap?.get(tableName) || null;
  }

  /**
   * Get table config by topic
   * @param {string} topic - Full topic name
   * @returns {object|null}
   */
  getTableByTopic(topic) {
    const parts = topic.split('.');
    if (parts.length < 3) return null;

    const tableName = parts[parts.length - 1];
    const app = this.getAppByTopic(topic);

    if (!app) return null;

    return app.tableMap?.get(tableName) || null;
  }

  /**
   * Get all registered apps
   * @returns {Array<object>}
   */
  getAllApps() {
    return Array.from(this.apps.values());
  }

  /**
   * Get list of target tables for an app
   * @param {string} appName 
   * @returns {Array<string>}
   */
  getTargetTables(appName) {
    const app = this.apps.get(appName);
    if (!app || !app.tables) return [];

    return app.tables.map(t => t.name);
  }

  /**
   * Register a new app configuration at runtime
   * @param {object} config 
   */
  registerApp(config) {
    this.validateConfig(config);

    config.tableMap = new Map();
    if (config.tables) {
      for (const table of config.tables) {
        config.tableMap.set(table.name, table);
      }
    }

    this.apps.set(config.name, config);

    const topicPrefix = config.kafka?.topicPrefix || config.name;
    this.topicToApp.set(topicPrefix, config.name);

    return config;
  }

  /**
   * Save app config to file
   * @param {object} config 
   * @param {string} filename 
   */
  saveConfig(config, filename = null) {
    const fname = filename || `${config.name}.yaml`;
    const filePath = path.join(this.configDir, fname);

    // Remove runtime properties before saving
    const saveConfig = { ...config };
    delete saveConfig.tableMap;

    const yamlContent = yaml.dump(saveConfig, {
      indent: 2,
      lineWidth: 120,
      noRefs: true
    });

    fs.writeFileSync(filePath, yamlContent);
    console.log(`Saved config to: ${filePath}`);

    return filePath;
  }
}

// Singleton instance
let registryInstance = null;

/**
 * Get or create the registry instance
 * @returns {Promise<AppRegistry>}
 */
async function getRegistry() {
  if (!registryInstance) {
    registryInstance = new AppRegistry();
    await registryInstance.init();
  }
  return registryInstance;
}

/**
 * Get registry synchronously (must be initialized first)
 * @returns {AppRegistry}
 */
function getRegistrySync() {
  if (!registryInstance) {
    registryInstance = new AppRegistry();
    // Synchronous init - loads configs immediately
    registryInstance.loadAllConfigs();
  }
  return registryInstance;
}

module.exports = {
  AppRegistry,
  getRegistry,
  getRegistrySync
};
