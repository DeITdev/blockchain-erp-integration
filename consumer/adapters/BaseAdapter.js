/**
 * BaseAdapter - Abstract base class for database adapters
 * 
 * Defines the interface for handling database-specific logic:
 * - Timestamp normalization
 * - Operation detection (CREATE/UPDATE/DELETE)
 * - Field filtering
 * - Data transformation for blockchain
 */

class BaseAdapter {
  /**
   * @param {object} appConfig - App configuration from registry
   */
  constructor(appConfig) {
    this.config = appConfig;
    this.dbConfig = appConfig.database || {};
    this.timezoneOffsetMs = (this.dbConfig.timezoneOffsetHours || 0) * 60 * 60 * 1000;
  }

  /**
   * Get the database type
   * @returns {string}
   */
  getType() {
    return this.dbConfig.type || 'unknown';
  }

  /**
   * Normalize a timestamp to ISO string
   * Override in subclasses for database-specific handling
   * @param {any} timestamp - Raw timestamp from database
   * @returns {string} ISO timestamp string
   */
  normalizeTimestamp(timestamp) {
    if (!timestamp) return new Date().toISOString();

    // Default: try to parse as date
    const date = new Date(timestamp);
    if (!isNaN(date.getTime())) {
      return date.toISOString();
    }

    return new Date().toISOString();
  }

  /**
   * Get the created timestamp from change data
   * @param {object} changeData - CDC change data
   * @returns {string} ISO timestamp
   */
  getCreatedTimestamp(changeData) {
    const field = this.dbConfig.timestampFields?.created || 'created_at';
    return this.normalizeTimestamp(changeData[field]);
  }

  /**
   * Get the modified timestamp from change data
   * @param {object} changeData - CDC change data
   * @returns {string} ISO timestamp
   */
  getModifiedTimestamp(changeData) {
    const field = this.dbConfig.timestampFields?.modified || 'updated_at';
    return this.normalizeTimestamp(changeData[field]);
  }

  /**
   * Get the modified by user from change data
   * @param {object} changeData - CDC change data
   * @returns {string}
   */
  getModifiedBy(changeData) {
    const field = this.dbConfig.timestampFields?.modifiedBy || 'updated_by';
    return changeData[field] || 'system';
  }

  /**
   * Extract record ID from change data
   * @param {object} changeData - CDC change data
   * @returns {string}
   */
  extractRecordId(changeData) {
    const idField = this.dbConfig.idField || 'id';
    return changeData[idField] || changeData.name || changeData._id || `record_${Date.now()}`;
  }

  /**
   * Detect the operation type from CDC event
   * @param {object} changeEvent - Full CDC event
   * @param {object} changeData - Change data payload
   * @returns {string} CREATE, UPDATE, DELETE, or READ
   */
  detectOperation(changeEvent, changeData) {
    // Debezium operation codes
    if (changeEvent.payload?.op) {
      const opMap = { 'c': 'CREATE', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ' };
      return opMap[changeEvent.payload.op] || changeEvent.payload.op;
    }

    // Check for delete markers
    if (changeData.__deleted === 'true' || changeData.__deleted === true) {
      return 'DELETE';
    }

    // Infer from timestamps
    const createdField = this.dbConfig.timestampFields?.created || 'created_at';
    const modifiedField = this.dbConfig.timestampFields?.modified || 'updated_at';

    if (changeData[createdField] && changeData[modifiedField]) {
      const created = new Date(changeData[createdField]).getTime();
      const modified = new Date(changeData[modifiedField]).getTime();
      const timeDiff = Math.abs(modified - created);

      // If created and modified are within 5 seconds, likely a CREATE
      return timeDiff < 5000 ? 'CREATE' : 'UPDATE';
    }

    return 'CREATE';
  }

  /**
   * Get event timestamp for latency calculation
   * @param {object} changeEvent - Full CDC event
   * @param {object} changeData - Change data
   * @param {number} kafkaTimestamp - Kafka message timestamp
   * @returns {number} Timestamp in milliseconds
   */
  getEventTimestamp(changeEvent, changeData, kafkaTimestamp) {
    // Prefer Debezium source timestamp
    if (changeEvent.payload?.source?.ts_ms) {
      return changeEvent.payload.source.ts_ms;
    }

    if (changeEvent.payload?.ts_ms) {
      return changeEvent.payload.ts_ms;
    }

    if (changeEvent.connector_processing_time) {
      return changeEvent.connector_processing_time;
    }

    // Fall back to Kafka timestamp
    return kafkaTimestamp;
  }

  /**
   * Filter fields based on table configuration
   * @param {string} tableName - Table name
   * @param {object} rawData - Raw change data
   * @returns {object} Filtered data
   */
  filterFields(tableName, rawData) {
    const tableConfig = this.config.tableMap?.get(tableName);

    // If table has specific field list, use it
    if (tableConfig?.fields && tableConfig.fields.length > 0) {
      return this.filterByFieldList(rawData, tableConfig.fields);
    }

    // Otherwise, use generic filtering
    return this.filterGeneric(rawData);
  }

  /**
   * Filter data to include only specified fields
   * @param {object} rawData 
   * @param {Array<string>} fieldList 
   * @returns {object}
   */
  filterByFieldList(rawData, fieldList) {
    const filtered = {};

    for (const field of fieldList) {
      const value = rawData[field];
      if (value !== null && value !== undefined && value !== '' && value !== 'null') {
        filtered[field] = value;
      }
    }

    return filtered;
  }

  /**
   * Generic filtering - remove nulls and system fields
   * @param {object} rawData 
   * @returns {object}
   */
  filterGeneric(rawData) {
    const filtered = {};

    for (const [key, value] of Object.entries(rawData)) {
      // Skip null/empty values
      if (value === null || value === undefined || value === '' || value === 'null') {
        continue;
      }

      // Skip system fields (starting with _ or __)
      if (key.startsWith('__') || (key.startsWith('_') && key !== '_id')) {
        continue;
      }

      filtered[key] = value;
    }

    return filtered;
  }

  /**
   * Transform data for blockchain API
   * @param {string} tableName - Table name
   * @param {object} changeData - Raw change data
   * @returns {object} Transformed data ready for blockchain
   */
  transformForBlockchain(tableName, changeData) {
    const tableConfig = this.config.tableMap?.get(tableName);
    const filteredData = this.filterFields(tableName, changeData);

    const baseData = {
      recordId: this.extractRecordId(changeData),
      createdTimestamp: this.getCreatedTimestamp(changeData),
      modifiedTimestamp: this.getModifiedTimestamp(changeData),
      modifiedBy: this.getModifiedBy(changeData),
      allData: filteredData
    };

    // Use configured dataKey or derive from table name
    const dataKey = tableConfig?.dataKey || `${tableName.replace(/^tab/, '').toLowerCase()}Data`;

    // Calculate optimization stats
    const originalSize = JSON.stringify(changeData).length;
    const filteredSize = JSON.stringify(filteredData).length;
    const reduction = originalSize - filteredSize;
    const reductionPercent = Math.round((reduction / originalSize) * 100);

    return {
      [dataKey]: baseData,
      optimization: {
        originalSize,
        filteredSize,
        reduction,
        reductionPercent
      }
    };
  }

  /**
   * Get blockchain endpoint for a table
   * @param {string} tableName 
   * @returns {string}
   */
  getEndpoint(tableName) {
    const tableConfig = this.config.tableMap?.get(tableName);
    return tableConfig?.endpoint || `/${tableName.toLowerCase()}`;
  }

  /**
   * Get blockchain API URL
   * @returns {string}
   */
  getApiEndpoint() {
    return this.config.blockchain?.apiEndpoint || process.env.API_ENDPOINT || 'http://127.0.0.1:4001';
  }
}

module.exports = BaseAdapter;
