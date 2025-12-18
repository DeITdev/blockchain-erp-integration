/**
 * MongoDBAdapter - Adapter for MongoDB databases
 * 
 * Handles MongoDB-specific data types including ObjectId, ISODate, etc.
 */

const BaseAdapter = require('./BaseAdapter');

class MongoDBAdapter extends BaseAdapter {
  constructor(appConfig) {
    super(appConfig);
  }

  /**
   * Normalize MongoDB timestamp to ISO string
   * MongoDB uses ISODate or $date objects
   * @param {any} timestamp - MongoDB timestamp
   * @returns {string} ISO timestamp string
   */
  normalizeTimestamp(timestamp) {
    if (!timestamp) return new Date().toISOString();

    // MongoDB $date format from Debezium
    if (timestamp.$date) {
      if (typeof timestamp.$date === 'string') {
        return new Date(timestamp.$date).toISOString();
      }
      if (timestamp.$date.$numberLong) {
        return new Date(parseInt(timestamp.$date.$numberLong)).toISOString();
      }
    }

    // MongoDB ISODate string
    if (typeof timestamp === 'string') {
      const date = new Date(timestamp);
      if (!isNaN(date.getTime())) {
        return date.toISOString();
      }
    }

    // Numeric timestamp (milliseconds)
    if (typeof timestamp === 'number') {
      return new Date(timestamp - this.timezoneOffsetMs).toISOString();
    }

    // Date object
    if (timestamp instanceof Date) {
      return timestamp.toISOString();
    }

    return new Date().toISOString();
  }

  /**
   * Extract record ID - MongoDB uses ObjectId
   */
  extractRecordId(changeData) {
    // MongoDB _id field
    if (changeData._id) {
      // ObjectId format from Debezium
      if (changeData._id.$oid) {
        return changeData._id.$oid;
      }
      return String(changeData._id);
    }

    const idField = this.dbConfig.idField || 'id';
    if (changeData[idField]) {
      return String(changeData[idField]);
    }

    return `record_${Date.now()}`;
  }

  /**
   * Detect operation from MongoDB change stream event
   */
  detectOperation(changeEvent, changeData) {
    // MongoDB change stream operation types
    if (changeEvent.operationType) {
      const opMap = {
        'insert': 'CREATE',
        'update': 'UPDATE',
        'replace': 'UPDATE',
        'delete': 'DELETE'
      };
      return opMap[changeEvent.operationType] || 'UPDATE';
    }

    // Debezium operation codes
    if (changeEvent.payload?.op) {
      const opMap = { 'c': 'CREATE', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ' };
      return opMap[changeEvent.payload.op] || changeEvent.payload.op;
    }

    return 'CREATE';
  }

  /**
   * Get event timestamp from MongoDB change event
   */
  getEventTimestamp(changeEvent, changeData, kafkaTimestamp) {
    // MongoDB clusterTime
    if (changeEvent.clusterTime) {
      if (changeEvent.clusterTime.$timestamp) {
        return changeEvent.clusterTime.$timestamp.t * 1000;
      }
    }

    // Debezium source timestamp
    if (changeEvent.payload?.source?.ts_ms) {
      return changeEvent.payload.source.ts_ms;
    }

    return kafkaTimestamp;
  }

  /**
   * Filter data with MongoDB-aware handling
   */
  filterGeneric(rawData) {
    const filtered = {};

    for (const [key, value] of Object.entries(rawData)) {
      // Skip null/undefined
      if (value === null || value === undefined) {
        continue;
      }

      // Keep _id as it's the primary key
      if (key === '_id') {
        filtered[key] = this.normalizeObjectId(value);
        continue;
      }

      // Skip other system fields
      if (key.startsWith('_') && key !== '_id') {
        continue;
      }

      // Handle MongoDB extended JSON types
      filtered[key] = this.normalizeMongoType(value);
    }

    return filtered;
  }

  /**
   * Normalize MongoDB ObjectId
   */
  normalizeObjectId(value) {
    if (value && value.$oid) {
      return value.$oid;
    }
    return String(value);
  }

  /**
   * Normalize MongoDB extended JSON types
   */
  normalizeMongoType(value) {
    if (value === null || value === undefined) {
      return null;
    }

    // ObjectId
    if (value.$oid) {
      return value.$oid;
    }

    // Date
    if (value.$date) {
      return this.normalizeTimestamp(value);
    }

    // NumberLong
    if (value.$numberLong) {
      return parseInt(value.$numberLong);
    }

    // NumberDecimal
    if (value.$numberDecimal) {
      return parseFloat(value.$numberDecimal);
    }

    // Binary
    if (value.$binary) {
      return value.$binary.base64;
    }

    // Array
    if (Array.isArray(value)) {
      return value.map(item => this.normalizeMongoType(item));
    }

    // Nested object
    if (typeof value === 'object') {
      const normalized = {};
      for (const [k, v] of Object.entries(value)) {
        normalized[k] = this.normalizeMongoType(v);
      }
      return normalized;
    }

    return value;
  }
}

module.exports = MongoDBAdapter;
