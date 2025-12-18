/**
 * MySQLAdapter - Adapter for MySQL/MariaDB databases
 * 
 * Handles MySQL-specific timestamp formats and ERPNext conventions.
 * This is the primary adapter for ERPNext as it uses MariaDB/MySQL.
 */

const BaseAdapter = require('./BaseAdapter');

class MySQLAdapter extends BaseAdapter {
  constructor(appConfig) {
    super(appConfig);
  }

  /**
   * Normalize MySQL timestamp to ISO string
   * Handles ERPNext's microsecond timestamp format
   * @param {any} timestamp - MySQL timestamp (microseconds or datetime string)
   * @returns {string} ISO timestamp string
   */
  normalizeTimestamp(timestamp) {
    if (!timestamp) return new Date().toISOString();

    // ERPNext stores timestamps as microseconds since epoch
    if (typeof timestamp === 'number' || typeof timestamp === 'bigint') {
      const numTimestamp = Number(timestamp);

      // Check if it's microseconds (ERPNext format)
      if (numTimestamp > 1e15) {
        const milliseconds = Math.floor(numTimestamp / 1000);
        const correctedMs = milliseconds - this.timezoneOffsetMs;
        return new Date(correctedMs).toISOString();
      }

      // Check if it's milliseconds
      if (numTimestamp > 1e12) {
        return new Date(numTimestamp - this.timezoneOffsetMs).toISOString();
      }

      // Assume seconds
      return new Date((numTimestamp * 1000) - this.timezoneOffsetMs).toISOString();
    }

    // String timestamp (DATETIME format)
    if (typeof timestamp === 'string') {
      const date = new Date(timestamp);
      if (!isNaN(date.getTime())) {
        // Apply timezone offset
        return new Date(date.getTime() - this.timezoneOffsetMs).toISOString();
      }
    }

    return new Date().toISOString();
  }

  /**
   * Get event timestamp with MySQL/ERPNext specific handling
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

    // Use modified timestamp from ERPNext
    const modifiedField = this.dbConfig.timestampFields?.modified || 'modified';
    if (changeData[modifiedField]) {
      const timestamp = changeData[modifiedField];
      if (typeof timestamp === 'number' || typeof timestamp === 'bigint') {
        const numTimestamp = Number(timestamp);
        if (numTimestamp > 1e15) {
          return Math.floor(numTimestamp / 1000) - this.timezoneOffsetMs;
        }
        if (numTimestamp > 1e12) {
          return numTimestamp - this.timezoneOffsetMs;
        }
      }
    }

    // Use creation timestamp
    const createdField = this.dbConfig.timestampFields?.created || 'creation';
    if (changeData[createdField]) {
      const timestamp = changeData[createdField];
      if (typeof timestamp === 'number' || typeof timestamp === 'bigint') {
        const numTimestamp = Number(timestamp);
        if (numTimestamp > 1e15) {
          return Math.floor(numTimestamp / 1000) - this.timezoneOffsetMs;
        }
        if (numTimestamp > 1e12) {
          return numTimestamp - this.timezoneOffsetMs;
        }
      }
    }

    return kafkaTimestamp;
  }

  /**
   * Detect operation with MySQL/ERPNext specific logic
   */
  detectOperation(changeEvent, changeData) {
    // Use base Debezium detection first
    if (changeEvent.payload?.op) {
      const opMap = { 'c': 'CREATE', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ' };
      return opMap[changeEvent.payload.op] || changeEvent.payload.op;
    }

    // Check ERPNext delete marker
    if (changeData.__deleted === 'true' || changeData.__deleted === true) {
      return 'DELETE';
    }

    // Infer from ERPNext timestamps
    const createdField = this.dbConfig.timestampFields?.created || 'creation';
    const modifiedField = this.dbConfig.timestampFields?.modified || 'modified';

    if (changeData[createdField] && changeData[modifiedField]) {
      const timeDiff = Math.abs(changeData[modifiedField] - changeData[createdField]);
      // ERPNext stores as microseconds, 5 seconds = 5000000 microseconds
      return timeDiff < 5000000 ? 'CREATE' : 'UPDATE';
    }

    return 'CREATE';
  }
}

module.exports = MySQLAdapter;
