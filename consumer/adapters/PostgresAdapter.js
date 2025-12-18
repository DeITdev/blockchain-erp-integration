/**
 * PostgresAdapter - Adapter for PostgreSQL databases
 * 
 * Handles PostgreSQL-specific timestamp formats and data types.
 */

const BaseAdapter = require('./BaseAdapter');

class PostgresAdapter extends BaseAdapter {
  constructor(appConfig) {
    super(appConfig);
  }

  /**
   * Normalize PostgreSQL timestamp to ISO string
   * PostgreSQL uses RFC3339/ISO8601 format natively
   * @param {any} timestamp - PostgreSQL timestamp
   * @returns {string} ISO timestamp string
   */
  normalizeTimestamp(timestamp) {
    if (!timestamp) return new Date().toISOString();

    // PostgreSQL TIMESTAMPTZ comes as ISO string
    if (typeof timestamp === 'string') {
      const date = new Date(timestamp);
      if (!isNaN(date.getTime())) {
        // Apply timezone offset if configured
        return new Date(date.getTime() - this.timezoneOffsetMs).toISOString();
      }
    }

    // Numeric timestamp (epoch seconds or milliseconds)
    if (typeof timestamp === 'number') {
      if (timestamp > 1e12) {
        return new Date(timestamp - this.timezoneOffsetMs).toISOString();
      }
      return new Date((timestamp * 1000) - this.timezoneOffsetMs).toISOString();
    }

    // PostgreSQL Infinity values
    if (timestamp === 'infinity') {
      return new Date('9999-12-31T23:59:59Z').toISOString();
    }
    if (timestamp === '-infinity') {
      return new Date('0001-01-01T00:00:00Z').toISOString();
    }

    return new Date().toISOString();
  }

  /**
   * Get event timestamp for PostgreSQL
   */
  getEventTimestamp(changeEvent, changeData, kafkaTimestamp) {
    // Prefer Debezium source timestamp
    if (changeEvent.payload?.source?.ts_ms) {
      return changeEvent.payload.source.ts_ms;
    }

    if (changeEvent.payload?.ts_ms) {
      return changeEvent.payload.ts_ms;
    }

    // PostgreSQL LSN can be used for ordering but not timing
    // Fall back to Kafka timestamp
    return kafkaTimestamp;
  }

  /**
   * Extract record ID - PostgreSQL typically uses integer or UUID
   */
  extractRecordId(changeData) {
    const idField = this.dbConfig.idField || 'id';

    // Handle UUID format
    if (changeData[idField] && typeof changeData[idField] === 'string') {
      return changeData[idField];
    }

    // Handle integer ID
    if (changeData[idField] !== undefined) {
      return String(changeData[idField]);
    }

    return `record_${Date.now()}`;
  }

  /**
   * Filter data with PostgreSQL-aware handling
   */
  filterGeneric(rawData) {
    const filtered = {};

    for (const [key, value] of Object.entries(rawData)) {
      // Skip null/empty values
      if (value === null || value === undefined || value === '') {
        continue;
      }

      // Skip PostgreSQL system columns
      if (['xmin', 'xmax', 'cmin', 'cmax', 'ctid', 'tableoid'].includes(key)) {
        continue;
      }

      // Skip system fields starting with underscore
      if (key.startsWith('_')) {
        continue;
      }

      // Handle PostgreSQL array types
      if (Array.isArray(value)) {
        if (value.length > 0) {
          filtered[key] = value;
        }
        continue;
      }

      // Handle PostgreSQL JSON/JSONB
      if (typeof value === 'object') {
        filtered[key] = value;
        continue;
      }

      filtered[key] = value;
    }

    return filtered;
  }
}

module.exports = PostgresAdapter;
