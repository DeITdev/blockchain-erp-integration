/**
 * SQLServerAdapter - Adapter for Microsoft SQL Server databases
 * 
 * Handles SQL Server-specific timestamp formats and CDC data types.
 */

const BaseAdapter = require('./BaseAdapter');

class SQLServerAdapter extends BaseAdapter {
  constructor(appConfig) {
    super(appConfig);
  }

  /**
   * Normalize SQL Server timestamp to ISO string
   * SQL Server uses DATETIME, DATETIME2, DATETIMEOFFSET, etc.
   * @param {any} timestamp - SQL Server timestamp
   * @returns {string} ISO timestamp string
   */
  normalizeTimestamp(timestamp) {
    if (!timestamp) return new Date().toISOString();

    // SQL Server DATETIME2 with precision
    if (typeof timestamp === 'string') {
      // Handle .NET ticks format from some drivers
      if (timestamp.includes('Date(')) {
        const match = timestamp.match(/Date\((\d+)\)/);
        if (match) {
          return new Date(parseInt(match[1])).toISOString();
        }
      }

      const date = new Date(timestamp);
      if (!isNaN(date.getTime())) {
        return new Date(date.getTime() - this.timezoneOffsetMs).toISOString();
      }
    }

    // Numeric timestamp (milliseconds or .NET ticks)
    if (typeof timestamp === 'number') {
      // .NET ticks are 100-nanosecond intervals since 1/1/0001
      // Check if this looks like ticks (very large number)
      if (timestamp > 1e17) {
        // Convert .NET ticks to JS milliseconds
        const ticksToMs = (timestamp - 621355968000000000) / 10000;
        return new Date(ticksToMs - this.timezoneOffsetMs).toISOString();
      }

      if (timestamp > 1e12) {
        return new Date(timestamp - this.timezoneOffsetMs).toISOString();
      }
      return new Date((timestamp * 1000) - this.timezoneOffsetMs).toISOString();
    }

    return new Date().toISOString();
  }

  /**
   * Get event timestamp for SQL Server
   */
  getEventTimestamp(changeEvent, changeData, kafkaTimestamp) {
    // Debezium source timestamp
    if (changeEvent.payload?.source?.ts_ms) {
      return changeEvent.payload.source.ts_ms;
    }

    // SQL Server CDC commit timestamp
    if (changeEvent.payload?.source?.commit_lsn) {
      // LSN doesn't directly translate to time, use ts_ms instead
    }

    if (changeEvent.payload?.ts_ms) {
      return changeEvent.payload.ts_ms;
    }

    return kafkaTimestamp;
  }

  /**
   * Extract record ID - SQL Server typically uses INT or BIGINT identity
   */
  extractRecordId(changeData) {
    const idField = this.dbConfig.idField || 'id';

    // Handle UNIQUEIDENTIFIER (GUID)
    if (changeData[idField] && typeof changeData[idField] === 'string') {
      // GUID format
      if (changeData[idField].match(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i)) {
        return changeData[idField];
      }
      return changeData[idField];
    }

    // Handle integer ID
    if (changeData[idField] !== undefined) {
      return String(changeData[idField]);
    }

    return `record_${Date.now()}`;
  }

  /**
   * Detect operation from SQL Server CDC event
   */
  detectOperation(changeEvent, changeData) {
    // Debezium operation codes
    if (changeEvent.payload?.op) {
      const opMap = { 'c': 'CREATE', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ' };
      return opMap[changeEvent.payload.op] || changeEvent.payload.op;
    }

    // SQL Server CDC __$operation column
    // 1 = delete, 2 = insert, 3 = update (before), 4 = update (after)
    if (changeData['__$operation'] !== undefined) {
      const opMap = { 1: 'DELETE', 2: 'CREATE', 3: 'UPDATE', 4: 'UPDATE' };
      return opMap[changeData['__$operation']] || 'UPDATE';
    }

    return 'CREATE';
  }

  /**
   * Filter data with SQL Server-aware handling
   */
  filterGeneric(rawData) {
    const filtered = {};

    for (const [key, value] of Object.entries(rawData)) {
      // Skip null/empty values
      if (value === null || value === undefined || value === '') {
        continue;
      }

      // Skip SQL Server CDC metadata columns
      if (key.startsWith('__$')) {
        continue;
      }

      // Skip system columns
      if (['$rowversion', 'rowversion', 'timestamp'].includes(key.toLowerCase())) {
        continue;
      }

      // Skip fields starting with underscore
      if (key.startsWith('_')) {
        continue;
      }

      // Handle SQL Server money type (comes as decimal string)
      if (typeof value === 'string' && /^\d+\.\d{4}$/.test(value)) {
        filtered[key] = parseFloat(value);
        continue;
      }

      // Handle SQL Server bit type (0 or 1 to boolean)
      if (value === 0 || value === 1) {
        // Keep as number, let application decide interpretation
        filtered[key] = value;
        continue;
      }

      filtered[key] = value;
    }

    return filtered;
  }
}

module.exports = SQLServerAdapter;
