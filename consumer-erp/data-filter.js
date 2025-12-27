/**
 * Data filtering and optimization for blockchain storage
 * 
 * Configuration-driven filtering that works with the App Registry.
 * Maintains backward compatibility with ERPNext field lists.
 */

const { getRegistrySync } = require('./config/registry');

/**
 * Filter data by a list of allowed fields
 * @param {object} rawData - Raw data from database
 * @param {Array<string>} fieldList - List of fields to include
 * @returns {object} Filtered data
 */
function filterByFieldList(rawData, fieldList) {
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
 * Generic filter - remove nulls and system fields
 * @param {object} rawData - Raw data from database
 * @returns {object} Filtered data
 */
function filterGeneric(rawData) {
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
 * Get field list from app configuration
 * @param {string} appName - App name
 * @param {string} tableName - Table name
 * @returns {Array<string>|null} Field list or null for generic filtering
 */
function getFieldList(appName, tableName) {
  try {
    const registry = getRegistrySync();
    const tableConfig = registry.getTableConfig(appName, tableName);

    if (tableConfig?.fields && tableConfig.fields.length > 0) {
      return tableConfig.fields;
    }
  } catch (error) {
    // Registry not initialized, fall back to legacy
  }

  return null;
}

/**
 * Optimize data for blockchain storage
 * @param {string} tableName - Table name
 * @param {object} rawData - Raw data from database
 * @param {string} appName - Optional app name for config lookup
 * @returns {object} Optimization result with filteredData and stats
 */
function optimizeForBlockchain(tableName, rawData, appName = null) {
  let filteredData;

  // Try to get field list from config
  const fieldList = appName ? getFieldList(appName, tableName) : null;

  if (fieldList) {
    // Use configured field list
    filteredData = filterByFieldList(rawData, fieldList);
  } else {
    // Use generic filtering
    filteredData = filterGeneric(rawData);
  }

  // Calculate optimization stats
  const originalSize = JSON.stringify(rawData).length;
  const filteredSize = JSON.stringify(filteredData).length;
  const reduction = originalSize - filteredSize;
  const reductionPercent = Math.round((reduction / originalSize) * 100);

  return {
    filteredData,
    stats: {
      originalSize,
      filteredSize,
      reduction,
      reductionPercent
    }
  };
}

// ============================================================
// Legacy ERPNext-specific functions (for backward compatibility)
// ============================================================

/**
 * @deprecated Use optimizeForBlockchain with app config instead
 */
function filterEmployeeData(rawData) {
  const allowedFields = [
    'name', 'employee', 'first_name', 'middle_name', 'last_name',
    'employee_name', 'gender', 'date_of_birth', 'date_of_joining',
    'status', 'company', 'department', 'designation', 'employee_number',
    'reports_to', 'branch', 'cell_number', 'personal_email', 'company_email',
    'current_address', 'permanent_address', 'emergency_phone_number',
    'marital_status', 'blood_group', 'salary_currency', 'salary_mode',
    'bank_name', 'bank_ac_no', 'employment_type', 'grade', 'default_shift'
  ];

  return filterByFieldList(rawData, allowedFields);
}

/**
 * @deprecated Use optimizeForBlockchain with app config instead
 */
function filterAttendanceData(rawData) {
  const allowedFields = [
    'name', 'employee', 'employee_name', 'attendance_date', 'status',
    'working_hours', 'in_time', 'out_time', 'late_entry', 'early_exit',
    'shift', 'company', 'department'
  ];

  return filterByFieldList(rawData, allowedFields);
}

module.exports = {
  optimizeForBlockchain,
  filterByFieldList,
  filterGeneric,
  getFieldList,
  // Legacy exports (deprecated)
  filterEmployeeData,
  filterAttendanceData
};