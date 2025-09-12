// Data filtering and optimization for blockchain storage

function filterEmployeeData(rawData) {
  // Remove null, undefined, empty string values and system fields
  const filtered = {};

  // Core employee fields to include (non-null only)
  const allowedFields = [
    'name', 'employee', 'first_name', 'middle_name', 'last_name',
    'employee_name', 'gender', 'date_of_birth', 'date_of_joining',
    'status', 'company', 'department', 'designation', 'employee_number',
    'reports_to', 'branch', 'cell_number', 'personal_email', 'company_email',
    'current_address', 'permanent_address', 'emergency_phone_number',
    'marital_status', 'blood_group', 'salary_currency', 'salary_mode',
    'bank_name', 'bank_ac_no', 'employment_type', 'grade', 'default_shift'
  ];

  allowedFields.forEach(field => {
    const value = rawData[field];
    if (value !== null && value !== undefined && value !== '' && value !== 'null') {
      filtered[field] = value;
    }
  });

  return filtered;
}

function filterAttendanceData(rawData) {
  const filtered = {};

  const allowedFields = [
    'name', 'employee', 'employee_name', 'attendance_date', 'status',
    'working_hours', 'in_time', 'out_time', 'late_entry', 'early_exit',
    'shift', 'company', 'department'
  ];

  allowedFields.forEach(field => {
    const value = rawData[field];
    if (value !== null && value !== undefined && value !== '' && value !== 'null') {
      filtered[field] = value;
    }
  });

  return filtered;
}

function optimizeForBlockchain(tableName, rawData) {
  let filteredData;

  switch (tableName) {
    case 'tabEmployee':
      filteredData = filterEmployeeData(rawData);
      break;
    case 'tabAttendance':
      filteredData = filterAttendanceData(rawData);
      break;
    default:
      // Generic filter - remove nulls and system fields
      filteredData = {};
      Object.keys(rawData).forEach(key => {
        const value = rawData[key];
        if (value !== null && value !== undefined && value !== '' &&
          value !== 'null' && !key.startsWith('_') && !key.startsWith('__')) {
          filteredData[key] = value;
        }
      });
  }

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

module.exports = {
  optimizeForBlockchain,
  filterEmployeeData,
  filterAttendanceData
};