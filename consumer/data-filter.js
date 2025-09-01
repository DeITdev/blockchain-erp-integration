// data-filter.js - Filter and optimize data before sending to blockchain

// Define essential fields for each table type
const ESSENTIAL_FIELDS = {
  tabUser: [
    'name', 'email', 'first_name', 'last_name', 'username',
    'enabled', 'user_type', 'role_profile_name', 'phone', 'mobile_no'
  ],
  tabEmployee: [
    'name', 'employee_number', 'first_name', 'last_name', 'status',
    'company', 'department', 'designation', 'date_of_joining',
    'date_of_birth', 'gender', 'email', 'phone'
  ],
  tabTask: [
    'name', 'subject', 'status', 'priority', 'project', 'company',
    'assigned_to', 'description', 'is_group', 'task_weight'
  ],
  tabCompany: [
    'name', 'company_name', 'domain', 'country', 'default_currency',
    'tax_id', 'website', 'email', 'phone'
  ],
  tabAttendance: [
    'name', 'employee', 'employee_name', 'attendance_date', 'status',
    'company', 'department', 'in_time', 'out_time', 'working_hours'
  ]
};

// Function to clean and filter data
function filterDataForBlockchain(tableName, rawData) {
  if (!rawData || typeof rawData !== 'object') {
    return {};
  }

  const essentialFields = ESSENTIAL_FIELDS[tableName] || [];
  const filteredData = {};

  // Always include core identification fields
  const coreFields = ['name', 'creation', 'modified', 'modified_by', 'owner'];
  const allImportantFields = [...new Set([...coreFields, ...essentialFields])];

  allImportantFields.forEach(field => {
    const value = rawData[field];

    // Only include non-null, non-empty values
    if (value !== null && value !== undefined && value !== '' && value !== 0) {
      // For strings, trim whitespace
      if (typeof value === 'string') {
        const trimmed = value.trim();
        if (trimmed.length > 0) {
          filteredData[field] = trimmed;
        }
      } else {
        filteredData[field] = value;
      }
    }
  });

  return filteredData;
}

// Function to calculate data size
function calculateDataSize(data) {
  return JSON.stringify(data).length;
}

// Function to get size reduction stats
function getOptimizationStats(originalData, filteredData) {
  const originalSize = calculateDataSize(originalData);
  const filteredSize = calculateDataSize(filteredData);
  const reduction = originalSize - filteredSize;
  const reductionPercent = Math.round((reduction / originalSize) * 100);

  return {
    originalSize,
    filteredSize,
    reduction,
    reductionPercent,
    originalFieldCount: Object.keys(originalData).length,
    filteredFieldCount: Object.keys(filteredData).length
  };
}

// Main function to optimize data for blockchain storage
function optimizeForBlockchain(tableName, eventData) {
  console.log(`🔍 Optimizing data for ${tableName}...`);

  // Extract the main data from the event
  const rawData = eventData.allData || eventData;

  // Filter the data
  const filteredData = filterDataForBlockchain(tableName, rawData);

  // Get optimization stats
  const stats = getOptimizationStats(rawData, filteredData);

  console.log(`📊 Optimization Results:`);
  console.log(`   Original: ${stats.originalFieldCount} fields, ${stats.originalSize} chars`);
  console.log(`   Filtered: ${stats.filteredFieldCount} fields, ${stats.filteredSize} chars`);
  console.log(`   Reduction: ${stats.reductionPercent}% (${stats.reduction} chars)`);

  // Warn if data is still large
  if (stats.filteredSize > 2000) {
    console.log(`   ⚠️  Warning: Filtered data is still large (${stats.filteredSize} chars)`);
  } else if (stats.filteredSize > 1000) {
    console.log(`   📝 Note: Filtered data is moderate size (${stats.filteredSize} chars)`);
  } else {
    console.log(`   ✅ Filtered data is optimal size (${stats.filteredSize} chars)`);
  }

  return {
    originalData: rawData,
    filteredData: filteredData,
    stats: stats
  };
}

// Export functions
module.exports = {
  filterDataForBlockchain,
  optimizeForBlockchain,
  calculateDataSize,
  getOptimizationStats,
  ESSENTIAL_FIELDS
};

// Test function if run directly
if (require.main === module) {
  console.log('🧪 Testing data filter...\n');

  // Test with sample employee data
  const sampleEmployeeData = {
    name: 'HR-EMP-00006',
    employee_number: 'EMP-006',
    first_name: 'John',
    last_name: 'Doe',
    status: 'Active',
    company: 'Test Company',
    department: 'IT',
    designation: 'Developer',
    date_of_joining: '2024-01-01',
    date_of_birth: '1990-06-15',
    gender: 'Male',
    email: 'john.doe@example.com',
    phone: '+1234567890',
    // Fields that should be filtered out
    custom_field_null: null,
    empty_field: '',
    zero_field: 0,
    undefined_field: undefined,
    whitespace_field: '   ',
    very_long_description: 'This is a very long description that might not be essential for blockchain storage but takes up a lot of space and increases gas costs significantly when stored on the blockchain.',
    creation: '2025-07-15 06:23:47.323000',
    modified: '2025-07-15 06:31:52.321000',
    modified_by: 'Administrator',
    owner: 'Administrator'
  };

  const result = optimizeForBlockchain('tabEmployee', sampleEmployeeData);

  console.log('\n📋 Filtered Data:');
  console.log(JSON.stringify(result.filteredData, null, 2));
}