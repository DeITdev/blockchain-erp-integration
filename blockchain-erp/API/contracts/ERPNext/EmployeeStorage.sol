// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract EmployeeStorage {
    // Employee data structure
    struct Employee {
        string recordId;          // HR-EMP-00101
        uint256 createdTimestamp; // 2025-06-25T14:03:23.015Z converted to timestamp
        uint256 modifiedTimestamp; // 2025-06-26T09:41:45.823Z converted to timestamp
        string modifiedBy;        // danarikramtirta@gmail.com
        string allData;           // Complete JSON string of all employee data
    }
    
    // Mapping from employee record ID to employee data
    mapping(string => Employee) public employees;
    
    // Array to store all employee record IDs for enumeration
    string[] public employeeIds;
    
    // Mapping to check if employee exists
    mapping(string => bool) public employeeExists;
    
    // Events
    event EmployeeStored(string indexed recordId, uint256 createdTimestamp, uint256 modifiedTimestamp);
    event EmployeeUpdated(string indexed recordId, uint256 modifiedTimestamp);
    
    // Store or update employee data
    function storeEmployee(
        string memory _recordId,
        uint256 _createdTimestamp,
        uint256 _modifiedTimestamp,
        string memory _modifiedBy,
        string memory _allData
    ) public returns (bool) {
        // Check if this is a new employee
        bool isNewEmployee = !employeeExists[_recordId];
        
        // Store employee data
        employees[_recordId] = Employee({
            recordId: _recordId,
            createdTimestamp: _createdTimestamp,
            modifiedTimestamp: _modifiedTimestamp,
            modifiedBy: _modifiedBy,
            allData: _allData
        });
        
        // If new employee, add to array and mark as existing
        if (isNewEmployee) {
            employeeIds.push(_recordId);
            employeeExists[_recordId] = true;
            emit EmployeeStored(_recordId, _createdTimestamp, _modifiedTimestamp);
        } else {
            emit EmployeeUpdated(_recordId, _modifiedTimestamp);
        }
        
        return true;
    }
    
    // Get employee data
    function getEmployee(string memory _recordId) public view returns (
        string memory recordId,
        uint256 createdTimestamp,
        uint256 modifiedTimestamp,
        string memory modifiedBy,
        string memory allData
    ) {
        require(employeeExists[_recordId], "Employee does not exist");
        
        Employee memory emp = employees[_recordId];
        return (
            emp.recordId,
            emp.createdTimestamp,
            emp.modifiedTimestamp,
            emp.modifiedBy,
            emp.allData
        );
    }
    
    // Get total number of employees
    function getTotalEmployees() public view returns (uint256) {
        return employeeIds.length;
    }
    
    // Get employee record ID by index
    function getEmployeeIdByIndex(uint256 _index) public view returns (string memory) {
        require(_index < employeeIds.length, "Index out of bounds");
        return employeeIds[_index];
    }
    
    // Get all employee IDs
    function getAllEmployeeIds() public view returns (string[] memory) {
        return employeeIds;
    }
    
    // Check if employee exists
    function doesEmployeeExist(string memory _recordId) public view returns (bool) {
        return employeeExists[_recordId];
    }
    
    // Retrieve summary data (total employees and all IDs)
    function retrieve() public view returns (
        uint256 totalEmployees,
        string[] memory allEmployeeIds
    ) {
        return (employeeIds.length, employeeIds);
    }
    
    // Get employee metadata only (without full data)
    function getEmployeeMetadata(string memory _recordId) public view returns (
        string memory recordId,
        uint256 createdTimestamp,
        uint256 modifiedTimestamp,
        string memory modifiedBy
    ) {
        require(employeeExists[_recordId], "Employee does not exist");
        
        Employee memory emp = employees[_recordId];
        return (
            emp.recordId,
            emp.createdTimestamp,
            emp.modifiedTimestamp,
            emp.modifiedBy
        );
    }
}