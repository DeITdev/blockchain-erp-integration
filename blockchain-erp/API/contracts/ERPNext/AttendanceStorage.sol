// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract AttendanceStorage {
    // Attendance data structure
    struct Attendance {
        string recordId;          // HR-ATT-2025-01223
        uint256 createdTimestamp; // creation timestamp converted to unix
        uint256 modifiedTimestamp; // modified timestamp converted to unix
        string modifiedBy;        // Administrator or user email
        string allData;           // Complete JSON string of all attendance data
    }
    
    // Mapping from attendance record ID to attendance data
    mapping(string => Attendance) public attendances;
    
    // Array to store all attendance record IDs for enumeration
    string[] public attendanceIds;
    
    // Mapping to check if attendance exists
    mapping(string => bool) public attendanceExists;
    
    // Events
    event AttendanceStored(string indexed recordId, uint256 createdTimestamp, uint256 modifiedTimestamp);
    event AttendanceUpdated(string indexed recordId, uint256 modifiedTimestamp);
    
    // Store or update attendance data
    function storeAttendance(
        string memory _recordId,
        uint256 _createdTimestamp,
        uint256 _modifiedTimestamp,
        string memory _modifiedBy,
        string memory _allData
    ) public returns (bool) {
        // Check if this is a new attendance record
        bool isNewAttendance = !attendanceExists[_recordId];
        
        // Store attendance data
        attendances[_recordId] = Attendance({
            recordId: _recordId,
            createdTimestamp: _createdTimestamp,
            modifiedTimestamp: _modifiedTimestamp,
            modifiedBy: _modifiedBy,
            allData: _allData
        });
        
        // If new attendance, add to array and mark as existing
        if (isNewAttendance) {
            attendanceIds.push(_recordId);
            attendanceExists[_recordId] = true;
            emit AttendanceStored(_recordId, _createdTimestamp, _modifiedTimestamp);
        } else {
            emit AttendanceUpdated(_recordId, _modifiedTimestamp);
        }
        
        return true;
    }
    
    // Get attendance data
    function getAttendance(string memory _recordId) public view returns (
        string memory recordId,
        uint256 createdTimestamp,
        uint256 modifiedTimestamp,
        string memory modifiedBy,
        string memory allData
    ) {
        require(attendanceExists[_recordId], "Attendance does not exist");
        
        Attendance memory attendance = attendances[_recordId];
        return (
            attendance.recordId,
            attendance.createdTimestamp,
            attendance.modifiedTimestamp,
            attendance.modifiedBy,
            attendance.allData
        );
    }
    
    // Get total number of attendance records
    function getTotalAttendances() public view returns (uint256) {
        return attendanceIds.length;
    }
    
    // Get attendance record ID by index
    function getAttendanceIdByIndex(uint256 _index) public view returns (string memory) {
        require(_index < attendanceIds.length, "Index out of bounds");
        return attendanceIds[_index];
    }
    
    // Get all attendance IDs
    function getAllAttendanceIds() public view returns (string[] memory) {
        return attendanceIds;
    }
    
    // Check if attendance exists
    function doesAttendanceExist(string memory _recordId) public view returns (bool) {
        return attendanceExists[_recordId];
    }
    
    // Retrieve summary data (total attendances and all IDs)
    function retrieve() public view returns (
        uint256 totalAttendances,
        string[] memory allAttendanceIds
    ) {
        return (attendanceIds.length, attendanceIds);
    }
    
    // Get attendance metadata only (without full data)
    function getAttendanceMetadata(string memory _recordId) public view returns (
        string memory recordId,
        uint256 createdTimestamp,
        uint256 modifiedTimestamp,
        string memory modifiedBy
    ) {
        require(attendanceExists[_recordId], "Attendance does not exist");
        
        Attendance memory attendance = attendances[_recordId];
        return (
            attendance.recordId,
            attendance.createdTimestamp,
            attendance.modifiedTimestamp,
            attendance.modifiedBy
        );
    }
}