// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract UserStorage {
    // User data structure
    struct User {
        string recordId;          // User email or ID
        uint256 createdTimestamp; // creation timestamp converted to unix
        uint256 modifiedTimestamp; // modified timestamp converted to unix
        string modifiedBy;        // Administrator or user email
        string allData;           // Complete JSON string of all user data
    }
    
    // Mapping from user record ID to user data
    mapping(string => User) public users;
    
    // Array to store all user record IDs for enumeration
    string[] public userIds;
    
    // Mapping to check if user exists
    mapping(string => bool) public userExists;
    
    // Events
    event UserStored(string indexed recordId, uint256 createdTimestamp, uint256 modifiedTimestamp);
    event UserUpdated(string indexed recordId, uint256 modifiedTimestamp);
    
    // Store or update user data
    function storeUser(
        string memory _recordId,
        uint256 _createdTimestamp,
        uint256 _modifiedTimestamp,
        string memory _modifiedBy,
        string memory _allData
    ) public returns (bool) {
        // Check if this is a new user
        bool isNewUser = !userExists[_recordId];
        
        // Store user data
        users[_recordId] = User({
            recordId: _recordId,
            createdTimestamp: _createdTimestamp,
            modifiedTimestamp: _modifiedTimestamp,
            modifiedBy: _modifiedBy,
            allData: _allData
        });
        
        // If new user, add to array and mark as existing
        if (isNewUser) {
            userIds.push(_recordId);
            userExists[_recordId] = true;
            emit UserStored(_recordId, _createdTimestamp, _modifiedTimestamp);
        } else {
            emit UserUpdated(_recordId, _modifiedTimestamp);
        }
        
        return true;
    }
    
    // Get user data
    function getUser(string memory _recordId) public view returns (
        string memory recordId,
        uint256 createdTimestamp,
        uint256 modifiedTimestamp,
        string memory modifiedBy,
        string memory allData
    ) {
        require(userExists[_recordId], "User does not exist");
        
        User memory user = users[_recordId];
        return (
            user.recordId,
            user.createdTimestamp,
            user.modifiedTimestamp,
            user.modifiedBy,
            user.allData
        );
    }
    
    // Get total number of users
    function getTotalUsers() public view returns (uint256) {
        return userIds.length;
    }
    
    // Get user record ID by index
    function getUserIdByIndex(uint256 _index) public view returns (string memory) {
        require(_index < userIds.length, "Index out of bounds");
        return userIds[_index];
    }
    
    // Get all user IDs
    function getAllUserIds() public view returns (string[] memory) {
        return userIds;
    }
    
    // Check if user exists
    function doesUserExist(string memory _recordId) public view returns (bool) {
        return userExists[_recordId];
    }
    
    // Retrieve summary data (total users and all IDs)
    function retrieve() public view returns (
        uint256 totalUsers,
        string[] memory allUserIds
    ) {
        return (userIds.length, userIds);
    }
    
    // Get user metadata only (without full data)
    function getUserMetadata(string memory _recordId) public view returns (
        string memory recordId,
        uint256 createdTimestamp,
        uint256 modifiedTimestamp,
        string memory modifiedBy
    ) {
        require(userExists[_recordId], "User does not exist");
        
        User memory user = users[_recordId];
        return (
            user.recordId,
            user.createdTimestamp,
            user.modifiedTimestamp,
            user.modifiedBy
        );
    }
}