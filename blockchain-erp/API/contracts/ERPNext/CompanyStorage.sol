// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract CompanyStorage {
    // Company data structure
    struct Company {
        string recordId;          // PT Fiyansa Mulya
        uint256 createdTimestamp; // creation timestamp converted to unix
        uint256 modifiedTimestamp; // modified timestamp converted to unix
        string modifiedBy;        // Administrator or user email
        string allData;           // Complete JSON string of all company data
    }
    
    // Mapping from company record ID to company data
    mapping(string => Company) public companies;
    
    // Array to store all company record IDs for enumeration
    string[] public companyIds;
    
    // Mapping to check if company exists
    mapping(string => bool) public companyExists;
    
    // Events
    event CompanyStored(string indexed recordId, uint256 createdTimestamp, uint256 modifiedTimestamp);
    event CompanyUpdated(string indexed recordId, uint256 modifiedTimestamp);
    
    // Store or update company data
    function storeCompany(
        string memory _recordId,
        uint256 _createdTimestamp,
        uint256 _modifiedTimestamp,
        string memory _modifiedBy,
        string memory _allData
    ) public returns (bool) {
        // Check if this is a new company
        bool isNewCompany = !companyExists[_recordId];
        
        // Store company data
        companies[_recordId] = Company({
            recordId: _recordId,
            createdTimestamp: _createdTimestamp,
            modifiedTimestamp: _modifiedTimestamp,
            modifiedBy: _modifiedBy,
            allData: _allData
        });
        
        // If new company, add to array and mark as existing
        if (isNewCompany) {
            companyIds.push(_recordId);
            companyExists[_recordId] = true;
            emit CompanyStored(_recordId, _createdTimestamp, _modifiedTimestamp);
        } else {
            emit CompanyUpdated(_recordId, _modifiedTimestamp);
        }
        
        return true;
    }
    
    // Get company data
    function getCompany(string memory _recordId) public view returns (
        string memory recordId,
        uint256 createdTimestamp,
        uint256 modifiedTimestamp,
        string memory modifiedBy,
        string memory allData
    ) {
        require(companyExists[_recordId], "Company does not exist");
        
        Company memory company = companies[_recordId];
        return (
            company.recordId,
            company.createdTimestamp,
            company.modifiedTimestamp,
            company.modifiedBy,
            company.allData
        );
    }
    
    // Get total number of companies
    function getTotalCompanies() public view returns (uint256) {
        return companyIds.length;
    }
    
    // Get company record ID by index
    function getCompanyIdByIndex(uint256 _index) public view returns (string memory) {
        require(_index < companyIds.length, "Index out of bounds");
        return companyIds[_index];
    }
    
    // Get all company IDs
    function getAllCompanyIds() public view returns (string[] memory) {
        return companyIds;
    }
    
    // Check if company exists
    function doesCompanyExist(string memory _recordId) public view returns (bool) {
        return companyExists[_recordId];
    }
    
    // Retrieve summary data (total companies and all IDs)
    function retrieve() public view returns (
        uint256 totalCompanies,
        string[] memory allCompanyIds
    ) {
        return (companyIds.length, companyIds);
    }
    
    // Get company metadata only (without full data)
    function getCompanyMetadata(string memory _recordId) public view returns (
        string memory recordId,
        uint256 createdTimestamp,
        uint256 modifiedTimestamp,
        string memory modifiedBy
    ) {
        require(companyExists[_recordId], "Company does not exist");
        
        Company memory company = companies[_recordId];
        return (
            company.recordId,
            company.createdTimestamp,
            company.modifiedTimestamp,
            company.modifiedBy
        );
    }
}