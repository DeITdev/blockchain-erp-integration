// SPDX-License-Identifier: MIT
pragma solidity ^0.8.11;

contract ContractRegistry {
    mapping(string => address) public contracts;
    mapping(string => bool) public isDeployed;
    mapping(string => address[]) public contractHistory;
    string[] public contractNames;

    event ContractRegistered(string indexed name, address indexed contractAddress, bool isUpdate);
    event ContractUpdated(string indexed name, address indexed oldAddress, address indexed newAddress);

    function registerContract(string memory name, address contractAddress) public {
        require(contractAddress != address(0), "Invalid contract address");
        
        bool isUpdate = isDeployed[name];
        address oldAddress = contracts[name];
        
        if (!isDeployed[name]) {
            contractNames.push(name);
            isDeployed[name] = true;
        } else {
            emit ContractUpdated(name, oldAddress, contractAddress);
        }
        
        contracts[name] = contractAddress;
        contractHistory[name].push(contractAddress);
        
        emit ContractRegistered(name, contractAddress, isUpdate);
    }

    function getContract(string memory name) public view returns (address) {
        return contracts[name];
    }

    function getAllContracts() public view returns (string[] memory) {
        return contractNames;
    }

    function isContractDeployed(string memory name) public view returns (bool) {
        return isDeployed[name];
    }

    function getContractCount() public view returns (uint256) {
        return contractNames.length;
    }

    function getContractHistory(string memory name) public view returns (address[] memory) {
        return contractHistory[name];
    }

    function getContractVersion(string memory name) public view returns (uint256) {
        return contractHistory[name].length;
    }
}
