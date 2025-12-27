// SPDX-License-Identifier: MIT
pragma solidity ^0.8.11;

contract DocumentCertificate {
    address public cidAddress;  // indexed, address of document
    string public cid;          // cid of file
    uint public nik;            // indexed, nik of patient
    address public from;        // indexed, address of sender(admin,patient)
    string public category;     // category of document (medical record, prescription, etc.)
    string public status;       // status of document (owner, shared, etc.)
    string public action;       // indexed (upload, view, share, delete, download)
    string public details;      // details of document (name, size, type, fromwho, towho, etc)
    uint public did;            // time of upload
    uint public date;           // date of action

    event dataDocumentCertificate(
        address indexed cidAddress,
        string cid,
        uint indexed nik, 
        address indexed from, 
        string category,
        string status,
        string action,
        string details,
        uint did,
        uint date
    );

    // Store document data
    function setDocument(
        address _cidAddress,
        string memory _cid,
        uint _nik,
        address _from,
        string memory _category,
        string memory _status,
        string memory _action,
        string memory _details,
        uint _did,
        uint _date
    ) external {
        cidAddress = _cidAddress;
        cid = _cid;
        nik = _nik;
        from = _from;
        category = _category;
        status = _status;
        action = _action;
        details = _details;
        did = _did;
        date = _date;

        emit dataDocumentCertificate(cidAddress, cid, nik, from, category, status, action, details, did, date);
    }

    // Retrieve all document data with named returns
    function retrieve() external view returns (
        address _cidAddress,
        string memory _cid,
        uint _nik,
        address _from,
        string memory _category,
        string memory _status,
        string memory _action,
        string memory _details,
        uint _did,
        uint _date
    ) {
        return (
            cidAddress,
            cid,
            nik,
            from,
            category,
            status,
            action,
            details,
            did,
            date
        );
    }

    // Get document info as a structured response
    function getDocumentInfo() external view returns (
        address _cidAddress,
        string memory _cid,
        uint _nik,
        address _from,
        string memory _category,
        string memory _status,
        string memory _action,
        string memory _details,
        uint _did,
        uint _date
    ) {
        return (
            cidAddress,
            cid,
            nik,
            from,
            category,
            status,
            action,
            details,
            did,
            date
        );
    }
}