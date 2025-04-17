// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @title ERPStorage
 * @dev Contract for storing ERPNext document hashes and metadata on blockchain
 */
contract ERPStorage {
    address public owner;
    
    // Document structure
    struct Document {
        string doctype;      // ERPNext document type (e.g., "Sales Invoice")
        string name;         // Document name in ERPNext (e.g., "SINV-00001")
        string eventType;    // Event type (e.g., "on_update", "on_submit")
        uint256 timestamp;   // Unix timestamp when recorded
        bytes32 dataHash;    // Hash of the document data
        address recorder;    // Address that recorded this document
    }
    
    // Mapping from document hash to Document struct
    mapping(bytes32 => Document) public documents;
    
    // Array to keep track of all recorded document hashes
    bytes32[] public documentHashes;
    
    // Mapping from doctype+name to array of hashes (for document history)
    mapping(bytes32 => bytes32[]) public documentHistory;
    
    // Events
    event DocumentRecorded(
        bytes32 indexed dataHash,
        string doctype,
        string name,
        string eventType,
        uint256 timestamp
    );
    
    // Constructor
    constructor() {
        owner = msg.sender;
    }
    
    /**
     * @dev Create a unique key for doctype+name combination
     * @param doctype The ERPNext document type
     * @param name The document name/ID
     * @return A bytes32 hash representing the document
     */
    function _createDocumentKey(string memory doctype, string memory name) 
        internal pure returns (bytes32) 
    {
        return keccak256(abi.encodePacked(doctype, ":", name));
    }
    
    /**
     * @dev Record a document to the blockchain
     * @param doctype Document type from ERPNext
     * @param name Document name/ID from ERPNext
     * @param eventType Event type that triggered recording
     * @param dataHash Hash of the document data
     * @return success Boolean indicating success
     */
    function recordDocument(
        string memory doctype,
        string memory name,
        string memory eventType,
        bytes32 dataHash
    ) 
        public 
        returns (bool success) 
    {
        // Create the document record
        Document memory doc = Document({
            doctype: doctype,
            name: name,
            eventType: eventType,
            timestamp: block.timestamp,
            dataHash: dataHash,
            recorder: msg.sender
        });
        
        // Store the document
        documents[dataHash] = doc;
        
        // Add to the list of all documents
        documentHashes.push(dataHash);
        
        // Track document history
        bytes32 docKey = _createDocumentKey(doctype, name);
        documentHistory[docKey].push(dataHash);
        
        // Emit event
        emit DocumentRecorded(
            dataHash,
            doctype,
            name,
            eventType,
            block.timestamp
        );
        
        return true;
    }
    
     /**
     * @dev Get document details by hash
     * @param dataHash The hash of the document data
     * @return doctype The document type
     * @return name The document name/ID
     * @return eventType The event type that triggered recording
     * @return timestamp When the document was recorded
     * @return recorder Address that recorded the document
     */
    function getDocument(bytes32 dataHash) 
        public 
        view 
        returns (
            string memory doctype,
            string memory name,
            string memory eventType,
            uint256 timestamp,
            address recorder
        ) 
    {
        Document memory doc = documents[dataHash];
        return (
            doc.doctype,
            doc.name,
            doc.eventType,
            doc.timestamp,
            doc.recorder
        );
    }
    
    /**
     * @dev Get the count of documents recorded
     * @return count Number of documents
     */
    function getDocumentCount() public view returns (uint256) {
        return documentHashes.length;
    }
    
    /**
     * @dev Check if a document hash exists
     * @param dataHash The hash to check
     * @return exists Boolean indicating if document exists
     */
    function documentExists(bytes32 dataHash) public view returns (bool) {
        return documents[dataHash].recorder != address(0);
    }
    
    /**
     * @dev Get document history length for a specific document
     * @param doctype Document type
     * @param name Document name/ID
     * @return count Number of history entries
     */
    function getDocumentHistoryCount(string memory doctype, string memory name) 
        public 
        view 
        returns (uint256) 
    {
        bytes32 docKey = _createDocumentKey(doctype, name);
        return documentHistory[docKey].length;
    }
    
    /**
     * @dev Get a specific history entry hash for a document
     * @param doctype Document type
     * @param name Document name/ID
     * @param index Index in history array
     * @return dataHash Hash of the document at that history point
     */
    function getDocumentHistoryHash(
        string memory doctype,
        string memory name,
        uint256 index
    ) 
        public 
        view 
        returns (bytes32) 
    {
        bytes32 docKey = _createDocumentKey(doctype, name);
        require(index < documentHistory[docKey].length, "Index out of bounds");
        return documentHistory[docKey][index];
    }
}