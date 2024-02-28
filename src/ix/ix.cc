#include "src/include/ix.h"
#include "src/ix/ix_utils.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        if(PagedFileManager::instance().createFile(fileName) == 0){
            return 0;
        }
        return -1;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        if(PagedFileManager::instance().destroyFile(fileName) == 0){
            return 0;
        }
        return -1;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if(ixFileHandle.fileHandle.openFileStream != nullptr){
            std::cout << "error: File already opened" << std::endl;
            return -1;
        }
        if(PagedFileManager::instance().openFile(fileName, ixFileHandle.fileHandle) == 0){
            return 0;
        }
        return -1;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        if(PagedFileManager::instance().closeFile(ixFileHandle.fileHandle) == 0){
            return 0;
        }
        return -1;
    }

    /*
     * This method should insert a new entry, a <key, rid> pair, into the index file.
     * The second parameter is the attribute descriptor of the key, and the parameter rid identifies the record that will be paired with the key in the index file. (An index contains only the records' ids, not the records themselves.)
     * The format for the passed-in key is the following:
     * For INT and REAL: use 4 bytes;
     * For VARCHAR: use 4 bytes for the length followed by the characters.
     * Note that the given key-value doesn't contain Null flags. In this project, you can safely assume all the keys are not null. Of course, real systems that support index-query plans DO enter null keys, into their indexes for completeness.
     * An overflow can happen if we try to insert an entry into a fully-occupied node. As we discussed in the B+ Tree part, you are required to restructure the tree shape.
     */
    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        short entryInPage = getNumOfEntryInPage(attribute);
        if(ixFileHandle.fileHandle.getNumberOfPages() == 0){
            dummyNode(ixFileHandle.fileHandle); // pageNum = 0
            initialLeafNodePage(ixFileHandle.fileHandle, attribute, entryInPage);   // pageNum = 1
        }
        NewEntry *newChildEntry = nullptr;
        recursiveInsertBTree(ixFileHandle.fileHandle, 0, attribute, key, rid, newChildEntry, entryInPage);
        return 0;
    }
    /*
     * This method should delete the entry for the <key, rid> pair from the index.
     * You should return an error code if this method is called with a non-existing entry.
     * The format of the key is the same as that is given to insertEntry() function.
     * Simplification: You may implement lazy deletion.
     * In this approach, when an entry is deleted, even if it causes a leaf page to become less than half full, no redistribution or node merging takes place -- the underfilled page remains in the tree.
     * Regardless of which approach you use, deletion must work: once an IX component client asks for an entry to be deleted, that deleted entry should not appear in a subsequent index search (scan).
     */
    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        AttrLength typeLen = attribute.type == TypeVarChar ? 4 : sizeof (int) + attribute.length;
        SearchEntryInfo searchEntryInfo = searchEntry(ixFileHandle, attribute, key);

        if(searchEntryInfo.targetIndex == -1) return -1;

        char nodeData [PAGE_SIZE];
        ixFileHandle.fileHandle.readPage(searchEntryInfo.targetPage, nodeData);

        LeafNode leafNode;
        deserializeLeafNode(leafNode, nodeData);

        clearEntry(leafNode.key, searchEntryInfo.targetIndex, typeLen);
        leafNode.rid.erase(leafNode.rid.begin() + searchEntryInfo.targetIndex);
        leafNode.rid.emplace_back();
        leafNode.currentKey--;

        serializeLeafNode(leafNode, nodeData);

        ixFileHandle.fileHandle.writePage(searchEntryInfo.targetPage, nodeData);

        return 0;
    }

    SearchEntryInfo IndexManager::searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key) {
        AttrLength typeLen = attribute.type == TypeVarChar ? 4 : sizeof (int) + attribute.length;
        PageNum currentPage = getRootPage(ixFileHandle.fileHandle);

        char nodeData [PAGE_SIZE];
        NodeHeader nodeHeader;

        while (true) {
            ixFileHandle.fileHandle.readPage(currentPage, nodeData);
             getNodeHeader(nodeData, nodeHeader);

            if(nodeHeader.isLeaf){ // leaf node
                LeafNode leafNode;
                deserializeLeafNode(leafNode, nodeData);
                SearchEntryInfo searchEntryInfo;
                searchEntryInfo.targetIndex = -1;
                for(int i = 0; i < leafNode.currentKey; i++){
                    int temp;
                    getEntry(leafNode.key, i, typeLen, &temp);
                    if(*reinterpret_cast<const int*>(key) == temp){
                        searchEntryInfo.targetPage = currentPage;
                        searchEntryInfo.targetIndex = i;
                        return searchEntryInfo;
                    }
                }
                return searchEntryInfo;
            }
            else{ // non-leaf node
                NonLeafNode nonLeafNode;
                deserializeNonLeafNode(nonLeafNode, nodeData);
                PeterDB::PageNum nextNode = -1;
                for(int i = 0; i < nonLeafNode.currentKey; i++){
                    int temp;
                    getEntry(nonLeafNode.routingKey, i, typeLen, &temp);
                    if(*reinterpret_cast<const int*>(key) < temp){
                        nextNode = nonLeafNode.pointers[i];
                    }
                }
                if(nextNode == -1){
                    nextNode = nonLeafNode.pointers[nonLeafNode.currentKey];
                }

                currentPage = nextNode;
            }
        }
    }

    /*
     * This method should initialize a condition-based scan over the entries in the open index.
     * Then, when IX_ScanIterator::getNextEntry() is invoked, the iterator should incrementally produce the entries for all records
     * whose indexed attribute key falls into the range specified by the lowKey, highKey, and inclusive flags.
     * If lowKey is a NULL pointer, it can be interpreted as -infinity.
     * If highKey is NULL, it can be interpreted as +infinity.
     * Otherwise, the format of the parameter lowKey and highKey is the same as the format of the key in IndexManager::insertEntry().
     */
    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        PageNum currentPage = getRootPage(ixFileHandle.fileHandle);
        char nodeData [PAGE_SIZE];
        NodeHeader nodeHeader;

        while(true){
            ixFileHandle.fileHandle.readPage(currentPage, nodeData);
            getNodeHeader(nodeData, nodeHeader);
            if(nodeHeader.isLeaf){
                LeafNode leafNode;
                deserializeLeafNode(leafNode, nodeData);
                bool FindingHighKey = true;

                while(FindingHighKey){
                    for(int i = 0; i < leafNode.currentKey; i++){
                        int temp;
                        getEntry(leafNode.key, i, attribute.type == TypeVarChar ? 4 : sizeof (int) + attribute.length, &temp);
                        if(temp >= *reinterpret_cast<const int*>(lowKey)){
                            if(!lowKeyInclusive && temp == *reinterpret_cast<const int*>(lowKey)){
                                continue;
                            }
                            ix_ScanIterator.candidates.emplace_back(leafNode.rid[i]);
                            ix_ScanIterator.keys.emplace_back(static_cast<void*>(&temp));
                        }
                        if(temp >= *reinterpret_cast<const int*>(highKey)){
                            if(!highKeyInclusive && temp == *reinterpret_cast<const int*>(highKey)){
                                ix_ScanIterator.candidates.pop_back();
                            }
                            FindingHighKey = false;
                            break;
                        }
                    }

                    if(FindingHighKey){
                        currentPage = nodeHeader.rightSibling;
                        ixFileHandle.fileHandle.readPage(currentPage, nodeData);
                        deserializeLeafNode(leafNode, nodeData);
                    }
                }
            }
            else{
                NonLeafNode nonLeafNode;
                deserializeNonLeafNode(nonLeafNode, nodeData);
                PeterDB::PageNum nextNode = -1;

                if(lowKey == nullptr){
                    nextNode = nonLeafNode.pointers[0];
                }
                else{
                    for(int i = 0; i < nonLeafNode.currentKey; i++){
                        int temp;
                        getEntry(nonLeafNode.routingKey, i, attribute.type == TypeVarChar ? 4 : sizeof (int) + attribute.length, &temp);
                        if(*reinterpret_cast<const int*>(lowKey) < temp){
                            nextNode = nonLeafNode.pointers[i];
                            break;
                        }
                    }
                    if(nextNode == -1){
                        nextNode = nonLeafNode.pointers[nonLeafNode.currentKey];
                    }
                }
                currentPage = nextNode;
            }

            ix_ScanIterator.fileHandle = &ixFileHandle.fileHandle;
            ix_ScanIterator.attribute = attribute;

            return 0;
        }
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {

    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        if(candidates.empty() || keys.empty() || currentIndex >= candidates.size()){
            return IX_EOF;
        }

        rid = candidates[currentIndex];
        if(attribute.type == TypeVarChar){
            int length;
            memmove(&length, keys[currentIndex], 4);
            memmove(key, keys[currentIndex], 4 + length);
        }
        else{
            memmove(key, keys[currentIndex], 4);
        }

        currentIndex ++;
        return 0;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        setCounterValues();
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }

    RC IXFileHandle::setCounterValues() {
        ixReadPageCounter = fileHandle.readPageCounter;
        ixWritePageCounter = fileHandle.writePageCounter;
        ixAppendPageCounter = fileHandle.appendPageCounter;
        return 0;
    }

} // namespace PeterDB