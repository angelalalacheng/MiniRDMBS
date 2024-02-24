#include "src/include/ix.h"
#include "src/ix/ix_utils.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        if(!PagedFileManager::instance().createFile(fileName)){
            return 0;
        }
        return -1;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        if(!PagedFileManager::instance().destroyFile(fileName)){
            return 0;
        }
        return -1;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if(!PagedFileManager::instance().openFile(fileName, ixFileHandle.fileHandle)){
            return 0;
        }
        return -1;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        if(!PagedFileManager::instance().closeFile(ixFileHandle.fileHandle)){
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
        if(ixFileHandle.fileHandle.getNumberOfPages() == 0){
            size_t entryInPage = getNumOfEntryInPage(attribute);
            dummyNode(ixFileHandle.fileHandle);
//            initialNewNode(ixFileHandle.fileHandle, 1);
        }

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
        return -1;
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
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
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