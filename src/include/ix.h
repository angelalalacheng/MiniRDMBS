#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    typedef struct {
        short isLeaf;
        short isDummy;
        int parent;       // no parent then -1
//        int leftSibling;  // page number of the left sibling (if is not leaf then save it -1)
        int rightSibling; // page number of the right sibling
    } NodeHeader;

    typedef struct{
        short currentKey;
        short maxKeys;
        std::vector<char> routingKey;
        std::vector<PageNum> pointers;
    } NonLeafNode;

    typedef struct {
        short currentKey;
        short maxKeys;
        std::vector<char> key;
        std::vector<RID> rid;
    } LeafNode;

    typedef struct {
        void *key;
        PageNum pageNum;
    } NewEntry;

    typedef struct {
        PageNum targetPage;
        int targetIndex;
    } SearchEntryInfo;

    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        SearchEntryInfo searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IX_ScanIterator {
    public:

        FileHandle *fileHandle;
        size_t currentIndex = 0;
        Attribute attribute;
        std::vector<void*> keys;
        std::vector<RID> candidates;

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;
        FileHandle fileHandle;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PFM FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        // Set the counter values from PFM FileHandles
        RC setCounterValues();

    };
}// namespace PeterDB
#endif // _ix_h_
