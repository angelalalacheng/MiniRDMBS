#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_map>
#include "src/include/rbfm.h"
#include "src/include/ix.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    typedef enum {
        INSERT_IDX = 0,
        DELETE_IDX
    } Mode;

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RM_ScanIterator();

        ~RM_ScanIterator();

        RBFM_ScanIterator rbfm_ScanIterator;

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        IX_ScanIterator ix_ScanIterator;

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan
    };

    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();

        static FileHandle &getFileHandle(const std::string& fileName);

        static RC closeAndRemoveFileHandle(const std::string& fileName);

        static IXFileHandle &getIXFileHandle(const std::string& indexFileName);

        static RC closeAndRemoveIXFileHandle(const std::string& indexFileName);

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        // QE IX related (Catalog how to maintain info of index file and index attribute, check attributeName is in table or not)
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        RC handleRelatedIndex(const Mode mode, const std::string &tableName, std::vector<Attribute> &attrs, const RID &rid);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment
        static std::unordered_map<std::string, FileHandle> fileHandleCache;
        static std::unordered_map<std::string, IXFileHandle> ixFileHandleCache;
        std::unordered_map<std::string, std::vector<std::string>> indexFileAttributesMap;
    };

} // namespace PeterDB

#endif // _rm_h_