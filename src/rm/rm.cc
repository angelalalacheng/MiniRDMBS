#include "src/include/rm.h"
#include "rm_utils.h"
#include <string>
#include <cstring>

namespace PeterDB {
    std::unordered_map<std::string, FileHandle> RelationManager::fileHandleCache;

    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::getFileHandle(const std::string& tableName, FileHandle& fileHandle)
    {
        auto it = fileHandleCache.find(tableName);
        if (it != fileHandleCache.end()) {
            // 如果缓存中已存在，直接使用缓存的FileHandle
            fileHandle = it->second;
            return 0;
        }
        else {
            // 如果缓存中不存在，尝试打开文件并添加到缓存
            RC rc = RecordBasedFileManager::instance().openFile(tableName, fileHandle);
            if (rc == 0) {
                // 打开文件成功，将FileHandle添加到缓存
                fileHandleCache[tableName] = fileHandle;
            }
            return rc; // 返回打开文件的结果
        }
    }

    RC RelationManager::closeAndRemoveFileHandle(const std::string& fileName){
        auto it = fileHandleCache.find(fileName);
        if (it != fileHandleCache.end()) {
            // 如果找到，关闭文件并从缓存中移除
            RC rc = RecordBasedFileManager::instance().closeFile(it->second);
            fileHandleCache.erase(it);
            return rc; // 返回关闭文件的结果
        }
        return -1; // 文件未打开或不存在
    }

    RC RelationManager::createCatalog() {
        RC createTables = RecordBasedFileManager::instance().createFile("Tables");
        if (createTables == -1) return -1;
        RC createColumns = RecordBasedFileManager::instance().createFile("Columns");
        if (createColumns == -1) return -1;

        FileHandle fileHandleForTables, fileHandleForColumns;
        if (getFileHandle("Tables", fileHandleForTables) != 0) {
            return -1;
        }
        if (getFileHandle("Columns", fileHandleForColumns) != 0) {
            return -1;
        }

        std::vector<PeterDB::Attribute> ColumnsAttr = getColumnsAttr();

        // use insertRecord
        insertTablesCatalogInfo(fileHandleForTables);
        insertColumnsCatalogInfo(fileHandleForColumns);

        fileHandleCache["Tables"] = fileHandleForTables;
        fileHandleCache["Columns"] = fileHandleForColumns;

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        std::cout << "### deleteCatalog"<< std::endl;
        FileHandle fileHandleForTables, fileHandleForColumns;
        if (getFileHandle("Tables", fileHandleForTables) != 0 || getFileHandle("Columns", fileHandleForColumns) != 0) {
            return -1;
        }
        if(fileHandleCache.find("Tables") == fileHandleCache.end() || fileHandleCache.find("Columns") == fileHandleCache.end()) return -1;

        if (closeAndRemoveFileHandle("Tables") != 0 || closeAndRemoveFileHandle("Columns") != 0) {
            return -1;
        }

        RC deleteTables = RecordBasedFileManager::instance().destroyFile("Columns");
        if(deleteTables == -1) return -1;

        RC deleteColumns = RecordBasedFileManager::instance().destroyFile("Tables");
        if(deleteColumns == -1) return -1;

        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
//        std::cout << "### createTable: " + tableName <<std::endl;
        if(tableName == "Tables" || tableName == "Columns") return -1;
        FileHandle fileHandleForTables, fileHandleForColumns;
        if (getFileHandle("Tables", fileHandleForTables) != 0 || getFileHandle("Columns", fileHandleForColumns) != 0) {
            return -1;
        }

        if(RecordBasedFileManager::instance().createFile(tableName) != 0) {
            return -1;
        }

        FileHandle fileHandle;
        if (getFileHandle(tableName, fileHandle) != 0) {
            return -1;
        }

        int tableId = insertNewTableIntoTables(fileHandleForTables, tableName, attrs);
        insertNewAttrIntoColumns(fileHandleForColumns, tableId, tableName, attrs);

        fileHandleCache["Tables"] = fileHandleForTables;
        fileHandleCache["Columns"] = fileHandleForColumns;
        fileHandleCache[tableName] = fileHandle;

        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
//        std::cout << "### deleteTable: " + tableName << std::endl;
        if(tableName == "Tables" || tableName == "Columns") return -1;
        FileHandle fileHandle;
        if (getFileHandle(tableName, fileHandle) != 0) {
            return -1;
        }
        if(fileHandleCache.find(tableName) == fileHandleCache.end()) return -1;
        if (closeAndRemoveFileHandle(tableName) != 0) {
            return -1;
        }
        RecordBasedFileManager::instance().destroyFile(tableName);

        FileHandle fileHandleForTables, fileHandleForColumns;
        if (getFileHandle("Tables", fileHandleForTables) != 0 || getFileHandle("Columns", fileHandleForColumns) != 0) {
            return -1;
        }

        int tableNameLen = tableName.length();
        void *value = malloc(sizeof(int) + tableNameLen);
        memmove((char *) value, &tableNameLen, sizeof(int));
        memmove((char *) value + sizeof(int), &tableName[0], tableNameLen);

        std::vector<std::string> projectedAttr = {"table-id"};
        int nullIndicatorSize = 1;
        RBFM_ScanIterator rbfmScanIterator;
        RecordBasedFileManager::instance().scan(fileHandleForTables, getTablesAttr(), "table-name", EQ_OP, value, projectedAttr, rbfmScanIterator);

        RID tableRid;
        void* data = malloc(nullIndicatorSize + sizeof(int));
        int tableId;
        while(rbfmScanIterator.getNextRecord(tableRid, data) != RBFM_EOF){
            memmove(&tableId, (char *)data + nullIndicatorSize, sizeof(int));
        }
        free(data);
        free(value);

        RecordBasedFileManager::instance().deleteRecord(fileHandleForTables, getTablesAttr(), tableRid);
        rbfmScanIterator.close();

        void *value2 = malloc(sizeof(int));
        memmove(value2, &tableId, sizeof(int));
        std::vector<std::string> projectedAttr2 = {"column-type"};

        RecordBasedFileManager::instance().scan(fileHandleForColumns, getColumnsAttr(), "table-id", EQ_OP, value2, projectedAttr2, rbfmScanIterator);

        RID columnRid;
        void* data2 = malloc(nullIndicatorSize + sizeof(int));
        while(rbfmScanIterator.getNextRecord(columnRid, data2) != RBFM_EOF){
            RecordBasedFileManager::instance().deleteRecord(fileHandleForColumns, getColumnsAttr(), columnRid);
        }

        fileHandleCache["Tables"] = fileHandleForTables;
        fileHandleCache["Columns"] = fileHandleForColumns;

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        FileHandle fileHandleForTables, fileHandleForColumns;
        if (getFileHandle("Tables", fileHandleForTables) != 0 || getFileHandle("Columns", fileHandleForColumns) != 0) {
            return -1;
        }

        std::vector<std::string> projectedAttrs1 = {"table-id"};
        std::vector<std::string> projectedAttrs2 = {"column-name", "column-type", "column-length"};
        int tableNameLen = tableName.length(), nullIndicatorSize = 1;
        void *value = malloc(sizeof(int) + tableNameLen);
        memmove((char *) value, &tableNameLen, sizeof(int));
        memmove((char *) value + sizeof(int), &tableName[0], tableNameLen);

        RBFM_ScanIterator rbfmScanIterator;
        RecordBasedFileManager::instance().scan(fileHandleForTables, getTablesAttr(), "table-name", EQ_OP, value, projectedAttrs1, rbfmScanIterator);

        RID tableRid;
        int tableId;
        void *table = malloc(nullIndicatorSize + sizeof(int));
        while(rbfmScanIterator.getNextRecord(tableRid, table) != RBFM_EOF){
            memmove(&tableId, (char *)table + nullIndicatorSize, sizeof(int));
        }
        rbfmScanIterator.close();

        RecordBasedFileManager::instance().scan(fileHandleForColumns, getColumnsAttr(), "table-id", EQ_OP, &tableId, projectedAttrs2, rbfmScanIterator);

        RID rid;
        Attribute attr;
        void* data = malloc(200);
        while(rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF){
            int len, type, nameLen;
            std::string name;
            int offset = 1;

            memmove(&nameLen, (char *) data + offset, sizeof(int));
            offset += sizeof(int);
            name.resize(nameLen);
            memmove(&name[0], (char *) data + offset, nameLen);
            offset += nameLen;
            memmove(&type, (char *) data + offset, sizeof(int));
            offset += sizeof(int);
            memmove(&len, (char *) data + offset, sizeof(int));
            offset += sizeof(int);

            attr.name = name;
            attr.type = static_cast<AttrType>(type);
            attr.length = len;

            attrs.push_back(attr);
        }

        rbfmScanIterator.close();
        free(value);
        free(table);
        free(data);

//        fileHandleCache["Tables"] = fileHandleForTables;
//        fileHandleCache["Columns"] = fileHandleForColumns;

        if (closeAndRemoveFileHandle("Tables") != 0) {
            return -1;
        }
        if (closeAndRemoveFileHandle("Columns") != 0) {
            return -1;
        }

        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if(tableName == "Tables" || tableName == "Columns") return -1;

        FileHandle fileHandle;
        if (getFileHandle(tableName, fileHandle) != 0) {
            return -1;
        }

        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        RecordBasedFileManager::instance().insertRecord(fileHandle, attrs, data, rid);
//        void * readVal = malloc(1000);
//        std::cout << "insert and read\n";
//        RecordBasedFileManager::instance().readRecord(fileHandle, attrs, rid, readVal);
        fileHandleCache[tableName] = fileHandle;
//        readTuple(tableName, rid, readVal);
//        if (closeAndRemoveFileHandle(tableName) != 0) {
//            return -1;
//        }
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if(tableName == "Tables" || tableName == "Columns") return -1;

        FileHandle fileHandle;
        if (getFileHandle(tableName, fileHandle) != 0) {
            return -1;
        }

        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        RecordBasedFileManager::instance().deleteRecord(fileHandle, attrs, rid);

        fileHandleCache[tableName] = fileHandle;
//        if (closeAndRemoveFileHandle(tableName) != 0) {
//            return -1;
//        }
        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if(tableName == "Tables" || tableName == "Columns") return -1;

        FileHandle fileHandle;
        if (getFileHandle(tableName, fileHandle) != 0) {
            return -1;
        }

        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        RecordBasedFileManager::instance().updateRecord(fileHandle, attrs, data, rid);

        fileHandleCache[tableName] = fileHandle;
//        if (closeAndRemoveFileHandle(tableName) != 0) {
//            return -1;
//        }

        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        FileHandle fileHandle;
        if (getFileHandle(tableName, fileHandle) != 0) {
            return -1;
        }

        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        RC readResult = RecordBasedFileManager::instance().readRecord(fileHandle, attrs, rid, data);
//        void * val = malloc(50);
//        RecordBasedFileManager::instance().readAttribute(fileHandle, attrs, rid, "attr0", val);
//        int len;
//        memmove(&len, val, 4);
//        std::string s;
//        s.resize(len);
//        memmove(&s[0], (char*)val + 4, len);
        if(readResult == -1){
            return -1;
        }

        fileHandleCache[tableName] = fileHandle;
//        if (closeAndRemoveFileHandle(tableName) != 0) {
//            return -1;
//        }
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager::instance().printRecord(attrs, data, out);
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        FileHandle fileHandle;
        if (getFileHandle(tableName, fileHandle) != 0) {
            return -1;
        }

        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        unsigned int len;
        for(const Attribute& attr: attrs){
            if(attr.name == attributeName){
                len = attr.length;
            }
        }
        unsigned char * indicator = getNullIndicator();
        RecordBasedFileManager::instance().readAttribute(fileHandle, attrs, rid, attributeName, data);

        memmove((char *) data + 1, data, len);
        memmove(data, indicator, 1);

        fileHandleCache[tableName] = fileHandle;

        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        FileHandle fileHandle;
        if (getFileHandle(tableName, fileHandle) != 0) {
            return -1;
        }
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);

        RecordBasedFileManager::instance().scan(fileHandle, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);

        fileHandleCache[tableName] = fileHandle;

        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        if(rbfm_ScanIterator.getNextRecord(rid, data) == -1){
            return RM_EOF;
        }
        return 0;
    }

    RC RM_ScanIterator::close() {
        rbfm_ScanIterator.close();
        return 0;
    }

    //======================================================//

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB