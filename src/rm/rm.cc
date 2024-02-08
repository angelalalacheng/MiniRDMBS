#include "src/include/rm.h"
#include "rm_utils.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        FileHandle fileHandleForTables, fileHandleForColumns;
        RC createTables = RecordBasedFileManager::instance().createFile("Tables.tbl");
        if (createTables == -1) return -1;

        RC createColumns = RecordBasedFileManager::instance().createFile("Columns.tbl");
        if (createColumns == -1) return -1;

        void *TablesData = nullptr, *ColumnsData = nullptr;
        getTablesCatalogInfo(TablesData);
        getColumnsCatalogInfo(ColumnsData);

        std::vector<PeterDB::Attribute> TablesAttr = getTablesAttr();
        std::vector<PeterDB::Attribute> ColumnsAttr = getColumnsAttr();

        // use insertRecord
        RecordBasedFileManager::instance().openFile("Tables.tbl", fileHandleForTables);
        RecordBasedFileManager::instance().openFile("Columns.tbl", fileHandleForColumns);
        getFileHandle["Tables"] = fileHandleForTables;
        getFileHandle["Columns"] = fileHandleForColumns;

        RID rid;
        RecordBasedFileManager::instance().insertRecord(fileHandleForTables, TablesAttr, TablesData, rid);
        RecordBasedFileManager::instance().insertRecord(fileHandleForColumns, ColumnsAttr, ColumnsData, rid);

        // close and flush
        RecordBasedFileManager::instance().closeFile(fileHandleForTables);
        RecordBasedFileManager::instance().closeFile(fileHandleForColumns);
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RecordBasedFileManager::instance().destroyFile("Columns.tbl");
        RecordBasedFileManager::instance().destroyFile("Tables.tbl");

        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        FileHandle fileHandle;
        RecordBasedFileManager::instance().createFile(tableName + ".tbl");
        RecordBasedFileManager::instance().openFile(tableName + ".tbl", fileHandle);
        RecordBasedFileManager::instance().closeFile(fileHandle);

        getFileHandle[tableName] = fileHandle;
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        std::vector<Attribute> attrs;
        getAttributes("Tables", attrs);

        int tableNameLen = tableName.length();
        void *value = malloc(sizeof(int) + tableNameLen);
        memmove((char *) value, &tableNameLen, sizeof(int));
        memmove((char *) value + sizeof(int), &tableName[0], tableNameLen);

        std::vector<std::string> projectedAttr = {"table-id"};

        RBFM_ScanIterator rbfmScanIterator;
        RecordBasedFileManager::instance().scan(getFileHandle["Tables"], attrs, "table-name", EQ_OP, value, projectedAttr, rbfmScanIterator);

        RID tableRid;
        void* data = malloc(sizeof(int));
        int tableId;
        while(rbfmScanIterator.getNextRecord(tableRid, data) != RBFM_EOF){
            memmove(&tableId, data, sizeof(int));
        }
        free(data);
        free(value);

        RecordBasedFileManager::instance().deleteRecord(getFileHandle["Tables"], attrs, tableRid);
        rbfmScanIterator.close();

        void *value2 = malloc(sizeof(int));
        memmove(value2, &tableId, sizeof(int));
        std::vector<std::string> projectedAttr2 = {"column-type"};
        getAttributes("Columns", attrs);

        RecordBasedFileManager::instance().scan(getFileHandle["Columns"], attrs, "table-id", EQ_OP, value2, projectedAttr2, rbfmScanIterator);

        RID columnRid;
        void* data2 = malloc(sizeof(int));
        while(rbfmScanIterator.getNextRecord(columnRid, data2) != RBFM_EOF){
            RecordBasedFileManager::instance().deleteRecord(getFileHandle["Columns"], attrs, columnRid);
        }

        RecordBasedFileManager::instance().destroyFile(tableName + ".tbl");

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        std::vector<std::string> projectedAttrs1 = {"table-id"};
        std::vector<std::string> projectedAttrs2 = {"column-name", "column-type", "column-length"};
        int tableNameLen = tableName.length();
        void *value = malloc(sizeof(int) + tableNameLen);
        memmove((char *) value, &tableNameLen, sizeof(int));
        memmove((char *) value + sizeof(int), &tableName[0], tableNameLen);

        RBFM_ScanIterator rbfmScanIterator;
        RecordBasedFileManager::instance().scan(getFileHandle["Tables"], getTablesAttr(), "table-name", EQ_OP, value, projectedAttrs1, rbfmScanIterator);

        RID tableRid;
        void *tableId = malloc(sizeof(int));
        while(rbfmScanIterator.getNextRecord(tableRid, tableId) != RBFM_EOF){}
        rbfmScanIterator.close();

        RecordBasedFileManager::instance().scan(getFileHandle["Columns"], getColumnsAttr(), "table-id", EQ_OP, tableId, projectedAttrs2, rbfmScanIterator);

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
        free(tableId);
        free(data);
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        RecordBasedFileManager::instance().insertRecord(getFileHandle[tableName], attrs, data, rid);
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        RecordBasedFileManager::instance().deleteRecord(getFileHandle[tableName], attrs, rid);
        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        RecordBasedFileManager::instance().updateRecord(getFileHandle[tableName], attrs, data, rid);
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        RecordBasedFileManager::instance().readRecord(getFileHandle[tableName], attrs, rid, data);
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager::instance().printRecord(attrs, data, out);
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        RecordBasedFileManager::instance().readAttribute(getFileHandle[tableName], attrs, rid, attributeName, data);
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {

        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);

        RecordBasedFileManager::instance().scan(getFileHandle[tableName], attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);

        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        if(!rbfm_ScanIterator.getNextRecord(rid, data)){
            return 0;
        }
        return RM_EOF;
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