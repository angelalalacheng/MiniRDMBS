//
// Created by 鄭筠蓉 on 2024/2/7.
//

#ifndef PETERDB_RM_UTILS_H
#define PETERDB_RM_UTILS_H
#include <string>
#include <cstring>
#include <vector>
#include <algorithm>
#include <cassert>

unsigned char* getNullIndicator(){
    int nullFieldsIndicatorActualSize = 1;
    auto indicator = new unsigned char[nullFieldsIndicatorActualSize];
    memset(indicator, 0, nullFieldsIndicatorActualSize);

    return indicator;
}

std::vector<PeterDB::Attribute> getTablesAttr(){
    PeterDB::Attribute id;
    id.name = "table-id";
    id.length = (PeterDB::AttrLength) 4;
    id.type = PeterDB::TypeInt;

    PeterDB::Attribute tableName;
    tableName.name = "table-name";
    tableName.length = (PeterDB::AttrLength) 50;
    tableName.type = PeterDB::TypeVarChar;

    PeterDB::Attribute fileName;
    fileName.name = "file-name";
    fileName.length = (PeterDB::AttrLength) 50;
    fileName.type = PeterDB::TypeVarChar;

    std::vector<PeterDB::Attribute> attrs = {id, tableName, fileName};

    return attrs;
}

std::vector<PeterDB::Attribute> getColumnsAttr(){
    PeterDB::Attribute id;
    id.name = "table-id";
    id.length = (PeterDB::AttrLength) 4;
    id.type = PeterDB::TypeInt;

    PeterDB::Attribute columnName;
    columnName.name = "column-name";
    columnName.length = (PeterDB::AttrLength) 50;
    columnName.type = PeterDB::TypeVarChar;

    PeterDB::Attribute columnType;
    columnType.name = "column-type";
    columnType.length = (PeterDB::AttrLength) 4;
    columnType.type = PeterDB::TypeInt;

    PeterDB::Attribute columnLen;
    columnLen.name = "column-length";
    columnLen.length = (PeterDB::AttrLength) 4;
    columnLen.type = PeterDB::TypeInt;

    PeterDB::Attribute columnPos;
    columnPos.name = "column-position";
    columnPos.length = (PeterDB::AttrLength) 4;
    columnPos.type = PeterDB::TypeInt;

    std::vector<PeterDB::Attribute> attrs = {id, columnName, columnType, columnLen, columnPos};

    return attrs;
}

std::vector<PeterDB::Attribute> getIndicesAttr(){
    PeterDB::Attribute id;
    id.name = "index-id";
    id.length = (PeterDB::AttrLength) 4;
    id.type = PeterDB::TypeInt;

    PeterDB::Attribute indexName;
    indexName.name = "index-name";
    indexName.length = (PeterDB::AttrLength) 50;
    indexName.type = PeterDB::TypeVarChar;

    PeterDB::Attribute attributeName;
    attributeName.name = "attribute-name";
    attributeName.length = (PeterDB::AttrLength) 50;
    attributeName.type = PeterDB::TypeVarChar;

    std::vector<PeterDB::Attribute> attrs = {indexName, attributeName};

    return attrs;
}

void insertTablesCatalogInfo(PeterDB::FileHandle &fileHandle){
    unsigned char* nullIndicator = getNullIndicator();
    PeterDB::RID rid;

    std::vector<int> tableID = {1, 2, 3};
    std::vector<std::string> tableName = {"Tables", "Columns", "Indices"};
    std::vector<std::string> fileName = {"Tables", "Columns", "Indices"};

    for(int i = 0; i < tableName.size(); i++){
        unsigned offset = 0;
        char data[100];
        memmove(data + offset, nullIndicator, 1);
        offset += 1;

        memmove(data + offset, &tableID[i], sizeof(int));
        offset += sizeof(int);

        int tableNameLen = tableName[i].length();
        memmove(data + offset, &tableNameLen, sizeof(int));
        offset += sizeof(int);
        memmove(data + offset, &tableName[i][0], tableNameLen);
        offset += tableNameLen;

        int fileNameLen = fileName[i].length();
        memmove(data + offset, &fileNameLen, sizeof(int));
        offset += sizeof(int);
        memmove(data + offset, &fileName[i][0], fileNameLen);
        offset += fileNameLen;

        PeterDB::RecordBasedFileManager::instance().insertRecord(fileHandle, getTablesAttr(), data, rid);
    }
}

void insertColumnsCatalogInfo(PeterDB::FileHandle &fileHandle) {
    unsigned char* nullIndicator = getNullIndicator();
    PeterDB::RID rid;

    std::vector<int> tableID = {1, 1, 1, 2, 2, 2, 2, 2};
    std::vector<std::string> columnName = {"table-id", "table-name", "file-name", "table-id", "column-name", "column-type", "column-length", "column-position"};
    std::vector<int> columnType = {PeterDB::TypeInt, PeterDB::TypeVarChar, PeterDB::TypeVarChar, PeterDB::TypeInt, PeterDB::TypeVarChar, PeterDB::TypeInt, PeterDB::TypeInt, PeterDB::TypeInt};
    std::vector<int> columnLength = {4, 50, 50, 4, 50, 4, 4, 4};
    std::vector<int> columnPosition = {1, 2, 3, 1, 2, 3, 4, 5};

    for(int i = 0; i < columnName.size(); i++){
        unsigned offset = 0;
        char data[100];

        memmove(data + offset, nullIndicator, 1);
        offset += 1;
        memmove(data + offset, &tableID[i], sizeof(int));
        offset += sizeof(int);
        int columnNameLen = (int)columnName[i].length();
        memmove(data + offset, &columnNameLen, sizeof(int));
        offset += sizeof(int);
        memmove(data + offset, &columnName[i][0], columnNameLen);
        offset += columnNameLen;
        memmove(data + offset, &columnType[i], sizeof(int));
        offset += sizeof(int);
        memmove(data + offset, &columnLength[i], sizeof(int));
        offset += sizeof(int);
        memmove(data + offset, &columnPosition[i], sizeof(int));
        offset += sizeof(int);

        PeterDB::RecordBasedFileManager::instance().insertRecord(fileHandle, getColumnsAttr(), data, rid);
    }
}

int insertNewTableIntoTables(PeterDB::FileHandle &fileHandle, const std::string &tableName){
    PeterDB::RBFM_ScanIterator rbfm_ScanIterator;
    PeterDB::RecordBasedFileManager::instance().scan(fileHandle, getTablesAttr(), "table-id", PeterDB::NO_OP,
                                                     nullptr, {"table-id"}, rbfm_ScanIterator);
    int nullIndicatorSize = 1;
    PeterDB::RID rid;
    int maxId = 0;
    void *data = malloc(nullIndicatorSize + sizeof(int));
    while(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF){
        int id;
        memmove(&id, (char *)data + nullIndicatorSize , sizeof(int));
        maxId = std::max(maxId, id);
    }
    maxId += 1;

    unsigned offset = 0;
    void *record = malloc(100);
    unsigned char* nullIndicator = getNullIndicator();

    memmove((char *) record + offset, nullIndicator, 1);
    offset += 1;

    memmove((char *) record + offset, &maxId, sizeof(int));
    offset += sizeof(int);

    int tableNameLen = tableName.length();
    memmove((char *) record + offset, &tableNameLen, sizeof(int));
    offset += sizeof(int);
    memmove((char *) record + offset, &tableName[0], tableNameLen);
    offset += tableNameLen;

    std::string fileName = tableName;
    int fileNameLen = fileName.length();
    memmove((char *) record + offset, &fileNameLen, sizeof(int));
    offset += sizeof(int);
    memmove((char *) record + offset, &fileName[0], fileNameLen);
    offset += fileNameLen;

    PeterDB::RecordBasedFileManager::instance().insertRecord(fileHandle, getTablesAttr(), record, rid);
    free(data);
    free(record);
    return maxId;
}

void insertNewAttrIntoColumns(PeterDB::FileHandle &fileHandle, const int tableId, const std::string &tableName, const std::vector<PeterDB::Attribute> &attrs){
    unsigned char* nullIndicator = getNullIndicator();
    int columnPosition = 1;

    for(const auto & attr : attrs){
        std::string columnName = attr.name;
        int columnNameLen = (int)columnName.length();
        int columnType = attr.type;
        int columnLength = (int)attr.length;
        char data[100];
        PeterDB::RID rid;
        unsigned offset = 0;

        memmove(data + offset, nullIndicator, 1);
        offset += 1;
        memmove(data + offset, &tableId, sizeof(int));
        offset += sizeof(int);
        memmove(data + offset, &columnNameLen, sizeof(int));
        offset += sizeof(int);
        memmove( data + offset, &columnName[0], columnNameLen);
        offset += columnNameLen;
        memmove(data + offset, &columnType, sizeof(int));
        offset += sizeof(int);
        memmove(data + offset, &columnLength, sizeof(int));
        offset += sizeof(int);
        memmove(data + offset, &columnPosition, sizeof(int));
        offset += sizeof(int);

        columnPosition += 1;

        PeterDB::RecordBasedFileManager::instance().insertRecord(fileHandle, getColumnsAttr(), data, rid);
    }
}

void insertNewIndexIntoIndices(PeterDB::FileHandle &fileHandle, const int &indexId, const std::string &indexName, const std::string &attributeName){
    unsigned char* nullIndicator = getNullIndicator();
    PeterDB::RID rid;
    unsigned offset = 0;
    char data[100];

    memmove(data + offset, nullIndicator, 1);
    offset += 1;
    memmove(data + offset, &indexId, sizeof(int));
    offset += sizeof(int);
    int indexNameLen = indexName.length();
    memmove(data + offset, &indexNameLen, sizeof(int));
    offset += sizeof(int);
    memmove(data + offset, &indexName[0], indexNameLen);
    offset += indexNameLen;
    int attributeNameLen = attributeName.length();
    memmove(data + offset, &attributeNameLen, sizeof(int));
    offset += sizeof(int);
    memmove(data + offset, &attributeName[0], attributeNameLen);
    offset += attributeNameLen;

    PeterDB::RecordBasedFileManager::instance().insertRecord(fileHandle, getIndicesAttr(), data, rid);
}

#endif //PETERDB_RM_UTILS_H