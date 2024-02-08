//
// Created by 鄭筠蓉 on 2024/2/7.
//

#ifndef PETERDB_RM_UTILS_H
#define PETERDB_RM_UTILS_H
#include <string>
#include <cstring>
#include <vector>

unsigned char* getNullIndicator(){
    int nullFieldsIndicatorActualSize = 1;
    auto indicator = new unsigned char[nullFieldsIndicatorActualSize];
    memset(indicator, 0, nullFieldsIndicatorActualSize);

    return indicator;
}

void getTablesCatalogInfo(void* TablesData){
    unsigned offset = 0;
    unsigned char* nullIndicator = getNullIndicator();

    std::string TablesHeading = "table-id:int, table-name:varchar(50), file-name:varchar(50)";
    memmove((char *) TablesData + offset, &TablesHeading[0], TablesHeading.length());
    offset += (int)TablesHeading.length();

    std::vector<int> tableID = {1, 2};
    std::vector<std::string> tableName = {"Tables", "Columns"};
    std::vector<std::string> fileName = {"Tables.tbl", "Columns.tbl"};

    for(int i = 0; i < tableName.size(); i++){
        memmove((char *) TablesData + offset, nullIndicator, 1);
        offset += 1;

        memmove((char *) TablesData + offset, &tableID[i], sizeof(int));
        offset += sizeof(int);

        int tableNameLen = tableName[i].length();
        memmove((char *) TablesData + offset, &tableNameLen, sizeof(int));
        offset += sizeof(int);
        memmove((char *) TablesData + offset, &tableName[i][0], tableNameLen);
        offset += tableNameLen;

        int fileNameLen = fileName[i].length();
        memmove((char *) TablesData + offset, &fileNameLen, sizeof(int));
        offset += sizeof(int);
        memmove((char *) TablesData + offset, &fileName[i][0], fileNameLen);
        offset += fileNameLen;
    }
}

void getColumnsCatalogInfo(void* ColumnsData) {
    unsigned offset = 0;
    unsigned char* nullIndicator = getNullIndicator();

    std::string ColumnsHeading = "table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int";
    memmove((char *) ColumnsData + offset, &ColumnsHeading[0], ColumnsHeading.length());
    offset += (int)ColumnsHeading.length();

    std::vector<int> tableID = {1, 1, 1, 2, 2, 2, 2, 2};
    std::vector<std::string> columnName = {"table-id", "table-name", "file-name", "table-id", "column-name", "column-type", "column-length", " column-position"};
    std::vector<int> columnType = {PeterDB::TypeInt, PeterDB::TypeVarChar, PeterDB::TypeVarChar, PeterDB::TypeInt, PeterDB::TypeVarChar, PeterDB::TypeInt, PeterDB::TypeInt, PeterDB::TypeInt};
    std::vector<int> columnLength = {4, 50, 50, 4, 50, 4, 4, 4};
    std::vector<int> columnPosition = {1, 2, 3, 1, 2, 3, 4, 5};

    for(int i = 0; i < columnName.size(); i++){
        memmove((char *) ColumnsData + offset, nullIndicator, 1);
        offset += 1;
        memmove((char *) ColumnsData + offset, &tableID[i], sizeof(int));
        offset += sizeof(int);
        int columnNameLen = (int)columnName[i].length();
        memmove((char *) ColumnsData + offset, &columnNameLen, sizeof(int));
        offset += sizeof(int);
        memmove((char *) ColumnsData + offset, &columnName[i][0], columnNameLen);
        offset += columnNameLen;
        memmove((char *) ColumnsData + offset, &columnType[i], sizeof(int));
        offset += sizeof(int);
        memmove((char *) ColumnsData + offset, &columnLength[i], sizeof(int));
        offset += sizeof(int);
        memmove((char *) ColumnsData + offset, &columnPosition[i], sizeof(int));
        offset += sizeof(int);
    }
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
    columnLen.name = "column-position";
    columnLen.length = (PeterDB::AttrLength) 4;
    columnLen.type = PeterDB::TypeInt;

    std::vector<PeterDB::Attribute> attrs = {id, columnName, columnType, columnLen, columnPos};

    return attrs;
}
#endif //PETERDB_RM_UTILS_H