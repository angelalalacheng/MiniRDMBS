#include <cassert>
#include <sstream>
#include <cstring>

#include "src/include/rbfm.h"
#include "src/rbfm/rbfm_utils.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }
    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        if(!PagedFileManager::instance().createFile(fileName)){
            return 0;
        }
        return -1;
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        if(!PagedFileManager::instance().destroyFile(fileName)){
            return 0;
        }
        return -1;
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if(!PagedFileManager::instance().openFile(fileName, fileHandle)){
            return 0;
        }
        return -1;
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        if(!PagedFileManager::instance().closeFile(fileHandle)){
            return 0;
        }
        return -1;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        int offset = 0, recordMetaSize = 0, recordDataSize = 0;
        int fields = (int)recordDescriptor.size();
        int indicatorSize = getNullIndicatorSize(fields);
        char indicator[indicatorSize];

        memset(indicator, 0, indicatorSize);
        memmove(indicator, (char *) data + offset, indicatorSize);
        offset += indicatorSize;
        // Get the Null Indicator Data
        std::vector<int> nullFlags = getNullFlags(fields, indicator, indicatorSize);

        void* recordMeta = malloc(indicatorSize + fields * 2);
        void* recordData = malloc(getActualDataSize(nullFlags, recordDescriptor, indicatorSize, data));

        // Extract the data
        for(int i = 0; i< nullFlags.size(); i++){
            if(!nullFlags[i]){
                if(recordDescriptor[i].type == TypeInt){
                    int intVal;
                    memmove(&intVal, (char *) data + offset, sizeof(int));
                    offset += 4;
                    memmove((char *)recordData + recordDataSize, &intVal, sizeof(int));
                    recordDataSize += 4;
                }
                else if(recordDescriptor[i].type == TypeReal){
                    float floatVal;
                    memmove(&floatVal, (char *) data + offset, 4);
                    offset += 4;
                    memmove((char *)recordData + recordDataSize, &floatVal, sizeof(floatVal));
                    recordDataSize += 4;
                }
                else if(recordDescriptor[i].type == TypeVarChar) {
                    int length;
                    std::string s;
                    memmove(&length, (char *) data + offset, 4);
                    offset += 4;
                    s.resize(length);
                    memmove(&s[0], (char *) data + offset, length);
                    offset += length;
                    memmove((char *)recordData + recordDataSize, &length, sizeof(int));
                    recordDataSize += 4;
                    memmove((char *)recordData + recordDataSize, &s[0], length);
                    recordDataSize += length;
                }

                int offVal = (int)(indicatorSize + fields * sizeof(int16_t) + recordDataSize);
                memmove((char *)recordMeta + recordMetaSize, &offVal, 2);
            }
            else{
                int offVal;
                memmove(&offVal, (char *)recordMeta + recordMetaSize - 2, 2);
            }
            recordMetaSize += 2;
        }

        // Calculate the record size and prepare the record
        int totalRecordSize = indicatorSize + recordMetaSize + recordDataSize;
//        std::cout << "### totalRecordSize: " << totalRecordSize <<std::endl;

        void* record = malloc(totalRecordSize);
        int recordOff = 0;
        memmove((char *)record + recordOff, indicator, indicatorSize);
        recordOff += indicatorSize;
        memmove((char *)record + recordOff, recordMeta, recordMetaSize);
        recordOff += recordMetaSize;
        memmove((char *)record + recordOff, recordData, recordDataSize);

        // find available page: first check the last page
        int availablePage = checkFreeSpaceOfLastPage(fileHandle, totalRecordSize + SLOT_DIR_SIZE);
        // find from first page
        if(availablePage == -1) {
            availablePage = findFreePageFromFirst(fileHandle, totalRecordSize + SLOT_DIR_SIZE);
            // no suitable page in file
            if(availablePage == -1) {
                initialNewPage(fileHandle);
                availablePage = fileHandle.getNumberOfPages() - 1;
            }
        }

        // read the available page
        char buffer[PAGE_SIZE];
        fileHandle.readPage(availablePage, buffer);
        std::vector<int16_t> dirInfo = getDirInfo(buffer, -1);
        int16_t freeSpace = dirInfo[0], recordNum = dirInfo[1], lastRecordOffset = dirInfo[2], lastRecordLen = dirInfo[3];

        // Insert the record to the page
        memmove(buffer + lastRecordOffset + lastRecordLen, record, totalRecordSize);
        updateSlotDirectory(buffer, (int16_t)(freeSpace - totalRecordSize - SLOT_DIR_SIZE), recordNum + 1, lastRecordOffset + lastRecordLen, totalRecordSize);
        fileHandle.writePage(availablePage, buffer);

        rid.pageNum = availablePage;
        rid.slotNum = recordNum + 1;

        free(recordMeta);
        free(recordData);
        free(record);

        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        int fields = (int)recordDescriptor.size();
        int indicatorSize = getNullIndicatorSize(fields);
        int metaSize = indicatorSize + fields * 2;

        char buffer[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, buffer);
        std::vector<int16_t> dirInfo = getDirInfo(buffer, rid.slotNum);
        int16_t recordOffset = dirInfo[2], recordLen = dirInfo[3];

        memmove((char *)data, buffer + recordOffset, indicatorSize);
        memmove((char *)data + indicatorSize, buffer + recordOffset + metaSize, recordLen - metaSize);

//        delete[](buffer);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        int offset = 0;
        int fields = recordDescriptor.size();

        // Get Null Indicator
        int indicatorSize = getNullIndicatorSize(fields);
        char indicator[indicatorSize];

        memset(indicator, 0, indicatorSize);
        memmove(indicator, (char *) data + offset, indicatorSize);
        offset += indicatorSize;

        std::vector<int> nullFlags = getNullFlags(fields, indicator, indicatorSize);

        // Extract the data
        std::string output;
        for(int i = 0;i < nullFlags.size(); i++){
            if(!nullFlags[i]){
                if(recordDescriptor[i].type == TypeInt){
                    int intVal;
                    memmove(&intVal, (char *) data + offset, 4);
                    offset += 4;

                    output += (recordDescriptor[i].name + ": " + std::to_string(intVal) + ", ");
                }
                else if(recordDescriptor[i].type == TypeReal){
                    float floatVal;
                    memmove(&floatVal, (char *) data + offset, 4);
                    offset += 4;
                    output += (recordDescriptor[i].name + ": " + std::to_string(floatVal) + ", ");
                }
                else if(recordDescriptor[i].type == TypeVarChar){
                    int length;
                    std::string s;
                    memmove(&length, (char *) data + offset, 4);
                    offset += 4;

                    s.resize(length);
                    memmove(&s[0], (char *) data + offset, length);
                    offset += length;

                    output += (recordDescriptor[i].name + ": " + s + ", ");
                }
            }
            else{
                output += (recordDescriptor[i].name + ": " + "NULL" + ", ");
            }
        }
        output.pop_back();
        output.pop_back();

        out<< output;

        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }
} // namespace PeterDB

