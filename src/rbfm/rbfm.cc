#include <cassert>
#include <sstream>
#include <cstring>
#include <climits>
#include <cmath>
#include <fstream>
#include "src/include/rbfm.h"

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
        std::fstream file(fileHandle.pageFileName, std::ios::in | std::ios::out | std::ios::binary);

        int offset = 0, recordMetaSize = 0, recordDataSize = 0;
        int fields = (int)recordDescriptor.size();
        int indicatorSize = ceil((double)fields / CHAR_BIT);

        void* recordMeta = malloc(indicatorSize + fields * 2);
        void* recordData = malloc(PAGE_SIZE);
        auto indicator = new unsigned char[indicatorSize];

        memset(indicator, 0, indicatorSize);
        memmove(indicator, (char *) data + offset, indicatorSize);
        offset += indicatorSize;

        // Get the Null Indicator Data
        std::vector<int> nullFlag(fields, 0);
        int count = 0;
        for (int byteIndex = 0; byteIndex < indicatorSize; byteIndex++) {
            for (int bitIndex = CHAR_BIT - 1; bitIndex >=0; bitIndex--) {
                bool isNull = (indicator[byteIndex] & (1 << bitIndex)) != 0; // mask
                if(isNull) nullFlag[count] = 1;
                if(count == (fields - 1)) break;
                count++;
            }
        }

        // Extract the data
        for(int i = 0; i< nullFlag.size(); i++){
            if(!nullFlag[i]){
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

        // insert the record
        int16_t freeSpace = PAGE_SIZE;
        int16_t recordNum = 0;
        int16_t lastOff = 0, lastLen = 0;
        int lastRecordDir;

        int availablePage = findPageWithFreeSpace(fileHandle, totalRecordSize + SLOT_DIR_SIZE);

        if(availablePage == -1) {
            fileHandle.appendPage(record);
            availablePage = fileHandle.getNumberOfPages() - 1;
            freeSpace = PAGE_SIZE - 4;
            recordNum = 0;
            lastRecordDir = 4;
        }
        else{
            // Get the info about freeSpace and recordNum
            file.seekg((availablePage + 2) * PAGE_SIZE - 4, std::ios::beg);
            file.read(reinterpret_cast<char*>(&recordNum), 2);
            file.read(reinterpret_cast<char*>(&freeSpace), 2);

            // Get lastRecordOffset
            lastRecordDir = 2 + 2 + recordNum * SLOT_DIR_SIZE;
            file.seekg(-lastRecordDir, std::ios::cur);
            file.read(reinterpret_cast<char*>(&lastOff), 2);
            file.read(reinterpret_cast<char*>(&lastLen), 2);
        }

        // Insert the record to the page
        file.seekp((availablePage + 1) * PAGE_SIZE, std::ios::beg);
        file.seekp(lastOff+lastLen, std::ios::cur);

        file.write(reinterpret_cast<char*>(record), totalRecordSize);

        // add new slot directory
        int newRecordOff = lastOff + lastLen;
        file.seekp((availablePage + 2) * PAGE_SIZE - lastRecordDir - SLOT_DIR_SIZE, std::ios::beg);
        file.write(reinterpret_cast<char*>(&newRecordOff), 2);
        file.write(reinterpret_cast<char*>(&totalRecordSize), 2);

        // update metadata of slot directory
        freeSpace -= (totalRecordSize + SLOT_DIR_SIZE);
        recordNum += 1;

        file.seekp((availablePage + 2) * PAGE_SIZE - 4, std::ios::beg);
        file.write(reinterpret_cast<char*>(&recordNum), 2);
        file.write(reinterpret_cast<char*>(&freeSpace), 2);


        int val;
        file.seekp((availablePage + 1) * PAGE_SIZE + 1, std::ios::beg);
        file.read(reinterpret_cast<char*>(&val), 2);

        rid.pageNum = availablePage;
        rid.slotNum = recordNum;


        free(recordMeta);
        free(recordData);
        free(record);

        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        std::fstream file(fileHandle.pageFileName, std::ios::in | std::ios::out | std::ios::binary);
        int fields = (int)recordDescriptor.size();
        int indicatorSize = ceil((double)fields / CHAR_BIT);


        // Calculate the offset of tail of pageNum
        int pageNumTail = PAGE_SIZE * (rid.pageNum + 2);
        file.seekg(pageNumTail, std::ios::beg);

        // Move forward to the slot directory
        int recordSlotDirOffset = 2 + 2 + rid.slotNum * SLOT_DIR_SIZE;
        file.seekg(-recordSlotDirOffset, std::ios ::cur);

        // Get offset and length of the record
        int16_t offset, length;
        file.read(reinterpret_cast<char*>(&offset), 2);
        file.read(reinterpret_cast<char*>(&length), 2);

        // Calculate the position of the record in the file
        int recordOffset = PAGE_SIZE * (rid.pageNum + 1) + offset;

        // Seek to the position of the record in the file
        file.seekg(recordOffset, std::ios::beg);

        // Read the record to buffer (need to change the format)
        char* buffer = new char[length];
        file.read(buffer, length);

        // move to data
        memmove((char *) data, buffer, indicatorSize);

        int meta = indicatorSize + fields * 2;
        memmove((char *) data + indicatorSize, buffer + meta, length - meta);

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
        int indicatorSize = ceil((double)fields / CHAR_BIT);
        auto indicator = new unsigned char[indicatorSize];

        memset(indicator, 0, indicatorSize);
        memmove(indicator, (char *) data + offset, indicatorSize);
        offset += indicatorSize;

        std::vector<int> nullFlag(fields, 0);
        int count = 0;
        for (int byteIndex = 0; byteIndex < indicatorSize; byteIndex++) {
            for (int bitIndex = CHAR_BIT - 1; bitIndex >=0; bitIndex--) {
                bool isNull = (indicator[byteIndex] & (1 << bitIndex)) != 0; // mask
                if(isNull) nullFlag[count] = 1;
                if(count == (fields - 1)) break;
                count++;
            }
        }

        // Extract the data
        std::string output;
        for(int i = 0;i < nullFlag.size(); i++){
            if(!nullFlag[i]){
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
//        std::cout << "print output: " << output << std::endl;

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

    int RecordBasedFileManager::findPageWithFreeSpace(FileHandle &fileHandle, int recordSize) {
        if(fileHandle.getNumberOfPages() == 0) return -1;

        std::fstream file(fileHandle.pageFileName, std::ios::binary | std::ios::in | std::ios::out);

        // Skip the hidden page
        file.seekg(PAGE_SIZE, std::ios::beg);

        int pageNum = 0;
        while (pageNum <= fileHandle.getNumberOfPages()) {
            int16_t freeSpace = 0;
            // Move the file pointer to the location of free space information
            file.seekg(PAGE_SIZE - 2, std::ios::cur);

            // Read the last two bytes which contain free space information
            file.read(reinterpret_cast<char*>(&freeSpace), 2);

            // Return the page number if free space is sufficient
            if (freeSpace >= recordSize) {
                return pageNum;
            }

            pageNum++;
        }

        return -1; // Return -1 if no suitable page is found
    }

} // namespace PeterDB

