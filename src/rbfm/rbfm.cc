#include <cassert>
#include <sstream>
#include <cstring>
#include <unordered_map>
#include <cmath>
#include <iostream>

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
        int fields = (int)recordDescriptor.size();
        int indicatorSize = getNullIndicatorSize(fields);
        char indicator[indicatorSize];
        memmove(indicator, (char *) data, indicatorSize);

        // Get the Null Indicator Data
        std::vector<int> nullFlags = getNullFlags(fields, indicator, indicatorSize);

        // Calculate each part data size
        int fieldOffsetSize = fields * 2;
        int actualDataSize = getActualDataSize(nullFlags, recordDescriptor, indicatorSize, data);
        std::vector<int> recordSizeInfo = {indicatorSize, fieldOffsetSize, actualDataSize};
        int totalRecordSize = sizeof(RID) + indicatorSize + fieldOffsetSize + actualDataSize;

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
        std::vector<short> pageInfo = getPageInfo(buffer);

        // Check if there is empty slot
        short emptySlot = getEmptySlotNumber(buffer, pageInfo[1]);
        SlotInfo previousRecord;
        short slotInfoPos;
        if(emptySlot == -1){
            // Get the last record in the page
            previousRecord = getSlotInfo(buffer, pageInfo[1], -1);
            pageInfo[1] += 1;
            slotInfoPos = -1;
        }
        else{
            // Get the previous record of the empty one
            previousRecord = getSlotInfo(buffer, pageInfo[1], emptySlot - 1);
            SlotInfo lastRecord = getSlotInfo(buffer, pageInfo[1], -1);
            short headOfRemainRecord = previousRecord.offset + previousRecord.len;
            short tailOfRemainRecord = lastRecord.offset + lastRecord.len;
            // Shift the records right
            memmove(buffer + headOfRemainRecord + totalRecordSize, buffer + headOfRemainRecord, tailOfRemainRecord - headOfRemainRecord);
            // Update all slot in directory
            updateSlotDirectory(buffer, pageInfo[1], emptySlot, totalRecordSize, 1);
            slotInfoPos = emptySlot;
        }

        // Prepare new record information
        SlotInfo newRecord;
        newRecord.len = totalRecordSize;
        newRecord.offset = previousRecord.offset + previousRecord.len;
        newRecord.tombstone = 0;

        rid.pageNum = availablePage;
        rid.slotNum = (emptySlot == -1) ? pageInfo[1] : emptySlot;

        // serialize data
        char record [totalRecordSize];
        serializeData(nullFlags, recordDescriptor, data, indicator, rid, recordSizeInfo, record);

        RID nRid;
        memmove(&nRid, record, sizeof(RID));

        // Insert the record to the page
        memmove(buffer + previousRecord.offset + previousRecord.len, record, totalRecordSize);

        // Update the slot directory
        setPageInfo(buffer, pageInfo[0] - totalRecordSize - SLOT_DIR_SIZE, pageInfo[1]);
        setSlotInfo(buffer, newRecord, pageInfo[1], slotInfoPos);

        fileHandle.writePage(availablePage, buffer);

        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        // check whether it is tombstone
        char buffer[PAGE_SIZE];
        RID targetRID = resolveTombstone(fileHandle, rid);
        fileHandle.readPage(targetRID.pageNum, buffer);
        std::vector<short> pageInfo = getPageInfo(buffer);
        SlotInfo targetRecord = getSlotInfo(buffer, pageInfo[1], targetRID.slotNum);

        int fields = (int)recordDescriptor.size();
        int indicatorSize = getNullIndicatorSize(fields);
        int metaSize = sizeof(RID) + indicatorSize + fields * 2;

        if(targetRecord.offset == -1){
            return -1;
        }

        memmove((char *)data, buffer + targetRecord.offset + sizeof(RID), indicatorSize);
        memmove((char *)data + indicatorSize, buffer + targetRecord.offset + metaSize, targetRecord.len - metaSize);
//        std::stringstream stream;
//        printRecord(recordDescriptor, data, stream);

        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        char buffer[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, buffer);

        // get info
        std::vector<short> pageInfo = getPageInfo(buffer);
        SlotInfo deletedRecord = getSlotInfo(buffer, pageInfo[1], rid.slotNum);
        SlotInfo lastRecord = getSlotInfo(buffer, pageInfo[1], -1);

        // check whether it is tombstone
        if(deletedRecord.tombstone == 1){
            RID nextRid;
            memmove(&nextRid, buffer + deletedRecord.offset, sizeof(RID));
            deleteRecord(fileHandle, recordDescriptor, nextRid);
        }

        // edge case: delete the deleted record
        if(deletedRecord.offset == -1){
            return -1;
        }

        // delete record(shift to left)
        short headOfRemainRecord = deletedRecord.offset + deletedRecord.len;
        short tailOfRemainRecord = lastRecord.offset + lastRecord.len;
        short moveLen = tailOfRemainRecord - headOfRemainRecord;
        memmove(buffer + deletedRecord.offset, buffer + headOfRemainRecord, moveLen);
        setPageInfo(buffer, pageInfo[0] + deletedRecord.len, pageInfo[1]);

        // update all records offset
        updateSlotDirectory(buffer, pageInfo[1],  rid.slotNum, deletedRecord.len, 0);

        // update the slot info of deleted record
        deletedRecord.offset = -1;
        deletedRecord.len = -1;
        deletedRecord.tombstone = 0;

        int dirOff = PAGE_SIZE - DIR_META - rid.slotNum * SLOT_DIR_SIZE;
        memmove(buffer + dirOff, &deletedRecord, sizeof (deletedRecord));

        fileHandle.writePage(rid.pageNum, buffer);
        // reuse slot -> update implementation in insert
        return 0;
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
//        std::cout << output <<std::endl;
        out<< output;

        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        // check whether it is tombstone
        char buffer[PAGE_SIZE];
        RID targetRID = resolveTombstone(fileHandle, rid);
        fileHandle.readPage(targetRID.pageNum, buffer);
        std::vector<short> pageInfo = getPageInfo(buffer);
        SlotInfo targetRecord = getSlotInfo(buffer, pageInfo[1], targetRID.slotNum);

        // Get null flags
        int fields = (int)recordDescriptor.size();
        int indicatorSize = getNullIndicatorSize(fields);
        char indicator[indicatorSize];
        memmove(indicator, (char *) data, indicatorSize);
        std::vector<int> nullFlags = getNullFlags(fields, indicator, indicatorSize);

        // Get records info
        SlotInfo lastRecord = getSlotInfo(buffer, pageInfo[1], -1);

        short startPositionOfRemainRecord = targetRecord.offset + targetRecord.len;
        short lastPositionOfAllRecords = lastRecord.offset + lastRecord.len;

        int fieldOffsetSize = fields * 2;
        int actualDataSize = getActualDataSize(nullFlags, recordDescriptor, indicatorSize, data);
        std::vector<int> recordSizeInfo = {indicatorSize, fieldOffsetSize, actualDataSize};
        short updatedRecordLength = sizeof(RID) + indicatorSize + fields * 2 + actualDataSize;
        char serializeUpdatedData[updatedRecordLength];
        serializeData(nullFlags, recordDescriptor, data, indicator, targetRID, recordSizeInfo, serializeUpdatedData);

        int delta = updatedRecordLength - targetRecord.len;
        // Free space is enough
        if(abs(delta) <= pageInfo[0]){
            // shift
            memmove(buffer + targetRecord.offset + updatedRecordLength, buffer + targetRecord.offset + targetRecord.len, lastPositionOfAllRecords - startPositionOfRemainRecord);
            memmove(buffer + targetRecord.offset, serializeUpdatedData, updatedRecordLength);

            // update length of target record
            targetRecord.len = updatedRecordLength;
            memmove(buffer + PAGE_SIZE - DIR_META - SLOT_DIR_SIZE * targetRID.slotNum, &targetRecord, SLOT_DIR_SIZE);

            // update remained slot
            if(updatedRecordLength < targetRecord.len){ // shift left
                updateSlotDirectory(buffer, pageInfo[1], targetRID.slotNum, abs(delta), 0);
            }
            else{ // shift right
                updateSlotDirectory(buffer, pageInfo[1], targetRID.slotNum, abs(delta), 1);
            }
        }
        else{
            // insert the update record to new page
            RID newRid;
            insertRecord(fileHandle, recordDescriptor, data, newRid);

            // shrink original one to tombstone
            int delta2 = targetRecord.len - sizeof(RID);
            memmove(buffer + targetRecord.offset + sizeof(RID), buffer + targetRecord.offset + targetRecord.len, delta2);
            memmove(buffer + targetRecord.offset, &newRid, sizeof(RID));

            // update the slot info of target tombstone =1 and len = sizeof(RID)
            targetRecord.len = sizeof(RID);
            targetRecord.tombstone = 1;
            setSlotInfo(buffer, targetRecord, pageInfo[1], targetRID.slotNum);
            updateSlotDirectory(buffer, pageInfo[1], targetRID.slotNum, delta2, 0);

            // update the freeSpace
            setPageInfo(buffer, pageInfo[0] + delta2, pageInfo[1]);
        }

        fileHandle.writePage(targetRID.pageNum, buffer);

        return 0;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        // check whether it is tombstone
        char buffer[PAGE_SIZE];
        RID targetRID = resolveTombstone(fileHandle, rid);
        fileHandle.readPage(targetRID.pageNum, buffer);
        std::vector<short> pageInfo = getPageInfo(buffer);
        SlotInfo targetRecord = getSlotInfo(buffer, pageInfo[1], targetRID.slotNum);

        // offset == -1 -> deleted record
        if(targetRecord.offset == -1 || targetRecord.len == -1) return -1;

        // no condition(也不算bad)
        if(attributeName.empty()) return 0;

        // get the target record
        char record[targetRecord.len];
        memmove(record, buffer + targetRecord.offset, targetRecord.len);

        // get the target attribute of target record
        int numOfAttr = 0;
        for(int i = 0; i < recordDescriptor.size(); i++){
            if(recordDescriptor[i].name == attributeName){
                numOfAttr = i + 1;
                break;
            }
        }

        // get null flags
        int indicatorSize = getNullIndicatorSize((int)recordDescriptor.size());
        int isNull = getSpecificAttrNullFlag(record, indicatorSize, numOfAttr);

        if(isNull){
            data = nullptr;
            return 1;
        }
        else{
            short startPos, endPos;

            startPos = getSpecificAttrOffset(record, indicatorSize, (int)recordDescriptor.size(), numOfAttr - 1);
            endPos = getSpecificAttrOffset(record, indicatorSize, (int)recordDescriptor.size(), numOfAttr);

            memmove(data, record + startPos, endPos - startPos);
        }

        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        Attribute compareAttr;
        std::unordered_map<std::string, Attribute> attributeMap = convertRecordDescriptor(recordDescriptor);
        if(attributeMap.find(conditionAttribute) != attributeMap.end()){
            compareAttr = attributeMap[conditionAttribute];
        }
        else if (conditionAttribute.empty()){
            compareAttr.length = 0;
        }
        else{
            std::cout << "Attribute does not exist.\n";
            return -1;
        }

        PageNum currentPage = 0;
        char buffer[PAGE_SIZE];
        std::vector<short> pageInfo;
        RID rid;
        void *data = malloc(compareAttr.length + 4);
        while(currentPage < fileHandle.getNumberOfPages()){
            fileHandle.readPage(currentPage, buffer);
            pageInfo = getPageInfo(buffer);

            for(int i = 1; i <= pageInfo[1]; i++){
                bool good = false;
                rid.pageNum = currentPage;
                rid.slotNum = i;

                SlotInfo slotInfo = getSlotInfo(buffer, pageInfo[1], i);
                if(slotInfo.offset == -1) continue;

                memset(data, 0, compareAttr.length + 4);
                RC rc = readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, data);

                if(conditionAttribute.empty() || compOp == PeterDB::NO_OP){
                    if(rc == 0) good = true;
                }
                else{
                    if(rc == -1 || rc == 1) continue;
                    switch(compareAttr.type){
                        case TypeInt:
                            int intVal1, intVal2;
                            if(!data) break;
                            intVal1 = *reinterpret_cast<int*>(data);
                            intVal2 = *reinterpret_cast<const int*>(value);
                            if(compareInt(intVal1, intVal2, compOp)) good = true;
                            break;
                        case TypeReal:
                            float floatVal1, floatVal2;
                            if(!data) break;
                            floatVal1 = *reinterpret_cast<float*>(data);
                            floatVal2 = *reinterpret_cast<const float*>(value);
                            if(compareFloat(floatVal1, floatVal2, compOp)) good = true;
                            break;
                        case TypeVarChar:
                            int length1, length2;
                            if(!data) break;
                            memmove(&length1, (char *)data, sizeof(int));
                            memmove(&length2, (char *)value, sizeof(int));
                            std::string strVal1, strVal2;
                            strVal1.resize(length1);
                            memmove(&strVal1[0], (char *)data + sizeof(int), length1);
                            strVal2.resize(length2);
                            memmove(&strVal2[0], (char *)value + sizeof(int), length2);
                            if(compareString(strVal1, strVal2, compOp)) good = true;
                            break;
                    }
                }

                if(good){
                    rbfm_ScanIterator.candidates.push_back(rid);
                }
            }

            currentPage += 1;
        }

        rbfm_ScanIterator.fileHandle = &fileHandle;
        rbfm_ScanIterator.projectedAttributes = attributeNames;
        rbfm_ScanIterator.recordDescriptor = recordDescriptor;
        rbfm_ScanIterator.attributeMap = attributeMap;
        free(data);

        return 0;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
        // Check if we've reached the end of the candidates vector
        if (currentIndex >= candidates.size() || candidates.empty()) {
            return RBFM_EOF;
        }

        // prepare nullIndicator
        rid = candidates[currentIndex];
        int nullFieldsIndicatorActualSize = getNullIndicatorSize((int) projectedAttributes.size());
        auto indicator = new unsigned char[nullFieldsIndicatorActualSize];
        memset(indicator, 0, nullFieldsIndicatorActualSize);

        // processing the data (go through each projected attribute)
        int offset = 0;
        void* getValue = malloc(500);

        for(int i = 0; i < projectedAttributes.size(); i++){
            std::string s = projectedAttributes[i];
            RecordBasedFileManager::instance().readAttribute(*fileHandle, recordDescriptor, rid, s, getValue);

            if(getValue == nullptr){
                setSpecificAttrNullFlag(indicator, i + 1);
            }
            else{
                if(attributeMap[s].type == TypeVarChar){
                    int length;
                    memmove(&length, getValue, sizeof(int));
                    memmove((char *)data + offset, getValue, sizeof(int) + length);
                    offset += (sizeof(int) + length);
                }
                else{
                    memmove((char *)data + offset, getValue, 4);
                    offset += 4;
                }
            }
            memset(getValue, 0, 500);
        }

        memmove((char *) data + nullFieldsIndicatorActualSize, data, offset);
        memmove((char *)data, indicator, nullFieldsIndicatorActualSize);
        free(getValue);

        currentIndex++; // Move to the next candidate
        return 0; // Assuming 0 is the success code
    }

    RC RBFM_ScanIterator::close() {
        currentIndex = 0;
        candidates.clear();
        fileHandle = nullptr;
        return 0;
    }
} // namespace PeterDB

