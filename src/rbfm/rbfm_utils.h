//
// Created by 鄭筠蓉 on 2024/1/26.
//

#ifndef PETERDB_RBFM_UTILS_H
#define PETERDB_RBFM_UTILS_H
#include <climits>
#include <cmath>
#include <cstring>
#include <vector>
#include <iostream>
#include <string>
#include <unordered_map>

int getNullIndicatorSize(int fields){
    return ceil((double)fields / CHAR_BIT);
}

int getSpecificAttrNullFlag(const char* record, int indicatorSize, int target){
    char indicator[indicatorSize];
    memmove(indicator, record + sizeof(PeterDB::RID), indicatorSize);

    target -= 1;
    return indicator[target / 8] & (1 << (7 - target % 8));
}

void setSpecificAttrNullFlag(unsigned char* indicator, int fieldIndex){
    // Calculate the byte and bit position of fieldIndex
    fieldIndex -= 1;
    int byteIndex = fieldIndex / 8;
    int bitPosition = fieldIndex % 8;

    // Set the bit at bitPosition in indicator[byteIndex]
    indicator[byteIndex] |= (1 << bitPosition);
}

short getSpecificAttrOffset(const char* record, int indicatorSize, int fields, int targetAttr){
    if (targetAttr == 0){
        return sizeof(PeterDB::RID) + indicatorSize + fields * sizeof(short);
    }

    int targetAttrOffsetPos = sizeof(PeterDB::RID) + indicatorSize + (targetAttr - 1) * 2;
    short targetAttrOffset;

    memmove(&targetAttrOffset, record + targetAttrOffsetPos, sizeof(short));

    return targetAttrOffset;
}

std::vector<int> getNullFlags(int fields, const char* indicator, int indicatorSize){
    std::vector<int> nullFlags(fields, 0);
    int count = 0;
    for (int byteIndex = 0; byteIndex < indicatorSize; byteIndex++) {
        for (int bitIndex = CHAR_BIT - 1; bitIndex >=0; bitIndex--) {
            bool isNull = (indicator[byteIndex] & (1 << bitIndex)) != 0; // mask
            if(isNull) nullFlags[count] = 1;
            if(count == (fields - 1)) break;
            count++;
        }
    }
    return nullFlags;
}

int getActualDataSize(const std::vector<int>& nullFlags, const std::vector<PeterDB::Attribute> &recordDescriptor, const int &indicatorSize, const void* data){
    int actualDataSize = 0;
    for(int i = 0; i< nullFlags.size(); i++){
        if(!nullFlags[i]){
            if(recordDescriptor[i].type == PeterDB::TypeInt){
                actualDataSize += 4;
            }
            else if(recordDescriptor[i].type == PeterDB::TypeReal){
                actualDataSize += 4;
            }
            else if(recordDescriptor[i].type == PeterDB::TypeVarChar){
                int length;
                memmove(&length, (char *) data + indicatorSize + actualDataSize, 4);
                actualDataSize += (4 + length);
            }
        }
    }

    return actualDataSize;
}

void serializeData(const std::vector<int>& nullFlags, const std::vector<PeterDB::Attribute> &recordDescriptor, const void* data, const char * indicator, const PeterDB::RID &rid, const std::vector<int>&recordSizeInfo, char* serializeData){
    int offset = 0, recordMetaSize = 0, recordDataSize = 0;
    int indicatorSize = recordSizeInfo[0];
    int metaFieldSize = recordSizeInfo[1];
    int actualDataSize = recordSizeInfo[2];
    char recordMeta[metaFieldSize], recordData[actualDataSize];

    offset += indicatorSize;
    for(int i = 0; i< nullFlags.size(); i++){
        if(!nullFlags[i]){
            if(recordDescriptor[i].type == PeterDB::TypeInt){
                int intVal;
                memmove(&intVal, (char *) data + offset, sizeof(int));
                offset += 4;
                memmove(recordData + recordDataSize, &intVal, sizeof(int));
                recordDataSize += 4;
            }
            else if(recordDescriptor[i].type == PeterDB::TypeReal){
                float floatVal;
                memmove(&floatVal, (char *) data + offset, 4);
                offset += 4;
                memmove(recordData + recordDataSize, &floatVal, sizeof(floatVal));
                recordDataSize += 4;
            }
            else if(recordDescriptor[i].type == PeterDB::TypeVarChar) {
                int length;
                std::string s;
                memmove(&length, (char *) data + offset, 4);
                offset += 4;
                s.resize(length);
                memmove(&s[0], (char *) data + offset, length);
                offset += length;
                memmove(recordData + recordDataSize, &length, sizeof(int));
                recordDataSize += 4;
                memmove(recordData + recordDataSize, &s[0], length);
                recordDataSize += length;
            }

            int offVal = (int)(sizeof(PeterDB::RID) + indicatorSize + recordDescriptor.size() * sizeof(short) + recordDataSize);
            memmove(recordMeta + recordMetaSize, &offVal, 2);
//            std::cout << "offVal " + std::to_string(i) + ": " << offVal <<std::endl;
        }
        else{
            int offVal;
            memmove(&offVal, recordMeta + recordMetaSize - 2, 2);
            memmove(recordMeta + recordMetaSize, &offVal, 2);
//            std::cout << "offVal null " + std::to_string(i) + ": " << offVal <<std::endl;
        }
        recordMetaSize += 2;
    }

    int pos = 0;
    memmove(serializeData + pos, &rid, sizeof(PeterDB::RID));
    pos += sizeof(PeterDB::RID);
    memmove(serializeData + pos, indicator, indicatorSize);
    pos += indicatorSize;
    memmove(serializeData + pos, recordMeta, recordMetaSize);
    pos += recordMetaSize;
    memmove(serializeData + pos, recordData, recordDataSize);
}

void initialNewPage(PeterDB::FileHandle &fileHandle){
    void* dummy = malloc(sizeof(int));
    int16_t freeSpace = PAGE_SIZE - 4, slotNum = 0;
    fileHandle.openFileStream->seekg(0, std::ios::end);
    fileHandle.appendPage(dummy);
    free(dummy);

    fileHandle.openFileStream->seekp(-4, std::ios::end);
    fileHandle.openFileStream->write(reinterpret_cast<char*>(&slotNum), sizeof(slotNum));
    fileHandle.openFileStream->write(reinterpret_cast<char*>(&freeSpace), sizeof(freeSpace));
}

std::vector<short> getPageInfo(const char* page){
    short freeSpace, slotNum;
    int offset = PAGE_SIZE - DIR_META;

    memmove(&slotNum, page + offset, sizeof(slotNum));
    offset += 2;
    memmove(&freeSpace, page + offset, sizeof(freeSpace));

    std::vector<short> info = {freeSpace, slotNum};

    return info;
}

void setPageInfo(char* page, short freeSpace, short slotNum){
    int offset = PAGE_SIZE - DIR_META;

    memmove(page + offset, &slotNum, sizeof(slotNum));
    offset += 2;
    memmove(page + offset, &freeSpace, sizeof(freeSpace));
}

short getEmptySlotNumber(const char* page, short totalSlot){
    int offset = PAGE_SIZE - DIR_META;

    short cnt = 1;
    while(cnt <= totalSlot) {
        PeterDB::SlotInfo tmp;
        offset -= SLOT_DIR_SIZE;
        memmove(&tmp, page + offset, sizeof (tmp));
        if (tmp.offset == -1){
            return cnt;
        }
        cnt++;
    }
    return -1;
}

PeterDB::SlotInfo getSlotInfo(const char* page, int totalSlot, int rid) {
    int offset;
    PeterDB::SlotInfo info;
    if(totalSlot == 0 || rid == 0){
        info.offset = 0;
        info.len = 0;
        info.tombstone = 0;
    }
    else if(rid == -1){ // get last record info
        offset = PAGE_SIZE - DIR_META - totalSlot * SLOT_DIR_SIZE;
        memmove(&info, page + offset, sizeof (info));
    }
    else{ // get specific record info
        offset = PAGE_SIZE - DIR_META - rid * SLOT_DIR_SIZE;
        memmove(&info, page + offset, sizeof (info));
    }

    return info;
}

void setSlotInfo(char* page, const PeterDB::SlotInfo &info, int newTotalSlot, short slotNum) {
    int offset;

    if(slotNum == -1){ // set new record at the end
        offset = PAGE_SIZE - DIR_META - newTotalSlot * SLOT_DIR_SIZE;
    }
    else{ // set new record at specific position(empty slot)
        offset = PAGE_SIZE - DIR_META - slotNum * SLOT_DIR_SIZE;;
    }

    memmove(page + offset, &info, sizeof (info));
}

void updateSlotDirectory(char* page, int totalSlotNum, int target, int delta, int mode){ // mode 0: shift left(add), move 1: shift right
    int dirOff = PAGE_SIZE - DIR_META - target * SLOT_DIR_SIZE;
    if(mode == 1) {
        delta = delta * (-1);
    }

    for(int i = 0; i < totalSlotNum - target; i++){
        dirOff -= SLOT_DIR_SIZE;
        PeterDB::SlotInfo slot_;
        memmove(&slot_, page + dirOff, sizeof (slot_));
        slot_.offset -= delta;
        memmove(page + dirOff, &slot_, sizeof (slot_));
    }
}

int checkFreeSpaceOfLastPage(PeterDB::FileHandle &fileHandle, int recordSize) {
    // no pages in file
    if(fileHandle.getNumberOfPages() == 0) {
        initialNewPage(fileHandle);
        return fileHandle.getNumberOfPages() - 1;
    }
    // have pages in file -> check the last page
    int16_t freeSpace;
    fileHandle.openFileStream->seekg(-2, std::ios::end);
    fileHandle.openFileStream->read(reinterpret_cast<char*>(&freeSpace), 2);
    if (freeSpace >= recordSize) {
        return fileHandle.getNumberOfPages() - 1;
    }

    // the last page does not have enough page
    return -1;
}

int findFreePageFromFirst(PeterDB::FileHandle &fileHandle, int recordSize) {
    // Skip the hidden page
    fileHandle.openFileStream->seekg(PAGE_SIZE, std::ios::beg);

    int pageNum = 0;
    while (pageNum <= fileHandle.getNumberOfPages() - 1) {
        int16_t freeSpace = 0;
        // Move the file pointer to the location of free space information
        fileHandle.openFileStream->seekg(PAGE_SIZE - 2, std::ios::cur);

        // Read the last two bytes which contain free space information
        fileHandle.openFileStream->read(reinterpret_cast<char*>(&freeSpace), 2);

        // Return the page number if free space is sufficient
        if (freeSpace >= recordSize) {
            return pageNum;
        }

        pageNum++;
    }

    return -1; // Return -1 if no suitable page is found
}

bool compareInt(const int &value1, const int &value2, PeterDB::CompOp operation) {
    switch(operation) {
        case PeterDB::EQ_OP:
            return value1 == value2;
        case PeterDB::LT_OP:
            return value1 < value2;
        case PeterDB::LE_OP:
            return value1 <= value2;
        case PeterDB::GT_OP:
            return value1 > value2;
        case PeterDB::GE_OP:
            return value1 >= value2;
        case PeterDB::NE_OP:
            return value1 != value2;
        case PeterDB::NO_OP:
            // 在NO_OP情况下，你可能想要特殊处理，例如总是返回true或false
            return true;
        default:
            std::cerr << "Unknown comparison operator." << std::endl;
            return false;
    }
}

bool compareFloat(const float &value1, const float &value2, PeterDB::CompOp operation) {
    switch(operation) {
        case PeterDB::EQ_OP:
            return value1 == value2;
        case PeterDB::LT_OP:
            return value1 < value2;
        case PeterDB::LE_OP:
            return value1 <= value2;
        case PeterDB::GT_OP:
            return value1 > value2;
        case PeterDB::GE_OP:
            return value1 >= value2;
        case PeterDB::NE_OP:
            return value1 != value2;
        case PeterDB::NO_OP:
            // 在NO_OP情况下，你可能想要特殊处理，例如总是返回true或false
            return true;
        default:
            std::cerr << "Unknown comparison operator." << std::endl;
            return false;
    }
}

bool compareString(const std::string& value1, const std::string& value2, PeterDB::CompOp operation) {
    switch(operation) {
        case PeterDB::EQ_OP:
            return value1 == value2;
        case PeterDB::LT_OP:
            return value1 < value2;
        case PeterDB::LE_OP:
            return value1 <= value2;
        case PeterDB::GT_OP:
            return value1 > value2;
        case PeterDB::GE_OP:
            return value1 >= value2;
        case PeterDB::NE_OP:
            return value1 != value2;
        case PeterDB::NO_OP:
            // 在NO_OP情况下，你可能想要特殊处理，例如总是返回true或false
            return true;
        default:
            std::cerr << "Unknown comparison operator." << std::endl;
            return false;
    }
}

PeterDB::RID resolveTombstone(PeterDB::FileHandle &fileHandle, PeterDB::RID rid) {
    char buffer[PAGE_SIZE];
    bool isTombstone = true;
    PeterDB::RID currentRID = rid;

    while (isTombstone) {
        fileHandle.readPage(currentRID.pageNum, buffer);
        std::vector<short> pageInfo = getPageInfo(buffer);
        PeterDB::SlotInfo slotInfo = getSlotInfo(buffer, pageInfo[1], currentRID.slotNum);

        if (slotInfo.tombstone == 1) {
            memmove(&currentRID, buffer + slotInfo.offset, sizeof(PeterDB::RID));
        } else {
            isTombstone = false;
        }
    }

    return currentRID;
}

char* getRecordFromRID(PeterDB::FileHandle &fileHandle, PeterDB::RID rid, short &len) {
    rid = resolveTombstone(fileHandle, rid);

    char page[PAGE_SIZE];
    fileHandle.readPage(rid.pageNum, page);

    std::vector<short> pageInfo = getPageInfo(page);
    PeterDB::SlotInfo recordInfo = getSlotInfo(page, pageInfo[1], rid.slotNum);

    char* record = new char[recordInfo.len];
    memmove(record, page + recordInfo.offset, recordInfo.len);

    len = recordInfo.len;
    return record;
}

std::unordered_map<std::string, PeterDB::Attribute> convertRecordDescriptor(const std::vector<PeterDB::Attribute> &recordDescriptor){
    std::unordered_map<std::string, PeterDB::Attribute> attributeMap;

    for(PeterDB::Attribute attr: recordDescriptor){
        attributeMap[attr.name] = attr;
    }

    return attributeMap;
}
#endif //PETERDB_RBFM_UTILS_H
