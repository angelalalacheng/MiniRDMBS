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

int getNullIndicatorSize(int fields){
    return ceil((double)fields / CHAR_BIT);
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
        short recordOff_;
        memmove(&recordOff_, page + dirOff, sizeof (recordOff_));
        short recordOff = recordOff_ - delta;
        memmove(page + dirOff, &recordOff, sizeof (recordOff));
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
    if(freeSpace > PAGE_SIZE){
        std::cout << freeSpace <<std::endl;
        assert(0);
    }
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

//--------------------no use function-----------------------//
void insertNewSlotDirectory(char* page, int16_t newFreeSpace_, int16_t newSlotNum_, int16_t newRecordOff_, int16_t newRecordSize_){
    int offset = PAGE_SIZE - 2;
    int16_t newFreeSpace = newFreeSpace_, newSlotNum = newSlotNum_, newRecordOff = newRecordOff_, newRecordSize = newRecordSize_;

    memmove(page + offset, &newFreeSpace, sizeof(newFreeSpace));
    offset -= 2;

    memmove(page + offset, &newSlotNum, sizeof(newSlotNum));
    offset -= (newSlotNum * SLOT_DIR_SIZE);

    memmove(page + offset, &newRecordOff, sizeof(newRecordOff));
    offset += 2;

    memmove(page + offset, &newRecordSize, sizeof(newRecordSize));
}

std::vector<int16_t> getDirInfo(const char* page, short recordNum){
    int16_t freeSpace, slotNum, recordOffset, recordLen;
    int offset = PAGE_SIZE - 2;

    memmove(&freeSpace, page + offset, sizeof(freeSpace));
    offset -= 2;

    if(recordNum == -1){ // recordNum = -1 -> find the last
        memmove(&slotNum, page + offset, sizeof(slotNum));
    }
    else{ // get specific slot
        slotNum = recordNum;
    }
    offset -= (slotNum * SLOT_DIR_SIZE);

    if(slotNum == 0){
        recordLen = 0;
        recordOffset = 0;
    }
    else{
        memmove(&recordOffset, page + offset, sizeof(recordOffset));
        memmove(&recordLen, page + offset + 2, sizeof(recordLen));
    }
    std::vector<int16_t> info = {freeSpace, slotNum, recordOffset, recordLen};

    return info;
}
#endif //PETERDB_RBFM_UTILS_H
