//
// Created by 鄭筠蓉 on 2024/2/20.
//

#ifndef PETERDB_IX_UTILS_H
#define PETERDB_IX_UTILS_H

#include <string>
#include <vector>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <memory>
#include <map>

// TODO: 需要加上compare RID
int compareKey(const char* key1, const PeterDB::RID &rid1, const char* key2, const PeterDB::RID &rid2, const PeterDB::Attribute& attribute, bool compareLow) {
    if (attribute.type == PeterDB::TypeInt) {
        if(compareLow && key2 == nullptr) return 1;
        if(!compareLow && key2 == nullptr) return -1;
        int k1 = *reinterpret_cast<const int*>(key1);
        int k2 = *reinterpret_cast<const int*>(key2);
        if(k1 == k2){
            if(rid1.pageNum == rid2.pageNum) return (rid1.slotNum > rid2.slotNum) - (rid1.slotNum < rid2.slotNum);
            return (rid1.pageNum > rid2.pageNum) - (rid1.pageNum < rid2.pageNum);
        }
        return (k1 > k2) - (k1 < k2);
    }
    else if (attribute.type == PeterDB::TypeReal) {
        if(compareLow && key2 == nullptr) return 1;
        if(!compareLow && key2 == nullptr) return -1;
        float k1 = *reinterpret_cast<const float*>(key1);
        float k2 = *reinterpret_cast<const float*>(key2);
        if(k1 == k2){
            if(rid1.pageNum == rid2.pageNum) return (rid1.slotNum > rid2.slotNum) - (rid1.slotNum < rid2.slotNum);
            return (rid1.pageNum > rid2.pageNum) - (rid1.pageNum < rid2.pageNum);
        }
        return (k1 > k2) - (k1 < k2);
    }
    else if (attribute.type == PeterDB::TypeVarChar) {
        if(compareLow && key2 == nullptr) return 1;
        if(!compareLow && key2 == nullptr) return -1;
        int len1, len2;
        memmove(&len1, key1, sizeof(int));
        memmove(&len2, key2, sizeof(int));
        std::string k1(static_cast<const char*>(key1) + sizeof(int), len1);
        std::string k2(static_cast<const char*>(key2) + sizeof(int), len2);
        if(k1 == k2){
            if(rid1.pageNum == rid2.pageNum) return (rid1.slotNum > rid2.slotNum) - (rid1.slotNum < rid2.slotNum);
            return (rid1.pageNum > rid2.pageNum) - (rid1.pageNum < rid2.pageNum);
        }
        return k1.compare(k2);
    }
    return -2;
}

void setEntry(std::vector<char>& arr, size_t index, size_t typeLen, const void* entryData) {
    if (index * typeLen < arr.size()) {
        std::memmove(arr.data() + (index * typeLen), entryData, typeLen);
    }
}

void getEntry(const std::vector<char>& arr, size_t index, void* entryData, const PeterDB::Attribute &attribute) {
    if(attribute.type == PeterDB::TypeVarChar){
        int offset = 0;
        for (int current = 0; current < index; current++) {
            int len;
            memmove(&len, arr.data() + offset, sizeof(int));
            offset += sizeof(int);
            offset += len; // skip the string
        }

        int varCharLen;
        memmove(&varCharLen, arr.data() + offset, sizeof(int));
        memmove(entryData, arr.data() + offset, sizeof(int) + varCharLen); // 包括长度字段和字符串数据
    }
    else{ // INT or REAL
        PeterDB::AttrLength typeLen = attribute.length;
        if (index * typeLen < arr.size()) {
            memmove(entryData, arr.data() + (index * typeLen), typeLen);
        }
    }
}

void clearEntry(std::vector<char>& arr, size_t index, const PeterDB::Attribute &attribute) {
    if(attribute.type == PeterDB::TypeVarChar){
        size_t startPos = 0;
        for (int current = 0; current < index; current++) {
            int len;
            memmove(&len, arr.data() + startPos, sizeof(int));
            startPos += sizeof(int);
            startPos += len; // skip the string
        }

        int lenToRemove;
        memmove(&lenToRemove, arr.data() + startPos, sizeof(int));
        size_t endPos = startPos + sizeof(int) + lenToRemove;
        arr.erase(arr.begin() + startPos, arr.begin() + endPos);
    }
    else{ // INT or REAL
        PeterDB::AttrLength typeLen = attribute.length;
        size_t startPos = index * typeLen;
        size_t endPos = startPos + typeLen;

        if (startPos < arr.size() && endPos <= arr.size()) {
            arr.erase(arr.begin() + startPos, arr.begin() + endPos);
        }
    }
}

// TODO: key一樣要compare RID
void insertIntEntry(PeterDB::LeafNode* leafNodeInfo, PeterDB::NonLeafNode* nonLeafNodeInfo, bool isLeaf, const PeterDB::Attribute &attribute, const void* entryData, const PeterDB::RID* entryRID, const PeterDB::PageNum* pageNum) {
    PeterDB::AttrLength typeLen = attribute.length;

    std::vector<char>& arr = isLeaf ? leafNodeInfo->key : nonLeafNodeInfo->routingKey;
    std::vector<PeterDB::RID>& rids = isLeaf ? leafNodeInfo->rid : nonLeafNodeInfo->rid;
    short& entryCount = isLeaf ? leafNodeInfo->currentKey : nonLeafNodeInfo->currentKey;
    short& freeSpace = isLeaf ? leafNodeInfo->freeSpace : nonLeafNodeInfo->freeSpace;

    size_t left = 0, right = entryCount, mid = 0;
    int target = *reinterpret_cast<const int*>(entryData);

    while (left < right && entryCount > 0) {
        mid = (left + right) / 2;
        char midData[typeLen];
        getEntry(arr, mid, midData, attribute);
        int midValue = *reinterpret_cast<int*>(midData);
        if (midValue == target) {
            if(rids[mid].pageNum < entryRID->pageNum || (rids[mid].pageNum == entryRID->pageNum && rids[mid].slotNum < entryRID->slotNum)){
                left = mid + 1;
            }
            else{
                right = mid;
            }
        }
        else if (target < midValue) right = mid;
        else left = mid + 1;
    }

    const char* entryBytes = reinterpret_cast<const char*>(entryData);
    arr.insert(arr.begin() + (left * typeLen), entryBytes, entryBytes + typeLen);


    rids.insert(rids.begin() + left, *entryRID);
    freeSpace -= sizeof(PeterDB::RID);

    if(!isLeaf){
        std::vector<PeterDB::PageNum>& pointers = nonLeafNodeInfo->pointers;
        pointers.insert(pointers.begin() + left + 1, *pageNum);
        freeSpace -= sizeof(PeterDB::PageNum);
    }

//    if (isLeaf) {
//        std::vector<PeterDB::RID>& rids = leafNodeInfo->rid;
//        rids.insert(rids.begin() + left, *entryRID);
//        freeSpace -= sizeof(PeterDB::RID);
//    } else {
//        std::vector<PeterDB::PageNum>& pointers = nonLeafNodeInfo->pointers;
//        pointers.insert(pointers.begin() + left + 1, *pageNum);
//        freeSpace -= sizeof(PeterDB::PageNum);
//    }

    entryCount += 1; // 更新键值计数
    freeSpace -= typeLen;
}

void insertFloatEntry(PeterDB::LeafNode* leafNodeInfo, PeterDB::NonLeafNode* nonLeafNodeInfo, bool isLeaf, const PeterDB::Attribute &attribute, const void* entryData, const PeterDB::RID* entryRID, const PeterDB::PageNum* pageNum){
    PeterDB::AttrLength typeLen = attribute.length;

    std::vector<char>& arr = isLeaf ? leafNodeInfo->key : nonLeafNodeInfo->routingKey;
    std::vector<PeterDB::RID>& rids = isLeaf ? leafNodeInfo->rid : nonLeafNodeInfo->rid;
    short& entryCount = isLeaf ? leafNodeInfo->currentKey : nonLeafNodeInfo->currentKey;
    short& freeSpace = isLeaf ? leafNodeInfo->freeSpace : nonLeafNodeInfo->freeSpace;

    size_t left = 0, right = entryCount, mid = 0;
    float target = *reinterpret_cast<const float*>(entryData);

    while (left < right) {
        mid = (left + right) / 2;
        char midData[typeLen];
        getEntry(arr, mid, midData, attribute);
        int midValue = *reinterpret_cast<float*>(midData);
        if (midValue == target) {
            if(rids[mid].pageNum < entryRID->pageNum || (rids[mid].pageNum == entryRID->pageNum && rids[mid].slotNum < entryRID->slotNum)){
                left = mid + 1;
            }
            else{
                right = mid;
            }
        }
        else if (target < midValue) right = mid;
        else left = mid + 1;
    }

    const char* entryBytes = reinterpret_cast<const char*>(entryData);
    arr.insert(arr.begin() + (left * typeLen), entryBytes, entryBytes + typeLen);

    rids.insert(rids.begin() + left, *entryRID);
    freeSpace -= sizeof(PeterDB::RID);

    if(!isLeaf){
        std::vector<PeterDB::PageNum>& pointers = nonLeafNodeInfo->pointers;
        pointers.insert(pointers.begin() + left + 1, *pageNum);
        freeSpace -= sizeof(PeterDB::PageNum);
    }

//    if (isLeaf) {
//        std::vector<PeterDB::RID>& rids = leafNodeInfo->rid;
//        rids.insert(rids.begin() + left, *entryRID);
//        freeSpace -= sizeof(PeterDB::RID);
//    } else {
//        std::vector<PeterDB::PageNum>& pointers = nonLeafNodeInfo->pointers;
//        pointers.insert(pointers.begin() + left + 1, *pageNum);
//        freeSpace -= sizeof(PeterDB::PageNum);
//    }

    entryCount += 1; // 更新键值计数
    freeSpace -= typeLen;
}

// Calculate the position of a VARCHAR entry
int getVarCharIndexPos(const std::vector<char>& arr, size_t index){
    int varCharLen = 0;
    for (int current = 0; current < index; current++) {
        int len;
        memmove(&len, arr.data() + varCharLen, sizeof(int));
        varCharLen += sizeof(int);
        varCharLen += len;
    }

    return varCharLen;
}

void insertVarCharEntry(PeterDB::LeafNode* leafNodeInfo, PeterDB::NonLeafNode* nonLeafNodeInfo, bool isLeaf, const PeterDB::Attribute &attribute, const void* entryData, const PeterDB::RID* entryRID, const PeterDB::PageNum* pageNum){
    PeterDB::AttrLength typeLen = attribute.length;

    std::vector<char>& arr = isLeaf ? leafNodeInfo->key : nonLeafNodeInfo->routingKey;
    std::vector<PeterDB::RID>& rids = isLeaf ? leafNodeInfo->rid : nonLeafNodeInfo->rid;
    short& entryCount = isLeaf ? leafNodeInfo->currentKey : nonLeafNodeInfo->currentKey;
    short& freeSpace = isLeaf ? leafNodeInfo->freeSpace : nonLeafNodeInfo->freeSpace;

    size_t left = 0, right = entryCount, mid = 0;

    int len;
    memmove(&len, entryData, sizeof (int));
    std::string target;
    target.resize(len);
    memmove(&target[0], (char *)entryData + sizeof (int), len);

    while (left < right) {
        mid = (left + right) / 2;
        char midData[typeLen];
        getEntry(arr, mid, midData, attribute);

        int tempLen;
        memmove(&tempLen, midData, sizeof (int));
        std::string midValue;
        midValue.resize(tempLen);
        memmove(&midValue[0], (char *)midData + sizeof (int), tempLen);

        if (midValue == target) {
            if(rids[mid].pageNum < entryRID->pageNum || (rids[mid].pageNum == entryRID->pageNum && rids[mid].slotNum < entryRID->slotNum)){
                left = mid + 1;
            }
            else{
                right = mid;
            }
        }
        else if (target < midValue) right = mid;
        else left = mid + 1;
    }

    char entryBytes[sizeof(int) + len];
    memmove(entryBytes, entryData, sizeof(int) + len);
    arr.insert(arr.begin() + getVarCharIndexPos(arr, left), entryBytes, entryBytes + len + sizeof(int));

    rids.insert(rids.begin() + left, *entryRID);
    freeSpace -= sizeof(PeterDB::RID);

    if(!isLeaf){
        std::vector<PeterDB::PageNum>& pointers = nonLeafNodeInfo->pointers;
        pointers.insert(pointers.begin() + left + 1, *pageNum);
        freeSpace -= sizeof(PeterDB::PageNum);
    }

    entryCount += 1; // 更新键值计数
    freeSpace -= (sizeof(int) + len);
}

void insertEntry(bool isLeaf, PeterDB::LeafNode* leafNodeInfo, PeterDB::NonLeafNode* nonLeafNodeInfo, const PeterDB::Attribute &attribute, const void* entryData, const PeterDB::RID* entryRID, PeterDB::PageNum* pageNum){
    if(attribute.type == PeterDB::TypeInt){
        insertIntEntry(leafNodeInfo, nonLeafNodeInfo, isLeaf, attribute, entryData, entryRID, pageNum);
    }
    else if(attribute.type == PeterDB::TypeReal){
        insertFloatEntry(leafNodeInfo, nonLeafNodeInfo, isLeaf, attribute, entryData, entryRID, pageNum);
    }
    else if(attribute.type == PeterDB::TypeVarChar){
        insertVarCharEntry(leafNodeInfo, nonLeafNodeInfo, isLeaf, attribute, entryData, entryRID, pageNum);
    }
}

void serializeNonLeafNode(const PeterDB::NonLeafNode& node, char* buffer) {
    size_t offset = 0;

    memmove(buffer + offset, &node.currentKey, sizeof(node.currentKey));
    offset += sizeof(node.currentKey);
    memmove(buffer + offset, &node.freeSpace, sizeof(node.freeSpace));
    offset += sizeof(node.freeSpace);

    size_t routingKeySize = node.routingKey.size();
    memmove(buffer + offset, &routingKeySize, sizeof(routingKeySize));
    offset += sizeof(routingKeySize);
    memmove(buffer + offset, node.routingKey.data(), routingKeySize);
    offset += node.routingKey.size();

    size_t pointerSize = node.pointers.size();
    memmove(buffer + offset, &pointerSize, sizeof(pointerSize));
    offset += sizeof(pointerSize);
    memmove(buffer + offset, node.pointers.data(), pointerSize * sizeof(PeterDB::PageNum));
    offset += pointerSize * sizeof(PeterDB::PageNum);

    size_t ridSize = node.rid.size();
    memmove(buffer + offset, &ridSize, sizeof(ridSize));
    offset += sizeof(ridSize);
    memmove(buffer + offset, node.rid.data(), ridSize * sizeof(PeterDB::RID));
}

void deserializeNonLeafNode(PeterDB::NonLeafNode& node, const char* buffer) {
    size_t offset = 0;

    memmove(&node.currentKey, buffer + offset, sizeof(node.currentKey));
    offset += sizeof(node.currentKey);
    memmove(&node.freeSpace, buffer + offset, sizeof(node.freeSpace));
    offset += sizeof(node.freeSpace);

    size_t routingKeySize; // 8 bytes
    memmove(&routingKeySize, buffer + offset, sizeof(routingKeySize));
    offset += sizeof(routingKeySize);
    node.routingKey.resize(routingKeySize);
    memmove(node.routingKey.data(), buffer + offset, routingKeySize);
    offset += routingKeySize;

    size_t childrenSize; // 8 bytes
    memmove(&childrenSize, buffer + offset, sizeof(childrenSize));
    offset += sizeof(childrenSize);
    node.pointers.resize(childrenSize);
    for (size_t i = 0; i < childrenSize; ++i) {
        PeterDB::PageNum child;
        memmove(&child, buffer + offset, sizeof(PeterDB::PageNum));
        node.pointers[i] = child;
        offset += sizeof(PeterDB::PageNum);
    }

    size_t ridSize; // 8 bytes
    memmove(&ridSize, buffer + offset, sizeof(ridSize));
    offset += sizeof(ridSize);
    node.rid.resize(ridSize);
    for (size_t i = 0; i < ridSize; ++i) {
        PeterDB::RID rid;
        memmove(&rid, buffer + offset, sizeof(PeterDB::RID));
        node.rid[i] = rid;
        offset += sizeof(PeterDB::RID);
    }
}

void serializeLeafNode(const PeterDB::LeafNode& node, char* buffer) {
    size_t offset = 0;

    memmove(buffer + offset, &node.currentKey, sizeof(node.currentKey));
    offset += sizeof(node.currentKey);
    memmove(buffer + offset, &node.freeSpace, sizeof(node.freeSpace));
    offset += sizeof(node.freeSpace);

    size_t keySize = node.key.size();
    memmove(buffer + offset, &keySize, sizeof(keySize));
    offset += sizeof(keySize);
    memmove(buffer + offset, node.key.data(), keySize);
    offset += keySize;

    size_t ridSize = node.rid.size();
    memmove(buffer + offset, &ridSize, sizeof(ridSize));
    offset += sizeof(ridSize);
    memmove(buffer + offset, node.rid.data(), ridSize * sizeof(PeterDB::RID));
}

void deserializeLeafNode(PeterDB::LeafNode& node, const char* buffer){
    size_t offset = 0;

    memmove(&node.currentKey, buffer + offset, sizeof(node.currentKey));
    offset += sizeof(node.currentKey);
    memmove(&node.freeSpace, buffer + offset, sizeof(node.freeSpace));
    offset += sizeof(node.freeSpace);

    size_t keySize; // 8 bytes
    memmove(&keySize, buffer + offset, sizeof(keySize));
    offset += sizeof(keySize);
    node.key.resize(keySize);
    memmove(node.key.data(), buffer + offset, keySize);
    offset += keySize;

    size_t ridSize; // 8 bytes
    memmove(&ridSize, buffer + offset, sizeof(ridSize));
    offset += sizeof(ridSize);
    node.rid.resize(ridSize);
    for (size_t i = 0; i < ridSize; ++i) {
        PeterDB::RID rid;
        memmove(&rid, buffer + offset, sizeof(PeterDB::RID));
        node.rid[i] = rid;
        offset += sizeof(PeterDB::RID);
    }
}

void getNodeHeader(const char* buffer, PeterDB::NodeHeader &nodeHeader){
    memmove(&nodeHeader, buffer, sizeof(nodeHeader));
}

void setNodeHeader(char* buffer, PeterDB::NodeHeader &nodeHeader){
    memmove(buffer, &nodeHeader, sizeof(nodeHeader));
}

void initialNonLeafNodePage(PeterDB::FileHandle &fileHandle, char* buffer){
    char node[PAGE_SIZE];
    memset(node, 0, PAGE_SIZE);
    PeterDB::NodeHeader nodeHeader;
    PeterDB::NonLeafNode nonLeafNode;

    nodeHeader.isLeaf = 0;
    nodeHeader.isDummy = 0;
    nodeHeader.parent = -1;
    nodeHeader.rightSibling = -1;

    nonLeafNode.currentKey = 0;
    nonLeafNode.freeSpace = PAGE_SIZE - sizeof(PeterDB::NodeHeader) - sizeof(size_t) * 3; // size_t is for routingKeySize and pointerSize and ridSize

    setNodeHeader(node, nodeHeader);
    serializeNonLeafNode(nonLeafNode, node + sizeof(PeterDB::NodeHeader));

    memmove(buffer, node, PAGE_SIZE);

    fileHandle.openFileStream->seekp(0, std::ios::end);
    fileHandle.appendPage(node);
}

void initialLeafNodePage(PeterDB::FileHandle &fileHandle, char* buffer){
    char node[PAGE_SIZE];
    memset(node, 0, PAGE_SIZE);
    PeterDB::NodeHeader nodeHeader;
    PeterDB::LeafNode leafNode;

    nodeHeader.isLeaf = 1;
    nodeHeader.isDummy = 0;
    nodeHeader.parent = -1;
    nodeHeader.rightSibling = -1;

    leafNode.currentKey = 0;
    leafNode.freeSpace = PAGE_SIZE - sizeof(PeterDB::NodeHeader) - sizeof(size_t) * 2;

    setNodeHeader(node, nodeHeader);
    serializeLeafNode(leafNode, node + sizeof(PeterDB::NodeHeader));

    memmove(buffer, node, PAGE_SIZE);

    fileHandle.openFileStream->seekp(0, std::ios::end);
    fileHandle.appendPage(node);
}

void dummyNode(PeterDB::FileHandle &fileHandle){ // dummy node (pageNum = 0) point to the current root
    char dummy[PAGE_SIZE];
    memset(dummy, 0, PAGE_SIZE);
    PeterDB::NodeHeader nodeHeader;

    nodeHeader.isLeaf = 0;
    nodeHeader.isDummy = 1;
    nodeHeader.parent = -1;
    nodeHeader.rightSibling = -1;

    memmove(dummy, &nodeHeader, sizeof(PeterDB::NodeHeader));

    PeterDB::PageNum rootPage = 1;
    memmove((char *)dummy + sizeof(nodeHeader), &rootPage, sizeof(PeterDB::PageNum));

    fileHandle.openFileStream->seekp(0, std::ios::end);
    fileHandle.appendPage(dummy);
}

PeterDB::PageNum getRootPage(PeterDB::FileHandle &fileHandle){
    char dummy[PAGE_SIZE];
    memset(dummy, 0, PAGE_SIZE);
    fileHandle.readPage(0, dummy);

    PeterDB::PageNum rootPage;
    memmove(&rootPage, dummy + sizeof(PeterDB::NodeHeader), sizeof(rootPage));

    return rootPage;
}

void setRootPage(PeterDB::FileHandle &fileHandle, PeterDB::PageNum rootPage){
    char dummy[PAGE_SIZE];
    memset(dummy, 0, PAGE_SIZE);
    fileHandle.readPage(0, dummy);

    memmove(dummy + sizeof(PeterDB::NodeHeader), &rootPage, sizeof(rootPage));
    fileHandle.writePage(0, dummy);
}

void setFirstPointer(PeterDB::NonLeafNode &nonLeafNodeInfo, PeterDB::PageNum firstPointer){
    nonLeafNodeInfo.pointers.push_back(firstPointer);
    nonLeafNodeInfo.freeSpace -= sizeof(PeterDB::PageNum);
}

PeterDB::PageNum recursiveInsertBTree(PeterDB::FileHandle &fileHandle, PeterDB::PageNum pageNumber, const PeterDB::Attribute &attribute, const void *key, const PeterDB::RID &rid, PeterDB::NewEntry*& newChildEntry){
    PeterDB::AttrLength typeLen = attribute.type == PeterDB::TypeVarChar ? sizeof (int) + attribute.length : 4;
    int keyLen;
    if(attribute.type == PeterDB::TypeVarChar){
        memmove(&keyLen, key, sizeof(int));
        keyLen += sizeof(int);
    }
    else{
        keyLen = typeLen;
    }

    // Special case: only dummy node in file
    if(fileHandle.getNumberOfPages() == 1) {
        char buffer[PAGE_SIZE];
        initialLeafNodePage(fileHandle, buffer);
        PeterDB::LeafNode leafNodeInfo;
        deserializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
        insertEntry(true, &leafNodeInfo, nullptr, attribute, key, &rid, nullptr);
        serializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
        fileHandle.writePage(1, buffer);
        return 0;
    }

    char buffer[PAGE_SIZE];
    PeterDB::NodeHeader nodeHeader;
    memset(buffer,0, PAGE_SIZE);
    fileHandle.readPage(pageNumber, buffer);
    getNodeHeader(buffer, nodeHeader);

    if(!nodeHeader.isLeaf){
        PeterDB::NonLeafNode nonLeafNodeInfo;
        deserializeNonLeafNode(nonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
        PeterDB::PageNum nextNode = -1;

        // find the routing key
        if(nodeHeader.isDummy){
            PeterDB::PageNum rootPage = getRootPage(fileHandle);
            nextNode = rootPage;
        }
        else{
            for (size_t i = 0; i < nonLeafNodeInfo.currentKey; ++i) {
                char routing[typeLen];
                getEntry(nonLeafNodeInfo.routingKey, i, routing, attribute);
                if(compareKey((char *)key, rid, routing, nonLeafNodeInfo.rid[i], attribute,true) < 0){
                    nextNode = nonLeafNodeInfo.pointers[i];
                    break;
                }
            }
            if(nextNode == -1){
                nextNode = nonLeafNodeInfo.pointers[nonLeafNodeInfo.currentKey];
            }
        }
        // go to the next node
        PeterDB::PageNum previousNodeNum = recursiveInsertBTree(fileHandle, nextNode, attribute, key, rid, newChildEntry);
        if(newChildEntry == nullptr) return 0;

        // Special case: we need a new root node
        if(nodeHeader.isDummy){
            std::cout << "# You are in dummy node!!" << std::endl;
            PeterDB::PageNum newRootPage = fileHandle.getNumberOfPages();
            char newRootBuffer[PAGE_SIZE];
            initialNonLeafNodePage(fileHandle,newRootBuffer);

            PeterDB::NonLeafNode newRootNodeInfo;
            deserializeNonLeafNode(newRootNodeInfo, newRootBuffer + sizeof(PeterDB::NodeHeader));

            setFirstPointer(newRootNodeInfo, previousNodeNum);
            insertEntry(false, nullptr, &newRootNodeInfo, attribute, newChildEntry->key, &newChildEntry->rid, &newChildEntry->pageNum);

            setRootPage(fileHandle, newRootPage);
            serializeNonLeafNode(newRootNodeInfo, newRootBuffer + sizeof(PeterDB::NodeHeader));
            fileHandle.writePage(newRootPage, newRootBuffer);
            return 0;
        }

        // if non-leaf node has enough space
        if(keyLen + sizeof(PeterDB::PageNum) + sizeof (PeterDB::RID) < nonLeafNodeInfo.freeSpace){
            insertEntry(false, nullptr, &nonLeafNodeInfo, attribute, newChildEntry->key, &newChildEntry->rid, &newChildEntry->pageNum);
            serializeNonLeafNode(nonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            newChildEntry = nullptr;
            fileHandle.writePage(pageNumber, buffer);
            return 0;
        }
        else{  // non-leaf node doesn't have enough space
            // temp key and pointer (size: max + 1)
            PeterDB::NonLeafNode tempNode;
            tempNode.currentKey = nonLeafNodeInfo.currentKey;
            tempNode.routingKey = nonLeafNodeInfo.routingKey;
            tempNode.pointers = nonLeafNodeInfo.pointers;
            tempNode.rid = nonLeafNodeInfo.rid;

            // insert new key
            insertEntry(false, nullptr, &tempNode, attribute, newChildEntry->key, &newChildEntry->rid, &newChildEntry->pageNum);

            // create a new non-leaf node
            PeterDB::PageNum newNonLeafPage = fileHandle.getNumberOfPages();
            char newNodeBuffer[PAGE_SIZE];
            initialNonLeafNodePage(fileHandle,newNodeBuffer);

            // half of the entries will be moved to the new non-leaf node
            PeterDB::NonLeafNode newNonLeafNodeInfo;
            deserializeNonLeafNode(newNonLeafNodeInfo, newNodeBuffer + sizeof(PeterDB::NodeHeader));

            // split
            short splitPoint = tempNode.currentKey / 2;

            // nonLeafNodeInfo(old)得到tempNode前半
            nonLeafNodeInfo.routingKey.clear();
            nonLeafNodeInfo.pointers.clear();
            nonLeafNodeInfo.rid.clear();
            nonLeafNodeInfo.freeSpace = PAGE_SIZE - sizeof(PeterDB::NodeHeader) - sizeof (size_t) * 3;

            if(attribute.type == PeterDB::TypeVarChar){
                nonLeafNodeInfo.routingKey.insert(nonLeafNodeInfo.routingKey.begin(),tempNode.routingKey.begin(),tempNode.routingKey.begin() + getVarCharIndexPos(tempNode.routingKey, splitPoint));
            }
            else{
                nonLeafNodeInfo.routingKey.insert(nonLeafNodeInfo.routingKey.begin(),tempNode.routingKey.begin(),tempNode.routingKey.begin() + splitPoint * typeLen);
            }
            nonLeafNodeInfo.pointers.insert(nonLeafNodeInfo.pointers.begin(),tempNode.pointers.begin(),tempNode.pointers.begin() + splitPoint + 1); //記得多1
            nonLeafNodeInfo.rid.insert(nonLeafNodeInfo.rid.begin(),tempNode.rid.begin(),tempNode.rid.begin() + splitPoint);
            nonLeafNodeInfo.currentKey = splitPoint;
            nonLeafNodeInfo.freeSpace -= (nonLeafNodeInfo.routingKey.size() + (nonLeafNodeInfo.currentKey + 1) * sizeof(PeterDB::PageNum) + nonLeafNodeInfo.rid.size() * sizeof(PeterDB::RID));

            // newNonLeafNode得到tempNode後半
            if(attribute.type == PeterDB::TypeVarChar){
                newNonLeafNodeInfo.routingKey.insert(newNonLeafNodeInfo.routingKey.begin(),tempNode.routingKey.begin() + getVarCharIndexPos(tempNode.routingKey, splitPoint + 1),tempNode.routingKey.end());
            }
            else{
                newNonLeafNodeInfo.routingKey.insert(newNonLeafNodeInfo.routingKey.begin(),tempNode.routingKey.begin() + (splitPoint + 1) * typeLen,tempNode.routingKey.end());
            }
            newNonLeafNodeInfo.pointers.insert(newNonLeafNodeInfo.pointers.begin(),tempNode.pointers.begin() + splitPoint + 1,tempNode.pointers.end());
            newNonLeafNodeInfo.rid.insert(newNonLeafNodeInfo.rid.begin(),tempNode.rid.begin() + splitPoint + 1,tempNode.rid.end());
            newNonLeafNodeInfo.currentKey = tempNode.currentKey - splitPoint - 1; // 有一個pointer要丟上去
            newNonLeafNodeInfo.freeSpace -= (newNonLeafNodeInfo.routingKey.size() + (newNonLeafNodeInfo.currentKey + 1) * sizeof(PeterDB::PageNum) + newNonLeafNodeInfo.rid.size() * sizeof(PeterDB::RID));

            serializeNonLeafNode(nonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            serializeNonLeafNode(newNonLeafNodeInfo, newNodeBuffer + sizeof(PeterDB::NodeHeader));

            newChildEntry = new PeterDB::NewEntry();
            newChildEntry->key = new char[typeLen];
            getEntry(tempNode.routingKey, splitPoint, newChildEntry->key, attribute);
            newChildEntry->pageNum = newNonLeafPage;
            newChildEntry->rid = tempNode.rid[splitPoint];

            fileHandle.writePage(pageNumber, buffer);
            fileHandle.writePage(newNonLeafPage, newNodeBuffer);

            return pageNumber;
        }
    }

    if(nodeHeader.isLeaf){
        PeterDB::LeafNode leafNodeInfo;
        deserializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));

        // if leaf node has enough space
        if(keyLen + sizeof(PeterDB::RID) < leafNodeInfo.freeSpace){
            insertEntry(true, &leafNodeInfo, nullptr, attribute, key, &rid, nullptr);
            serializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            newChildEntry = nullptr;
            fileHandle.writePage(pageNumber, buffer);
            return 0;
        }
        else { // leaf node doesn't have enough space
            // temp key and rid (size: max + 1)
            PeterDB::LeafNode tempNode;
            tempNode.currentKey = leafNodeInfo.currentKey;
            tempNode.key = leafNodeInfo.key;
            tempNode.rid = leafNodeInfo.rid;

            // insert new key
            insertEntry(true, &tempNode, nullptr, attribute, key, &rid, nullptr);

            // split
            // create a new leaf node
            PeterDB::PageNum newLeafPage = fileHandle.getNumberOfPages();
            char newNodeBuffer[PAGE_SIZE];
            initialLeafNodePage(fileHandle, newNodeBuffer);
            PeterDB::NodeHeader newLeafNodeHeader;
            getNodeHeader(newNodeBuffer, newLeafNodeHeader);

            // half of the entries will be moved to the new leaf node
            PeterDB::LeafNode newLeafNodeInfo;
            deserializeLeafNode(newLeafNodeInfo, newNodeBuffer + sizeof(PeterDB::NodeHeader));

            // get split point
            short splitPoint = tempNode.currentKey / 2;

            // leafNodeInfo(old)得到tempNode前半
            leafNodeInfo.key.clear();
            leafNodeInfo.rid.clear();
            leafNodeInfo.freeSpace = PAGE_SIZE - sizeof(PeterDB::NodeHeader) - sizeof(size_t) * 2;

            if(attribute.type == PeterDB::TypeVarChar){
                leafNodeInfo.key.insert(leafNodeInfo.key.begin(),tempNode.key.begin(),tempNode.key.begin() + getVarCharIndexPos(tempNode.key, splitPoint));
            }
            else{
                leafNodeInfo.key.insert(leafNodeInfo.key.begin(),tempNode.key.begin(),tempNode.key.begin() + splitPoint * typeLen);
            }
            leafNodeInfo.rid.insert(leafNodeInfo.rid.begin(),tempNode.rid.begin(),tempNode.rid.begin() + splitPoint);
            leafNodeInfo.currentKey = splitPoint;
            leafNodeInfo.freeSpace -= (leafNodeInfo.key.size() + sizeof(PeterDB::RID) * leafNodeInfo.currentKey);

            // newLeafNode得到tempNode後半
            if(attribute.type == PeterDB::TypeVarChar){
                newLeafNodeInfo.key.insert(newLeafNodeInfo.key.begin(),tempNode.key.begin() + getVarCharIndexPos(tempNode.key, splitPoint),tempNode.key.end());
            }
            else{
                newLeafNodeInfo.key.insert(newLeafNodeInfo.key.begin(),tempNode.key.begin() + splitPoint * typeLen,tempNode.key.end());
            }
            newLeafNodeInfo.rid.insert(newLeafNodeInfo.rid.begin(),tempNode.rid.begin() + splitPoint,tempNode.rid.end());
            newLeafNodeInfo.currentKey = tempNode.currentKey - splitPoint;
            newLeafNodeInfo.freeSpace -= (newLeafNodeInfo.key.size() + sizeof(PeterDB::RID) * newLeafNodeInfo.currentKey);

            // set sibling info
            newLeafNodeHeader.rightSibling = nodeHeader.rightSibling;
            nodeHeader.rightSibling = (int)newLeafPage;

            setNodeHeader(buffer, nodeHeader);
            setNodeHeader(newNodeBuffer, newLeafNodeHeader);

            serializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            serializeLeafNode(newLeafNodeInfo, newNodeBuffer + sizeof(PeterDB::NodeHeader));

            // set newchildentry to the parent node
            newChildEntry = new PeterDB::NewEntry();
            newChildEntry->key = new char[typeLen];
            getEntry(newLeafNodeInfo.key, 0,newChildEntry->key, attribute);
            newChildEntry->pageNum = newLeafPage;
            newChildEntry->rid = newLeafNodeInfo.rid[0];

            fileHandle.writePage(pageNumber, buffer);
            fileHandle.writePage(newLeafPage, newNodeBuffer);

            return pageNumber;
        }
    }

}

void getKeyStringValue(char* temp, const PeterDB::Attribute& attribute, std::string& keyString) {
    switch (attribute.type) {
        case PeterDB::TypeInt: {
            int intValue = *reinterpret_cast<int*>(temp);
            keyString = std::to_string(intValue);
            break;
        }
        case PeterDB::TypeReal: {
            float floatValue = *reinterpret_cast<float*>(temp);
            keyString = std::to_string(floatValue);
            break;
        }
        case PeterDB::TypeVarChar: {
            int len;
            memmove(&len, temp, sizeof(int));
            keyString.assign(temp + sizeof(int), len);
            break;
        }
        default:
            // 处理未知类型
            break;
    }
}

void recursiveGenerateJsonString(PeterDB::FileHandle &fileHandle, PeterDB::PageNum pageNum, const PeterDB::Attribute &attribute, std::string &outputJsonString){
    PeterDB::AttrLength typeLen = attribute.type == PeterDB::TypeVarChar ? sizeof (int) + attribute.length : 4;
    char buffer[PAGE_SIZE];
    memset(buffer, 0, PAGE_SIZE);
    fileHandle.readPage(pageNum, buffer);
    PeterDB::NodeHeader nodeHeader;
    getNodeHeader(buffer, nodeHeader);

    if (!nodeHeader.isLeaf){
        PeterDB::NonLeafNode nonLeafNodeInfo;
        deserializeNonLeafNode(nonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
        outputJsonString += "{\"keys\":[";
        for (size_t i = 0; i < nonLeafNodeInfo.currentKey; ++i) {
            if (i != 0) outputJsonString += ",";
            outputJsonString += "\"";

            char temp[typeLen];
            getEntry(nonLeafNodeInfo.routingKey, i, temp, attribute);
            // support different type of key

            std::string keyString;
            getKeyStringValue(temp, attribute, keyString);
            outputJsonString += keyString;
            outputJsonString += "\"";
        }
        outputJsonString += "],\n";
        outputJsonString += "\"children\":[";
        for (size_t i = 0; i < nonLeafNodeInfo.currentKey + 1; ++i) {
            if (i != 0) outputJsonString += ",";
            recursiveGenerateJsonString(fileHandle, nonLeafNodeInfo.pointers[i], attribute, outputJsonString);
        }
        outputJsonString += "]}\n";
    }
    else {
        PeterDB::LeafNode leafNodeInfo;
        deserializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));

        std::map<std::string, std::vector<PeterDB::RID>> keyMap;
        for (size_t i = 0; i < leafNodeInfo.currentKey; ++i) {
            char temp[typeLen];
            getEntry(leafNodeInfo.key, i, temp, attribute);
            std::string keyString;
            getKeyStringValue(temp, attribute, keyString);
            keyMap[keyString].push_back(leafNodeInfo.rid[i]);
        }

        outputJsonString += "{\"keys\":[";

        for (auto it = keyMap.begin(); it != keyMap.end(); ++it) {
            if (it != keyMap.begin()) outputJsonString += ",";
            outputJsonString += "\"";
            outputJsonString += it->first;
            outputJsonString += ":[";
            for (size_t i = 0; i < it->second.size(); ++i) {
                if (i != 0) outputJsonString += ",";
                outputJsonString += "(" + std::to_string(it->second[i].pageNum) + "," + std::to_string(it->second[i].slotNum) + ")";
            }
            outputJsonString += "]";
            outputJsonString += "\"";
        }
        outputJsonString += "]}";
        return;
    }
}
#endif //PETERDB_IX_UTILS_H