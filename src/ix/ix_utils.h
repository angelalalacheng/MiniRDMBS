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

short getNumOfEntryInPage(const PeterDB::Attribute &attribute){
    short size = attribute.length + sizeof(PeterDB::RID) + sizeof(PeterDB::PageNum); // sizeof(RID): 8 bytes, sizeof(PageNum): 4 bytes
    // leafNode: size of key + size of RID
    // nonLeafNode: size of key + size of PageNum (還會多一個pointer)
    short numOfNode = (PAGE_SIZE - sizeof(PeterDB::NodeHeader))/ size;

    return numOfNode;
}

int compareKey(const char* key1, const char* key2, const PeterDB::Attribute& attribute, bool compareLow) {
    if (attribute.type == PeterDB::TypeInt) {
        if(compareLow && key2 == nullptr) return 1;
        if(!compareLow && key2 == nullptr) return -1;
        int k1 = *reinterpret_cast<const int*>(key1);
        int k2 = *reinterpret_cast<const int*>(key2);
        return (k1 > k2) - (k1 < k2);
    }
    else if (attribute.type == PeterDB::TypeReal) {
        if(compareLow && key2 == nullptr) return 1;
        if(!compareLow && key2 == nullptr) return -1;
        float k1 = *reinterpret_cast<const float*>(key1);
        float k2 = *reinterpret_cast<const float*>(key2);
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
        return k1.compare(k2);
    }
    return -2;
}

void setEntry(std::vector<char>& arr, size_t index, size_t typeLen, const void* entryData) {
    if (index * typeLen < arr.size()) {
        std::memmove(arr.data() + (index * typeLen), entryData, typeLen);
    }
}

void getEntry(const std::vector<char>& arr, size_t index, size_t typeLen, void* entryData) {
    if (index * typeLen < arr.size()) {
        std::memmove(entryData, arr.data() + (index * typeLen), typeLen);
    }
}

void clearEntry(std::vector<char>& arr, size_t index, size_t typeLen) {
    size_t startPos = index * typeLen;
    size_t endPos = startPos + typeLen;

    if (startPos < arr.size() && endPos < arr.size()) {
        // 使用memmove将后续元素向前移动覆盖被删除的键
        memmove(arr.data() + startPos, arr.data() + endPos, arr.size() - endPos);
        // 将最后一个键的位置填充为0（因为我们不改变vector的大小）
        memset(arr.data() + arr.size() - typeLen, 0, typeLen);
    }
}

void insertIntEntry(PeterDB::LeafNode* leafNodeInfo, PeterDB::NonLeafNode* nonLeafNodeInfo, bool isLeaf, size_t typeLen, const void* entryData, const PeterDB::RID* entryRID, const PeterDB::PageNum* pageNum) {
    std::vector<char>& arr = isLeaf ? leafNodeInfo->key : nonLeafNodeInfo->routingKey;
    short& entryCount = isLeaf ? leafNodeInfo->currentKey : nonLeafNodeInfo->currentKey;

    size_t left = 0, right = entryCount, mid = 0;
    int target = *reinterpret_cast<const int*>(entryData);

    while (left < right) {
        mid = (left + right) / 2;
        char midData[typeLen];
        getEntry(arr, mid, typeLen, midData);
        int midValue = *reinterpret_cast<int*>(midData);
        if (midValue == target) {
            left = mid + 1;
            break;
        }
        else if (target < midValue) right = mid;
        else left = mid + 1;
    }

    // 为新元素腾出空间
    memmove(arr.data() + ((left + 1) * typeLen), arr.data() + (left * typeLen), (entryCount - left) * typeLen);
    memmove(arr.data() + (left * typeLen), entryData, typeLen); // 复制新键值

    if (isLeaf) {
        std::vector<PeterDB::RID>& rids = leafNodeInfo->rid;
        move_backward(rids.begin() + left, rids.begin() + entryCount, rids.begin() + entryCount + 1);
        rids[left] = *entryRID; // 对叶节点插入新RID
    } else {
        std::vector<PeterDB::PageNum>& pointers = nonLeafNodeInfo->pointers;
        move_backward(pointers.begin() + left + 1, pointers.begin() + entryCount + 1, pointers.begin() + entryCount + 2);
        pointers[left + 1] = *pageNum; // 对非叶节点插入新页号
    }

    entryCount += 1; // 更新键值计数
}

void insertFloatEntry(PeterDB::LeafNode* leafNodeInfo, PeterDB::NonLeafNode* nonLeafNodeInfo, bool isLeaf, size_t typeLen, const void* entryData, const PeterDB::RID* entryRID, const PeterDB::PageNum* pageNum){
    std::vector<char>& arr = isLeaf ? leafNodeInfo->key : nonLeafNodeInfo->routingKey;
    short& entryCount = isLeaf ? leafNodeInfo->currentKey : nonLeafNodeInfo->currentKey;

    size_t left = 0, right = entryCount, mid = 0;
    float target = *reinterpret_cast<const float*>(entryData);

    while (left < right) {
        mid = (left + right) / 2;
        char midData[typeLen];
        getEntry(arr, mid, typeLen, midData);
        float midValue = *reinterpret_cast<float*>(midData);
        if (midValue == target) {
            left = mid + 1;
            break;
        }
        else if (target < midValue) right = mid;
        else left = mid + 1;
    }

    // 为新元素腾出空间
    memmove(arr.data() + ((left + 1) * typeLen), arr.data() + (left * typeLen), (entryCount - left) * typeLen);
    memmove(arr.data() + (left * typeLen), entryData, typeLen); // 复制新键值

    if (isLeaf) {
        std::vector<PeterDB::RID>& rids = leafNodeInfo->rid;
        move_backward(rids.begin() + left, rids.begin() + entryCount, rids.begin() + entryCount + 1);
        rids[left] = *entryRID; // 对叶节点插入新RID
    } else {
        std::vector<PeterDB::PageNum>& pointers = nonLeafNodeInfo->pointers;
        move_backward(pointers.begin() + left + 1, pointers.begin() + entryCount + 1, pointers.begin() + entryCount + 2);
        pointers[left + 1] = *pageNum; // 对非叶节点插入新页号
    }

    entryCount += 1; // 更新键值计数
}

void insertVarCharEntry(PeterDB::LeafNode* leafNodeInfo, PeterDB::NonLeafNode* nonLeafNodeInfo, bool isLeaf, size_t typeLen, const void* entryData, const PeterDB::RID* entryRID, const PeterDB::PageNum* pageNum){
    std::vector<char>& arr = isLeaf ? leafNodeInfo->key : nonLeafNodeInfo->routingKey;
    short& entryCount = isLeaf ? leafNodeInfo->currentKey : nonLeafNodeInfo->currentKey;

    size_t left = 0, right = entryCount, mid = 0;

    int len;
    memmove(&len, entryData, sizeof (int));
    std::string target;
    target.resize(len);
    memmove(&target[0], (char *)entryData + sizeof (int), len);

    while (left < right) {
        mid = (left + right) / 2;
        char midData[typeLen];
        getEntry(arr, mid, typeLen, midData);

        int tempLen;
        memmove(&tempLen, midData, sizeof (int));
        std::string midValue;
        midValue.resize(tempLen);
        memmove(&midValue[0], (char *)midData + sizeof (int), tempLen);

        if (midValue == target) {
            left = mid + 1;
            break;
        }
        else if (target < midValue) right = mid;
        else left = mid + 1;
    }

    // 为新元素腾出空间
    memmove(arr.data() + ((left + 1) * typeLen), arr.data() + (left * typeLen), (entryCount - left) * typeLen);
    memmove(arr.data() + (left * typeLen), entryData, typeLen);

    if (isLeaf) {
        std::vector<PeterDB::RID>& rids = leafNodeInfo->rid;
        move_backward(rids.begin() + left, rids.begin() + entryCount, rids.begin() + entryCount + 1);
        rids[left] = *entryRID; // 对叶节点插入新RID
    } else {
        std::vector<PeterDB::PageNum>& pointers = nonLeafNodeInfo->pointers;
        move_backward(pointers.begin() + left + 1, pointers.begin() + entryCount + 1, pointers.begin() + entryCount + 2);
        pointers[left + 1] = *pageNum; // 对非叶节点插入新页号
    }

    entryCount += 1; // 更新键值计数

}

void insertEntry(bool isLeaf, PeterDB::LeafNode* leafNodeInfo, PeterDB::NonLeafNode* nonLeafNodeInfo, const PeterDB::Attribute &attribute, const void* entryData, const PeterDB::RID* entryRID, PeterDB::PageNum* pageNum){
    size_t typeLen = attribute.length;

    if(attribute.type == PeterDB::TypeInt){
        insertIntEntry(leafNodeInfo, nonLeafNodeInfo, isLeaf, typeLen, entryData, entryRID, pageNum);
    }
    else if(attribute.type == PeterDB::TypeReal){
        insertFloatEntry(leafNodeInfo, nonLeafNodeInfo, isLeaf, typeLen, entryData, entryRID, pageNum);
    }
    else if(attribute.type == PeterDB::TypeVarChar){
        insertVarCharEntry(leafNodeInfo, nonLeafNodeInfo, isLeaf, typeLen, entryData, entryRID, pageNum);
    }
}

void serializeNonLeafNode(const PeterDB::NonLeafNode& node, char* buffer) {
    size_t offset = 0;

    memmove(buffer + offset, &node.currentKey, sizeof(node.currentKey));
    offset += sizeof(node.currentKey);
    memmove(buffer + offset, &node.maxKeys, sizeof(node.maxKeys));
    offset += sizeof(node.maxKeys);

    size_t routingKeySize = node.routingKey.size();
    memmove(buffer + offset, &routingKeySize, sizeof(routingKeySize));
    offset += sizeof(routingKeySize);
    memmove(buffer + offset, node.routingKey.data(), routingKeySize);
    offset += node.routingKey.size();

    size_t pointerSize = node.pointers.size();
    memmove(buffer + offset, &pointerSize, sizeof(pointerSize));
    offset += sizeof(pointerSize);
    memmove(buffer + offset, node.pointers.data(), pointerSize * sizeof(PeterDB::PageNum));
    // offset += node.children.size() * sizeof(PageNum); // 更新 offset 如有必要
}

void deserializeNonLeafNode(PeterDB::NonLeafNode& node, const char* buffer) {
    size_t offset = 0;

    memmove(&node.currentKey, buffer + offset, sizeof(node.currentKey));
    offset += sizeof(node.currentKey);
    memmove(&node.maxKeys, buffer + offset, sizeof(node.maxKeys));
    offset += sizeof(node.maxKeys);

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
}

void serializeLeafNode(const PeterDB::LeafNode& node, char* buffer) {
    size_t offset = 0;

    memmove(buffer + offset, &node.currentKey, sizeof(node.currentKey));
    offset += sizeof(node.currentKey);
    memmove(buffer + offset, &node.maxKeys, sizeof(node.maxKeys));
    offset += sizeof(node.maxKeys);

    size_t keySize = node.key.size();
    memmove(buffer + offset, &keySize, sizeof(keySize));
    offset += sizeof(keySize);
    memmove(buffer + offset, node.key.data(), keySize);
    offset += keySize;

    size_t ridSize = node.rid.size();
    memmove(buffer + offset, &ridSize, sizeof(ridSize));
    offset += sizeof(ridSize);
    memmove(buffer + offset, node.rid.data(), ridSize * sizeof(PeterDB::RID));
//    offset += ridSize * sizeof(PeterDB::RID);
}

void deserializeLeafNode(PeterDB::LeafNode& node, const char* buffer){
    size_t offset = 0;

    memmove(&node.currentKey, buffer + offset, sizeof(node.currentKey));
    offset += sizeof(node.currentKey);
    memmove(&node.maxKeys, buffer + offset, sizeof(node.maxKeys));
    offset += sizeof(node.maxKeys);

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

void initialNonLeafNodePage(PeterDB::FileHandle &fileHandle, const PeterDB::Attribute &attribute, short numOfEntry, char* buffer){
    char node[PAGE_SIZE];
    memset(node, 0, PAGE_SIZE);
    PeterDB::NodeHeader nodeHeader;
    PeterDB::NonLeafNode nonLeafNode;

    nodeHeader.isLeaf = 0;
    nodeHeader.isDummy = 0;
    nodeHeader.parent = -1;
//    nodeHeader.leftSibling = -1;
    nodeHeader.rightSibling = -1;

    nonLeafNode.currentKey = 0;
    nonLeafNode.maxKeys = numOfEntry;
    nonLeafNode.routingKey.resize(numOfEntry * attribute.length, 0);
    nonLeafNode.pointers.resize(numOfEntry + 1, 0);

    setNodeHeader(node, nodeHeader);
    serializeNonLeafNode(nonLeafNode, node + sizeof(nodeHeader));

    memmove(buffer, node, PAGE_SIZE);

    fileHandle.openFileStream->seekp(0, std::ios::end);
    fileHandle.appendPage(node);
}

void initialLeafNodePage(PeterDB::FileHandle &fileHandle, const PeterDB::Attribute &attribute, short numOfEntry, char* buffer){
    char node[PAGE_SIZE];
    memset(node, 0, PAGE_SIZE);
    PeterDB::NodeHeader nodeHeader;
    PeterDB::LeafNode leafNode;

    PeterDB::AttrLength typeLen = attribute.length;
    if(attribute.type == PeterDB::TypeVarChar) typeLen += sizeof(int);

    nodeHeader.isLeaf = 1;
    nodeHeader.isDummy = 0;
    nodeHeader.parent = -1;
//    nodeHeader.leftSibling = -1;
    nodeHeader.rightSibling = -1;

    leafNode.currentKey = 0;
    leafNode.maxKeys = numOfEntry;
    leafNode.key.resize(numOfEntry * typeLen, 0);
    leafNode.rid.resize(numOfEntry);

    setNodeHeader(node, nodeHeader);
    serializeLeafNode(leafNode, node + sizeof(nodeHeader));

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
//    nodeHeader.leftSibling = -1;
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
    nonLeafNodeInfo.pointers[0] = firstPointer;
}

PeterDB::PageNum recursiveInsertBTree(PeterDB::FileHandle &fileHandle, PeterDB::PageNum pageNumber, const PeterDB::Attribute &attribute, const void *key, const PeterDB::RID &rid, PeterDB::NewEntry*& newChildEntry, short entryInPage){
    PeterDB::AttrLength typeLen = attribute.type == PeterDB::TypeVarChar ? sizeof (int) + attribute.length : 4;

    // Special case: only dummy node in file
    if(fileHandle.getNumberOfPages() == 1) {
        char buffer[PAGE_SIZE];
        initialLeafNodePage(fileHandle, attribute, entryInPage, buffer);
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
                getEntry(nonLeafNodeInfo.routingKey, i, typeLen, routing);
                //TODO: key type
                if(compareKey((char *)key, routing, attribute, true) < 0){
                    nextNode = nonLeafNodeInfo.pointers[i];
                    break;
                }
            }
            if(nextNode == -1){
                nextNode = nonLeafNodeInfo.pointers[nonLeafNodeInfo.currentKey];
            }
        }
        // go to the next node
        PeterDB::PageNum previousNodeNum = recursiveInsertBTree(fileHandle, nextNode, attribute, key, rid, newChildEntry, entryInPage);
        if(newChildEntry == nullptr) return 0;

        // Special case: we need a new root node
        if(nodeHeader.isDummy){
            std::cout << "# You are in dummy node!!" << std::endl;
            PeterDB::PageNum newRootPage = fileHandle.getNumberOfPages();
            char newRootBuffer[PAGE_SIZE];
            initialNonLeafNodePage(fileHandle, attribute, entryInPage, newRootBuffer);

            PeterDB::NonLeafNode newRootNodeInfo;
            deserializeNonLeafNode(newRootNodeInfo, newRootBuffer + sizeof(PeterDB::NodeHeader));

            setFirstPointer(newRootNodeInfo, previousNodeNum);
            insertEntry(false, nullptr, &newRootNodeInfo, attribute, newChildEntry->key, nullptr, &newChildEntry->pageNum);

            setRootPage(fileHandle, newRootPage);
            serializeNonLeafNode(newRootNodeInfo, newRootBuffer + sizeof(PeterDB::NodeHeader));
            fileHandle.writePage(newRootPage, newRootBuffer);
            return 0;
        }

        // if non-leaf node has enough space
        if(nonLeafNodeInfo.currentKey < nonLeafNodeInfo.maxKeys){
            insertEntry(false, nullptr, &nonLeafNodeInfo, attribute, newChildEntry->key, nullptr, &newChildEntry->pageNum);
            serializeNonLeafNode(nonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            newChildEntry = nullptr;
            fileHandle.writePage(pageNumber, buffer);
            return 0;
        }
        else{  // non-leaf node doesn't have enough space
            // temp key and pointer (size: max + 1)
            PeterDB::NonLeafNode tempNode;
            tempNode.currentKey = nonLeafNodeInfo.currentKey;
            tempNode.routingKey.resize((entryInPage + 1) * typeLen, 0);
            tempNode.pointers.resize(entryInPage + 2, 0);

            memmove(tempNode.routingKey.data(), nonLeafNodeInfo.routingKey.data(), nonLeafNodeInfo.currentKey * typeLen);
            copy(nonLeafNodeInfo.pointers.begin(), nonLeafNodeInfo.pointers.end(), tempNode.pointers.begin());

            // insert new key
            insertEntry(false, nullptr, &tempNode, attribute, newChildEntry->key, nullptr, &newChildEntry->pageNum);

            // create a new non-leaf node
            PeterDB::PageNum newNonLeafPage = fileHandle.getNumberOfPages();
            char newNodeBuffer[PAGE_SIZE];
            initialNonLeafNodePage(fileHandle, attribute, nonLeafNodeInfo.maxKeys, newNodeBuffer);

            // half of the entries will be moved to the new non-leaf node
            PeterDB::NonLeafNode newNonLeafNodeInfo;
            deserializeNonLeafNode(newNonLeafNodeInfo, newNodeBuffer + sizeof(PeterDB::NodeHeader));

            // split
            short splitPoint = tempNode.currentKey / 2;

            // nonLeafNodeInfo(old)得到tempNode前半
            nonLeafNodeInfo.currentKey = splitPoint;
            memmove(nonLeafNodeInfo.routingKey.data(), tempNode.routingKey.data(), splitPoint * typeLen);
            memset(nonLeafNodeInfo.routingKey.data() + splitPoint * typeLen, 0, (nonLeafNodeInfo.maxKeys - splitPoint) * typeLen);
            std::copy(tempNode.pointers.begin(), tempNode.pointers.begin() + splitPoint + 1, nonLeafNodeInfo.pointers.begin()); //記得多1
            std::fill(nonLeafNodeInfo.pointers.begin() + splitPoint + 1, nonLeafNodeInfo.pointers.end(), 0);

            // newNonLeafNode得到tempNode後半
            newNonLeafNodeInfo.currentKey = tempNode.currentKey - splitPoint - 1; // 有一個key要丟上去
            memmove(newNonLeafNodeInfo.routingKey.data(), tempNode.routingKey.data() + (splitPoint + 1) * typeLen, newNonLeafNodeInfo.currentKey * typeLen);
            std::copy(tempNode.pointers.begin() + splitPoint + 1, tempNode.pointers.end(), newNonLeafNodeInfo.pointers.begin());

            serializeNonLeafNode(nonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            serializeNonLeafNode(newNonLeafNodeInfo, newNodeBuffer + sizeof(PeterDB::NodeHeader));

            newChildEntry = new PeterDB::NewEntry();
            newChildEntry->key = new char[typeLen];
            getEntry(tempNode.routingKey, splitPoint, typeLen, newChildEntry->key);
            newChildEntry->pageNum = newNonLeafPage;

            fileHandle.writePage(pageNumber, buffer);
            fileHandle.writePage(newNonLeafPage, newNodeBuffer);

            return pageNumber;
        }
    }

    if(nodeHeader.isLeaf){
        PeterDB::LeafNode leafNodeInfo;
        deserializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));

        // if leaf node has enough space
        if(leafNodeInfo.currentKey < leafNodeInfo.maxKeys){
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
            tempNode.key.resize((leafNodeInfo.maxKeys + 1) * typeLen, 0);
            tempNode.rid.resize(leafNodeInfo.maxKeys + 1);
            memmove(tempNode.key.data(), leafNodeInfo.key.data(), leafNodeInfo.currentKey * typeLen);
            copy(leafNodeInfo.rid.begin(), leafNodeInfo.rid.end(),  tempNode.rid.begin());

            // insert new key
            insertEntry(true, &tempNode, nullptr, attribute, key, &rid, nullptr);

            // split
            // create a new leaf node
            PeterDB::PageNum newLeafPage = fileHandle.getNumberOfPages();
            char newNodeBuffer[PAGE_SIZE];
            initialLeafNodePage(fileHandle, attribute, entryInPage,newNodeBuffer);
            PeterDB::NodeHeader newLeafNodeHeader;
            getNodeHeader(newNodeBuffer, newLeafNodeHeader);

            // half of the entries will be moved to the new leaf node
            PeterDB::LeafNode newLeafNodeInfo;
            deserializeLeafNode(newLeafNodeInfo, newNodeBuffer + sizeof(PeterDB::NodeHeader));

            // get split point
            short splitPoint = tempNode.currentKey / 2;

            // leafNodeInfo(old)得到tempNode前半
            leafNodeInfo.currentKey = splitPoint;
            memmove(leafNodeInfo.key.data(), tempNode.key.data(), splitPoint * typeLen);
            memset(leafNodeInfo.key.data() + splitPoint * typeLen, 0, (leafNodeInfo.maxKeys - splitPoint) * typeLen);
            std::copy(tempNode.rid.begin(), tempNode.rid.begin() + splitPoint, leafNodeInfo.rid.begin());
            std::fill(leafNodeInfo.rid.begin() + splitPoint, leafNodeInfo.rid.end(), PeterDB::RID());

            // newLeafNode得到tempNode後半
            newLeafNodeInfo.currentKey = tempNode.currentKey - splitPoint;
            memmove(newLeafNodeInfo.key.data(), tempNode.key.data() + splitPoint * typeLen, newLeafNodeInfo.currentKey * typeLen);
            std::copy(tempNode.rid.begin() + splitPoint, tempNode.rid.end(), newLeafNodeInfo.rid.begin());

            // set sibling info
            // newLeafNodeHeader.leftSibling = (int)pageNumber;
            newLeafNodeHeader.rightSibling = nodeHeader.rightSibling;
            nodeHeader.rightSibling = (int)newLeafPage;

            setNodeHeader(buffer, nodeHeader);
            setNodeHeader(newNodeBuffer, newLeafNodeHeader);

            serializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            serializeLeafNode(newLeafNodeInfo, newNodeBuffer + sizeof(PeterDB::NodeHeader));

            // set newchildentry to the parent node
            newChildEntry = new PeterDB::NewEntry();
            newChildEntry->key = new char[typeLen];
            getEntry(newLeafNodeInfo.key, 0, typeLen, newChildEntry->key);
            newChildEntry->pageNum = newLeafPage;

            fileHandle.writePage(pageNumber, buffer);
            fileHandle.writePage(newLeafPage, newNodeBuffer);

            return pageNumber;
        }
    }

}

void recursiveGenerateJsonString(PeterDB::FileHandle &fileHandle, PeterDB::PageNum pageNum, std::ostream &out) {
    char buffer[PAGE_SIZE];
    memset(buffer, 0, PAGE_SIZE);
    fileHandle.readPage(pageNum, buffer);
    PeterDB::NodeHeader nodeHeader;
    getNodeHeader(buffer, nodeHeader);

    if (!nodeHeader.isLeaf){
        PeterDB::NonLeafNode nonLeafNodeInfo;
        deserializeNonLeafNode(nonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
        out << "{\"keys\":[";
        for (size_t i = 0; i < nonLeafNodeInfo.currentKey; ++i) {
            if (i != 0) out << ",";
            out << "\"";
            for (size_t j = 0; j < nonLeafNodeInfo.maxKeys; ++j) {
                int temp;
                getEntry(nonLeafNodeInfo.routingKey, i * nonLeafNodeInfo.maxKeys + j, sizeof(int), &temp);
                out << temp;
                if (j != nonLeafNodeInfo.maxKeys - 1) out << ",";
            }
            out << "\"";
        }
        out << "],\n";
        out << "\"children\":[";
        for (size_t i = 0; i < nonLeafNodeInfo.currentKey + 1; ++i) {
            if (i != 0) out << ",";
            recursiveGenerateJsonString(fileHandle, nonLeafNodeInfo.pointers[i], out);
        }
        out << "]}\n";
    }
    else {
        PeterDB::LeafNode leafNodeInfo;
        deserializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
        out << "{\"keys\":[";
        for (size_t i = 0; i < leafNodeInfo.currentKey; ++i) {
            if (i != 0) out << ",";
            int temp;
            getEntry(leafNodeInfo.key, i, sizeof(int), &temp);
            out << "\"";
            out << temp << ":[(" << leafNodeInfo.rid[i].pageNum << "," << leafNodeInfo.rid[i].slotNum << ")]";
            out << "\"";
        }
        out << "]}";

        if(nodeHeader.rightSibling != -1) out << ",\n";

        return;
    }
}
#endif //PETERDB_IX_UTILS_H