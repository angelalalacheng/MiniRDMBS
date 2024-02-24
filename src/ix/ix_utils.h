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

size_t getNumOfEntryInPage(const PeterDB::Attribute &attribute){
    size_t size = attribute.length + sizeof(PeterDB::RID) * 2;
    size_t numOfNode = (PAGE_SIZE - sizeof(PeterDB::NodeHeader))/ size;

    return numOfNode;
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

    if (startPos < arr.size() && (startPos + typeLen) <= arr.size()) {
        std::memset(arr.data() + startPos, 0, typeLen);
    }
}

void insertIntEntry(PeterDB::LeafNode& leafNodeInfo, size_t typeLen, const void* entryData, const PeterDB::RID &entryRID) {
    std::vector<char> arr = leafNodeInfo.key;
    std::vector<PeterDB::RID> rids = leafNodeInfo.rid;
    short entryCount = leafNodeInfo.currentKey;

    size_t left = 0;
    size_t right = entryCount;
    size_t mid = 0;
    int target = *reinterpret_cast<const int*>(entryData);

    while (left < right) {
        mid = (left + right) / 2;
        char midData[typeLen];
        getEntry(leafNodeInfo.key, mid, typeLen, midData);
        int midValue = *reinterpret_cast<int*>(midData);
        if (midValue == target) {
            return;
        }
        else if (target < midValue) {
            right = mid;
        }
        else {
            left = mid + 1;
        }
    }
    // 从插入点开始向后移动元素，为新元素腾出空间
    memmove(arr.data() + ((left + 1) * typeLen), arr.data() + (left * typeLen), (entryCount - left) * typeLen);
    move_backward(rids.begin() + left, rids.begin() + entryCount, rids.begin() + entryCount + 1);

    // 将新元素复制到腾出的空间
    memmove(arr.data() + (left * typeLen), entryData, typeLen);
    rids[left] = entryRID;

    leafNodeInfo.key = arr;
    leafNodeInfo.rid = rids;
    leafNodeInfo.currentKey = entryCount + 1;
}

void insertFloatEntry(PeterDB::LeafNode& leafNodeInfo, size_t typeLen, const void* entryData, const PeterDB::RID &entryRID){
    std::vector<char> arr = leafNodeInfo.key;
    std::vector<PeterDB::RID> rids = leafNodeInfo.rid;
    short entryCount = leafNodeInfo.currentKey;

    size_t left = 0;
    size_t right = entryCount;
    size_t mid = 0;
    float target = *reinterpret_cast<const float *>(entryData);

    while (left < right) {
        mid = (left + right) / 2;
        char midData[typeLen];
        getEntry(arr, mid, typeLen, midData);
        float midValue = *reinterpret_cast<float *>(midData);
        if (midValue == target) {
            return;
        }
        else if (target < midValue) {
            right = mid;
        }
        else {
            left = mid + 1;
        }
    }
    // 从插入点开始向后移动元素，为新元素腾出空间
    memmove(arr.data() + ((left + 1) * typeLen), arr.data() + (left * typeLen), (entryCount - left) * typeLen);
    std::move_backward(rids.begin() + left, rids.begin() + entryCount, rids.begin() + entryCount + 1);

    // 将新元素复制到腾出的空间
    memmove(arr.data() + (left * typeLen), entryData, typeLen);
    rids[left] = entryRID;

    leafNodeInfo.key = arr;
    leafNodeInfo.rid = rids;
    leafNodeInfo.currentKey = entryCount + 1;
}

void insertVarCharEntry(PeterDB::LeafNode& leafNodeInfo, size_t typeLen, const void* entryData, const PeterDB::RID &entryRID){
    std::vector<char> arr = leafNodeInfo.key;
    std::vector<PeterDB::RID> rids = leafNodeInfo.rid;
    short entryCount = leafNodeInfo.currentKey;

    size_t left = 0;
    size_t right = entryCount;
    size_t mid = 0;

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
            return;
        }
        else if (target < midValue) {
            right = mid;
        }
        else {
            left = mid + 1;
        }
    }
    // 从插入点开始向后移动元素，为新元素腾出空间
    memmove(arr.data() + ((left + 1) * typeLen), arr.data() + (left * typeLen), (entryCount - left) * typeLen);
    std::move_backward(rids.begin() + left, rids.begin() + entryCount, rids.begin() + entryCount + 1);

    // 将新元素复制到腾出的空间
    memmove(arr.data() + (left * typeLen), entryData, typeLen);
    rids[left] = entryRID;

    leafNodeInfo.key = arr;
    leafNodeInfo.rid = rids;
    leafNodeInfo.currentKey = entryCount + 1;
}

void insertEntry(PeterDB::LeafNode& leafNodeInfo, const PeterDB::Attribute &attribute, const void* entryData, const PeterDB::RID &entryRID){
    size_t typeLen = attribute.length;

    if(attribute.type == PeterDB::TypeInt){
        insertIntEntry(leafNodeInfo, typeLen, entryData, entryRID);
    }
    else if(attribute.type == PeterDB::TypeReal){
        insertFloatEntry(leafNodeInfo, typeLen, entryData, entryRID);
    }
    else if(attribute.type == PeterDB::TypeVarChar){
        insertVarCharEntry(leafNodeInfo, typeLen, entryData, entryRID);
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

void initialNonLeafNodePage(PeterDB::FileHandle &fileHandle, const PeterDB::Attribute &attribute, short numOfEntry){
    char node[PAGE_SIZE];
    memset(node, 0, PAGE_SIZE);
    PeterDB::NodeHeader nodeHeader;
    PeterDB::NonLeafNode nonLeafNode;

    nodeHeader.isLeaf = 0;
    nodeHeader.parent = -1;
    nodeHeader.leftSibling = -1;
    nodeHeader.rightSibling = -1;

    nonLeafNode.currentKey = 0;
    nonLeafNode.maxKeys = numOfEntry;
    nonLeafNode.routingKey.resize(numOfEntry * attribute.length, 0);
    nonLeafNode.pointers.resize(numOfEntry, 0);

    serializeNonLeafNode(nonLeafNode, node + sizeof(nodeHeader));

    fileHandle.openFileStream->seekp(0, std::ios::end);
    fileHandle.appendPage(node);
}

void initialLeafNodePage(PeterDB::FileHandle &fileHandle, const PeterDB::Attribute &attribute, short numOfEntry){
    char node[PAGE_SIZE];
    memset(node, 0, PAGE_SIZE);
    PeterDB::NodeHeader nodeHeader;
    PeterDB::LeafNode leafNode;

    PeterDB::AttrLength typeLen = attribute.length;
    if(attribute.type == PeterDB::TypeVarChar) typeLen += sizeof(int);

    nodeHeader.isLeaf = 1;
    nodeHeader.parent = -1;
    nodeHeader.leftSibling = -1;
    nodeHeader.rightSibling = -1;

    leafNode.currentKey = 0;
    leafNode.maxKeys = numOfEntry;
    leafNode.key.resize(numOfEntry * typeLen, 0);
    leafNode.rid.resize(numOfEntry);

    serializeLeafNode(leafNode, node + sizeof(nodeHeader));

    fileHandle.openFileStream->seekp(0, std::ios::end);
    fileHandle.appendPage(node);
}

void dummyNode(PeterDB::FileHandle &fileHandle){ // dummy node (pageNum = 0) point to the current root
    char dummy[PAGE_SIZE];
    memset(dummy, 0, PAGE_SIZE);

    PeterDB::PageNum rootPage = 1;
    memmove(dummy, &rootPage, sizeof(rootPage));

    fileHandle.openFileStream->seekp(0, std::ios::end);
    fileHandle.appendPage(dummy);
}

PeterDB::NodeHeader getNodeHeader(PeterDB::FileHandle &fileHandle, PeterDB::PageNum pageNumber){
    char data[PAGE_SIZE];
    memset(data, 0, PAGE_SIZE);
    fileHandle.readPage(pageNumber, data);

    PeterDB::NodeHeader nodeHeader;
    memmove(&nodeHeader, data, sizeof(nodeHeader));

    return nodeHeader;
}

void setNodeHeader(char* buffer, PeterDB::NodeHeader &nodeHeader){
    memmove(buffer, &nodeHeader, sizeof(nodeHeader));
}

PeterDB::PageNum getRootPage(PeterDB::FileHandle &fileHandle){
    char dummy[PAGE_SIZE];
    memset(dummy, 0, PAGE_SIZE);
    fileHandle.readPage(0, dummy);

    PeterDB::PageNum rootPage;
    memmove(&rootPage, dummy, sizeof(rootPage));

    return rootPage;
}

void setRootPage(PeterDB::FileHandle &fileHandle, PeterDB::PageNum rootPage){
    char dummy[PAGE_SIZE];
    memset(dummy, 0, PAGE_SIZE);
    fileHandle.readPage(0, dummy);

    memmove(dummy, &rootPage, sizeof(rootPage));
    fileHandle.writePage(0, dummy);
}

void recursiveInsertBTree(PeterDB::FileHandle &fileHandle, PeterDB::PageNum pageNumber, const PeterDB::Attribute &attribute, const void *key, const PeterDB::RID &rid, void* &newChildEntry){
    PeterDB::NodeHeader nodeHeader = getNodeHeader(fileHandle, pageNumber);
    PeterDB::AttrLength typeLen = attribute.length;
    if(attribute.type == PeterDB::TypeVarChar) typeLen += sizeof(int);

    char buffer[PAGE_SIZE];
    memset(buffer,0, PAGE_SIZE);
    fileHandle.readPage(pageNumber, buffer);

    if(!nodeHeader.isLeaf){
        PeterDB::NonLeafNode nonLeafNodeInfo;
        deserializeNonLeafNode(nonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
        PeterDB::PageNum nextNode = -1;
        // find the routing key
        for (size_t i = 0; i < nonLeafNodeInfo.currentKey; ++i) {
            int temp;
            getEntry(nonLeafNodeInfo.routingKey, i, typeLen, &temp);
            if(*reinterpret_cast<const int*>(key) < temp){
                nextNode = nonLeafNodeInfo.pointers[i];
            }
        }
        if(nextNode == -1){
            nextNode = nonLeafNodeInfo.pointers[nonLeafNodeInfo.currentKey];
        }
        // go to the next node
        recursiveInsertBTree(fileHandle, nextNode, attribute, key, rid, newChildEntry);
        if(newChildEntry == nullptr) return;

        // if non-leaf node has enough space
        if(nonLeafNodeInfo.currentKey < nonLeafNodeInfo.maxKeys){
            setEntry(nonLeafNodeInfo.routingKey, nonLeafNodeInfo.currentKey, typeLen, newChildEntry);
            nonLeafNodeInfo.currentKey += 1;
            //TODO: need to sort the routing key

            serializeNonLeafNode(nonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            newChildEntry = nullptr;
            fileHandle.writePage(pageNumber, buffer);
            return;
        }
        else{  // non-leaf node doesn't have enough space
            // create a new non-leaf node
            PeterDB::PageNum newNonLeafPage = fileHandle.getNumberOfPages();
            initialNonLeafNodePage(fileHandle, attribute, nonLeafNodeInfo.maxKeys);

            // half of the entries will be moved to the new non-leaf node
            PeterDB::NonLeafNode newNonLeafNodeInfo;
            newNonLeafNodeInfo.currentKey = nonLeafNodeInfo.currentKey / 2 + 1;
            newNonLeafNodeInfo.maxKeys = nonLeafNodeInfo.maxKeys;
            newNonLeafNodeInfo.routingKey.resize(newNonLeafNodeInfo.maxKeys * typeLen, 0);
            newNonLeafNodeInfo.pointers.resize(newNonLeafNodeInfo.maxKeys + 1, 0);

            for (size_t i = 0; i < newNonLeafNodeInfo.currentKey; ++i) {
                int temp;
                getEntry(nonLeafNodeInfo.routingKey, i + nonLeafNodeInfo.currentKey / 2, typeLen, &temp);
                setEntry(newNonLeafNodeInfo.routingKey, i, typeLen, &temp);
                clearEntry(nonLeafNodeInfo.routingKey, i + nonLeafNodeInfo.currentKey / 2, typeLen);
            }
            for (size_t i = 0; i < newNonLeafNodeInfo.currentKey + 1; ++i) {
                newNonLeafNodeInfo.pointers[i] = nonLeafNodeInfo.pointers[i + nonLeafNodeInfo.currentKey / 2];
                nonLeafNodeInfo.pointers[i + nonLeafNodeInfo.currentKey / 2] = 0;
            }

            // update the original node
            nonLeafNodeInfo.currentKey = nonLeafNodeInfo.currentKey / 2;
            serializeNonLeafNode(nonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));

            // update the new non-leaf node
            serializeNonLeafNode(newNonLeafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            getEntry(newNonLeafNodeInfo.routingKey, 0, typeLen, &newChildEntry);

            /// if it is root(待確認是不是對的)
            if(getRootPage(fileHandle) == pageNumber){
                short newRootPage = fileHandle.getNumberOfPages();
                initialNonLeafNodePage(fileHandle, attribute, nonLeafNodeInfo.maxKeys);
                PeterDB::NodeHeader newRootNodeHeader = getNodeHeader(fileHandle, newRootPage);
                newRootNodeHeader.isLeaf = 0;
                setNodeHeader(buffer, newRootNodeHeader);
                setRootPage(fileHandle, newRootPage);
            }

            fileHandle.writePage(pageNumber, buffer);
            return;
        }
    }

    if(nodeHeader.isLeaf){
        PeterDB::LeafNode leafNodeInfo;
        deserializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));

        // if leaf node has enough space
        if(leafNodeInfo.currentKey < leafNodeInfo.maxKeys){
            insertEntry(leafNodeInfo, attribute, key, rid);
            serializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            newChildEntry = nullptr;
            fileHandle.writePage(pageNumber, buffer);
            return;
        }
        else { // leaf node doesn't have enough space
            // temp key and rid (size: max + 1)
            PeterDB::LeafNode tempNode;
            tempNode.currentKey = leafNodeInfo.currentKey;
            tempNode.key.resize((leafNodeInfo.maxKeys + 1) * typeLen, 0);
            tempNode.rid.resize(leafNodeInfo.maxKeys + 1);
            memmove(tempNode.key.data(), leafNodeInfo.key.data(), leafNodeInfo.currentKey * typeLen);
            copy(leafNodeInfo.rid.begin(), leafNodeInfo.rid.begin() + leafNodeInfo.currentKey,  tempNode.rid.begin());

            // insert new key
            insertEntry(tempNode, attribute, key, rid);

            // split
            // create a new leaf node
            PeterDB::PageNum newLeafPage = fileHandle.getNumberOfPages();
            initialLeafNodePage(fileHandle, attribute, leafNodeInfo.maxKeys);
            char newNodeBuffer[PAGE_SIZE];
            PeterDB::NodeHeader newLeafNodeHeader = getNodeHeader(fileHandle, newLeafPage);
            fileHandle.readPage(newLeafPage, newNodeBuffer);

            // half of the entries will be moved to the new leaf node
            PeterDB::LeafNode newLeafNodeInfo;
            newLeafNodeInfo.currentKey = leafNodeInfo.currentKey / 2 + 1;
            newLeafNodeInfo.maxKeys = leafNodeInfo.maxKeys;
            newLeafNodeInfo.key.resize(newLeafNodeInfo.maxKeys * typeLen, 0);
            newLeafNodeInfo.rid.resize(newLeafNodeInfo.maxKeys);

            // 更新原叶节点和新叶节点的键和RID
            short splitPoint = leafNodeInfo.maxKeys / 2; // 分裂点

            // leafNodeInfo(old)得到tempNode前半
            leafNodeInfo.currentKey = splitPoint;
            memmove(leafNodeInfo.key.data(), tempNode.key.data(), splitPoint * typeLen);
            memset(leafNodeInfo.key.data() + splitPoint * typeLen, 0, (leafNodeInfo.maxKeys - splitPoint) * typeLen);
            std::copy(tempNode.rid.begin(), tempNode.rid.begin() + splitPoint, leafNodeInfo.rid.begin());
            std::fill(leafNodeInfo.rid.begin() + splitPoint, leafNodeInfo.rid.end(), PeterDB::RID());

            // newLeafNode得到tempNode後半
            newLeafNodeInfo.currentKey = leafNodeInfo.maxKeys + 1 - splitPoint;
            memmove(newLeafNodeInfo.key.data(), tempNode.key.data() + splitPoint * typeLen, newLeafNodeInfo.currentKey * typeLen);
            std::copy(tempNode.rid.begin() + splitPoint, tempNode.rid.end(), newLeafNodeInfo.rid.begin());

            // set sibling info
            newLeafNodeHeader.leftSibling = pageNumber;
            nodeHeader.rightSibling = newLeafPage;

            setNodeHeader(buffer, nodeHeader);
            setNodeHeader(newNodeBuffer, newLeafNodeHeader);

            serializeLeafNode(leafNodeInfo, buffer + sizeof(PeterDB::NodeHeader));
            serializeLeafNode(newLeafNodeInfo, newNodeBuffer + sizeof(PeterDB::NodeHeader));

            // set newchildentry to the parent node
            getEntry(newLeafNodeInfo.key, 0, typeLen, &newChildEntry);

            fileHandle.writePage(pageNumber, buffer);
            fileHandle.writePage(newLeafPage, newNodeBuffer);

            return;
        }
    }

}
#endif //PETERDB_IX_UTILS_H