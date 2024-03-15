#include "src/include/qe.h"
#include "qe.utils.h"
#include <sstream>

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->input = input;
        this->condition = condition;
        this->input->getAttributes(this->inputAttrs);
    }

    Filter::~Filter() {
        this->input = nullptr;
    }

    RC Filter::getNextTuple(void *data) {
        int attrIdx = getAttrIndex(this->inputAttrs, this->condition.lhsAttr);
        int nullIndicatorSize = getNullIndicatorSizeQE(this->inputAttrs.size());

        while (this->input->getNextTuple(data) == 0) {
            if(!condition.bRhsIsAttr){
                if (isNull((char *)data, nullIndicatorSize, attrIdx)) {
                    continue;
                }
                int len = this->inputAttrs[attrIdx].type == TypeVarChar ? this->inputAttrs[attrIdx].length + sizeof(int) : 4;
                char lhsData[len]; // without null indicator
                readAttributeValue(data, nullIndicatorSize, this->inputAttrs, attrIdx, lhsData);
                if(matchCondition(lhsData, this->condition.rhsValue.data, this->condition.op, this->condition.rhsValue.type)){
                    return 0;
                }
            }
            else{
                if (compareStringVal(this->condition.lhsAttr, this->condition.rhsAttr, this->condition.op)) {
                    return 0;
                }
            }
        }

        return QE_EOF;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        // output the attributes of the input (because the filter does not change the attributes)
        attrs = this->inputAttrs;
        return 0;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        this->input = input;
        this->outputAttrNames = attrNames;
        input->getAttributes(this->inputAttrs);
    }

    Project::~Project() {
        this->input = nullptr;
    }

    RC Project::getNextTuple(void *data) {
        int nullIndicatorSize = getNullIndicatorSizeQE(this->inputAttrs.size());

        int projectNullIndicatorSize = getNullIndicatorSizeQE(this->outputAttrNames.size());
        char projectNullIndicator[projectNullIndicatorSize];
        memset(projectNullIndicator, 0, projectNullIndicatorSize);

        char buffer[PAGE_SIZE];
        while (this->input->getNextTuple(buffer) == 0) {
            int offset = projectNullIndicatorSize;
            for (int i = 0; i < this->outputAttrNames.size(); i++) {
                // get the index of the attribute in the buffer
                int oldAttrIdx = getAttrIndex(this->inputAttrs, this->outputAttrNames[i]);
                if (isNull(buffer, nullIndicatorSize,oldAttrIdx)) {
                    setNullBit(projectNullIndicator, i);
                }
                else {
                    int len = this->inputAttrs[oldAttrIdx].type == TypeVarChar ? this->inputAttrs[oldAttrIdx].length + sizeof(int) : 4;
                    char attrData[len]; // without null indicator
                    int valueLen = readAttributeValue(buffer, nullIndicatorSize, this->inputAttrs, oldAttrIdx, attrData);

                    memmove((char *)data + offset, attrData, valueLen);
                    offset += valueLen;
                }
            }
            memmove(data, projectNullIndicator, projectNullIndicatorSize);

            return 0;
        }

        return QE_EOF;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        std::vector<Attribute> inputAttrs;
        this->input->getAttributes(inputAttrs);

        for (const auto &attrName : this->outputAttrNames) {
            for (const auto &attr : inputAttrs) {
                if (attr.name == attrName) {
                    attrs.push_back(attr);
                }
            }
        }
        return 0;
    }

    // only for equal condition
    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->numPages = numPages;
        this->leftIn->getAttributes(this->leftAttrs);
        this->rightIn->getAttributes(this->rightAttrs);
    }

    BNLJoin::~BNLJoin() {
        this->currentMatchesWithLeftIn.clear();

        for (auto& val : this->hashTable) {
            for (auto& v : val.second) {
                if (v.data != nullptr) {
                    if (v.type == TypeVarChar) {
                        delete[] static_cast<char*>(v.data); // 释放字符串
                    } else {
                        delete static_cast<int*>(v.data); // 释放 int 或 float
                    }
                    v.data = nullptr; // 避免悬挂指针
                }
            }
            val.second.clear();
        }

        this->hashTable.clear();
    }

    void BNLJoin::loadHashTable() {
        hashTable.clear();
        int memoryUsed = 0, status = 0;
        char buffer[PAGE_SIZE];
        memset(buffer, 0, PAGE_SIZE);

        int joinAttrIdxL = getAttrIndex(this->leftAttrs, this->condition.lhsAttr);
        AttrType joinAttrType = this->leftAttrs[joinAttrIdxL].type;
        int len = this->leftAttrs[joinAttrIdxL].type == TypeVarChar ? this->leftAttrs[joinAttrIdxL].length + sizeof(int) : 4;

        // build the hash table
        while((status = this->leftIn->getNextTuple(buffer)) == 0 && memoryUsed < PAGE_SIZE * this->numPages){
            int dataSize = getDataSize(buffer, this->leftAttrs);
            char attrDataL[len]; // without null indicator
            readAttributeValue(buffer, getNullIndicatorSizeQE(this->leftAttrs.size()), this->leftAttrs, joinAttrIdxL, attrDataL);

            int key = *(int *)attrDataL;
            Value val;
            val.type = joinAttrType;
            val.data = new char[dataSize];
            memmove(val.data, buffer, dataSize);

            hashTable[key].emplace_back(val);

            memoryUsed += dataSize;
        }

        if (status == QE_EOF){
            leftInEnd = true;
        }
        needToLoadHashTable = false;
    }

    void BNLJoin::loadRightIn() {
        this->rightIn->setIterator();
    }
    // TODO: support float and varchar type
    RC BNLJoin::getNextTuple(void *data) {
        if(needToLoadHashTable){
            loadHashTable();
        }

        int joinAttrIdxR = getAttrIndex(this->rightAttrs, this->condition.rhsAttr);
        int len = this->leftAttrs[joinAttrIdxR].type == TypeVarChar ? this->leftAttrs[joinAttrIdxR].length + sizeof(int) : 4;
        int status = 0;

        int nullIndicatorSizeL = getNullIndicatorSizeQE(this->leftAttrs.size());
        int nullIndicatorSizeR = getNullIndicatorSizeQE(this->rightAttrs.size());
        char nullIndicatorL[nullIndicatorSizeL];
        char nullIndicatorR[nullIndicatorSizeR];

        char buffer[PAGE_SIZE];
        memset(buffer, 0, PAGE_SIZE);

        if (currentMatchesWithLeftIn.empty() || currentMatchesIndex >= currentMatchesWithLeftIn.size()){
            currentMatchesWithLeftIn.clear();
            currentMatchesIndex = 0;

            while((status = this->rightIn->getNextTuple(buffer)) == 0){
                char attrDataR[len];
                readAttributeValue(buffer, getNullIndicatorSizeQE(this->rightAttrs.size()), this->rightAttrs, joinAttrIdxR, attrDataR);
                int key = *(int *)attrDataR;

                if(hashTable.find(key) != hashTable.end()){
                    currentMatchesWithLeftIn = hashTable[key];
                    break;
                }
            }
            if(status == QE_EOF && !leftInEnd){
                loadRightIn();
                needToLoadHashTable = true;
            }
            else if(status == QE_EOF && leftInEnd){
                return QE_EOF;
            }
        }

        if (currentMatchesIndex < currentMatchesWithLeftIn.size()){
            int joinNullIndicatorSize = getNullIndicatorSizeQE(this->leftAttrs.size() + this->rightAttrs.size());
            char joinNullIndicator[joinNullIndicatorSize];
            memset(joinNullIndicator, 0, joinNullIndicatorSize);
            memmove(nullIndicatorL, currentMatchesWithLeftIn[currentMatchesIndex].data, nullIndicatorSizeL);
            memmove(nullIndicatorR, buffer, nullIndicatorSizeR);
            concatNullIndicator(joinNullIndicator, nullIndicatorL, nullIndicatorSizeL, nullIndicatorR, nullIndicatorSizeR, this->leftAttrs.size(), this->rightAttrs.size());

            int leftDataSize = getDataSize(currentMatchesWithLeftIn[currentMatchesIndex].data, this->leftAttrs);
            int rightDataSize = getDataSize(buffer, this->rightAttrs);

            int offset = 0;
            memmove((char *)data + offset, joinNullIndicator, joinNullIndicatorSize);
            offset += joinNullIndicatorSize;
            memmove((char *)data + offset, (char *)currentMatchesWithLeftIn[currentMatchesIndex].data + nullIndicatorSizeL, leftDataSize - nullIndicatorSizeL);
            offset += (leftDataSize - nullIndicatorSizeL);
            memmove((char *)data + offset, buffer + nullIndicatorSizeR, rightDataSize - nullIndicatorSizeR);
            offset += (rightDataSize - nullIndicatorSizeR);

            currentMatchesIndex++;
            return 0;
        }

        return QE_EOF;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        for (const auto &attr: this->leftAttrs) {
            attrs.push_back(attr);
        }
        for (const auto &attr: this->rightAttrs) {
            attrs.push_back(attr);
        }

        return 0;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
            this->leftIn = leftIn;
            this->rightIn = rightIn;
            this->condition = condition;
            this->leftIn->getAttributes(this->leftAttrs);
            this->rightIn->getAttributes(this->rightAttrs);
    }

    INLJoin::~INLJoin() {

    }
    // TODO: 感覺可以寫得更好？不用recursive 還有return條件 好像怪怪的
    RC INLJoin::getNextTuple(void *data) {
        std::string tableName = this->rightAttrs[0].name.substr(0, this->rightAttrs[0].name.find('.'));
        char bufferL[PAGE_SIZE];
        memset(bufferL, 0, PAGE_SIZE);
        char bufferR[PAGE_SIZE];
        memset(bufferR, 0, PAGE_SIZE);

        int nullIndicatorSizeL = getNullIndicatorSizeQE(this->leftAttrs.size());
        int nullIndicatorSizeR = getNullIndicatorSizeQE(this->rightAttrs.size());

        char nullIndicatorL[nullIndicatorSizeL];
        char nullIndicatorR[nullIndicatorSizeR];

        int joinAttrIdxL = getAttrIndex(this->leftAttrs, this->condition.lhsAttr);

        std::vector<char> attrDataL;
        if(IndexScanEnd){
            int status = 0;
            if ((status = this->leftIn->getNextTuple(bufferL)) == 0){
                attrDataL.resize(100);
                readAttributeValue(bufferL, nullIndicatorSizeL, this->leftAttrs, joinAttrIdxL, attrDataL.data());
                if(this->leftAttrs[joinAttrIdxL].type == TypeVarChar){
                    int len;
                    memmove(&len, attrDataL.data(), sizeof (int));
                    attrDataL.resize(len + sizeof(int));
                }
                else{
                    attrDataL.resize(4);
                }
            }
            this->rightIn->setIterator(attrDataL.data(), attrDataL.data(), true, true);
            IndexScanEnd = false;

            if(status == RM_EOF) LeftInEnd = true;
        }

        int status = 0;
        while((status = this->rightIn->getNextTuple(bufferR)) == 0){
            int joinNullIndicatorSize = getNullIndicatorSizeQE(this->leftAttrs.size() + this->rightAttrs.size());
            char joinNullIndicator[joinNullIndicatorSize];
            memset(joinNullIndicator, 0, joinNullIndicatorSize);
            memmove(nullIndicatorL, bufferL, nullIndicatorSizeL);
            memmove(nullIndicatorR, bufferR, nullIndicatorSizeR);
            concatNullIndicator(joinNullIndicator, nullIndicatorL, nullIndicatorSizeL, nullIndicatorR, nullIndicatorSizeR, this->leftAttrs.size(), this->rightAttrs.size());

            int leftDataSize = getDataSize(bufferL, this->leftAttrs);
            int rightDataSize = getDataSize(bufferR, this->rightAttrs);

            int offset = 0;
            memmove((char *)data + offset, joinNullIndicator, joinNullIndicatorSize);
            offset += joinNullIndicatorSize;
            memmove((char *)data + offset, bufferL + nullIndicatorSizeL, leftDataSize - nullIndicatorSizeL);
            offset += (leftDataSize - nullIndicatorSizeL);
            memmove((char *)data + offset, bufferR + nullIndicatorSizeR, rightDataSize - nullIndicatorSizeR);
            offset += (rightDataSize - nullIndicatorSizeR);

//            std::stringstream stream;
//            RelationManager::instance().printTuple(this->leftAttrs,  bufferL, stream);
//            std::cout << "Left: " << stream.str() << std::endl;
//            stream.str("");
//            RelationManager::instance().printTuple(this->rightAttrs,  bufferR, stream);
//            std::cout << "Right: " << stream.str() << std::endl;


            return 0;
        }

        if (status == RM_EOF){
            IndexScanEnd = true;
            getNextTuple(data);
        }

        return LeftInEnd ? QE_EOF : 0;

    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        for (const auto &attr : this->leftAttrs) {
            attrs.push_back(attr);
        }
        for (const auto &attr : this->rightAttrs) {
            attrs.push_back(attr);
        }

        return 0;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
        this->input = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->input->getAttributes(this->inputAttrs);
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        if (aggregateEnd) return QE_EOF;

        AttrType aggAttrType = this->aggAttr.type;
        int nullIndicatorSize = getNullIndicatorSizeQE(this->inputAttrs.size());
        char buffer[PAGE_SIZE];
        memset(buffer, 0, PAGE_SIZE);

        int attrIdx = getAttrIndex(this->inputAttrs, this->aggAttr.name);
        while (this->input->getNextTuple(buffer) == 0){
            char aggrData[4];
            if (isNull(buffer, nullIndicatorSize, attrIdx)) {
                continue;
            }
            readAttributeValue(buffer, nullIndicatorSize, this->inputAttrs, attrIdx, aggrData);

            if (aggAttrType == TypeInt) values.push_back(*(int *)aggrData);
            else if (aggAttrType == TypeReal) values.push_back(*(float *)aggrData);
        }

        float result = 0;
        switch(this->op){
            case PeterDB::MIN:
                std::sort(values.begin(), values.end());
                result = values[0];
                break;
            case PeterDB::MAX:
                std::sort(values.begin(), values.end(), std::greater<int>());
                result = values[0];
                break;
            case PeterDB::COUNT:
                result = values.size();
                break;
            case PeterDB::SUM:
                for(float val : values){
                    result += val;
                }
                break;
            case PeterDB::AVG:
                for(float val : values){
                    result += val;
                }
                result /= values.size();
                break;
        }
        char nullIndicator[1];
        memset(nullIndicator, 0, 1);

        memmove((char *)data, nullIndicator, 1);
        memmove((char *)data + 1, &result, sizeof(float));

        aggregateEnd = true;
        return 0;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        Attribute attr = this->aggAttr;

        switch (this->op) {
            case MIN:
                attr.name = "MIN(" + this->aggAttr.name + ")";
                break;
            case MAX:
                attr.name = "MAX(" + this->aggAttr.name + ")";
                break;
            case SUM:
                attr.name = "SUM(" + this->aggAttr.name + ")";
                break;
            case AVG:
                attr.name = "AVG(" + this->aggAttr.name + ")";
                break;
            case COUNT:
                attr.name = "COUNT(" + this->aggAttr.name + ")";
                break;
        }

        attr.type = TypeReal;
        attrs.push_back(attr);

        return 0;
    }
} // namespace PeterDB
