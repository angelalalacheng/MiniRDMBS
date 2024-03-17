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
        this->loadHashTable();
    }

    BNLJoin::~BNLJoin() {
        this->currentMatchesWithLeftIn.clear();

        if (joinAttrType == TypeInt){
            for (auto& val : this->intHashTable) {
                for (auto& v : val.second) {
                    if (v.data != nullptr) {
                        delete static_cast<int*>(v.data);
                        v.data = nullptr;
                    }
                }
                val.second.clear();
            }
            this->intHashTable.clear();
        }

        if (joinAttrType == TypeReal){
            for (auto& val : this->floatHashTable) {
                for (auto& v : val.second) {
                    if (v.data != nullptr) {
                        delete static_cast<float*>(v.data);
                        v.data = nullptr;
                    }
                }
                val.second.clear();
            }
            this->floatHashTable.clear();
        }

    }

    RC BNLJoin::loadHashTable() {
        intHashTable.clear();
        floatHashTable.clear();

        int memoryUsed = 0, status = 0;
        char buffer[PAGE_SIZE];
        memset(buffer, 0, PAGE_SIZE);

        int joinAttrIdxL = getAttrIndex(this->leftAttrs, this->condition.lhsAttr);
        joinAttrType = this->leftAttrs[joinAttrIdxL].type;
        int len = this->leftAttrs[joinAttrIdxL].type == TypeVarChar ? this->leftAttrs[joinAttrIdxL].length + sizeof(int) : 4;

        // build the hash table
        while(memoryUsed < PAGE_SIZE * this->numPages){
            status = this->leftIn->getNextTuple(buffer);
            if (status != 0) break;

            int dataSize = getDataSize(buffer, this->leftAttrs);
            char attrDataL[len]; // without null indicator
            readAttributeValue(buffer, getNullIndicatorSizeQE(this->leftAttrs.size()), this->leftAttrs, joinAttrIdxL, attrDataL);

            Value val;
            val.type = joinAttrType;
            val.data = new char[dataSize];
            memmove(val.data, buffer, dataSize);

            if (joinAttrType == TypeInt){
                int key = *(int *)attrDataL;
                intHashTable[key].emplace_back(val);
            }
            else if (joinAttrType == TypeReal){
                float key = *(float *)attrDataL;
                floatHashTable[key].emplace_back(val);
            }

            memoryUsed += dataSize;
        }

        if (status == QE_EOF){
            leftInEnd = true;
        }

        return 0;
   }

    void BNLJoin::loadRightIn() {
        this->rightIn->setIterator();
    }
    // TODO: support varchar type
    RC BNLJoin::getNextTuple(void *data) {
        int rightInStatus = 0;

        while(true){
            if (!currentMatchesWithLeftIn.empty() && currentMatchesIndex < currentMatchesWithLeftIn.size()){
                int joinNullIndicatorSize = getNullIndicatorSizeQE(this->leftAttrs.size() + this->rightAttrs.size());
                char joinNullIndicator[joinNullIndicatorSize];
                memset(joinNullIndicator, 0, joinNullIndicatorSize);
                int nullIndicatorSizeL = getNullIndicatorSizeQE(this->leftAttrs.size());
                int nullIndicatorSizeR = getNullIndicatorSizeQE(this->rightAttrs.size());
                char nullIndicatorL[nullIndicatorSizeL];
                char nullIndicatorR[nullIndicatorSizeR];
                memmove(nullIndicatorL, currentMatchesWithLeftIn[currentMatchesIndex].data, nullIndicatorSizeL);
                memmove(nullIndicatorR, bufferRightIn, nullIndicatorSizeR);
                concatNullIndicator(joinNullIndicator, nullIndicatorL, nullIndicatorSizeL, nullIndicatorR, nullIndicatorSizeR, this->leftAttrs.size(), this->rightAttrs.size());

                int leftDataSize = getDataSize(currentMatchesWithLeftIn[currentMatchesIndex].data, this->leftAttrs);
                int rightDataSize = getDataSize(bufferRightIn, this->rightAttrs);

                int offset = 0;
                memmove((char *)data + offset, joinNullIndicator, joinNullIndicatorSize);
                offset += joinNullIndicatorSize;
                memmove((char *)data + offset, (char *)currentMatchesWithLeftIn[currentMatchesIndex].data + nullIndicatorSizeL, leftDataSize - nullIndicatorSizeL);
                offset += (leftDataSize - nullIndicatorSizeL);
                memmove((char *)data + offset, bufferRightIn + nullIndicatorSizeR, rightDataSize - nullIndicatorSizeR);
                offset += (rightDataSize - nullIndicatorSizeR);

                currentMatchesIndex++;
                return 0;
            }
            else{
                rightInStatus = this->rightIn->getNextTuple(bufferRightIn);
                int joinAttrIdxR = getAttrIndex(this->rightAttrs, this->condition.rhsAttr);

                currentMatchesIndex = 0;
                currentMatchesWithLeftIn.clear();

                if (rightInStatus == 0){
                    char attrDataR[4];
                    readAttributeValue(bufferRightIn, getNullIndicatorSizeQE(this->rightAttrs.size()), this->rightAttrs, joinAttrIdxR, attrDataR);

                    if (joinAttrType == TypeInt){
                        int key;
                        memmove(&key, attrDataR, sizeof(int));

                        if(intHashTable.find(key) != intHashTable.end()){
                            currentMatchesWithLeftIn = intHashTable[key];
                        }
                    }
                    else if (joinAttrType == TypeReal){
                        float key;
                        memmove(&key, attrDataR, sizeof(float));

                        if(floatHashTable.find(key) != floatHashTable.end()){
                            currentMatchesWithLeftIn = floatHashTable[key];
                        }
                    }
                }
                else{
                    if (!leftInEnd){
                        loadRightIn();
                        rightInStatus = 0;
                        loadHashTable();
                    }
                    else{
                        break;
                    }
                }
            }
        }

        return (leftInEnd && rightInStatus == RM_EOF) ? QE_EOF : 0;
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
