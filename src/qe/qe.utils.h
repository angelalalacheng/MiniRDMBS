//
// Created by 鄭筠蓉 on 2024/3/9.
//

#ifndef PETERDB_QE_UTILS_H
#define PETERDB_QE_UTILS_H

#include "src/include/qe.h"

int getNullIndicatorSizeQE(int fields){
    return ceil((double)fields / CHAR_BIT);
}

int isNull(const char* data, int indicatorSize, int idx){
    char indicator[indicatorSize];
    memmove(indicator, data, indicatorSize);

    return (indicator[idx / 8] & (1 << (7 - idx % 8))) ? 1 : 0;
}

void setNullBit(char* indicator, int idx){
    indicator[idx / 8] |= 1 << (7 - idx % 8);
}

void concatNullIndicator(char* joinNullIndicator, char* nullIndicator1, int nullIndicatorSize1, char* nullIndicator2, int nullIndicatorSize2, int usedBits1, int usedBits2){
    for(int i = 0; i < usedBits1; i++){
        if(isNull(nullIndicator1, nullIndicatorSize1, i)){
            setNullBit(joinNullIndicator, i);
        }
    }
    for(int i = 0; i < usedBits2; i++){
        if(isNull(nullIndicator2, nullIndicatorSize2, i)){
            setNullBit(joinNullIndicator, usedBits1 + i);
        }
    }
}

int getAttrIndex(const std::vector<PeterDB::Attribute> &attrs, const std::string &attributeName){
    for(int i = 0; i < attrs.size(); i++){
        if(attrs[i].name == attributeName){
            return i;
        }
    }
    return -1;
}

bool compareIntVal(int intVal1, int intVal2, const PeterDB::CompOp &compOp) {
    switch (compOp) {
        case PeterDB::EQ_OP:
            return intVal1 == intVal2;
        case PeterDB::LT_OP:
            return intVal1 < intVal2;
        case PeterDB::LE_OP:
            return intVal1 <= intVal2;
        case PeterDB::GT_OP:
            return intVal1 > intVal2;
        case PeterDB::GE_OP:
            return intVal1 >= intVal2;
        case PeterDB::NE_OP:
            return intVal1 != intVal2;
        default:
            return false;
    }
}

bool compareFloatVal(float floatVal1, float floatVal2, const PeterDB::CompOp &compOp) {
    switch (compOp) {
        case PeterDB::EQ_OP:
            return floatVal1 == floatVal2;
        case PeterDB::LT_OP:
            return floatVal1 < floatVal2;
        case PeterDB::LE_OP:
            return floatVal1 <= floatVal2;
        case PeterDB::GT_OP:
            return floatVal1 > floatVal2;
        case PeterDB::GE_OP:
            return floatVal1 >= floatVal2;
        case PeterDB::NE_OP:
            return floatVal1 != floatVal2;
        default:
            return false;
    }
}

bool compareStringVal(const std::string& value1, const std::string& value2, PeterDB::CompOp operation) {
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
        default:
            return false;
    }
}

bool matchCondition (const char* left, const void *right, const PeterDB::CompOp &compOp, const PeterDB::AttrType type) {
    if (compOp == PeterDB::NO_OP) return true;

    switch(type) {
        case PeterDB::TypeInt:
            int intVal1, intVal2;
            intVal1 = *reinterpret_cast<const int*>(left);
            intVal2 = *reinterpret_cast<const int*>((const char*) right);
            return compareIntVal(intVal1, intVal2, compOp);
        case PeterDB::TypeReal:
            float floatVal1, floatVal2;
            floatVal1 = *reinterpret_cast<const float*>(left);
            floatVal2 = *reinterpret_cast<const float*>((const char*) right);
            return compareFloatVal(floatVal1, floatVal2, compOp);
        case PeterDB::TypeVarChar:
            int length1, length2;
            memmove(&length1, left, sizeof(int));
            memmove(&length2, (char *)right, sizeof(int));
            std::string str1(static_cast<const char*>(left) + sizeof(int), length1);
            std::string str2(static_cast<const char*>(right) + sizeof(int), length2);
            return compareStringVal(str1, str2, compOp);
    }
}

int readAttributeValue(const void* data, int NullIndicateSize, const std::vector<PeterDB::Attribute>& inputAttrs, int attrIdx, char *leftValue){
    int offset = NullIndicateSize;
    for(int i = 0; i < attrIdx; i++){
        if(isNull((char *)data, NullIndicateSize, i)){
            continue;
        }
        switch(inputAttrs[i].type){
            case PeterDB::TypeInt:
                offset += sizeof(int);
                break;
            case PeterDB::TypeReal:
                offset += sizeof(float);
                break;
            case PeterDB::TypeVarChar:
                int length;
                memmove(&length, (char *)data + offset, sizeof(int));
                offset += (sizeof(int) + length);
                break;
        }
    }

    switch(inputAttrs[attrIdx].type){
        case PeterDB::TypeInt:
            memmove(leftValue, (char *)data + offset, sizeof(int));
            return sizeof(int);
        case PeterDB::TypeReal:
            memmove(leftValue, (char *)data + offset, sizeof(float));
            return sizeof(float);
        case PeterDB::TypeVarChar:
            int length;
            memmove(&length, (char *)data + offset, sizeof(int));
            memmove(leftValue, (char *)data + offset, sizeof(int) + length);
            return sizeof(int) + length;
    }
}

int getDataSize(const void* data, const std::vector<PeterDB::Attribute>& attrs){
    int nullIndicatorSize = getNullIndicatorSizeQE(attrs.size());
    int offset = nullIndicatorSize;
    for(int i = 0; i < attrs.size(); i++){
        if(isNull((char *)data, nullIndicatorSize, i)){
            continue;
        }
        switch(attrs[i].type){
            case PeterDB::TypeInt:
                offset += sizeof(int);
                break;
            case PeterDB::TypeReal:
                offset += sizeof(float);
                break;
            case PeterDB::TypeVarChar:
                int length;
                memmove(&length, (char *)data + offset, sizeof(int));
                offset += (sizeof(int) + length);
                break;
        }
    }
    return offset;
}

#endif //PETERDB_QE_UTILS_H
