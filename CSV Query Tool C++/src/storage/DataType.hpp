#pragma once
#include <string>
#include <stdexcept>

enum class DataType {
    INTEGER,
    DOUBLE,
    BOOLEAN,
    STRING
};

inline DataType widenDataType(DataType a, DataType b) {
    if (a == b) return a;
    if (a == DataType::STRING || b == DataType::STRING) return DataType::STRING;
    if ((a == DataType::INTEGER && b == DataType::DOUBLE) ||
        (a == DataType::DOUBLE  && b == DataType::INTEGER)) return DataType::DOUBLE;
    return DataType::STRING;
}

inline std::string dataTypeToString(DataType dt) {
    switch (dt) {
        case DataType::INTEGER: return "INTEGER";
        case DataType::DOUBLE:  return "DOUBLE";
        case DataType::BOOLEAN: return "BOOLEAN";
        case DataType::STRING:  return "STRING";
    }
    return "STRING";
}

inline DataType parseDataTypeStr(const std::string& s) {
    if (s == "INTEGER" || s == "INT")                                   return DataType::INTEGER;
    if (s == "DOUBLE"  || s == "FLOAT" || s == "DECIMAL" || s == "NUMERIC") return DataType::DOUBLE;
    if (s == "BOOLEAN" || s == "BOOL")                                  return DataType::BOOLEAN;
    if (s == "STRING"  || s == "VARCHAR" || s == "TEXT")                return DataType::STRING;
    throw std::runtime_error("Unknown data type: " + s +
        ". Supported: INTEGER, DOUBLE, BOOLEAN, STRING");
}
