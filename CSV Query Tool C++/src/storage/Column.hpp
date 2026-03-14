#pragma once
#include "DataType.hpp"
#include <string>

struct Column {
    std::string name;
    DataType    type;
    std::string tableName; // empty if unset (set after JOIN for qualified resolution)

    Column(std::string name, DataType type, std::string tableName = "")
        : name(std::move(name)), type(type), tableName(std::move(tableName)) {}

    std::string getQualifiedName() const {
        return tableName.empty() ? name : tableName + "." + name;
    }

    Column withTableName(const std::string& tbl) const {
        return Column(name, type, tbl);
    }
};
