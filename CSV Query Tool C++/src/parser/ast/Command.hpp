#pragma once
#include <string>
#include <vector>
#include <unordered_map>

enum class CommandType {
    LOAD_TABLE,
    SHOW_TABLES,
    DESCRIBE_TABLE,
    CREATE_INDEX,
    DROP_INDEX,
    SHOW_INDEXES,
    CREATE_TABLE,
    INSERT_INTO,
    DELETE_FROM,
    ALTER_ADD_COLUMN,
    ALTER_DROP_COLUMN,
    ALTER_RENAME_COLUMN,
    ALTER_RENAME_TABLE
};

struct Command {
    CommandType                              type;
    std::unordered_map<std::string,std::string> parameters;
    std::vector<std::string>                 columnNames;  // CREATE TABLE / INSERT column list
    std::vector<std::string>                 columnTypes;  // CREATE TABLE types
    std::vector<std::vector<std::string>>    valueRows;    // INSERT INTO value rows

    std::string getParam(const std::string& key) const {
        auto it = parameters.find(key);
        return it != parameters.end() ? it->second : "";
    }
};
