#include "Schema.hpp"
#include <stdexcept>

Schema::Schema(const std::string& tableName, const std::vector<Column>& columns)
    : tableName(tableName), columns(columns) {
    buildIndexes();
}

void Schema::buildIndexes() {
    nameIndex_.clear();
    qualifiedIndex_.clear();

    for (int i = 0; i < (int)columns.size(); i++) {
        const Column& col = columns[i];
        std::string key = toLower(col.name);

        // Track ambiguity: if name already seen, mark −1
        if (nameIndex_.count(key))
            nameIndex_[key] = -1;
        else
            nameIndex_[key] = i;

        // Qualified key: "table.column"
        std::string tname = col.tableName.empty() ? tableName : col.tableName;
        if (!tname.empty())
            qualifiedIndex_[toLower(tname + "." + col.name)] = i;
    }
}

int Schema::resolveColumn(const std::string& table, const std::string& column) const {
    if (!table.empty()) {
        std::string qKey = toLower(table + "." + column);
        auto it = qualifiedIndex_.find(qKey);
        if (it != qualifiedIndex_.end()) return it->second;
        throw std::runtime_error("Unknown column: " + table + "." + column);
    }

    std::string key = toLower(column);
    auto it = nameIndex_.find(key);
    if (it == nameIndex_.end())
        throw std::runtime_error("Unknown column: " + column);
    if (it->second == -1)
        throw std::runtime_error("Ambiguous column reference: " + column +
            " — qualify with table name (e.g., table." + column + ")");
    return it->second;
}

int Schema::findColumn(const std::string& name) const {
    auto it = nameIndex_.find(toLower(name));
    if (it != nameIndex_.end() && it->second >= 0) return it->second;
    return -1;
}

Schema Schema::withTableName(const std::string& tname) const {
    std::vector<Column> tagged;
    tagged.reserve(columns.size());
    for (const auto& col : columns)
        tagged.push_back(col.withTableName(tname));
    return Schema(tname, tagged);
}

Schema Schema::merge(const Schema& left, const Schema& right) {
    std::vector<Column> merged = left.columns;
    merged.insert(merged.end(), right.columns.begin(), right.columns.end());
    return Schema("", merged);
}
