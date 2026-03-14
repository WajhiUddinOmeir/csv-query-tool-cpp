#pragma once
#include "Column.hpp"
#include <vector>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <stdexcept>

/**
 * Describes a table's structure: column names, types, and owning table.
 * Supports qualified column resolution for JOIN operations.
 * Schemas are value-types (copyable).
 */
class Schema {
public:
    std::string          tableName;
    std::vector<Column>  columns;

    Schema() = default;
    Schema(const std::string& tableName, const std::vector<Column>& columns);

    /** Resolves column ref (optional table qualifier) → index. Throws on unknown/ambiguous. */
    int resolveColumn(const std::string& table, const std::string& column) const;

    /** Case-insensitive search; returns -1 if not found. Does not throw on ambiguity. */
    int findColumn(const std::string& name) const;

    /** Returns new Schema with all columns tagged with the given table name. */
    Schema withTableName(const std::string& tname) const;

    /** Merges two schemas by concatenating their column lists (for JOINs). */
    static Schema merge(const Schema& left, const Schema& right);

    int           getColumnCount() const { return (int)columns.size(); }
    const Column& getColumn(int i) const { return columns[i]; }
    const std::vector<Column>& getColumns() const { return columns; }

private:
    // lowercase name → index (−1 if ambiguous)
    std::unordered_map<std::string, int> nameIndex_;
    // lowercase "table.name" → index
    std::unordered_map<std::string, int> qualifiedIndex_;

    void buildIndexes();

    static std::string toLower(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), ::tolower);
        return s;
    }
};
