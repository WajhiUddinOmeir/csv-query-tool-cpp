#pragma once
#include "HashIndex.hpp"
#include "storage/CsvTable.hpp"
#include "storage/Schema.hpp"
#include <unordered_map>
#include <vector>
#include <optional>
#include <stdexcept>
#include <algorithm>

/**
 * Manages all hash indexes for the query engine.
 *
 * Indexes are keyed by name (for DROP INDEX) and by "tableName.columnName"
 * (for fast lookup during query planning).
 */
class IndexManager {
public:
    IndexManager() = default;

    /**
     * Create an index on tableName.columnName, reading rows from table.
     * Throws if the index name already exists, or the column doesn't exist.
     */
    void createIndex(const std::string& indexName,
                     const std::string& tableName,
                     const std::string& columnName,
                     const CsvTable& table)
    {
        std::string lowerName = toLower(indexName);
        if (indexes_.count(lowerName))
            throw std::runtime_error("Index already exists: " + indexName);

        // Resolve column index
        int colIdx = table.getSchema().findColumn(columnName);
        if (colIdx < 0)
            throw std::runtime_error("Column not found: " + columnName
                                     + " in table " + tableName);

        // Build and store the index
        indexes_.emplace(lowerName,
                         HashIndex(lowerName, toLower(tableName), toLower(columnName),
                                   table.getRows(), colIdx));
        indexOrder_.push_back(lowerName);

        std::string qualKey = toLower(tableName) + "." + toLower(columnName);
        // Last-created index wins for a given (table, column) pair
        columnIndexes_[qualKey] = &indexes_.at(lowerName);
    }

    /** Drop an index by name. Throws if not found. */
    void dropIndex(const std::string& indexName) {
        std::string lowerName = toLower(indexName);
        auto it = indexes_.find(lowerName);
        if (it == indexes_.end())
            throw std::runtime_error("Index not found: " + indexName);

        std::string qualKey = it->second.getTableName() + "." + it->second.getColumnName();
        columnIndexes_.erase(qualKey);

        indexes_.erase(it);
        indexOrder_.erase(std::remove(indexOrder_.begin(), indexOrder_.end(), lowerName),
                          indexOrder_.end());
    }

    /** Check whether an index exists on (tableName, columnName). */
    bool hasIndex(const std::string& tableName, const std::string& columnName) const {
        std::string key = toLower(tableName) + "." + toLower(columnName);
        return columnIndexes_.count(key) > 0;
    }

    /**
     * Get the index on (tableName, columnName).
     * Returns nullptr if no index exists.
     */
    const HashIndex* getIndex(const std::string& tableName,
                              const std::string& columnName) const {
        std::string key = toLower(tableName) + "." + toLower(columnName);
        auto it = columnIndexes_.find(key);
        return it != columnIndexes_.end() ? it->second : nullptr;
    }

    /**
     * Returns all index names in creation order.
     */
    const std::vector<std::string>& getIndexOrder() const { return indexOrder_; }

    /**
     * Returns a reference to the index by name, or nullptr.
     */
    const HashIndex* getIndexByName(const std::string& indexName) const {
        auto it = indexes_.find(toLower(indexName));
        return it != indexes_.end() ? &it->second : nullptr;
    }

    /** Remove all indexes. */
    void clear() {
        indexes_.clear();
        columnIndexes_.clear();
        indexOrder_.clear();
    }

    /** Rebuild the column-pointer map (call after tables are re-loaded). */
    void rebuildColumnMap() {
        columnIndexes_.clear();
        for (const auto& name : indexOrder_) {
            auto& idx = indexes_.at(name);
            std::string key = idx.getTableName() + "." + idx.getColumnName();
            columnIndexes_[key] = &indexes_.at(name);
        }
    }

private:
    static std::string toLower(std::string s) {
        for (auto& c : s) c = (char)std::tolower((unsigned char)c);
        return s;
    }

    std::unordered_map<std::string, HashIndex>  indexes_;       // name → index
    std::unordered_map<std::string, HashIndex*> columnIndexes_; // "tbl.col" → index
    std::vector<std::string>                    indexOrder_;    // insertion order
};
