#pragma once
#include "storage/Row.hpp"
#include "storage/Value.hpp"
#include <unordered_map>
#include <vector>
#include <string>

/**
 * Hash-based index on a single column. Provides O(1) equality lookups.
 *
 * Analogous to a hash index in real databases:
 *   + O(1) average for equality (=) predicates
 *   − Does NOT support range queries (needs B-Tree for those)
 *   − Memory overhead proportional to table size
 */
class HashIndex {
public:
    HashIndex(std::string indexName, std::string tableName, std::string columnName,
              const std::vector<Row>& rows, int columnIndex)
        : indexName_(std::move(indexName)),
          tableName_(std::move(tableName)),
          columnName_(std::move(columnName)),
          rowCount_((int)rows.size())
    {
        for (int i = 0; i < (int)rows.size(); i++) {
            const Value& val = rows[i].get(columnIndex);
            if (!isNull(val))
                index_[valueToString(val)].push_back(i);
        }
    }

    /** Returns pointer to row indices for the given value, or nullptr if no match. */
    const std::vector<int>* lookup(const Value& value) const {
        if (isNull(value)) return nullptr;
        auto it = index_.find(valueToString(value));
        return it != index_.end() ? &it->second : nullptr;
    }

    const std::string& getIndexName()  const { return indexName_;  }
    const std::string& getTableName()  const { return tableName_;  }
    const std::string& getColumnName() const { return columnName_; }
    int getDistinctValues() const { return (int)index_.size(); }
    int getRowCount()       const { return rowCount_; }

private:
    std::string indexName_;
    std::string tableName_;
    std::string columnName_;
    int         rowCount_;
    std::unordered_map<std::string, std::vector<int>> index_;
};
