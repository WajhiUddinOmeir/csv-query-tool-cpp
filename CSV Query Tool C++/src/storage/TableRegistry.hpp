#pragma once
#include "CsvTable.hpp"
#include <unordered_map>
#include <string>
#include <stdexcept>
#include <algorithm>

/**
 * Registry of all loaded tables. Provides O(1) table lookup by name.
 * Table names are case-insensitive.
 */
class TableRegistry {
public:
    /** Registers a table. Overwrites if name already exists. */
    void registerTable(const CsvTable& table) {
        std::string key = toLower(table.getName());
        if (!tables_.count(key)) order_.push_back(key);
        tables_.insert_or_assign(key, table);
    }

    /** Move-register a table (avoids copy for freshly loaded tables). */
    void registerTable(CsvTable&& table) {
        std::string key = toLower(table.getName());
        if (!tables_.count(key)) order_.push_back(key);
        tables_.insert_or_assign(key, std::move(table));
    }

    /** Retrieves a table by name (case-insensitive). */
    CsvTable& getTable(const std::string& name) {
        auto it = tables_.find(toLower(name));
        if (it == tables_.end())
            throw std::runtime_error("Unknown table: " + name +
                ". Use LOAD 'file.csv' AS " + name + " to load it first.");
        return it->second;
    }

    const CsvTable& getTable(const std::string& name) const {
        auto it = tables_.find(toLower(name));
        if (it == tables_.end())
            throw std::runtime_error("Unknown table: " + name);
        return it->second;
    }

    bool hasTable(const std::string& name) const {
        return tables_.count(toLower(name)) > 0;
    }

    /** Returns all tables in insertion order. */
    std::vector<CsvTable*> getAllTables() {
        std::vector<CsvTable*> result;
        for (const auto& k : order_) {
            auto it = tables_.find(k);
            if (it != tables_.end()) result.push_back(&it->second);
        }
        return result;
    }

    void unregister(const std::string& name) {
        std::string key = toLower(name);
        tables_.erase(key);
        order_.erase(std::remove(order_.begin(), order_.end(), key), order_.end());
    }

    void rename(const std::string& oldName, const std::string& newName) {
        std::string oldKey = toLower(oldName);
        std::string newKey = toLower(newName);
        auto it = tables_.find(oldKey);
        if (it == tables_.end())
            throw std::runtime_error("Unknown table: " + oldName);
        CsvTable tbl = it->second;
        tables_.erase(oldKey);
        tables_.insert_or_assign(newKey, std::move(tbl));
        for (auto& k : order_)
            if (k == oldKey) { k = newKey; break; }
    }

    void clear() {
        tables_.clear();
        order_.clear();
    }

    int size() const { return (int)tables_.size(); }

    /** Returns table names in insertion order. */
    const std::vector<std::string>& getTableOrder() const { return order_; }

private:
    std::unordered_map<std::string, CsvTable> tables_;
    std::vector<std::string> order_; // insertion order

    static std::string toLower(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), ::tolower);
        return s;
    }
};
