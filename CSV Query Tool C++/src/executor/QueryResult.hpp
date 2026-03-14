#pragma once
#include "storage/DataType.hpp"
#include "storage/Value.hpp"
#include <vector>
#include <string>

/**
 * Result of a SELECT query: column metadata, data rows, and performance stats.
 */
struct QueryResult {
    std::vector<std::string>         columnNames;
    std::vector<DataType>            columnTypes;
    std::vector<std::vector<Value>>  rows;
    long    executionTimeMs = 0;
    int     rowsScanned     = 0;
    std::string message;    // non-empty for command results (e.g., "Table loaded")

    QueryResult() = default;

    /** Create a data-result. */
    QueryResult(std::vector<std::string> cols, std::vector<DataType> types,
                std::vector<std::vector<Value>> rows, long ms, int scanned)
        : columnNames(std::move(cols)), columnTypes(std::move(types)),
          rows(std::move(rows)), executionTimeMs(ms), rowsScanned(scanned) {}

    /** Create a message-only result. */
    explicit QueryResult(std::string msg) : message(std::move(msg)) {}

    bool isMessage()  const { return !message.empty(); }
    int  getRowCount() const { return (int)rows.size(); }
};
