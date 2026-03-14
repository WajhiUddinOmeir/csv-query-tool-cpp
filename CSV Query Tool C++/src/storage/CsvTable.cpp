#include "CsvTable.hpp"
#include <fstream>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <stdexcept>
#include <cmath>
#include <limits>

// ──────────────────────────────────────────────────────────────
//  Constructor & public API
// ──────────────────────────────────────────────────────────────

CsvTable::CsvTable(std::string name, Schema schema,
                   std::vector<Row> rows, std::string filePath, long loadTimeMs)
    : name_(std::move(name)), schema_(std::move(schema)),
      rows_(std::move(rows)), filePath_(std::move(filePath)), loadTimeMs_(loadTimeMs) {}

void CsvTable::addRow(Row row) {
    rows_.push_back(std::move(row));
}

void CsvTable::clearRows() {
    rows_.clear();
}

void CsvTable::addColumn(const Column& col) {
    std::vector<Column> newCols = schema_.columns;
    newCols.push_back(col);
    schema_ = Schema(schema_.tableName, newCols);

    // Extend every existing row with NULL in the new column position
    for (auto& row : rows_)
        row.values.push_back(std::monostate{});
}

void CsvTable::dropColumn(const std::string& columnName) {
    int colIdx = schema_.resolveColumn("", columnName);
    if (schema_.getColumnCount() == 1)
        throw std::runtime_error("Cannot drop the last column from table '" + name_ + "'");

    std::vector<Column> newCols;
    for (int i = 0; i < (int)schema_.columns.size(); i++)
        if (i != colIdx)
            newCols.push_back(schema_.columns[i]);
    schema_ = Schema(schema_.tableName, newCols);

    for (auto& row : rows_) {
        row.values.erase(row.values.begin() + colIdx);
    }
}

void CsvTable::renameColumn(const std::string& oldName, const std::string& newName) {
    int colIdx = schema_.resolveColumn("", oldName);
    std::vector<Column> newCols = schema_.columns;
    newCols[colIdx] = Column(newName, newCols[colIdx].type, newCols[colIdx].tableName);
    schema_ = Schema(schema_.tableName, newCols);
}

CsvTable CsvTable::createEmpty(const std::string& tableName,
                               const std::vector<Column>& columns) {
    return CsvTable(tableName, Schema(tableName, columns), {}, "<in-memory>", 0);
}

// ──────────────────────────────────────────────────────────────
//  CSV loading
// ──────────────────────────────────────────────────────────────

CsvTable CsvTable::load(const std::string& filePath, const std::string& tableName) {
    auto startMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    auto rawRows = parseCsvFile(filePath);
    if (rawRows.empty())
        throw std::runtime_error("CSV file is empty: " + filePath);

    auto& headers = rawRows[0];
    std::vector<std::vector<std::string>> dataRows(rawRows.begin() + 1, rawRows.end());

    Schema schema = inferSchema(tableName, headers, dataRows);

    std::vector<Row> rows;
    rows.reserve(dataRows.size());
    for (const auto& rawRow : dataRows) {
        std::vector<Value> values;
        values.reserve(headers.size());
        for (int i = 0; i < (int)headers.size(); i++) {
            std::string raw = (i < (int)rawRow.size()) ? rawRow[i] : "";
            values.push_back(parseValue(raw, schema.getColumn(i).type));
        }
        rows.emplace_back(std::move(values));
    }

    auto endMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    return CsvTable(tableName, std::move(schema), std::move(rows), filePath,
                    static_cast<long>(endMs - startMs));
}

// ──────────────────────────────────────────────────────────────
//  CSV parser: handles quoted fields, embedded commas, \r\n
// ──────────────────────────────────────────────────────────────

std::vector<std::vector<std::string>> CsvTable::parseCsvFile(const std::string& filePath) {
    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open())
        throw std::runtime_error("Cannot open file: " + filePath);

    std::string content((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());

    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> currentRow;
    std::string field;
    bool inQuotes = false;
    size_t i = 0;

    while (i < content.size()) {
        char c = content[i];

        if (inQuotes) {
            if (c == '"') {
                if (i + 1 < content.size() && content[i + 1] == '"') {
                    field += '"'; i += 2; // escaped quote "" → "
                } else {
                    inQuotes = false; i++;
                }
            } else {
                field += c; i++;
            }
        } else {
            if (c == '"') {
                inQuotes = true; i++;
            } else if (c == ',') {
                currentRow.push_back(field);
                // trim whitespace
                auto& f = currentRow.back();
                f.erase(0, f.find_first_not_of(" \t"));
                f.erase(f.find_last_not_of(" \t") + 1);
                field.clear(); i++;
            } else if (c == '\r') {
                currentRow.push_back(field);
                auto& f = currentRow.back();
                f.erase(0, f.find_first_not_of(" \t"));
                f.erase(f.find_last_not_of(" \t") + 1);
                field.clear();
                if (!currentRow.empty() && !(currentRow.size() == 1 && currentRow[0].empty()))
                    rows.push_back(std::move(currentRow));
                currentRow.clear();
                i++;
                if (i < content.size() && content[i] == '\n') i++;
            } else if (c == '\n') {
                currentRow.push_back(field);
                auto& f = currentRow.back();
                f.erase(0, f.find_first_not_of(" \t"));
                f.erase(f.find_last_not_of(" \t") + 1);
                field.clear();
                if (!currentRow.empty() && !(currentRow.size() == 1 && currentRow[0].empty()))
                    rows.push_back(std::move(currentRow));
                currentRow.clear();
                i++;
            } else {
                field += c; i++;
            }
        }
    }

    // Handle last field/row
    if (!field.empty() || !currentRow.empty()) {
        currentRow.push_back(field);
        auto& f = currentRow.back();
        f.erase(0, f.find_first_not_of(" \t"));
        if (!f.empty()) f.erase(f.find_last_not_of(" \t") + 1);
        if (!currentRow.empty() && !(currentRow.size() == 1 && currentRow[0].empty()))
            rows.push_back(std::move(currentRow));
    }

    return rows;
}

// ──────────────────────────────────────────────────────────────
//  Type inference
// ──────────────────────────────────────────────────────────────

Schema CsvTable::inferSchema(const std::string& tableName,
                              const std::vector<std::string>& headers,
                              const std::vector<std::vector<std::string>>& dataRows) {
    int numCols = (int)headers.size();
    std::vector<DataType> types(numCols, DataType::INTEGER); // start most-specific

    for (const auto& row : dataRows) {
        for (int i = 0; i < numCols && i < (int)row.size(); i++) {
            if (row[i].empty()) continue; // NULL — doesn't affect type
            types[i] = widenType(types[i], row[i]);
        }
    }

    std::vector<Column> columns;
    columns.reserve(numCols);
    for (int i = 0; i < numCols; i++) {
        std::string hdr = headers[i];
        hdr.erase(0, hdr.find_first_not_of(" \t"));
        hdr.erase(hdr.find_last_not_of(" \t") + 1);
        columns.emplace_back(hdr, types[i]);
    }
    return Schema(tableName, columns);
}

DataType CsvTable::widenType(DataType current, const std::string& value) {
    switch (current) {
        case DataType::INTEGER:
            if (isInteger(value)) return DataType::INTEGER;
            if (isDouble (value)) return DataType::DOUBLE;
            if (isBoolean(value)) return DataType::BOOLEAN;
            return DataType::STRING;
        case DataType::DOUBLE:
            if (isInteger(value) || isDouble(value)) return DataType::DOUBLE;
            return DataType::STRING;
        case DataType::BOOLEAN:
            if (isBoolean(value)) return DataType::BOOLEAN;
            return DataType::STRING;
        default:
            return DataType::STRING;
    }
}

bool CsvTable::isInteger(const std::string& s) {
    if (s.empty()) return false;
    size_t start = (s[0] == '-' || s[0] == '+') ? 1 : 0;
    if (start == s.size()) return false;
    for (size_t i = start; i < s.size(); i++)
        if (!std::isdigit((unsigned char)s[i])) return false;
    return true;
}

bool CsvTable::isDouble(const std::string& s) {
    if (s.empty()) return false;
    try {
        size_t pos;
        double d = std::stod(s, &pos);
        if (pos != s.size()) return false;
        (void)d;
        std::string sl = s;
        std::transform(sl.begin(), sl.end(), sl.begin(), ::tolower);
        return sl != "nan" && sl != "infinity" && sl != "-infinity";
    } catch (...) { return false; }
}

bool CsvTable::isBoolean(const std::string& s) {
    std::string sl = s;
    std::transform(sl.begin(), sl.end(), sl.begin(), ::tolower);
    return sl == "true" || sl == "false";
}

Value CsvTable::parseValue(const std::string& raw, DataType type) {
    if (raw.empty()) return std::monostate{};
    switch (type) {
        case DataType::INTEGER:
            try { return static_cast<long long>(std::stoll(raw)); }
            catch (...) { return std::monostate{}; }
        case DataType::DOUBLE:
            try { return std::stod(raw); }
            catch (...) { return std::monostate{}; }
        case DataType::BOOLEAN: {
            std::string sl = raw;
            std::transform(sl.begin(), sl.end(), sl.begin(), ::tolower);
            return sl == "true";
        }
        default:
            return raw;
    }
}
