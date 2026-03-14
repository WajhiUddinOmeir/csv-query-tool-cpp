#pragma once
#include "Schema.hpp"
#include "Row.hpp"
#include <vector>
#include <string>

/**
 * In-memory table loaded from a CSV file (or created via CREATE TABLE).
 *
 * Features:
 * - Robust CSV parsing (quoted fields, embedded commas, mixed line endings)
 * - Automatic type inference (INTEGER → DOUBLE → BOOLEAN → STRING)
 * - Tracks load time for performance reporting
 */
class CsvTable {
public:
    CsvTable(std::string name, Schema schema,
             std::vector<Row> rows, std::string filePath, long loadTimeMs);

    const std::string& getName()     const { return name_; }
    Schema&            getSchema()         { return schema_; }
    const Schema&      getSchema()   const { return schema_; }
    std::vector<Row>&  getRows()           { return rows_; }
    const std::vector<Row>& getRows() const { return rows_; }
    const std::string& getFilePath() const { return filePath_; }
    long               getLoadTimeMs() const { return loadTimeMs_; }
    int                getRowCount()   const { return (int)rows_.size(); }
    int                getColumnCount() const { return schema_.getColumnCount(); }

    void addRow(Row row);
    void clearRows();
    void addColumn(const Column& col);
    void dropColumn(const std::string& columnName);
    void renameColumn(const std::string& oldName, const std::string& newName);

    /** Creates an empty in-memory table (CREATE TABLE). */
    static CsvTable createEmpty(const std::string& tableName,
                                const std::vector<Column>& columns);

    /** Loads a CSV file into memory as a named table (LOAD). */
    static CsvTable load(const std::string& filePath, const std::string& tableName);

private:
    std::string      name_;
    Schema           schema_;
    std::vector<Row> rows_;
    std::string      filePath_;
    long             loadTimeMs_;

    static std::vector<std::vector<std::string>> parseCsvFile(const std::string& filePath);

    static Schema inferSchema(const std::string& tableName,
                              const std::vector<std::string>& headers,
                              const std::vector<std::vector<std::string>>& dataRows);

    static DataType widenType(DataType current, const std::string& value);
    static bool     isInteger(const std::string& s);
    static bool     isDouble (const std::string& s);
    static bool     isBoolean(const std::string& s);
    static Value    parseValue(const std::string& raw, DataType type);
};
