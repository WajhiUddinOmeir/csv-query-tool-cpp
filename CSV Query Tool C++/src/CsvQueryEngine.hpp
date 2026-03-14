#pragma once
#include "storage/TableRegistry.hpp"
#include "storage/CsvTable.hpp"
#include "index/IndexManager.hpp"
#include "executor/QueryExecutor.hpp"
#include "util/TablePrinter.hpp"
#include "parser/ast/Command.hpp"
#include <string>

/**
 * CSV Query Tool — Interactive SQL engine for CSV files.
 *
 * Entry point. Provides a REPL where users can:
 *   - Load CSV files as named tables
 *   - Execute SQL queries (SELECT, JOIN, GROUP BY, etc.)
 *   - Create/drop hash indexes
 *   - Inspect table schemas
 *   - Modify tables via DDL/DML
 */
class CsvQueryEngine {
public:
    CsvQueryEngine();

    /** Start the interactive REPL. Returns when the user quits. */
    void startRepl();

    /**
     * Execute a single SQL statement string (including trailing semicolon).
     * Parses, executes, and prints the result.
     * Errors are caught and printed rather than thrown.
     */
    void executeSql(const std::string& sql);

    /**
     * Execute a SQL statement and return the QueryResult.
     * Throws on parse / execution error (no catch).
     * Useful for testing.
     */
    QueryResult executeQuery(const std::string& sql);

private:
    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------
    TableRegistry  registry_;
    IndexManager   indexManager_;
    QueryExecutor  executor_;
    TablePrinter   printer_;
    bool           showStats_ = false;

    // -----------------------------------------------------------------------
    // Command handlers
    // -----------------------------------------------------------------------
    void handleCommand(const Command& cmd);

    void loadTable(const std::string& filePath, const std::string& tableName);
    void showTables();
    void describeTable(const std::string& tableName);
    void createIndex(const std::string& indexName,
                     const std::string& tableName,
                     const std::string& columnName);
    void dropIndex(const std::string& indexName);
    void showIndexes();
    void createTable(const Command& cmd);
    void insertInto(const Command& cmd);
    void deleteFrom(const std::string& tableName);
    void alterAddColumn(const Command& cmd);
    void alterDropColumn(const Command& cmd);
    void alterRenameColumn(const Command& cmd);
    void alterRenameTable(const Command& cmd);

    /** Execute an EXPORT statement: run the SELECT and write to a file. */
    void exportResults(const ExportStatement& stmt);

    // -----------------------------------------------------------------------
    // Dot-commands
    // -----------------------------------------------------------------------
    void handleDotCommand(const std::string& line);

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------
    static DataType parseDataType(const std::string& typeStr);
    static Value    parseTypedValue(const std::string& raw, DataType type,
                                    bool isNull = false);

    static void printWelcome();
    static void printHelp();
};
