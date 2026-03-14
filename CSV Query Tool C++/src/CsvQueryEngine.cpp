#include "CsvQueryEngine.hpp"
#include "parser/SQLParser.hpp"
#include "parser/ast/SelectStatement.hpp"
#include <fstream>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <cctype>

// ============================================================
//  Constructor
// ============================================================

CsvQueryEngine::CsvQueryEngine()
    : executor_(registry_, indexManager_)
{}

// ============================================================
//  REPL
// ============================================================

void CsvQueryEngine::startRepl() {
    printWelcome();

    std::string buffer;

    while (true) {
        // Prompt: "csv-query> " for fresh input, "         > " for continuation
        std::cout << (buffer.empty() ? "csv-query> " : "         > ");
        std::cout.flush();

        std::string line;
        if (!std::getline(std::cin, line)) break; // EOF

        // Trim leading/trailing whitespace
        auto start = line.find_first_not_of(" \t\r\n");
        auto end   = line.find_last_not_of(" \t\r\n");
        line = (start == std::string::npos) ? "" : line.substr(start, end - start + 1);

        // Dot-commands — single line, no semicolon required
        if (buffer.empty() && !line.empty() && line[0] == '.') {
            handleDotCommand(line);
            continue;
        }

        if (line.empty()) continue;

        buffer += line + " ";

        if (line.back() == ';') {
            std::string sql = buffer;
            buffer.clear();
            executeSql(sql);
        }
    }

    std::cout << "\nGoodbye!\n";
}

// ============================================================
//  SQL execution
// ============================================================

void CsvQueryEngine::executeSql(const std::string& sql) {
    try {
        SQLParser parser(sql);
        ParseResult parsed = parser.parse();

        std::visit([&](auto&& stmt) {
            using T = std::decay_t<decltype(stmt)>;
            if constexpr (std::is_same_v<T, SelectStatement>) {
                QueryResult result = executor_.execute(stmt);
                printer_.print(result);
            } else if constexpr (std::is_same_v<T, UpdateStatement>) {
                QueryResult result = executor_.executeUpdate(stmt);
                std::cout << result.message << "\n";
            } else if constexpr (std::is_same_v<T, ExportStatement>) {
                exportResults(stmt);
            } else if constexpr (std::is_same_v<T, Command>) {
                handleCommand(stmt);
            }
        }, parsed);

    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

QueryResult CsvQueryEngine::executeQuery(const std::string& sql) {
    SQLParser parser(sql);
    ParseResult parsed = parser.parse();

    QueryResult result;
    std::visit([&](auto&& stmt) {
        using T = std::decay_t<decltype(stmt)>;
        if constexpr (std::is_same_v<T, SelectStatement>) {
            result = executor_.execute(stmt);
        } else if constexpr (std::is_same_v<T, UpdateStatement>) {
            result = executor_.executeUpdate(stmt);
        } else if constexpr (std::is_same_v<T, ExportStatement>) {
            exportResults(stmt);
            result = QueryResult("Export complete.");
        } else if constexpr (std::is_same_v<T, Command>) {
            handleCommand(stmt);
            result = QueryResult("OK");
        }
    }, parsed);
    return result;
}

// ============================================================
//  Command dispatch
// ============================================================

void CsvQueryEngine::handleCommand(const Command& cmd) {
    switch (cmd.type) {
    case CommandType::LOAD_TABLE:
        loadTable(cmd.getParam("filePath"), cmd.getParam("tableName"));
        break;
    case CommandType::SHOW_TABLES:
        showTables();
        break;
    case CommandType::DESCRIBE_TABLE:
        describeTable(cmd.getParam("tableName"));
        break;
    case CommandType::CREATE_INDEX:
        createIndex(cmd.getParam("indexName"),
                    cmd.getParam("tableName"),
                    cmd.getParam("columnName"));
        break;
    case CommandType::DROP_INDEX:
        dropIndex(cmd.getParam("indexName"));
        break;
    case CommandType::SHOW_INDEXES:
        showIndexes();
        break;
    case CommandType::CREATE_TABLE:
        createTable(cmd);
        break;
    case CommandType::INSERT_INTO:
        insertInto(cmd);
        break;
    case CommandType::DELETE_FROM:
        deleteFrom(cmd.getParam("tableName"));
        break;
    case CommandType::ALTER_ADD_COLUMN:
        alterAddColumn(cmd);
        break;
    case CommandType::ALTER_DROP_COLUMN:
        alterDropColumn(cmd);
        break;
    case CommandType::ALTER_RENAME_COLUMN:
        alterRenameColumn(cmd);
        break;
    case CommandType::ALTER_RENAME_TABLE:
        alterRenameTable(cmd);
        break;
    }
}

// ============================================================
//  Command implementations
// ============================================================

void CsvQueryEngine::loadTable(const std::string& filePath,
                                const std::string& tableName)
{
    CsvTable table = CsvTable::load(filePath, tableName);
    int rows = table.getRowCount();
    int cols = table.getColumnCount();
    double ms  = table.getLoadTimeMs();
    registry_.registerTable(std::move(table));
    std::cout << "Table '" << tableName << "' loaded: "
              << rows << " rows, " << cols << " columns ("
              << std::fixed << std::setprecision(1) << ms << " ms)\n";
}

void CsvQueryEngine::showTables() {
    const auto& names = registry_.getTableOrder();
    if (names.empty()) {
        std::cout << "No tables loaded. Use: LOAD 'file.csv' AS table_name;\n";
        return;
    }
    std::cout << "+------------------+------+---------+----------------------+\n";
    std::cout << "| Table            | Rows | Columns | Source File          |\n";
    std::cout << "+------------------+------+---------+----------------------+\n";
    for (const auto& name : names) {
        const CsvTable& t = registry_.getTable(name);
        std::string fp = t.getFilePath();
        if (fp.size() > 20) fp = "..." + fp.substr(fp.size() - 17);
        std::cout << "| " << std::left  << std::setw(16) << t.getName()
                  << " | "  << std::right << std::setw(4)  << t.getRowCount()
                  << " | "  << std::right << std::setw(7)  << t.getColumnCount()
                  << " | "  << std::left  << std::setw(20) << fp
                  << " |\n";
    }
    std::cout << "+------------------+------+---------+----------------------+\n";
}

void CsvQueryEngine::describeTable(const std::string& tableName) {
    const CsvTable& t = registry_.getTable(tableName);
    std::cout << "Table: " << t.getName()     << "\n";
    std::cout << "File:  " << t.getFilePath() << "\n";
    std::cout << "Rows:  " << t.getRowCount() << "\n\n";
    std::cout << "+----+------------------+----------+\n";
    std::cout << "| #  | Column           | Type     |\n";
    std::cout << "+----+------------------+----------+\n";
    int i = 1;
    for (const auto& col : t.getSchema().getColumns()) {
        std::cout << "| " << std::right << std::setw(2) << i++
                  << " | " << std::left  << std::setw(16) << col.name
                  << " | " << std::left  << std::setw(8)
                  << dataTypeToString(col.type) << " |\n";
    }
    std::cout << "+----+------------------+----------+\n";
}

void CsvQueryEngine::createIndex(const std::string& indexName,
                                  const std::string& tableName,
                                  const std::string& columnName)
{
    const CsvTable& table = registry_.getTable(tableName);
    indexManager_.createIndex(indexName, tableName, columnName, table);
    std::cout << "Index '" << indexName << "' created on "
              << tableName << "(" << columnName << ")\n";
}

void CsvQueryEngine::dropIndex(const std::string& indexName) {
    indexManager_.dropIndex(indexName);
    std::cout << "Index '" << indexName << "' dropped\n";
}

void CsvQueryEngine::showIndexes() {
    const auto& order = indexManager_.getIndexOrder();
    if (order.empty()) {
        std::cout << "No indexes. Use: CREATE INDEX name ON table(column);\n";
        return;
    }
    std::cout << "+------------------+------------------+------------------+----------+\n";
    std::cout << "| Index            | Table            | Column           | Distinct |\n";
    std::cout << "+------------------+------------------+------------------+----------+\n";
    for (const auto& name : order) {
        const HashIndex* idx = indexManager_.getIndexByName(name);
        if (!idx) continue;
        std::cout << "| " << std::left  << std::setw(16) << idx->getIndexName()
                  << " | " << std::left  << std::setw(16) << idx->getTableName()
                  << " | " << std::left  << std::setw(16) << idx->getColumnName()
                  << " | " << std::right << std::setw(8)  << idx->getDistinctValues()
                  << " |\n";
    }
    std::cout << "+------------------+------------------+------------------+----------+\n";
}

void CsvQueryEngine::createTable(const Command& cmd) {
    std::string tableName = cmd.getParam("tableName");
    const auto& colNames = cmd.columnNames;
    const auto& colTypes = cmd.columnTypes;

    std::vector<Column> columns;
    for (size_t i = 0; i < colNames.size(); i++) {
        DataType dt = parseDataType(i < colTypes.size() ? colTypes[i] : "STRING");
        columns.push_back(Column{colNames[i], dt});
    }

    CsvTable table = CsvTable::createEmpty(tableName, columns);
    registry_.registerTable(std::move(table));
    std::cout << "Table '" << tableName << "' created with "
              << columns.size() << " column(s).\n";
}

void CsvQueryEngine::insertInto(const Command& cmd) {
    std::string tableName = cmd.getParam("tableName");
    CsvTable& table       = registry_.getTable(tableName);
    const Schema& schema  = table.getSchema();

    int insertedCount = 0;
    for (const auto& rawVals : cmd.valueRows) {
        std::vector<Value> values(schema.getColumnCount(), std::monostate{});

        if (!cmd.columnNames.empty()) {
            // Column-specified: INSERT INTO t (col1, col2) VALUES (...)
            for (size_t i = 0; i < cmd.columnNames.size(); i++) {
                int colIdx = schema.findColumn(cmd.columnNames[i]);
                if (colIdx < 0)
                    throw std::runtime_error("Unknown column: " + cmd.columnNames[i]);
                DataType dt = schema.getColumn(colIdx).type;
                std::string raw = (i < rawVals.size()) ? rawVals[i] : "";
                // Parser stores "" for SQL NULL
                values[colIdx] = parseTypedValue(raw, dt, raw.empty());
            }
        } else {
            // Positional: INSERT INTO t VALUES (...)
            if ((int)rawVals.size() != schema.getColumnCount())
                throw std::runtime_error("Expected " + std::to_string(schema.getColumnCount())
                                         + " values but got " + std::to_string(rawVals.size()));
            for (int i = 0; i < schema.getColumnCount(); i++) {
                DataType dt = schema.getColumn(i).type;
                const std::string& raw = rawVals[i];
                // Parser stores "" for SQL NULL
                values[i] = parseTypedValue(raw, dt, raw.empty());
            }
        }

        table.addRow(Row(std::move(values)));
        insertedCount++;
    }
    std::cout << insertedCount << " row(s) inserted into '" << tableName << "'.\n";
}

void CsvQueryEngine::deleteFrom(const std::string& tableName) {
    CsvTable& table = registry_.getTable(tableName);
    int count = table.getRowCount();
    table.clearRows();
    std::cout << count << " row(s) deleted from '" << tableName << "'.\n";
}

void CsvQueryEngine::alterAddColumn(const Command& cmd) {
    std::string tableName = cmd.getParam("tableName");
    std::string colName   = cmd.getParam("columnName");
    std::string colType   = cmd.getParam("columnType");
    CsvTable& table = registry_.getTable(tableName);
    DataType dt = parseDataType(colType);
    table.addColumn(Column{colName, dt});
    std::cout << "Column '" << colName << "' (" << dataTypeToString(dt)
              << ") added to '" << tableName << "'.\n";
}

void CsvQueryEngine::alterDropColumn(const Command& cmd) {
    std::string tableName = cmd.getParam("tableName");
    std::string colName   = cmd.getParam("columnName");
    CsvTable& table = registry_.getTable(tableName);
    table.dropColumn(colName);
    std::cout << "Column '" << colName << "' dropped from '" << tableName << "'.\n";
}

void CsvQueryEngine::alterRenameColumn(const Command& cmd) {
    std::string tableName = cmd.getParam("tableName");
    std::string oldName   = cmd.getParam("oldName");
    std::string newName   = cmd.getParam("newName");
    CsvTable& table = registry_.getTable(tableName);
    table.renameColumn(oldName, newName);
    std::cout << "Column '" << oldName << "' renamed to '" << newName
              << "' in '" << tableName << "'.\n";
}

void CsvQueryEngine::alterRenameTable(const Command& cmd) {
    std::string oldName = cmd.getParam("tableName");
    std::string newName = cmd.getParam("newName");
    registry_.rename(oldName, newName);
    std::cout << "Table '" << oldName << "' renamed to '" << newName << "'.\n";
}

// ============================================================
//  Dot-command handler
// ============================================================
//  Export
// ============================================================

void CsvQueryEngine::exportResults(const ExportStatement& stmt) {
    QueryResult result = executor_.execute(stmt.query);
    std::ofstream out(stmt.filePath);
    if (!out.is_open())
        throw std::runtime_error("Cannot open output file: " + stmt.filePath);

    const std::string& fmt = stmt.format; // "CSV" or "JSON"

    if (fmt == "JSON") {
        out << "[\n";
        for (size_t r = 0; r < result.rows.size(); r++) {
            out << "  {";
            for (size_t c = 0; c < result.columnNames.size(); c++) {
                out << "\"" << result.columnNames[c] << "\": ";
                const Value& v = result.rows[r][c];
                if (std::holds_alternative<std::monostate>(v)) {
                    out << "null";
                } else if (std::holds_alternative<long long>(v)) {
                    out << std::get<long long>(v);
                } else if (std::holds_alternative<double>(v)) {
                    out << std::get<double>(v);
                } else if (std::holds_alternative<bool>(v)) {
                    out << (std::get<bool>(v) ? "true" : "false");
                } else {
                    // String — escape backslash, double-quote and newline
                    const std::string& s = std::get<std::string>(v);
                    out << '"';
                    for (char ch : s) {
                        if (ch == '"')       out << "\\\"";
                        else if (ch == '\\') out << "\\\\";
                        else if (ch == '\n') out << "\\n";
                        else                 out << ch;
                    }
                    out << '"';
                }
                if (c + 1 < result.columnNames.size()) out << ", ";
            }
            out << '}';
            if (r + 1 < result.rows.size()) out << ",";
            out << "\n";
        }
        out << "]\n";
    } else { // CSV (default)
        // Header row
        for (size_t c = 0; c < result.columnNames.size(); c++) {
            if (c) out << ',';
            // Quote column names that contain commas or quotes
            const std::string& n = result.columnNames[c];
            bool needsQuote = n.find_first_of(",\"\n") != std::string::npos;
            if (needsQuote) {
                out << '"';
                for (char ch : n) { if (ch == '"') out << '"'; out << ch; }
                out << '"';
            } else {
                out << n;
            }
        }
        out << "\n";
        // Data rows
        for (const auto& row : result.rows) {
            for (size_t c = 0; c < row.size(); c++) {
                if (c) out << ',';
                const Value& v = row[c];
                if (std::holds_alternative<std::monostate>(v)) {
                    // NULL → empty cell
                } else if (std::holds_alternative<long long>(v)) {
                    out << std::get<long long>(v);
                } else if (std::holds_alternative<double>(v)) {
                    out << std::get<double>(v);
                } else if (std::holds_alternative<bool>(v)) {
                    out << (std::get<bool>(v) ? "true" : "false");
                } else {
                    const std::string& s = std::get<std::string>(v);
                    bool needsQuote = s.find_first_of(",\"\n") != std::string::npos;
                    if (needsQuote) {
                        out << '"';
                        for (char ch : s) { if (ch == '"') out << '"'; out << ch; }
                        out << '"';
                    } else {
                        out << s;
                    }
                }
            }
            out << "\n";
        }
    }

    out.close();
    std::cout << "Exported " << result.rows.size() << " row(s) to '" << stmt.filePath
              << "' (" << fmt << ").\n";
}

// ============================================================
//  Dot commands
// ============================================================

void CsvQueryEngine::handleDotCommand(const std::string& line) {
    std::string cmd = line;
    for (auto& c : cmd) c = (char)std::tolower((unsigned char)c);
    // strip trailing whitespace
    while (!cmd.empty() && std::isspace((unsigned char)cmd.back())) cmd.pop_back();

    if (cmd == ".help") {
        printHelp();
    } else if (cmd == ".quit" || cmd == ".exit") {
        std::cout << "Goodbye!\n";
        std::exit(0);
    } else if (cmd == ".stats") {
        showStats_ = !showStats_;
        printer_.setShowStats(showStats_);
        std::cout << "Query statistics: " << (showStats_ ? "ON" : "OFF") << "\n";
    } else if (cmd == ".clear") {
        registry_.clear();
        indexManager_.clear();
        std::cout << "All tables and indexes cleared.\n";
    } else if (cmd == ".tables") {
        showTables();
    } else {
        std::cout << "Unknown command: " << cmd
                  << ". Type .help for available commands.\n";
    }
}

// ============================================================
//  Helpers
// ============================================================

DataType CsvQueryEngine::parseDataType(const std::string& typeStr) {
    std::string t = typeStr;
    for (auto& c : t) c = (char)std::toupper((unsigned char)c);

    if (t == "INTEGER" || t == "INT")                                  return DataType::INTEGER;
    if (t == "DOUBLE"  || t == "FLOAT" || t == "DECIMAL" || t == "NUMERIC") return DataType::DOUBLE;
    if (t == "BOOLEAN" || t == "BOOL")                                 return DataType::BOOLEAN;
    if (t == "STRING"  || t == "VARCHAR" || t == "TEXT")               return DataType::STRING;
    throw std::runtime_error("Unknown data type: " + typeStr
                             + ". Supported: INTEGER, DOUBLE, BOOLEAN, STRING");
}

Value CsvQueryEngine::parseTypedValue(const std::string& raw, DataType type,
                                       bool isNull)
{
    // For non-string types, treat empty string as SQL NULL
    if (isNull || (raw.empty() && type != DataType::STRING))
        return std::monostate{};
    switch (type) {
    case DataType::INTEGER:
        try { return std::stoll(raw); }
        catch (...) { throw std::runtime_error("Invalid INTEGER value: " + raw); }
    case DataType::DOUBLE:
        try { return std::stod(raw); }
        catch (...) { throw std::runtime_error("Invalid DOUBLE value: " + raw); }
    case DataType::BOOLEAN: {
        std::string b = raw;
        for (auto& c : b) c = (char)std::tolower((unsigned char)c);
        return (b == "true" || b == "1" || b == "yes");
    }
    case DataType::STRING:
    default:
        return raw;
    }
}

// ============================================================
//  Welcome & Help
// ============================================================

void CsvQueryEngine::printWelcome() {
    std::cout << "╔══════════════════════════════════════════════════════╗\n";
    std::cout << "║            CSV Query Tool v1.0.0 (C++)              ║\n";
    std::cout << "║     SQL Query Engine for CSV Files                  ║\n";
    std::cout << "║                                                      ║\n";
    std::cout << "║  Type .help for commands, .quit to exit              ║\n";
    std::cout << "║  End SQL statements with ;                           ║\n";
    std::cout << "╚══════════════════════════════════════════════════════╝\n\n";
}

void CsvQueryEngine::printHelp() {
    std::cout << R"(
=== SQL Commands (end with ;) ===
  LOAD 'file.csv' AS table_name    Load a CSV file as a table
  CREATE TABLE t (c1 TYPE, ...)    Create an empty table
  INSERT INTO t VALUES (...)       Insert rows into a table
  UPDATE t SET col=expr [WHERE …]  Modify rows in a table
  DELETE FROM t                    Delete all rows from a table
  ALTER TABLE t ADD col TYPE        Add a column to a table
  ALTER TABLE t DROP COLUMN col     Drop a column from a table
  ALTER TABLE t RENAME col TO new   Rename a column
  ALTER TABLE t RENAME TO new       Rename a table
  SELECT ... FROM ... WHERE ...    Execute a SQL query
  EXPORT (SELECT …) TO 'file'      Export query results to a file
    [FORMAT CSV|JSON]               (default: CSV; auto-detected from extension)
  SHOW TABLES                      List loaded tables
  DESCRIBE table_name              Show table schema
  CREATE INDEX name ON tbl(col)    Create a hash index
  DROP INDEX name                  Remove an index
  SHOW INDEXES                     List all indexes

=== Dot Commands ===
  .help          Show this help message
  .tables        List loaded tables
  .stats         Toggle query statistics (row count, timing)
  .clear         Clear all tables and indexes
  .quit / .exit  Exit the REPL

=== Supported SQL ===
  SELECT [DISTINCT] col, ...
  FROM table [alias]
  [JOIN table [alias] ON expr]
  [WHERE expr]
  [GROUP BY col, ...]
  [HAVING expr]
  [ORDER BY col [ASC|DESC], ...]
  [LIMIT n]

  Subqueries are supported in WHERE (scalar and IN) and FROM (derived tables):
    WHERE salary > (SELECT AVG(salary) FROM employees)
    WHERE id IN (SELECT id FROM salaries WHERE amount > 100000)
    SELECT * FROM (SELECT name, salary FROM employees WHERE salary > 80000) AS sub

=== Aggregate Functions ===
  COUNT(*), COUNT(col), SUM(col), AVG(col), MIN(col), MAX(col)

=== Scalar Functions ===
  UPPER(s), LOWER(s), LENGTH(s), TRIM(s)
  ABS(n), ROUND(n), COALESCE(a,b,...), CONCAT(a,b,...)
  SUBSTRING(s, pos, len)

=== Data Types ===
  INTEGER, DOUBLE, BOOLEAN, STRING

=== Example Session ===
  LOAD 'sample-data/employees.csv' AS employees;
  LOAD 'sample-data/departments.csv' AS departments;
  UPDATE employees SET salary = salary * 1.1 WHERE salary < 70000;
  SELECT e.name, d.name AS dept
  FROM employees e
  INNER JOIN departments d ON e.department_id = d.id
  WHERE e.salary > 80000
  ORDER BY e.salary DESC;
  EXPORT (SELECT * FROM employees) TO 'out.csv' FORMAT CSV;
  EXPORT (SELECT * FROM employees) TO 'out.json' FORMAT JSON;

)";
}
