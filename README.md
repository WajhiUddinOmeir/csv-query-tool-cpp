# CSV Query Tool (C++)

A **lightweight SQL query engine** for CSV files — ported to C++17 with **zero runtime dependencies**. Implements core database internals: recursive-descent parser, query optimizer with predicate pushdown, hash join, hash indexing, and automatic type inference.

This is a faithful C++ port of the original Java implementation.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       REPL / CLI                             │
├──────────────────────────────────────────────────────────────┤
│  Lexer ──▶ Parser ──▶ AST ──▶ Optimizer ──▶ Executor       │
│                                                              │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐  │
│  │ Expression │  │    Join    │  │    Aggregation         │  │
│  │ Evaluator  │  │  Executor  │  │    Engine              │  │
│  └────────────┘  └────────────┘  └────────────────────────┘  │
│                                                              │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐  │
│  │ CsvTable   │  │  Schema    │  │   Hash Index           │  │
│  │ (Row Store)│  │ (Type Inf.)│  │  (O(1) Lookup)         │  │
│  └────────────┘  └────────────┘  └────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

## Features

| Category | Details |
|----------|---------|
| **Query** | `SELECT`, `WHERE`, `ORDER BY`, `LIMIT`, `DISTINCT`, aliases, arithmetic |
| **Filtering** | `AND/OR/NOT`, `LIKE`, `BETWEEN`, `IN`, `IS [NOT] NULL` |
| **Joins** | `INNER`, `LEFT`, `RIGHT` JOIN — multi-table chaining |
| **Aggregation** | `COUNT(*)`, `COUNT(DISTINCT)`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP BY`, `HAVING` |
| **Functions** | `UPPER`, `LOWER`, `LENGTH`, `TRIM`, `CONCAT`, `SUBSTRING`, `ABS`, `ROUND`, `COALESCE` |
| **DDL** | `CREATE TABLE`, `ALTER TABLE` (ADD/DROP/RENAME COLUMN, RENAME TABLE) |
| **DML** | `INSERT INTO` (positional, named-column, multi-row), `DELETE FROM`, `UPDATE … SET … WHERE` |
| **Subqueries** | Scalar subqueries, `IN (SELECT …)`, derived tables in `FROM (SELECT …) AS alias` |
| **Export** | `EXPORT (SELECT …) TO 'file'` — CSV and JSON output, auto-detected from extension |
| **Indexing** | `CREATE/DROP INDEX` — hash-based O(1) equality lookups |
| **Engine** | Auto type inference, predicate pushdown, hash join, query stats, robust CSV parsing |

---

## Quick Start

### Prerequisites
- CMake ≥ 3.16
- C++17 compiler (GCC 9+, Clang 9+, MSVC 2019+)

### Build

```bash
mkdir build && cd build
cmake ..
cmake --build . --config Release
```

### Run

```bash
# Linux / macOS
./csv-query-tool

# Windows
.\csv-query-tool.exe
```

### Example Session

```sql
csv-query> LOAD 'sample-data/employees.csv' AS employees;
csv-query> LOAD 'sample-data/departments.csv' AS departments;

csv-query> SELECT d.name AS department, COUNT(*) AS headcount,
         >        ROUND(AVG(e.salary), 2) AS avg_salary
         > FROM employees e JOIN departments d ON e.department_id = d.id
         > GROUP BY d.name ORDER BY avg_salary DESC;

csv-query> UPDATE employees SET salary = salary * 1.10 WHERE department_id = 1;

csv-query> SELECT name, salary FROM employees
         > WHERE salary > (SELECT AVG(salary) FROM employees);

csv-query> SELECT * FROM
         >   (SELECT name, salary FROM employees WHERE salary > 80000) AS high_earners
         > ORDER BY salary DESC;

csv-query> EXPORT (SELECT * FROM employees) TO 'employees_updated.csv' FORMAT CSV;
csv-query> EXPORT (SELECT name, salary FROM employees ORDER BY salary DESC)
         >   TO 'salaries.json' FORMAT JSON;

csv-query> CREATE TABLE projects (id INTEGER, name STRING, budget DOUBLE);
csv-query> INSERT INTO projects VALUES (1, 'Alpha', 150000), (2, 'Beta', 80000);
csv-query> ALTER TABLE projects ADD COLUMN status STRING;
csv-query> ALTER TABLE projects RENAME COLUMN name TO title;

csv-query> CREATE INDEX idx_dept ON employees(department_id);
csv-query> .quit
```

---

## Commands Reference

| Command | Description |
|---------|-------------|
| `LOAD 'file.csv' AS name` | Load CSV as a queryable table |
| `CREATE TABLE t (col TYPE, ...)` | Create in-memory table |
| `INSERT INTO t [(...)] VALUES (...)` | Insert rows (positional, named, multi-row) |
| `UPDATE t SET col=expr [WHERE cond]` | Modify rows that match the WHERE condition |
| `DELETE FROM t` | Remove all rows (truncate) |
| `ALTER TABLE t ADD/DROP/RENAME ...` | Modify table schema or name |
| `EXPORT (SELECT …) TO 'file' [FORMAT CSV\|JSON]` | Write query results to a file |
| `CREATE INDEX n ON t(col)` | Create hash index |
| `DROP INDEX n` | Remove index |
| `SHOW TABLES` / `DESCRIBE t` / `SHOW INDEXES` | Metadata inspection |
| `.help` `.stats` `.clear` `.quit` | REPL controls |

See [SQL_SYNTAX_GUIDE.md](SQL_SYNTAX_GUIDE.md) for full syntax with examples.

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Hand-written recursive-descent parser** | No external tools — mirrors how production databases parse SQL |
| **`std::variant` for values** | Type-safe, zero-overhead alternative to Java's `Object` boxing; NULL = `std::monostate` |
| **`ExprKind` enum tag dispatch** | Replaces Java `instanceof` with explicit, compiler-checked switching |
| **Automatic type inference** | Samples all rows to detect `INTEGER → DOUBLE → STRING` |
| **Hash join + nested-loop fallback** | Equi-joins use O(n+m) hash approach; complex predicates fall back to O(n×m) |
| **Predicate pushdown** | Single-table WHERE filters pushed before joins |
| **Zero dependencies** | Entire engine on the C++17 stdlib |

---

## Project Structure

```
CSV Query Tool C++/
├── CMakeLists.txt
├── sample-data/
│   ├── employees.csv
│   ├── departments.csv
│   └── salaries.csv
└── src/
    ├── main.cpp
    ├── CsvQueryEngine.hpp / .cpp   # REPL entry point
    ├── parser/
    │   ├── TokenType.hpp
    │   ├── Token.hpp
    │   ├── Lexer.hpp / .cpp        # SQL tokenizer
    │   ├── SQLParser.hpp / .cpp    # Recursive-descent parser
    │   └── ast/
    │       ├── Expr.hpp            # All AST expression types
    │       ├── SelectStatement.hpp
    │       └── Command.hpp
    ├── executor/
    │   ├── QueryResult.hpp
    │   ├── ExpressionEvaluator.hpp / .cpp
    │   ├── JoinExecutor.hpp / .cpp
    │   └── QueryExecutor.hpp / .cpp
    ├── storage/
    │   ├── DataType.hpp
    │   ├── Value.hpp               # std::variant-based universal value
    │   ├── Column.hpp
    │   ├── Row.hpp
    │   ├── Schema.hpp / .cpp
    │   ├── CsvTable.hpp / .cpp
    │   └── TableRegistry.hpp
    ├── optimizer/
    │   └── QueryOptimizer.hpp      # Predicate pushdown, index hints
    ├── index/
    │   ├── HashIndex.hpp           # O(1) hash index
    │   └── IndexManager.hpp
    └── util/
        └── TablePrinter.hpp / .cpp # ASCII table formatter
```

---

## Potential Enhancements

- [x] `CREATE TABLE` / `INSERT INTO` / `DELETE FROM`
- [x] `ALTER TABLE` (ADD/DROP/RENAME COLUMN, RENAME TABLE)
- [x] `UPDATE` with SET and WHERE
- [x] Subqueries in WHERE and FROM (scalar, `IN`, derived tables)
- [x] Export results to CSV / JSON
- [ ] `EXPLAIN` query plan visualization
- [ ] Window functions (`ROW_NUMBER`, `RANK`)
- [ ] B-Tree index for range queries

---

**Built with:** C++17 · CMake · Zero runtime dependencies

**License:** MIT
