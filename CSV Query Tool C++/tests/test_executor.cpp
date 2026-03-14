/**
 * Executor layer integration tests.
 *
 * All tests operate through CsvQueryEngine::executeQuery() so that DDL/DML
 * commands (INSERT, CREATE, DELETE, ALTER) work exactly as in production.
 *
 * The engine is re-created for each test via a fixture helper so tests are
 * fully isolated.
 */
#include "test_framework.hpp"
#include "CsvQueryEngine.hpp"
#include "storage/Value.hpp"

#include <fstream>
#include <functional>
#include <string>
#include <vector>
#include <stdexcept>

// ─────────────────────────────────────────────────────────────
//  Test fixture: an engine pre-loaded with employees & departments
// ─────────────────────────────────────────────────────────────
//
//  employees:  id, name, department_id, salary, is_active
//    1, Alice,   1, 85000, true
//    2, Bob,     2, 72000, true
//    3, Charlie, 1, 92000, true
//    4, Diana,   3, 68000, false
//    5, Eve,     2, 78000, true
//
//  departments: id, name, location
//    1, Engineering, SF
//    2, Marketing,   NY
//    3, Sales,       Chicago

static void setupEngine(CsvQueryEngine& engine) {
    engine.executeQuery("CREATE TABLE employees (id INTEGER, name STRING, department_id INTEGER, salary INTEGER, is_active BOOLEAN);");
    engine.executeQuery("INSERT INTO employees VALUES (1,'Alice',1,85000,true);");
    engine.executeQuery("INSERT INTO employees VALUES (2,'Bob',2,72000,true);");
    engine.executeQuery("INSERT INTO employees VALUES (3,'Charlie',1,92000,true);");
    engine.executeQuery("INSERT INTO employees VALUES (4,'Diana',3,68000,false);");
    engine.executeQuery("INSERT INTO employees VALUES (5,'Eve',2,78000,true);");

    engine.executeQuery("CREATE TABLE departments (id INTEGER, name STRING, location STRING);");
    engine.executeQuery("INSERT INTO departments VALUES (1,'Engineering','SF');");
    engine.executeQuery("INSERT INTO departments VALUES (2,'Marketing','NY');");
    engine.executeQuery("INSERT INTO departments VALUES (3,'Sales','Chicago');");
}

// Helper: get the string representation of cell (row, col)
static std::string cell(const QueryResult& r, int row, int col) {
    return valueToString(r.rows[row][col]);
}

// Helper: get integer value from cell
static long long icell(const QueryResult& r, int row, int col) {
    return std::get<long long>(r.rows[row][col]);
}

// ─────────────────────────────────────────────────────────────
//  Basic SELECT
// ─────────────────────────────────────────────────────────────

TEST("Executor: SELECT * returns all rows and columns") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT * FROM employees;");
    ASSERT_EQ(r.getRowCount(), 5);
    ASSERT_EQ((int)r.columnNames.size(), 5);
}

TEST("Executor: SELECT named columns") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name, salary FROM employees;");
    ASSERT_EQ(r.getRowCount(), 5);
    ASSERT_EQ((int)r.columnNames.size(), 2);
    ASSERT_EQ(r.columnNames[0], std::string("name"));
    ASSERT_EQ(r.columnNames[1], std::string("salary"));
}

// ─────────────────────────────────────────────────────────────
//  WHERE clauses
// ─────────────────────────────────────────────────────────────

TEST("Executor: WHERE salary > 80000 returns Alice and Charlie") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name FROM employees WHERE salary > 80000;");
    ASSERT_EQ(r.getRowCount(), 2);
    ASSERT_EQ(cell(r, 0, 0), std::string("Alice"));
    ASSERT_EQ(cell(r, 1, 0), std::string("Charlie"));
}

TEST("Executor: WHERE salary > 70000 AND department_id = 1") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name FROM employees WHERE salary > 70000 AND department_id = 1;");
    ASSERT_EQ(r.getRowCount(), 2);
}

TEST("Executor: WHERE department_id = 1 OR department_id = 3") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name FROM employees WHERE department_id = 1 OR department_id = 3;");
    ASSERT_EQ(r.getRowCount(), 3);
}

TEST("Executor: WHERE name LIKE 'A%'") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name FROM employees WHERE name LIKE 'A%';");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(cell(r, 0, 0), std::string("Alice"));
}

TEST("Executor: WHERE salary BETWEEN 70000 AND 85000") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name FROM employees WHERE salary BETWEEN 70000 AND 85000;");
    ASSERT_EQ(r.getRowCount(), 3);  // Alice 85k, Bob 72k, Eve 78k
}

TEST("Executor: WHERE department_id IN (1, 3)") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name FROM employees WHERE department_id IN (1, 3);");
    ASSERT_EQ(r.getRowCount(), 3);  // Alice, Charlie, Diana
}

TEST("Executor: WHERE name IS NOT NULL returns all rows") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name FROM employees WHERE name IS NOT NULL;");
    ASSERT_EQ(r.getRowCount(), 5);
}

// ─────────────────────────────────────────────────────────────
//  ORDER BY / LIMIT
// ─────────────────────────────────────────────────────────────

TEST("Executor: ORDER BY salary ASC - first row is Diana") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name FROM employees ORDER BY salary ASC;");
    ASSERT_EQ(r.getRowCount(), 5);
    ASSERT_EQ(cell(r, 0, 0), std::string("Diana"));
}

TEST("Executor: ORDER BY salary DESC LIMIT 3 - first row is Charlie") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name FROM employees ORDER BY salary DESC LIMIT 3;");
    ASSERT_EQ(r.getRowCount(), 3);
    ASSERT_EQ(cell(r, 0, 0), std::string("Charlie"));
}

// ─────────────────────────────────────────────────────────────
//  DISTINCT
// ─────────────────────────────────────────────────────────────

TEST("Executor: SELECT DISTINCT department_id returns 3 rows") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT DISTINCT department_id FROM employees;");
    ASSERT_EQ(r.getRowCount(), 3);
}

// ─────────────────────────────────────────────────────────────
//  Aggregate functions
// ─────────────────────────────────────────────────────────────

TEST("Executor: COUNT(*) = 5") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT COUNT(*) FROM employees;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(icell(r, 0, 0), 5LL);
}

TEST("Executor: SUM(salary) = 395000") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT SUM(salary) FROM employees;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(icell(r, 0, 0), 395000LL);
}

TEST("Executor: AVG(salary) = 79000") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT AVG(salary) FROM employees;");
    ASSERT_EQ(r.getRowCount(), 1);
    // AVG may be stored as double or integer depending on type
    const Value& v = r.rows[0][0];
    double avg = std::holds_alternative<double>(v)
                   ? std::get<double>(v)
                   : (double)std::get<long long>(v);
    ASSERT_NEAR(avg, 79000.0, 1.0);
}

TEST("Executor: MIN(salary) = 68000") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT MIN(salary) FROM employees;");
    ASSERT_EQ(icell(r, 0, 0), 68000LL);
}

TEST("Executor: MAX(salary) = 92000") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT MAX(salary) FROM employees;");
    ASSERT_EQ(icell(r, 0, 0), 92000LL);
}

// ─────────────────────────────────────────────────────────────
//  GROUP BY / HAVING
// ─────────────────────────────────────────────────────────────

TEST("Executor: GROUP BY department_id gives 3 groups") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT department_id, COUNT(*) FROM employees GROUP BY department_id;");
    ASSERT_EQ(r.getRowCount(), 3);
}

TEST("Executor: GROUP BY HAVING COUNT(*) >= 2 gives 2 groups") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery(
        "SELECT department_id, COUNT(*) FROM employees "
        "GROUP BY department_id HAVING COUNT(*) >= 2;");
    ASSERT_EQ(r.getRowCount(), 2);
}

TEST("Executor: COUNT(DISTINCT department_id) = 3") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT COUNT(DISTINCT department_id) FROM employees;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(icell(r, 0, 0), 3LL);
}

// ─────────────────────────────────────────────────────────────
//  JOINs
// ─────────────────────────────────────────────────────────────

TEST("Executor: INNER JOIN employees x departments = 5 rows") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery(
        "SELECT e.name, d.name FROM employees e "
        "INNER JOIN departments d ON e.department_id = d.id;");
    ASSERT_EQ(r.getRowCount(), 5);
}

TEST("Executor: LEFT JOIN employees x departments = 5 rows") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery(
        "SELECT e.name, d.name FROM employees e "
        "LEFT JOIN departments d ON e.department_id = d.id;");
    ASSERT_EQ(r.getRowCount(), 5);
}

TEST("Executor: JOIN + WHERE filters to SF location (2 rows)") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery(
        "SELECT e.name FROM employees e "
        "INNER JOIN departments d ON e.department_id = d.id "
        "WHERE d.location = 'SF';");
    ASSERT_EQ(r.getRowCount(), 2);
}

TEST("Executor: JOIN + GROUP BY d.name gives 3 groups") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery(
        "SELECT d.name, COUNT(*) FROM employees e "
        "INNER JOIN departments d ON e.department_id = d.id "
        "GROUP BY d.name;");
    ASSERT_EQ(r.getRowCount(), 3);
}

// ─────────────────────────────────────────────────────────────
//  Scalar functions and expressions
// ─────────────────────────────────────────────────────────────

TEST("Executor: UPPER(name) for Alice returns ALICE") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT UPPER(name) FROM employees WHERE id = 1;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(cell(r, 0, 0), std::string("ALICE"));
}

TEST("Executor: LENGTH(name) for Alice returns 5") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT LENGTH(name) FROM employees WHERE id = 1;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(icell(r, 0, 0), 5LL);
}

TEST("Executor: column alias appears in result") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT name AS employee_name FROM employees WHERE id = 1;");
    ASSERT_EQ(r.columnNames[0], std::string("employee_name"));
}

TEST("Executor: arithmetic salary * 12 for Alice = 1020000") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("SELECT salary * 12 FROM employees WHERE id = 1;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(icell(r, 0, 0), 1020000LL);
}

// ─────────────────────────────────────────────────────────────
//  Index usage
// ─────────────────────────────────────────────────────────────

TEST("Executor: index on department_id returns 2 rows for dept 1") {
    CsvQueryEngine engine;
    setupEngine(engine);
    engine.executeQuery("CREATE INDEX idx_dept ON employees(department_id);");
    auto r = engine.executeQuery("SELECT name FROM employees WHERE department_id = 1;");
    ASSERT_EQ(r.getRowCount(), 2);
}

// ─────────────────────────────────────────────────────────────
//  CREATE TABLE + INSERT + SELECT
// ─────────────────────────────────────────────────────────────

TEST("Executor: CREATE TABLE, INSERT three rows, SELECT * returns 3") {
    CsvQueryEngine engine;
    engine.executeQuery("CREATE TABLE products (name STRING, price DOUBLE, quantity INTEGER);");
    engine.executeQuery("INSERT INTO products VALUES ('Apple', 1.5, 10);");
    engine.executeQuery("INSERT INTO products VALUES ('Banana', 0.75, 25);");
    engine.executeQuery("INSERT INTO products VALUES ('Cherry', 2.0, 20);");
    auto r = engine.executeQuery("SELECT * FROM products;");
    ASSERT_EQ(r.getRowCount(), 3);
}

TEST("Executor: SELECT with WHERE on created table") {
    CsvQueryEngine engine;
    engine.executeQuery("CREATE TABLE products (name STRING, price DOUBLE, quantity INTEGER);");
    engine.executeQuery("INSERT INTO products VALUES ('Apple', 1.5, 10);");
    engine.executeQuery("INSERT INTO products VALUES ('Banana', 0.75, 25);");
    engine.executeQuery("INSERT INTO products VALUES ('Cherry', 2.0, 20);");
    auto r = engine.executeQuery("SELECT name, quantity FROM products WHERE price > 1.0;");
    ASSERT_EQ(r.getRowCount(), 2);
    // Apple and Cherry both have price > 1.0
}

TEST("Executor: AVG aggregate on created table") {
    CsvQueryEngine engine;
    engine.executeQuery("CREATE TABLE scores (name STRING, score DOUBLE);");
    engine.executeQuery("INSERT INTO scores VALUES ('Alice', 90.0);");
    engine.executeQuery("INSERT INTO scores VALUES ('Bob', 80.0);");
    engine.executeQuery("INSERT INTO scores VALUES ('Charlie', 70.0);");
    auto r = engine.executeQuery("SELECT AVG(score) FROM scores;");
    ASSERT_EQ(r.getRowCount(), 1);
    double avg = std::holds_alternative<double>(r.rows[0][0])
                   ? std::get<double>(r.rows[0][0])
                   : (double)std::get<long long>(r.rows[0][0]);
    ASSERT_NEAR(avg, 80.0, 0.001);
}

// ─────────────────────────────────────────────────────────────
//  DELETE
// ─────────────────────────────────────────────────────────────

TEST("Executor: DELETE FROM clears all rows") {
    CsvQueryEngine engine;
    engine.executeQuery("CREATE TABLE tmp (id INTEGER, val STRING);");
    engine.executeQuery("INSERT INTO tmp VALUES (1, 'a');");
    engine.executeQuery("INSERT INTO tmp VALUES (2, 'b');");
    engine.executeQuery("DELETE FROM tmp;");
    auto r = engine.executeQuery("SELECT * FROM tmp;");
    ASSERT_EQ(r.getRowCount(), 0);
}

// ─────────────────────────────────────────────────────────────
//  JOIN with a created table
// ─────────────────────────────────────────────────────────────

TEST("Executor: JOIN employees with created roles table") {
    CsvQueryEngine engine;
    setupEngine(engine);
    engine.executeQuery("CREATE TABLE roles (employee_id INTEGER, role STRING);");
    engine.executeQuery("INSERT INTO roles VALUES (1, 'Lead');");
    engine.executeQuery("INSERT INTO roles VALUES (2, 'Senior');");
    engine.executeQuery("INSERT INTO roles VALUES (3, 'Lead');");
    auto r = engine.executeQuery(
        "SELECT e.name, r.role FROM employees e "
        "INNER JOIN roles r ON e.id = r.employee_id;");
    ASSERT_EQ(r.getRowCount(), 3);
    ASSERT_EQ(cell(r, 0, 0), std::string("Alice"));
    ASSERT_EQ(cell(r, 0, 1), std::string("Lead"));
}

// ─────────────────────────────────────────────────────────────
//  ALTER TABLE DDL
// ─────────────────────────────────────────────────────────────

TEST("Executor: ALTER TABLE ADD COLUMN - new col is NULL for existing rows") {
    CsvQueryEngine engine;
    setupEngine(engine);
    engine.executeQuery("ALTER TABLE employees ADD COLUMN bonus INTEGER;");
    auto r = engine.executeQuery("SELECT name, bonus FROM employees WHERE id = 1;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ((int)r.columnNames.size(), 2);
    // bonus should be NULL
    ASSERT_TRUE(std::holds_alternative<std::monostate>(r.rows[0][1]));
}

TEST("Executor: ALTER TABLE DROP COLUMN") {
    CsvQueryEngine engine;
    setupEngine(engine);
    engine.executeQuery("ALTER TABLE employees DROP COLUMN is_active;");
    auto r = engine.executeQuery("SELECT * FROM employees WHERE id = 1;");
    ASSERT_EQ((int)r.columnNames.size(), 4);   // was 5, now 4
}

TEST("Executor: ALTER TABLE RENAME COLUMN preserves data") {
    CsvQueryEngine engine;
    setupEngine(engine);
    engine.executeQuery("ALTER TABLE employees RENAME COLUMN salary TO compensation;");
    auto r = engine.executeQuery("SELECT compensation FROM employees WHERE id = 1;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(icell(r, 0, 0), 85000LL);
}

TEST("Executor: ALTER TABLE RENAME TO - new name works, old name throws") {
    CsvQueryEngine engine;
    setupEngine(engine);
    engine.executeQuery("ALTER TABLE departments RENAME TO divisions;");
    auto ok = engine.executeQuery("SELECT * FROM divisions;");
    ASSERT_EQ(ok.getRowCount(), 3);
    ASSERT_THROWS(engine.executeQuery("SELECT * FROM departments;"));
}
// ─────────────────────────────────────────────────────────────
//  UPDATE
// ─────────────────────────────────────────────────────────────

TEST("Executor: UPDATE without WHERE updates all rows") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("UPDATE employees SET salary = 50000;");
    ASSERT_EQ(r.message, std::string("5 row(s) updated in 'employees'."));
    auto all = engine.executeQuery("SELECT salary FROM employees;");
    for (int i = 0; i < 5; i++)
        ASSERT_EQ(icell(all, i, 0), 50000LL);
}

TEST("Executor: UPDATE with WHERE only touches matching rows") {
    CsvQueryEngine engine;
    setupEngine(engine);
    engine.executeQuery("UPDATE employees SET salary = 99000 WHERE name = 'Alice';");
    auto alice = engine.executeQuery("SELECT salary FROM employees WHERE name = 'Alice';");
    ASSERT_EQ(icell(alice, 0, 0), 99000LL);
    // Bob's salary unchanged
    auto bob = engine.executeQuery("SELECT salary FROM employees WHERE name = 'Bob';");
    ASSERT_EQ(icell(bob, 0, 0), 72000LL);
}

TEST("Executor: UPDATE returns correct row count") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery("UPDATE employees SET salary = 70000 WHERE department_id = 1;");
    ASSERT_EQ(r.message, std::string("2 row(s) updated in 'employees'."));
}

TEST("Executor: UPDATE multiple SET columns") {
    CsvQueryEngine engine;
    setupEngine(engine);
    engine.executeQuery("UPDATE employees SET salary = 60000, is_active = false WHERE id = 2;");
    auto r = engine.executeQuery("SELECT salary, is_active FROM employees WHERE id = 2;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(icell(r, 0, 0), 60000LL);
    ASSERT_EQ(std::get<bool>(r.rows[0][1]), false);
}

TEST("Executor: UPDATE with arithmetic expression") {
    CsvQueryEngine engine;
    setupEngine(engine);
    engine.executeQuery("UPDATE employees SET salary = salary * 2 WHERE id = 1;");
    auto r = engine.executeQuery("SELECT salary FROM employees WHERE id = 1;");
    ASSERT_EQ(icell(r, 0, 0), 170000LL);
}

// ─────────────────────────────────────────────────────────────
//  Subqueries
// ─────────────────────────────────────────────────────────────

TEST("Executor: subquery IN WHERE (IN subquery)") {
    CsvQueryEngine engine;
    setupEngine(engine);
    // Employees whose department_id is in the set of departments in NY
    auto r = engine.executeQuery(
        "SELECT name FROM employees "
        "WHERE department_id IN (SELECT id FROM departments WHERE location = 'NY');");
    ASSERT_EQ(r.getRowCount(), 2); // Bob (dept 2) and Eve (dept 2)
    ASSERT_EQ(cell(r, 0, 0), std::string("Bob"));
    ASSERT_EQ(cell(r, 1, 0), std::string("Eve"));
}

TEST("Executor: subquery NOT IN WHERE") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery(
        "SELECT name FROM employees "
        "WHERE department_id NOT IN (SELECT id FROM departments WHERE location = 'NY') "
        "ORDER BY id;");
    ASSERT_EQ(r.getRowCount(), 3); // Alice, Charlie, Diana
    ASSERT_EQ(cell(r, 0, 0), std::string("Alice"));
}

TEST("Executor: scalar subquery in WHERE") {
    CsvQueryEngine engine;
    setupEngine(engine);
    // Employees earning more than Alice (85000)
    auto r = engine.executeQuery(
        "SELECT name FROM employees "
        "WHERE salary > (SELECT salary FROM employees WHERE name = 'Alice') "
        "ORDER BY salary;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(cell(r, 0, 0), std::string("Charlie")); // 92000
}

TEST("Executor: scalar subquery AVG in WHERE") {
    CsvQueryEngine engine;
    setupEngine(engine);
    // AVG = 395000/5 = 79000; employees with salary > 79000
    auto r = engine.executeQuery(
        "SELECT name FROM employees "
        "WHERE salary > (SELECT AVG(salary) FROM employees) "
        "ORDER BY salary;");
    ASSERT_EQ(r.getRowCount(), 2); // Alice 85000, Charlie 92000
    ASSERT_EQ(cell(r, 0, 0), std::string("Alice"));
    ASSERT_EQ(cell(r, 1, 0), std::string("Charlie"));
}

TEST("Executor: derived table subquery in FROM") {
    CsvQueryEngine engine;
    setupEngine(engine);
    auto r = engine.executeQuery(
        "SELECT sub.name, sub.salary "
        "FROM (SELECT name, salary FROM employees WHERE salary > 80000) AS sub "
        "ORDER BY sub.salary;");
    ASSERT_EQ(r.getRowCount(), 2);
    ASSERT_EQ(cell(r, 0, 0), std::string("Alice"));
    ASSERT_EQ(cell(r, 1, 0), std::string("Charlie"));
}

TEST("Executor: derived table with aggregate") {
    CsvQueryEngine engine;
    setupEngine(engine);
    // Count how many departments appear in the derived table
    auto r = engine.executeQuery(
        "SELECT COUNT(*) FROM "
        "(SELECT DISTINCT department_id FROM employees) AS sub;");
    ASSERT_EQ(r.getRowCount(), 1);
    ASSERT_EQ(icell(r, 0, 0), 3LL);
}

// ─────────────────────────────────────────────────────────────
//  EXPORT
// ─────────────────────────────────────────────────────────────

static std::string readFile(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open()) return "";
    return std::string(std::istreambuf_iterator<char>(f),
                       std::istreambuf_iterator<char>());
}

TEST("Executor: EXPORT CSV creates file with header and data") {
    CsvQueryEngine engine;
    setupEngine(engine);
    const std::string outPath = "test_export_out.csv";
    std::remove(outPath.c_str());
    engine.executeQuery(
        "EXPORT (SELECT id, name FROM employees ORDER BY id) TO '" + outPath + "' FORMAT CSV;");
    std::string content = readFile(outPath);
    ASSERT_FALSE(content.empty());
    // Header line
    ASSERT_TRUE(content.find("id,name") != std::string::npos ||
                content.find("id") != std::string::npos);
    // Data rows
    ASSERT_TRUE(content.find("Alice") != std::string::npos);
    ASSERT_TRUE(content.find("Charlie") != std::string::npos);
    std::remove(outPath.c_str());
}

TEST("Executor: EXPORT JSON creates valid array") {
    CsvQueryEngine engine;
    setupEngine(engine);
    const std::string outPath = "test_export_out.json";
    std::remove(outPath.c_str());
    engine.executeQuery(
        "EXPORT (SELECT id, name FROM employees WHERE id = 1) TO '" + outPath + "' FORMAT JSON;");
    std::string content = readFile(outPath);
    ASSERT_FALSE(content.empty());
    ASSERT_TRUE(content.find("[") != std::string::npos);
    ASSERT_TRUE(content.find("\"name\"") != std::string::npos);
    ASSERT_TRUE(content.find("Alice") != std::string::npos);
    std::remove(outPath.c_str());
}

TEST("Executor: EXPORT auto-detects CSV from .csv extension") {
    CsvQueryEngine engine;
    setupEngine(engine);
    const std::string outPath = "test_export_autodetect.csv";
    std::remove(outPath.c_str());
    // No FORMAT keyword — should default to CSV
    engine.executeQuery(
        "EXPORT (SELECT name FROM employees WHERE id = 1) TO '" + outPath + "';");
    std::string content = readFile(outPath);
    ASSERT_FALSE(content.empty());
    ASSERT_TRUE(content.find("Alice") != std::string::npos);
    // Should NOT contain JSON array brackets
    ASSERT_TRUE(content.find("[") == std::string::npos);
    std::remove(outPath.c_str());
}

TEST("Executor: EXPORT auto-detects JSON from .json extension") {
    CsvQueryEngine engine;
    setupEngine(engine);
    const std::string outPath = "test_export_autodetect.json";
    std::remove(outPath.c_str());
    engine.executeQuery(
        "EXPORT (SELECT name FROM employees WHERE id = 1) TO '" + outPath + "';");
    std::string content = readFile(outPath);
    ASSERT_FALSE(content.empty());
    ASSERT_TRUE(content.find("[") != std::string::npos);
    ASSERT_TRUE(content.find("Alice") != std::string::npos);
    std::remove(outPath.c_str());
}

TEST("Executor: EXPORT to non-existent directory throws") {
    CsvQueryEngine engine;
    setupEngine(engine);
    ASSERT_THROWS(engine.executeQuery(
        "EXPORT (SELECT * FROM employees) TO 'no/such/dir/out.csv' FORMAT CSV;"));
}