/**
 * Parser layer tests:
 *   - Lexer: tokenization, string literals, operators, comments
 *   - SQLParser: SELECT, WHERE, JOIN, GROUP BY, ORDER BY, DDL, DML
 */
#include "test_framework.hpp"
#include "parser/Lexer.hpp"
#include "parser/SQLParser.hpp"
#include "parser/ast/Expr.hpp"
#include "parser/ast/SelectStatement.hpp"
#include "parser/ast/Command.hpp"

// ─────────────────────────────────────────────────────────────
//  Helpers
// ─────────────────────────────────────────────────────────────
static SelectStatement parseSelect(const std::string& sql) {
    SQLParser p(sql);
    ParseResult r = p.parse();
    return std::get<SelectStatement>(r);
}

static Command parseCommand(const std::string& sql) {
    SQLParser p(sql);
    ParseResult r = p.parse();
    return std::get<Command>(r);
}

// ─────────────────────────────────────────────────────────────
//  Lexer tests
// ─────────────────────────────────────────────────────────────

TEST("Lexer: tokenizes simple SELECT keywords and identifiers") {
    Lexer lexer("SELECT name FROM employees;");
    auto tokens = lexer.tokenize();

    ASSERT_EQ(tokens[0].type, TokenType::SELECT);
    ASSERT_EQ(tokens[1].type, TokenType::IDENTIFIER);
    ASSERT_EQ(tokens[1].value, std::string("name"));
    ASSERT_EQ(tokens[2].type, TokenType::FROM);
    ASSERT_EQ(tokens[3].type, TokenType::IDENTIFIER);
    ASSERT_EQ(tokens[3].value, std::string("employees"));
    ASSERT_EQ(tokens[4].type, TokenType::SEMICOLON);
    ASSERT_EQ(tokens[5].type, TokenType::EOF_TOK);
}

TEST("Lexer: string literal with escaped single-quote") {
    Lexer lexer("SELECT 'O''Brien' FROM t;");
    auto tokens = lexer.tokenize();

    ASSERT_EQ(tokens[1].type, TokenType::STRING_LITERAL);
    ASSERT_EQ(tokens[1].value, std::string("O'Brien"));
}

TEST("Lexer: integer and decimal literals") {
    Lexer lexer("SELECT 42, 3.14 FROM t;");
    auto tokens = lexer.tokenize();

    ASSERT_EQ(tokens[1].type, TokenType::INTEGER_LITERAL);
    ASSERT_EQ(tokens[1].value, std::string("42"));
    ASSERT_EQ(tokens[3].type, TokenType::DECIMAL_LITERAL);
    ASSERT_EQ(tokens[3].value, std::string("3.14"));
}

TEST("Lexer: multi-char comparison operators") {
    Lexer lexer("a >= b AND c != d AND e <> f AND g <= h;");
    auto tokens = lexer.tokenize();

    ASSERT_EQ(tokens[1].type, TokenType::GREATER_THAN_EQUALS);
    ASSERT_EQ(tokens[5].type, TokenType::NOT_EQUALS);
    ASSERT_EQ(tokens[9].type, TokenType::NOT_EQUALS);
    ASSERT_EQ(tokens[13].type, TokenType::LESS_THAN_EQUALS);
}

TEST("Lexer: skips line comments") {
    Lexer lexer("SELECT name -- this is a comment\nFROM employees;");
    auto tokens = lexer.tokenize();

    ASSERT_EQ(tokens[0].type, TokenType::SELECT);
    ASSERT_EQ(tokens[1].type, TokenType::IDENTIFIER);
    ASSERT_EQ(tokens[2].type, TokenType::FROM);
    ASSERT_EQ(tokens[3].type, TokenType::IDENTIFIER);
}

TEST("Lexer: quoted identifier with double-quotes") {
    Lexer lexer("SELECT \"my column\" FROM t;");
    auto tokens = lexer.tokenize();

    ASSERT_EQ(tokens[1].type, TokenType::IDENTIFIER);
    ASSERT_EQ(tokens[1].value, std::string("my column"));
}

// ─────────────────────────────────────────────────────────────
//  Parser: SELECT basics
// ─────────────────────────────────────────────────────────────

TEST("Parser: SELECT * FROM table") {
    auto stmt = parseSelect("SELECT * FROM employees;");

    ASSERT_EQ((int)stmt.selectItems.size(), 1);
    ASSERT_EQ(stmt.selectItems[0].expression->kind(), ExprKind::STAR);
    ASSERT_EQ(stmt.fromTable.tableName, std::string("employees"));
    ASSERT_FALSE((bool)stmt.whereClause);
}

TEST("Parser: SELECT specific columns") {
    auto stmt = parseSelect("SELECT name, salary FROM employees;");

    ASSERT_EQ((int)stmt.selectItems.size(), 2);
    auto* col0 = static_cast<ColumnRefExpr*>(stmt.selectItems[0].expression.get());
    ASSERT_EQ(col0->columnName, std::string("name"));
}

TEST("Parser: SELECT with WHERE comparison") {
    auto stmt = parseSelect("SELECT name, salary FROM employees WHERE salary > 80000;");

    ASSERT_EQ((int)stmt.selectItems.size(), 2);
    ASSERT_TRUE((bool)stmt.whereClause);
    ASSERT_EQ(stmt.whereClause->kind(), ExprKind::BINARY);
    auto* bin = static_cast<BinaryExpr*>(stmt.whereClause.get());
    ASSERT_EQ(bin->op, std::string(">"));
}

TEST("Parser: table alias in FROM") {
    auto stmt = parseSelect("SELECT e.name FROM employees e;");

    ASSERT_EQ(stmt.fromTable.tableName, std::string("employees"));
    ASSERT_EQ(stmt.fromTable.alias, std::string("e"));
    ASSERT_EQ(stmt.fromTable.getEffectiveName(), std::string("e"));
}

TEST("Parser: SELECT DISTINCT") {
    auto stmt = parseSelect("SELECT DISTINCT department_id FROM employees;");
    ASSERT_TRUE(stmt.distinct);
}

TEST("Parser: ORDER BY DESC with LIMIT") {
    auto stmt = parseSelect("SELECT name FROM employees ORDER BY salary DESC LIMIT 5;");

    ASSERT_EQ((int)stmt.orderBy.size(), 1);
    ASSERT_FALSE(stmt.orderBy[0].ascending);
    ASSERT_TRUE((bool)stmt.limit);
    ASSERT_EQ(*stmt.limit, 5);
}

TEST("Parser: ORDER BY ASC (default)") {
    auto stmt = parseSelect("SELECT name FROM employees ORDER BY name ASC;");
    ASSERT_TRUE(stmt.orderBy[0].ascending);
}

TEST("Parser: GROUP BY with HAVING") {
    auto stmt = parseSelect(
        "SELECT department_id, COUNT(*) FROM employees "
        "GROUP BY department_id HAVING COUNT(*) > 2;");

    ASSERT_EQ((int)stmt.groupBy.size(), 1);
    ASSERT_TRUE((bool)stmt.havingClause);
}

// ─────────────────────────────────────────────────────────────
//  Parser: JOINs
// ─────────────────────────────────────────────────────────────

TEST("Parser: INNER JOIN with ON condition") {
    auto stmt = parseSelect(
        "SELECT e.name, d.name FROM employees e "
        "INNER JOIN departments d ON e.department_id = d.id;");

    ASSERT_EQ(stmt.fromTable.alias, std::string("e"));
    ASSERT_EQ((int)stmt.joins.size(), 1);
    ASSERT_EQ(stmt.joins[0].joinType, JoinType::INNER);
    ASSERT_EQ(stmt.joins[0].table.tableName, std::string("departments"));
    ASSERT_EQ(stmt.joins[0].table.alias, std::string("d"));
    ASSERT_TRUE((bool)stmt.joins[0].onCondition);
}

TEST("Parser: LEFT JOIN") {
    auto stmt = parseSelect(
        "SELECT * FROM employees e LEFT JOIN departments d ON e.department_id = d.id;");

    ASSERT_EQ((int)stmt.joins.size(), 1);
    ASSERT_EQ(stmt.joins[0].joinType, JoinType::LEFT);
}

TEST("Parser: JOIN without INNER keyword") {
    auto stmt = parseSelect(
        "SELECT * FROM employees e JOIN departments d ON e.department_id = d.id;");

    ASSERT_EQ((int)stmt.joins.size(), 1);
    ASSERT_EQ(stmt.joins[0].joinType, JoinType::INNER);
}

// ─────────────────────────────────────────────────────────────
//  Parser: expression types
// ─────────────────────────────────────────────────────────────

TEST("Parser: BETWEEN expression") {
    auto stmt = parseSelect("SELECT * FROM employees WHERE salary BETWEEN 70000 AND 90000;");

    ASSERT_EQ(stmt.whereClause->kind(), ExprKind::BETWEEN);
    auto* be = static_cast<BetweenExpr*>(stmt.whereClause.get());
    ASSERT_FALSE(be->negated);
}

TEST("Parser: NOT BETWEEN") {
    auto stmt = parseSelect("SELECT * FROM employees WHERE salary NOT BETWEEN 70000 AND 90000;");

    ASSERT_EQ(stmt.whereClause->kind(), ExprKind::BETWEEN);
    ASSERT_TRUE(static_cast<BetweenExpr*>(stmt.whereClause.get())->negated);
}

TEST("Parser: IN expression with 3 values") {
    auto stmt = parseSelect("SELECT * FROM employees WHERE department_id IN (1, 2, 3);");

    ASSERT_EQ(stmt.whereClause->kind(), ExprKind::IN_EXPR);
    auto* ie = static_cast<InExpr*>(stmt.whereClause.get());
    ASSERT_EQ((int)ie->values.size(), 3);
    ASSERT_FALSE(ie->negated);
}

TEST("Parser: NOT IN expression") {
    auto stmt = parseSelect("SELECT * FROM employees WHERE department_id NOT IN (1, 2);");

    ASSERT_EQ(stmt.whereClause->kind(), ExprKind::IN_EXPR);
    ASSERT_TRUE(static_cast<InExpr*>(stmt.whereClause.get())->negated);
}

TEST("Parser: IS NOT NULL") {
    auto stmt = parseSelect("SELECT * FROM employees WHERE name IS NOT NULL;");

    ASSERT_EQ(stmt.whereClause->kind(), ExprKind::IS_NULL);
    auto* isn = static_cast<IsNullExpr*>(stmt.whereClause.get());
    ASSERT_TRUE(isn->negated);
}

TEST("Parser: IS NULL") {
    auto stmt = parseSelect("SELECT * FROM employees WHERE name IS NULL;");

    ASSERT_EQ(stmt.whereClause->kind(), ExprKind::IS_NULL);
    ASSERT_FALSE(static_cast<IsNullExpr*>(stmt.whereClause.get())->negated);
}

TEST("Parser: LIKE expression") {
    auto stmt = parseSelect("SELECT * FROM employees WHERE name LIKE 'A%';");

    ASSERT_EQ(stmt.whereClause->kind(), ExprKind::BINARY);
    auto* bin = static_cast<BinaryExpr*>(stmt.whereClause.get());
    ASSERT_EQ(bin->op, std::string("LIKE"));
}

TEST("Parser: complex WHERE with AND/OR") {
    auto stmt = parseSelect(
        "SELECT * FROM employees WHERE (salary > 80000 AND department_id = 1) OR is_active = true;");

    ASSERT_EQ(stmt.whereClause->kind(), ExprKind::BINARY);
    auto* orExpr = static_cast<BinaryExpr*>(stmt.whereClause.get());
    ASSERT_EQ(orExpr->op, std::string("OR"));
}

TEST("Parser: NOT expression") {
    auto stmt = parseSelect("SELECT * FROM employees WHERE NOT is_active = true;");

    ASSERT_EQ(stmt.whereClause->kind(), ExprKind::NOT_EXPR);
}

// ─────────────────────────────────────────────────────────────
//  Parser: aggregate functions
// ─────────────────────────────────────────────────────────────

TEST("Parser: COUNT(*), SUM, AVG, MIN, MAX") {
    auto stmt = parseSelect(
        "SELECT COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM employees;");

    ASSERT_EQ((int)stmt.selectItems.size(), 5);
    for (const auto& item : stmt.selectItems) {
        ASSERT_EQ(item.expression->kind(), ExprKind::FUNCTION_CALL);
        auto* fc = static_cast<FunctionCallExpr*>(item.expression.get());
        ASSERT_TRUE(fc->isAggregate());
    }
}

TEST("Parser: COUNT(DISTINCT col)") {
    auto stmt = parseSelect("SELECT COUNT(DISTINCT department_id) FROM employees;");

    auto* fc = static_cast<FunctionCallExpr*>(stmt.selectItems[0].expression.get());
    ASSERT_EQ(fc->name, std::string("COUNT"));
    ASSERT_TRUE(fc->distinct);
}

TEST("Parser: arithmetic expression in SELECT") {
    auto stmt = parseSelect("SELECT salary * 12 AS annual FROM employees;");

    ASSERT_EQ(stmt.selectItems[0].alias, std::string("annual"));
    ASSERT_EQ(stmt.selectItems[0].expression->kind(), ExprKind::BINARY);
    auto* bin = static_cast<BinaryExpr*>(stmt.selectItems[0].expression.get());
    ASSERT_EQ(bin->op, std::string("*"));
}

TEST("Parser: qualified column reference (table.col)") {
    auto stmt = parseSelect("SELECT e.name, e.salary FROM employees e;");

    auto* cr = static_cast<ColumnRefExpr*>(stmt.selectItems[0].expression.get());
    ASSERT_EQ(cr->tableName, std::string("e"));
    ASSERT_EQ(cr->columnName, std::string("name"));
}

// ─────────────────────────────────────────────────────────────
//  Parser: DDL / DML commands
// ─────────────────────────────────────────────────────────────

TEST("Parser: LOAD command") {
    auto cmd = parseCommand("LOAD 'data/employees.csv' AS employees;");

    ASSERT_EQ(cmd.type, CommandType::LOAD_TABLE);
    ASSERT_EQ(cmd.getParam("filePath"), std::string("data/employees.csv"));
    ASSERT_EQ(cmd.getParam("tableName"), std::string("employees"));
}

TEST("Parser: SHOW TABLES command") {
    auto cmd = parseCommand("SHOW TABLES;");
    ASSERT_EQ(cmd.type, CommandType::SHOW_TABLES);
}

TEST("Parser: DESCRIBE command") {
    auto cmd = parseCommand("DESCRIBE employees;");
    ASSERT_EQ(cmd.type, CommandType::DESCRIBE_TABLE);
    ASSERT_EQ(cmd.getParam("tableName"), std::string("employees"));
}

TEST("Parser: CREATE TABLE with column definitions") {
    auto cmd = parseCommand("CREATE TABLE users (id INTEGER, name STRING, salary DOUBLE, active BOOLEAN);");

    ASSERT_EQ(cmd.type, CommandType::CREATE_TABLE);
    ASSERT_EQ(cmd.getParam("tableName"), std::string("users"));
    ASSERT_EQ((int)cmd.columnNames.size(), 4);
    ASSERT_EQ(cmd.columnNames[0], std::string("id"));
    ASSERT_EQ(cmd.columnTypes[0], std::string("INTEGER"));
    ASSERT_EQ(cmd.columnTypes[1], std::string("STRING"));
    ASSERT_EQ(cmd.columnTypes[2], std::string("DOUBLE"));
    ASSERT_EQ(cmd.columnTypes[3], std::string("BOOLEAN"));
}

TEST("Parser: INSERT INTO positional VALUES") {
    auto cmd = parseCommand("INSERT INTO users VALUES (1, 'Alice', 85000.0, true);");

    ASSERT_EQ(cmd.type, CommandType::INSERT_INTO);
    ASSERT_EQ(cmd.getParam("tableName"), std::string("users"));
    ASSERT_TRUE(cmd.columnNames.empty()); // positional
    ASSERT_EQ((int)cmd.valueRows.size(), 1);
    ASSERT_EQ((int)cmd.valueRows[0].size(), 4);
    ASSERT_EQ(cmd.valueRows[0][0], std::string("1"));
    ASSERT_EQ(cmd.valueRows[0][1], std::string("Alice"));
}

TEST("Parser: INSERT INTO with column list") {
    auto cmd = parseCommand("INSERT INTO users (name, salary) VALUES ('Bob', 72000);");

    ASSERT_EQ(cmd.type, CommandType::INSERT_INTO);
    ASSERT_EQ((int)cmd.columnNames.size(), 2);
    ASSERT_EQ(cmd.columnNames[0], std::string("name"));
    ASSERT_EQ(cmd.columnNames[1], std::string("salary"));
}

TEST("Parser: INSERT INTO with multiple value rows") {
    auto cmd = parseCommand("INSERT INTO users VALUES (1, 'Alice', 85000, true), (2, 'Bob', 72000, false);");

    ASSERT_EQ((int)cmd.valueRows.size(), 2);
    ASSERT_EQ(cmd.valueRows[0][0], std::string("1"));
    ASSERT_EQ(cmd.valueRows[1][0], std::string("2"));
}

TEST("Parser: DELETE FROM") {
    auto cmd = parseCommand("DELETE FROM users;");

    ASSERT_EQ(cmd.type, CommandType::DELETE_FROM);
    ASSERT_EQ(cmd.getParam("tableName"), std::string("users"));
}

TEST("Parser: CREATE INDEX") {
    auto cmd = parseCommand("CREATE INDEX idx_dept ON employees(department_id);");

    ASSERT_EQ(cmd.type, CommandType::CREATE_INDEX);
    ASSERT_EQ(cmd.getParam("indexName"), std::string("idx_dept"));
    ASSERT_EQ(cmd.getParam("tableName"), std::string("employees"));
    ASSERT_EQ(cmd.getParam("columnName"), std::string("department_id"));
}

TEST("Parser: DROP INDEX") {
    auto cmd = parseCommand("DROP INDEX idx_dept;");
    ASSERT_EQ(cmd.type, CommandType::DROP_INDEX);
    ASSERT_EQ(cmd.getParam("indexName"), std::string("idx_dept"));
}

TEST("Parser: ALTER TABLE ADD COLUMN") {
    auto cmd = parseCommand("ALTER TABLE users ADD COLUMN email STRING;");

    ASSERT_EQ(cmd.type, CommandType::ALTER_ADD_COLUMN);
    ASSERT_EQ(cmd.getParam("tableName"), std::string("users"));
    ASSERT_EQ(cmd.getParam("columnName"), std::string("email"));
    ASSERT_EQ(cmd.getParam("columnType"), std::string("STRING"));
}

TEST("Parser: ALTER TABLE ADD without COLUMN keyword") {
    auto cmd = parseCommand("ALTER TABLE users ADD age INTEGER;");
    ASSERT_EQ(cmd.type, CommandType::ALTER_ADD_COLUMN);
    ASSERT_EQ(cmd.getParam("columnName"), std::string("age"));
    ASSERT_EQ(cmd.getParam("columnType"), std::string("INTEGER"));
}

TEST("Parser: ALTER TABLE DROP COLUMN") {
    auto cmd = parseCommand("ALTER TABLE users DROP COLUMN email;");
    ASSERT_EQ(cmd.type, CommandType::ALTER_DROP_COLUMN);
    ASSERT_EQ(cmd.getParam("columnName"), std::string("email"));
}

TEST("Parser: ALTER TABLE RENAME COLUMN") {
    auto cmd = parseCommand("ALTER TABLE users RENAME COLUMN name TO full_name;");
    ASSERT_EQ(cmd.type, CommandType::ALTER_RENAME_COLUMN);
    ASSERT_EQ(cmd.getParam("oldName"), std::string("name"));
    ASSERT_EQ(cmd.getParam("newName"), std::string("full_name"));
}

TEST("Parser: ALTER TABLE RENAME TO (table rename)") {
    auto cmd = parseCommand("ALTER TABLE users RENAME TO people;");
    ASSERT_EQ(cmd.type, CommandType::ALTER_RENAME_TABLE);
    ASSERT_EQ(cmd.getParam("tableName"), std::string("users"));
    ASSERT_EQ(cmd.getParam("newName"), std::string("people"));
}

TEST("Parser: invalid SQL throws exception") {
    ASSERT_THROWS(SQLParser("GIBBERISH blah;").parse());
}
