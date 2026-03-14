#include "SQLParser.hpp"
#include <algorithm>
#include <stdexcept>

// ──────────────────────────────────────────────────────────────
//  Construction
// ──────────────────────────────────────────────────────────────

SQLParser::SQLParser(const std::vector<Token>& tokens) : tokens_(tokens), pos_(0) {}

SQLParser::SQLParser(const std::string& sql)
    : SQLParser(Lexer(sql).tokenize()) {}

// ──────────────────────────────────────────────────────────────
//  Top-level parse
// ──────────────────────────────────────────────────────────────

ParseResult SQLParser::parse() {
    switch (peek().type) {
        case TokenType::SELECT:   return parseSelect();
        case TokenType::LOAD:     return parseLoad();
        case TokenType::SHOW:     return parseShow();
        case TokenType::DESCRIBE: return parseDescribe();
        case TokenType::CREATE:   return parseCreate();
        case TokenType::DROP:     return parseDrop();
        case TokenType::INSERT:   return parseInsert();
        case TokenType::DELETE:   return parseDelete();
        case TokenType::ALTER:    return parseAlter();
        case TokenType::UPDATE:   return parseUpdate();
        case TokenType::EXPORT:   return parseExport();
        default:
            throw makeError("Expected SELECT, UPDATE, EXPORT, LOAD, SHOW, DESCRIBE, CREATE, "
                            "DROP, INSERT, DELETE, or ALTER but got " + peek().value);
    }
}

// ──────────────────────────────────────────────────────────────
//  SELECT statement
// ──────────────────────────────────────────────────────────────

SelectStatement SQLParser::parseSelect() {
    consume(TokenType::SELECT);
    SelectStatement stmt;

    if (check(TokenType::DISTINCT)) { advance(); stmt.distinct = true; }

    stmt.selectItems = parseSelectList();

    consume(TokenType::FROM);
    stmt.fromTable = parseTableRef();

    while (isJoinKeyword())
        stmt.joins.push_back(parseJoinClause());

    if (check(TokenType::WHERE)) { advance(); stmt.whereClause = parseExpression(); }

    if (check(TokenType::GROUP)) {
        advance(); consume(TokenType::BY);
        stmt.groupBy = parseExpressionList();
    }

    if (check(TokenType::HAVING)) { advance(); stmt.havingClause = parseExpression(); }

    if (check(TokenType::ORDER)) {
        advance(); consume(TokenType::BY);
        stmt.orderBy = parseOrderByList();
    }

    if (check(TokenType::LIMIT)) {
        advance();
        Token num = consume(TokenType::INTEGER_LITERAL);
        stmt.limit = std::stoi(num.value);
    }

    if (check(TokenType::SEMICOLON)) advance();
    return stmt;
}

std::vector<SelectItem> SQLParser::parseSelectList() {
    std::vector<SelectItem> items;
    items.push_back(parseSelectItem());
    while (check(TokenType::COMMA)) { advance(); items.push_back(parseSelectItem()); }
    return items;
}

SelectItem SQLParser::parseSelectItem() {
    // bare *
    if (check(TokenType::STAR)) { advance(); return SelectItem(std::make_shared<StarExpr>()); }

    // table.*
    if (check(TokenType::IDENTIFIER) && peekAt(1) && peekAt(1)->type == TokenType::DOT
        && peekAt(2) && peekAt(2)->type == TokenType::STAR) {
        std::string tbl = advance().value;
        advance(); advance(); // . *
        return SelectItem(std::make_shared<StarExpr>(tbl));
    }

    ExprPtr expr = parseExpression();
    std::string alias = parseOptionalAlias();
    return SelectItem(std::move(expr), alias);
}

TableRef SQLParser::parseTableRef() {
    // Subquery in FROM: (SELECT ...) AS alias
    if (check(TokenType::LEFT_PAREN)) {
        advance();
        auto subStmt = std::make_shared<SelectStatement>(parseSelect());
        consume(TokenType::RIGHT_PAREN);
        std::string alias = parseOptionalAlias();
        if (alias.empty())
            throw makeError("Subquery in FROM must have an alias (e.g. (SELECT ...) AS sub)");
        return TableRef("", alias, std::move(subStmt));
    }
    std::string name = advance().value;
    std::string alias = parseOptionalAlias();
    return TableRef(name, alias);
}

bool SQLParser::isJoinKeyword() {
    auto t = peek().type;
    return t == TokenType::JOIN  || t == TokenType::INNER
        || t == TokenType::LEFT  || t == TokenType::RIGHT
        || t == TokenType::CROSS;
}

JoinClause SQLParser::parseJoinClause() {
    JoinType jt = JoinType::INNER;
    if      (check(TokenType::INNER)) { advance(); jt = JoinType::INNER; }
    else if (check(TokenType::LEFT))  { advance(); if (check(TokenType::OUTER)) advance(); jt = JoinType::LEFT; }
    else if (check(TokenType::RIGHT)) { advance(); if (check(TokenType::OUTER)) advance(); jt = JoinType::RIGHT; }
    else if (check(TokenType::CROSS)) { advance(); jt = JoinType::CROSS; }

    consume(TokenType::JOIN);
    TableRef tbl = parseTableRef();

    ExprPtr onCond = nullptr;
    if (jt != JoinType::CROSS) { consume(TokenType::ON); onCond = parseExpression(); }

    return JoinClause(jt, std::move(tbl), std::move(onCond));
}

std::vector<OrderByItem> SQLParser::parseOrderByList() {
    std::vector<OrderByItem> items;
    items.push_back(parseOrderByItem());
    while (check(TokenType::COMMA)) { advance(); items.push_back(parseOrderByItem()); }
    return items;
}

OrderByItem SQLParser::parseOrderByItem() {
    ExprPtr expr = parseExpression();
    bool asc = true;
    if      (check(TokenType::ASC))  { advance(); }
    else if (check(TokenType::DESC)) { advance(); asc = false; }
    return OrderByItem(std::move(expr), asc);
}

// ──────────────────────────────────────────────────────────────
//  Expression parsing (recursive descent)
// ──────────────────────────────────────────────────────────────

ExprPtr SQLParser::parseExpression() { return parseOr(); }

ExprPtr SQLParser::parseOr() {
    ExprPtr left = parseAnd();
    while (check(TokenType::OR)) {
        advance();
        left = std::make_shared<BinaryExpr>(left, "OR", parseAnd());
    }
    return left;
}

ExprPtr SQLParser::parseAnd() {
    ExprPtr left = parseNot();
    while (check(TokenType::AND)) {
        advance();
        left = std::make_shared<BinaryExpr>(left, "AND", parseNot());
    }
    return left;
}

ExprPtr SQLParser::parseNot() {
    if (check(TokenType::NOT)) { advance(); return std::make_shared<NotExpr>(parseNot()); }
    return parseComparison();
}

ExprPtr SQLParser::parseComparison() {
    ExprPtr left = parseAddition();

    // IS [NOT] NULL
    if (check(TokenType::IS)) {
        advance();
        bool neg = false;
        if (check(TokenType::NOT)) { advance(); neg = true; }
        consume(TokenType::NULL_TOK);
        return std::make_shared<IsNullExpr>(left, neg);
    }

    // [NOT] BETWEEN / IN / LIKE
    bool negated = false;
    if (check(TokenType::NOT)) {
        Token* nx = peekAt(1);
        if (nx && (nx->type == TokenType::BETWEEN || nx->type == TokenType::IN
                || nx->type == TokenType::LIKE)) {
            advance(); negated = true;
        }
    }

    if (check(TokenType::BETWEEN)) {
        advance();
        ExprPtr low = parseAddition();
        consume(TokenType::AND);
        ExprPtr high = parseAddition();
        return std::make_shared<BetweenExpr>(left, low, high, negated);
    }

    if (check(TokenType::IN)) {
        advance();
        consume(TokenType::LEFT_PAREN);
        // IN (SELECT ...) subquery
        if (check(TokenType::SELECT)) {
            auto subStmt = std::make_shared<SelectStatement>(parseSelect());
            consume(TokenType::RIGHT_PAREN);
            return std::make_shared<InExpr>(left, std::move(subStmt), negated);
        }
        auto vals = parseExpressionList();
        consume(TokenType::RIGHT_PAREN);
        return std::make_shared<InExpr>(left, std::move(vals), negated);
    }

    if (check(TokenType::LIKE)) {
        advance();
        ExprPtr pat = parseAddition();
        ExprPtr likeExpr = std::make_shared<BinaryExpr>(left, "LIKE", pat);
        return negated ? std::make_shared<NotExpr>(likeExpr) : likeExpr;
    }

    if (isComparisonOp()) {
        std::string op = advance().value;
        if (op == "<>") op = "!=";
        return std::make_shared<BinaryExpr>(left, op, parseAddition());
    }

    return left;
}

bool SQLParser::isComparisonOp() {
    auto t = peek().type;
    return t == TokenType::EQUALS || t == TokenType::NOT_EQUALS
        || t == TokenType::LESS_THAN    || t == TokenType::GREATER_THAN
        || t == TokenType::LESS_THAN_EQUALS || t == TokenType::GREATER_THAN_EQUALS;
}

ExprPtr SQLParser::parseAddition() {
    ExprPtr left = parseMultiplication();
    while (check(TokenType::PLUS) || check(TokenType::MINUS)) {
        std::string op = advance().value;
        left = std::make_shared<BinaryExpr>(left, op, parseMultiplication());
    }
    return left;
}

ExprPtr SQLParser::parseMultiplication() {
    ExprPtr left = parseUnary();
    while (check(TokenType::STAR) || check(TokenType::DIVIDE) || check(TokenType::MODULO)) {
        std::string op = advance().value;
        left = std::make_shared<BinaryExpr>(left, op, parseUnary());
    }
    return left;
}

ExprPtr SQLParser::parseUnary() {
    if (check(TokenType::MINUS)) {
        advance();
        return std::make_shared<BinaryExpr>(LiteralExpr::makeInt(0), "-", parseUnary());
    }
    return parsePrimary();
}

ExprPtr SQLParser::parsePrimary() {
    Token& tok = peek();
    switch (tok.type) {
        case TokenType::INTEGER_LITERAL: { advance(); return LiteralExpr::makeInt(std::stoll(tok.value)); }
        case TokenType::DECIMAL_LITERAL: { advance(); return LiteralExpr::makeDouble(std::stod(tok.value)); }
        case TokenType::STRING_LITERAL:  { advance(); return LiteralExpr::makeString(tok.value); }
        case TokenType::TRUE_TOK:        { advance(); return LiteralExpr::makeBool(true); }
        case TokenType::FALSE_TOK:       { advance(); return LiteralExpr::makeBool(false); }
        case TokenType::NULL_TOK:        { advance(); return LiteralExpr::makeNull(); }
        case TokenType::LEFT_PAREN: {
            advance();
            // Scalar subquery: (SELECT ...)
            if (check(TokenType::SELECT)) {
                auto subStmt = std::make_shared<SelectStatement>(parseSelect());
                consume(TokenType::RIGHT_PAREN);
                return std::make_shared<SubqueryExpr>(std::move(subStmt));
            }
            ExprPtr e = parseExpression();
            consume(TokenType::RIGHT_PAREN);
            return e;
        }
        case TokenType::STAR: { advance(); return std::make_shared<StarExpr>(); }

        // Aggregate / scalar functions by keyword
        case TokenType::COUNT: case TokenType::SUM:    case TokenType::AVG:
        case TokenType::MIN_TOK: case TokenType::MAX_TOK:
        case TokenType::UPPER:   case TokenType::LOWER: case TokenType::LENGTH:
        case TokenType::TRIM_TOK: case TokenType::ABS_TOK:  case TokenType::ROUND_TOK:
        case TokenType::COALESCE_TOK: case TokenType::CONCAT_TOK: case TokenType::SUBSTRING_TOK:
            return parseFunctionCall();

        case TokenType::IDENTIFIER:
            return parseIdentifierExpression();

        default:
            throw makeError("Unexpected token: " + tok.value);
    }
}

ExprPtr SQLParser::parseFunctionCall() {
    Token nameToken = advance();
    std::string funcName = nameToken.value;
    std::transform(funcName.begin(), funcName.end(), funcName.begin(), ::toupper);

    consume(TokenType::LEFT_PAREN);

    bool distinct = false;
    std::vector<ExprPtr> args;

    if (check(TokenType::RIGHT_PAREN)) {
        advance(); // COUNT() with no args
    } else if (check(TokenType::STAR)) {
        advance();
        args.push_back(std::make_shared<StarExpr>());
        consume(TokenType::RIGHT_PAREN);
    } else {
        if (check(TokenType::DISTINCT)) { advance(); distinct = true; }
        args = parseExpressionList();
        consume(TokenType::RIGHT_PAREN);
    }

    std::string alias = parseOptionalAlias();
    return std::make_shared<FunctionCallExpr>(funcName, std::move(args), distinct, alias);
}

ExprPtr SQLParser::parseIdentifierExpression() {
    Token id = advance();
    std::string name = id.value;

    // table.column or table.*
    if (check(TokenType::DOT)) {
        advance();
        if (check(TokenType::STAR)) { advance(); return std::make_shared<StarExpr>(name); }
        Token col = advance();
        return std::make_shared<ColumnRefExpr>(name, col.value);
    }

    // function call with identifier name
    if (check(TokenType::LEFT_PAREN)) {
        advance();
        std::vector<ExprPtr> args;
        if (!check(TokenType::RIGHT_PAREN)) args = parseExpressionList();
        consume(TokenType::RIGHT_PAREN);
        std::string alias = parseOptionalAlias();
        std::string upper = name;
        std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
        return std::make_shared<FunctionCallExpr>(upper, std::move(args), false, alias);
    }

    return std::make_shared<ColumnRefExpr>("", name);
}

std::vector<ExprPtr> SQLParser::parseExpressionList() {
    std::vector<ExprPtr> list;
    list.push_back(parseExpression());
    while (check(TokenType::COMMA)) { advance(); list.push_back(parseExpression()); }
    return list;
}

std::string SQLParser::parseOptionalAlias() {
    if (check(TokenType::AS)) { advance(); return advance().value; }
    // Implicit alias: bare identifier after expression (not a clause starter)
    if (check(TokenType::IDENTIFIER)) return advance().value;
    return "";
}

// ──────────────────────────────────────────────────────────────
//  Meta-command parsing
// ──────────────────────────────────────────────────────────────

Command SQLParser::parseLoad() {
    consume(TokenType::LOAD);
    Token fp = consume(TokenType::STRING_LITERAL);
    consume(TokenType::AS);
    Token tbl = advance();
    if (check(TokenType::SEMICOLON)) advance();
    Command cmd; cmd.type = CommandType::LOAD_TABLE;
    cmd.parameters["filePath"]  = fp.value;
    cmd.parameters["tableName"] = tbl.value;
    return cmd;
}

ParseResult SQLParser::parseShow() {
    consume(TokenType::SHOW);
    if (check(TokenType::TABLES)) {
        advance(); if (check(TokenType::SEMICOLON)) advance();
        Command cmd; cmd.type = CommandType::SHOW_TABLES; return cmd;
    }
    if (check(TokenType::INDEXES)) {
        advance(); if (check(TokenType::SEMICOLON)) advance();
        Command cmd; cmd.type = CommandType::SHOW_INDEXES; return cmd;
    }
    throw makeError("Expected TABLES or INDEXES after SHOW");
}

Command SQLParser::parseDescribe() {
    consume(TokenType::DESCRIBE);
    Token tbl = advance();
    if (check(TokenType::SEMICOLON)) advance();
    Command cmd; cmd.type = CommandType::DESCRIBE_TABLE;
    cmd.parameters["tableName"] = tbl.value;
    return cmd;
}

ParseResult SQLParser::parseCreate() {
    consume(TokenType::CREATE);
    if (check(TokenType::TABLE)) return parseCreateTable();

    consume(TokenType::INDEX);
    Token idxName = advance();
    consume(TokenType::ON);
    Token tblName = advance();
    consume(TokenType::LEFT_PAREN);
    Token colName = advance();
    consume(TokenType::RIGHT_PAREN);
    if (check(TokenType::SEMICOLON)) advance();
    Command cmd; cmd.type = CommandType::CREATE_INDEX;
    cmd.parameters["indexName"]  = idxName.value;
    cmd.parameters["tableName"]  = tblName.value;
    cmd.parameters["columnName"] = colName.value;
    return cmd;
}

Command SQLParser::parseCreateTable() {
    consume(TokenType::TABLE);
    Token tblName = advance();
    consume(TokenType::LEFT_PAREN);

    std::vector<std::string> colNames, colTypes;
    Token cn = advance(); colNames.push_back(cn.value);
    Token ct = advance();
    std::string ctUpper = ct.value;
    std::transform(ctUpper.begin(), ctUpper.end(), ctUpper.begin(), ::toupper);
    colTypes.push_back(ctUpper);

    while (check(TokenType::COMMA)) {
        advance();
        cn = advance(); colNames.push_back(cn.value);
        ct = advance();
        ctUpper = ct.value;
        std::transform(ctUpper.begin(), ctUpper.end(), ctUpper.begin(), ::toupper);
        colTypes.push_back(ctUpper);
    }

    consume(TokenType::RIGHT_PAREN);
    if (check(TokenType::SEMICOLON)) advance();

    Command cmd; cmd.type = CommandType::CREATE_TABLE;
    cmd.parameters["tableName"] = tblName.value;
    cmd.columnNames = std::move(colNames);
    cmd.columnTypes = std::move(colTypes);
    return cmd;
}

Command SQLParser::parseInsert() {
    consume(TokenType::INSERT);
    consume(TokenType::INTO);
    Token tblName = advance();

    // Optional column list
    std::vector<std::string> columns;
    bool hasColumnList = false;
    if (check(TokenType::LEFT_PAREN)) {
        advance();
        columns.push_back(advance().value);
        while (check(TokenType::COMMA)) { advance(); columns.push_back(advance().value); }
        consume(TokenType::RIGHT_PAREN);
        hasColumnList = true;
    }

    consume(TokenType::VALUES);
    std::vector<std::vector<std::string>> valueRows;
    valueRows.push_back(parseValueRow());
    while (check(TokenType::COMMA)) { advance(); valueRows.push_back(parseValueRow()); }
    if (check(TokenType::SEMICOLON)) advance();

    Command cmd; cmd.type = CommandType::INSERT_INTO;
    cmd.parameters["tableName"] = tblName.value;
    if (hasColumnList) cmd.columnNames = std::move(columns);
    cmd.valueRows = std::move(valueRows);
    return cmd;
}

std::vector<std::string> SQLParser::parseValueRow() {
    consume(TokenType::LEFT_PAREN);
    std::vector<std::string> values;
    values.push_back(parseLiteralValue());
    while (check(TokenType::COMMA)) { advance(); values.push_back(parseLiteralValue()); }
    consume(TokenType::RIGHT_PAREN);
    return values;
}

std::string SQLParser::parseLiteralValue() {
    Token& tok = peek();
    switch (tok.type) {
        case TokenType::STRING_LITERAL:
        case TokenType::INTEGER_LITERAL:
        case TokenType::DECIMAL_LITERAL:  { advance(); return tok.value; }
        case TokenType::TRUE_TOK:         { advance(); return "true"; }
        case TokenType::FALSE_TOK:        { advance(); return "false"; }
        case TokenType::NULL_TOK:         { advance(); return ""; } // empty = NULL
        case TokenType::MINUS: {
            advance();
            Token num = advance();
            return "-" + num.value;
        }
        default:
            throw makeError("Expected literal value but got " + tok.value);
    }
}

Command SQLParser::parseDelete() {
    consume(TokenType::DELETE);
    consume(TokenType::FROM);
    Token tblName = advance();
    if (check(TokenType::SEMICOLON)) advance();
    Command cmd; cmd.type = CommandType::DELETE_FROM;
    cmd.parameters["tableName"] = tblName.value;
    return cmd;
}

Command SQLParser::parseAlter() {
    consume(TokenType::ALTER);
    consume(TokenType::TABLE);
    Token tblName = advance();

    if (check(TokenType::ADD)) {
        advance();
        if (check(TokenType::COLUMN)) advance();
        Token colName = advance();
        Token colType = advance();
        std::string ctUpper = colType.value;
        std::transform(ctUpper.begin(), ctUpper.end(), ctUpper.begin(), ::toupper);
        if (check(TokenType::SEMICOLON)) advance();
        Command cmd; cmd.type = CommandType::ALTER_ADD_COLUMN;
        cmd.parameters["tableName"]  = tblName.value;
        cmd.parameters["columnName"] = colName.value;
        cmd.parameters["columnType"] = ctUpper;
        return cmd;
    }

    if (check(TokenType::DROP)) {
        advance();
        if (check(TokenType::COLUMN)) advance();
        Token colName = advance();
        if (check(TokenType::SEMICOLON)) advance();
        Command cmd; cmd.type = CommandType::ALTER_DROP_COLUMN;
        cmd.parameters["tableName"]  = tblName.value;
        cmd.parameters["columnName"] = colName.value;
        return cmd;
    }

    if (check(TokenType::RENAME)) {
        advance();
        if (check(TokenType::TO)) {
            advance();
            Token newName = advance();
            if (check(TokenType::SEMICOLON)) advance();
            Command cmd; cmd.type = CommandType::ALTER_RENAME_TABLE;
            cmd.parameters["tableName"] = tblName.value;
            cmd.parameters["newName"]   = newName.value;
            return cmd;
        }
        if (check(TokenType::COLUMN)) advance();
        Token oldCol = advance();
        consume(TokenType::TO);
        Token newCol = advance();
        if (check(TokenType::SEMICOLON)) advance();
        Command cmd; cmd.type = CommandType::ALTER_RENAME_COLUMN;
        cmd.parameters["tableName"] = tblName.value;
        cmd.parameters["oldName"]   = oldCol.value;
        cmd.parameters["newName"]   = newCol.value;
        return cmd;
    }

    throw makeError("Expected ADD, DROP, or RENAME after ALTER TABLE " + tblName.value);
}

ParseResult SQLParser::parseDrop() {
    consume(TokenType::DROP);
    if (check(TokenType::TABLE)) {
        advance();
        Token tbl = advance();
        if (check(TokenType::SEMICOLON)) advance();
        Command cmd; cmd.type = CommandType::DELETE_FROM;
        cmd.parameters["tableName"] = tbl.value;
        return cmd;
    }
    consume(TokenType::INDEX);
    Token idxName = advance();
    if (check(TokenType::SEMICOLON)) advance();
    Command cmd; cmd.type = CommandType::DROP_INDEX;
    cmd.parameters["indexName"] = idxName.value;
    return cmd;
}

// ──────────────────────────────────────────────────────────────
//  UPDATE table SET col = expr [, ...] [WHERE condition]
// ──────────────────────────────────────────────────────────────
UpdateStatement SQLParser::parseUpdate() {
    consume(TokenType::UPDATE);
    UpdateStatement stmt;
    stmt.tableName = advance().value;
    consume(TokenType::SET);

    // First SET item
    SetItem first;
    first.columnName = advance().value;
    consume(TokenType::EQUALS);
    first.value = parseExpression();
    stmt.setItems.push_back(std::move(first));

    // Additional SET items
    while (check(TokenType::COMMA)) {
        advance();
        SetItem item;
        item.columnName = advance().value;
        consume(TokenType::EQUALS);
        item.value = parseExpression();
        stmt.setItems.push_back(std::move(item));
    }

    if (check(TokenType::WHERE)) { advance(); stmt.whereClause = parseExpression(); }
    if (check(TokenType::SEMICOLON)) advance();
    return stmt;
}

// ──────────────────────────────────────────────────────────────
//  EXPORT (SELECT ...) TO 'file' [FORMAT CSV|JSON]
// ──────────────────────────────────────────────────────────────
ExportStatement SQLParser::parseExport() {
    consume(TokenType::EXPORT);
    ExportStatement stmt;
    consume(TokenType::LEFT_PAREN);
    stmt.query = parseSelect();
    consume(TokenType::RIGHT_PAREN);
    consume(TokenType::TO);
    Token fp = consume(TokenType::STRING_LITERAL);
    stmt.filePath = fp.value;

    // Optional explicit FORMAT CSV|JSON; otherwise infer from file extension
    if (check(TokenType::FORMAT)) {
        advance();
        Token fmt = advance();
        std::string f = fmt.value;
        std::transform(f.begin(), f.end(), f.begin(), ::toupper);
        stmt.format = f;
    } else {
        size_t dot = stmt.filePath.rfind('.');
        if (dot != std::string::npos) {
            std::string ext = stmt.filePath.substr(dot + 1);
            std::transform(ext.begin(), ext.end(), ext.begin(), ::toupper);
            stmt.format = (ext == "JSON") ? "JSON" : "CSV";
        } else {
            stmt.format = "CSV";
        }
    }

    if (check(TokenType::SEMICOLON)) advance();
    return stmt;
}

// ──────────────────────────────────────────────────────────────
//  Token navigation
// ──────────────────────────────────────────────────────────────

Token& SQLParser::peek() {
    return tokens_[pos_];
}

Token* SQLParser::peekAt(int offset) {
    int idx = pos_ + offset;
    return (idx >= 0 && idx < (int)tokens_.size()) ? &tokens_[idx] : nullptr;
}

bool SQLParser::check(TokenType type) {
    return pos_ < (int)tokens_.size() && tokens_[pos_].type == type;
}

Token SQLParser::advance() {
    return tokens_[pos_++];
}

Token SQLParser::consume(TokenType expected) {
    Token& t = peek();
    if (t.type != expected)
        throw makeError("Expected token type but got '" + t.value + "'");
    return advance();
}

std::runtime_error SQLParser::makeError(const std::string& msg) {
    Token& t = (pos_ < (int)tokens_.size()) ? tokens_[pos_] : tokens_.back();
    return std::runtime_error("Parse error at line " + std::to_string(t.line) +
                              ", col " + std::to_string(t.col) + ": " + msg);
}
