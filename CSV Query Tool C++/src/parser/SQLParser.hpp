#pragma once
#include "Token.hpp"
#include "Lexer.hpp"
#include "ast/Expr.hpp"
#include "ast/SelectStatement.hpp"
#include "ast/Command.hpp"
#include <vector>
#include <variant>
#include <string>

// Parse result is a SelectStatement, UpdateStatement, ExportStatement, or a Command
using ParseResult = std::variant<SelectStatement, UpdateStatement, ExportStatement, Command>;

/**
 * Recursive-descent SQL parser.
 * Transforms a token stream into either a SelectStatement or a Command AST.
 *
 * Expression precedence (lowest → highest):
 *   OR → AND → NOT → comparison/BETWEEN/IN/IS/LIKE →
 *   addition/subtraction → multiplication/division → unary → primary
 */
class SQLParser {
public:
    explicit SQLParser(const std::vector<Token>& tokens);
    explicit SQLParser(const std::string& sql);

    ParseResult parse();

private:
    std::vector<Token> tokens_;
    int                pos_;

    // ── SELECT ─────────────────────────────────────────────────
    SelectStatement        parseSelect();
    std::vector<SelectItem> parseSelectList();
    SelectItem             parseSelectItem();
    TableRef               parseTableRef();
    bool                   isJoinKeyword();
    JoinClause             parseJoinClause();
    std::vector<OrderByItem> parseOrderByList();
    OrderByItem            parseOrderByItem();

    // ── Expressions ────────────────────────────────────────────
    ExprPtr parseExpression();
    ExprPtr parseOr();
    ExprPtr parseAnd();
    ExprPtr parseNot();
    ExprPtr parseComparison();
    ExprPtr parseAddition();
    ExprPtr parseMultiplication();
    ExprPtr parseUnary();
    ExprPtr parsePrimary();
    ExprPtr parseFunctionCall();
    ExprPtr parseIdentifierExpression();
    std::vector<ExprPtr> parseExpressionList();
    std::string          parseOptionalAlias();
    bool                 isComparisonOp();

    // ── Meta-commands ───────────────────────────────────────────
    Command     parseLoad();
    ParseResult parseShow();
    Command     parseDescribe();
    ParseResult parseCreate();
    Command     parseCreateTable();
    Command     parseInsert();
    std::vector<std::string> parseValueRow();
    std::string parseLiteralValue();
    Command     parseDelete();
    Command     parseAlter();
    ParseResult parseDrop();
    UpdateStatement  parseUpdate();
    ExportStatement  parseExport();

    // ── Token navigation ────────────────────────────────────────
    Token&  peek();
    Token*  peekAt(int offset);
    bool    check(TokenType type);
    Token   advance();
    Token   consume(TokenType expected);
    std::runtime_error makeError(const std::string& msg);
};
