#pragma once
#include "storage/Value.hpp"
#include <memory>
#include <string>
#include <vector>

// ──────────────────────────────────────────────────────────────
//  Expression kind tag
// ──────────────────────────────────────────────────────────────
enum class ExprKind {
    LITERAL,
    COLUMN_REF,
    BINARY,
    FUNCTION_CALL,
    NOT_EXPR,
    IS_NULL,
    BETWEEN,
    IN_EXPR,
    STAR,
    SUBQUERY   ///< scalar subquery / IN-subquery (materialized before evaluation)
};

// ──────────────────────────────────────────────────────────────
//  Base class
// ──────────────────────────────────────────────────────────────
class Expr {
public:
    virtual ExprKind kind() const = 0;
    virtual ~Expr() = default;
};

using ExprPtr = std::shared_ptr<Expr>;

// ──────────────────────────────────────────────────────────────
//  Literal value: NULL, long long, double, bool, string
// ──────────────────────────────────────────────────────────────
class LiteralExpr : public Expr {
public:
    Value val;

    explicit LiteralExpr(Value v) : val(std::move(v)) {}
    ExprKind kind() const override { return ExprKind::LITERAL; }

    static ExprPtr makeNull()                          { return std::make_shared<LiteralExpr>(std::monostate{}); }
    static ExprPtr makeInt(long long v)                { return std::make_shared<LiteralExpr>(v); }
    static ExprPtr makeDouble(double v)                { return std::make_shared<LiteralExpr>(v); }
    static ExprPtr makeBool(bool v)                    { return std::make_shared<LiteralExpr>(v); }
    static ExprPtr makeString(const std::string& v)    { return std::make_shared<LiteralExpr>(v); }
};

// ──────────────────────────────────────────────────────────────
//  Column reference: [table.]column [AS alias]
// ──────────────────────────────────────────────────────────────
class ColumnRefExpr : public Expr {
public:
    std::string tableName;  // empty if unqualified
    std::string columnName;
    std::string alias;      // empty if no alias

    ColumnRefExpr(std::string tbl, std::string col, std::string alias = "")
        : tableName(std::move(tbl)), columnName(std::move(col)), alias(std::move(alias)) {}

    ExprKind kind() const override { return ExprKind::COLUMN_REF; }
};

// ──────────────────────────────────────────────────────────────
//  Binary operation: left OP right
// ──────────────────────────────────────────────────────────────
class BinaryExpr : public Expr {
public:
    ExprPtr     left;
    std::string op;   // "+","-","*","/","%","=","!=","<",">","<=",">=","AND","OR","LIKE"
    ExprPtr     right;

    BinaryExpr(ExprPtr l, std::string op, ExprPtr r)
        : left(std::move(l)), op(std::move(op)), right(std::move(r)) {}

    ExprKind kind() const override { return ExprKind::BINARY; }
};

// ──────────────────────────────────────────────────────────────
//  Function call (aggregate + scalar)
// ──────────────────────────────────────────────────────────────
class FunctionCallExpr : public Expr {
public:
    std::string            name;
    std::vector<ExprPtr>   arguments;
    bool                   distinct = false;
    std::string            alias;

    FunctionCallExpr(std::string name, std::vector<ExprPtr> args,
                     bool distinct = false, std::string alias = "")
        : name(std::move(name)), arguments(std::move(args)),
          distinct(distinct), alias(std::move(alias)) {}

    ExprKind kind() const override { return ExprKind::FUNCTION_CALL; }

    bool isAggregate() const {
        return name == "COUNT" || name == "SUM" || name == "AVG"
            || name == "MIN"   || name == "MAX";
    }

    std::string toCanonicalName() const;
};

// ──────────────────────────────────────────────────────────────
//  NOT expression
// ──────────────────────────────────────────────────────────────
class NotExpr : public Expr {
public:
    ExprPtr expr;
    explicit NotExpr(ExprPtr e) : expr(std::move(e)) {}
    ExprKind kind() const override { return ExprKind::NOT_EXPR; }
};

// ──────────────────────────────────────────────────────────────
//  IS [NOT] NULL
// ──────────────────────────────────────────────────────────────
class IsNullExpr : public Expr {
public:
    ExprPtr expr;
    bool    negated; // true = IS NOT NULL

    IsNullExpr(ExprPtr e, bool neg) : expr(std::move(e)), negated(neg) {}
    ExprKind kind() const override { return ExprKind::IS_NULL; }
};

// ──────────────────────────────────────────────────────────────
//  [NOT] BETWEEN low AND high
// ──────────────────────────────────────────────────────────────
class BetweenExpr : public Expr {
public:
    ExprPtr expr, low, high;
    bool    negated;

    BetweenExpr(ExprPtr e, ExprPtr l, ExprPtr h, bool neg)
        : expr(std::move(e)), low(std::move(l)), high(std::move(h)), negated(neg) {}
    ExprKind kind() const override { return ExprKind::BETWEEN; }
};

// ──────────────────────────────────────────────────────────────
//  [NOT] IN (values...)  or  [NOT] IN (SELECT ...)
// ──────────────────────────────────────────────────────────────

// Forward declaration so InExpr can hold a subquery pointer without
// creating a circular include between Expr.hpp and SelectStatement.hpp
struct SelectStatement;

class InExpr : public Expr {
public:
    ExprPtr              expr;
    std::vector<ExprPtr> values;    ///< non-empty for literal IN list
    std::shared_ptr<SelectStatement> subquery; ///< non-null for IN (SELECT ...)
    bool                 negated;

    /// Constructor for literal value list: col IN (1, 2, 3)
    InExpr(ExprPtr e, std::vector<ExprPtr> vals, bool neg)
        : expr(std::move(e)), values(std::move(vals)), subquery(nullptr), negated(neg) {}

    /// Constructor for subquery: col IN (SELECT col FROM ...)
    InExpr(ExprPtr e, std::shared_ptr<SelectStatement> sq, bool neg)
        : expr(std::move(e)), subquery(std::move(sq)), negated(neg) {}

    ExprKind kind() const override { return ExprKind::IN_EXPR; }
};

// ──────────────────────────────────────────────────────────────
//  Star: * or table.*
// ──────────────────────────────────────────────────────────────
class StarExpr : public Expr {
public:
    std::string tableName; // empty for *, non-empty for table.*

    explicit StarExpr(std::string tbl = "") : tableName(std::move(tbl)) {}
    ExprKind kind() const override { return ExprKind::STAR; }
};

// ──────────────────────────────────────────────────────────────
//  FunctionCallExpr::toCanonicalName (needs StarExpr / ColumnRefExpr defined)
// ──────────────────────────────────────────────────────────────
inline std::string FunctionCallExpr::toCanonicalName() const {
    std::string result = name + "(";
    if (distinct) result += "DISTINCT ";
    if (arguments.empty()) {
        result += "*";
    } else {
        for (int i = 0; i < (int)arguments.size(); i++) {
            if (i > 0) result += ", ";
            const auto& arg = arguments[i];
            if (arg->kind() == ExprKind::STAR) {
                result += "*";
            } else if (arg->kind() == ExprKind::COLUMN_REF) {
                auto* ref = static_cast<const ColumnRefExpr*>(arg.get());
                result += ref->tableName.empty()
                    ? ref->columnName
                    : ref->tableName + "." + ref->columnName;
            } else {
                result += "?";
            }
        }
    }
    result += ")";
    return result;
}
