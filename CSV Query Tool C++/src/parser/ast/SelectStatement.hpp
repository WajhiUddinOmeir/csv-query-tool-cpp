#pragma once
#include "Expr.hpp"
#include <vector>
#include <string>
#include <optional>

// ──────────────────────────────────────────────────────────────
//  JoinType
// ──────────────────────────────────────────────────────────────
enum class JoinType { INNER, LEFT, RIGHT, CROSS };

// ──────────────────────────────────────────────────────────────
//  TableRef  — table name + optional alias
// ──────────────────────────────────────────────────────────────
struct TableRef {
    std::string tableName;
    std::string alias;     // empty if none
    std::shared_ptr<SelectStatement> subquery; // non-null for FROM (SELECT ...)

    TableRef() = default;
    TableRef(std::string name, std::string alias = "",
             std::shared_ptr<SelectStatement> sub = nullptr)
        : tableName(std::move(name)), alias(std::move(alias)), subquery(std::move(sub)) {}

    bool isSubquery() const { return subquery != nullptr; }

    /** Returns alias if set, otherwise tableName. */
    std::string getEffectiveName() const {
        return alias.empty() ? tableName : alias;
    }
};

// ──────────────────────────────────────────────────────────────
//  JoinClause
// ──────────────────────────────────────────────────────────────
struct JoinClause {
    JoinType joinType;
    TableRef table;
    ExprPtr  onCondition; // nullptr for CROSS JOIN

    JoinClause(JoinType jt, TableRef tbl, ExprPtr on)
        : joinType(jt), table(std::move(tbl)), onCondition(std::move(on)) {}
};

// ──────────────────────────────────────────────────────────────
//  SelectItem — one projection item
// ──────────────────────────────────────────────────────────────
struct SelectItem {
    ExprPtr     expression;
    std::string alias; // empty if none

    SelectItem(ExprPtr expr, std::string alias = "")
        : expression(std::move(expr)), alias(std::move(alias)) {}

    std::string getOutputName() const {
        if (!alias.empty()) return alias;
        if (expression->kind() == ExprKind::COLUMN_REF)
            return static_cast<const ColumnRefExpr*>(expression.get())->columnName;
        if (expression->kind() == ExprKind::FUNCTION_CALL) {
            auto* fc = static_cast<const FunctionCallExpr*>(expression.get());
            if (!fc->alias.empty()) return fc->alias;   // alias consumed by parseFunctionCall
            return fc->toCanonicalName();
        }
        return "?";
    }
};

// ──────────────────────────────────────────────────────────────
//  OrderByItem
// ──────────────────────────────────────────────────────────────
struct OrderByItem {
    ExprPtr expression;
    bool    ascending;

    OrderByItem(ExprPtr expr, bool asc) : expression(std::move(expr)), ascending(asc) {}
};

// ──────────────────────────────────────────────────────────────
//  SelectStatement — full AST for a SELECT query
// ──────────────────────────────────────────────────────────────
struct SelectStatement {
    bool                     distinct = false;
    std::vector<SelectItem>  selectItems;
    TableRef                 fromTable;
    std::vector<JoinClause>  joins;
    ExprPtr                  whereClause;   // nullptr if no WHERE
    std::vector<ExprPtr>     groupBy;
    ExprPtr                  havingClause;  // nullptr if no HAVING
    std::vector<OrderByItem> orderBy;
    std::optional<int>       limit;

    SelectStatement() = default;
};

// ──────────────────────────────────────────────────────────────
//  SubqueryExpr  — scalar subquery or source for IN (SELECT ...)
//  Defined here (after SelectStatement) so the shared_ptr target is complete.
// ──────────────────────────────────────────────────────────────
class SubqueryExpr : public Expr {
public:
    std::shared_ptr<SelectStatement> stmt;
    explicit SubqueryExpr(std::shared_ptr<SelectStatement> s) : stmt(std::move(s)) {}
    ExprKind kind() const override { return ExprKind::SUBQUERY; }
};

// ──────────────────────────────────────────────────────────────
//  UPDATE statement
// ──────────────────────────────────────────────────────────────
struct SetItem {
    std::string columnName;
    ExprPtr     value;
};

struct UpdateStatement {
    std::string          tableName;
    std::vector<SetItem> setItems;
    ExprPtr              whereClause; ///< nullptr = update every row
};

// ──────────────────────────────────────────────────────────────
//  EXPORT statement
// ──────────────────────────────────────────────────────────────
struct ExportStatement {
    SelectStatement query;
    std::string     filePath;
    std::string     format; ///< "CSV" or "JSON"
};
