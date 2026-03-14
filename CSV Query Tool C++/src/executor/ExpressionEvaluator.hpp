#pragma once
#include "parser/ast/Expr.hpp"
#include "storage/Row.hpp"
#include "storage/Schema.hpp"
#include "storage/Value.hpp"

/**
 * Evaluates AST Expression nodes against a Row in a Schema context.
 * Handles: column resolution, arithmetic, comparisons, SQL three-valued logic
 * (NULL propagation), LIKE pattern matching, and scalar functions.
 */
class ExpressionEvaluator {
public:
    /** Evaluates an expression. Returns NULL/Long/Double/Bool/String. */
    Value evaluate(const ExprPtr& expr, const Row& row, const Schema& schema) const;

    /** Evaluates and coerces to boolean (NULL → false). */
    bool evaluateBoolean(const ExprPtr& expr, const Row& row, const Schema& schema) const;

private:
    Value evaluateColumnRef (const ColumnRefExpr*  ref,     const Row& row, const Schema& schema) const;
    Value evaluateBinary    (const BinaryExpr*      expr,    const Row& row, const Schema& schema) const;
    Value evaluateFunction  (const FunctionCallExpr* func,   const Row& row, const Schema& schema) const;
    Value evaluateBetween   (const BetweenExpr*     expr,    const Row& row, const Schema& schema) const;
    Value evaluateIn        (const InExpr*          expr,    const Row& row, const Schema& schema) const;
    Value evaluateLike      (const Value& value, const Value& pattern) const;

    static int    compare(const Value& a, const Value& b);
    static bool   toBoolean(const Value& v);
};
