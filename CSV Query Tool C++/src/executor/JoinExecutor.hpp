#pragma once
#include "ExpressionEvaluator.hpp"
#include "parser/ast/SelectStatement.hpp"
#include "storage/Row.hpp"
#include "storage/Schema.hpp"
#include <vector>

/**
 * Executes JOIN operations between two row sets.
 *
 * Strategies:
 *   Hash Join   — O(n+m) for equi-joins (equality between one column pair)
 *   Nested Loop — O(n×m) fallback for complex predicates
 */
class JoinExecutor {
public:
    explicit JoinExecutor(const ExpressionEvaluator& eval) : evaluator_(eval) {}

    std::vector<Row> execute(const std::vector<Row>& leftRows,  const Schema& leftSchema,
                             const std::vector<Row>& rightRows, const Schema& rightSchema,
                             const ExprPtr&          onCondition,
                             JoinType                joinType) const;

private:
    const ExpressionEvaluator& evaluator_;

    struct EquiCols { int leftIdx; int rightIdx; };

    std::vector<Row> hashJoin(const std::vector<Row>&, const Schema&,
                               const std::vector<Row>&, const Schema&,
                               const EquiCols&, JoinType,
                               const Schema& merged) const;

    std::vector<Row> nestedLoopJoin(const std::vector<Row>&, const Schema&,
                                     const std::vector<Row>&, const Schema&,
                                     const ExprPtr&, JoinType,
                                     const Schema& merged) const;

    static std::optional<EquiCols> extractEquiJoin(const ExprPtr&,
                                                    const Schema&, const Schema&);
    static int tryResolve(const ColumnRefExpr*, const Schema&);
};
