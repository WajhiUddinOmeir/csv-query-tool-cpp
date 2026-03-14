#pragma once
#include "ExpressionEvaluator.hpp"
#include "JoinExecutor.hpp"
#include "QueryResult.hpp"
#include "optimizer/QueryOptimizer.hpp"
#include "index/IndexManager.hpp"
#include "parser/ast/SelectStatement.hpp"
#include "storage/TableRegistry.hpp"

/**
 * Main query execution pipeline (mirrors a real database engine):
 *  1. Optimize  — predicate pushdown + index scan selection
 *  2. Scan      — load rows from source table(s)
 *  3. Join      — combine rows from multiple tables
 *  4. Filter    — apply remaining WHERE clause
 *  5. Group     — GROUP BY + aggregation
 *  6. Having    — filter aggregated rows
 *  7. Project   — expand *, build column metadata, project rows, capture ORDER BY keys
 *  8. Sort      — ORDER BY
 *  9. Distinct  — remove duplicates
 * 10. Limit     — truncate result set
 */
class QueryExecutor {
public:
    QueryExecutor(TableRegistry& registry, IndexManager& indexManager);

    QueryResult execute(const SelectStatement& stmt);

    /** Execute an UPDATE statement; returns a message QueryResult with the row count. */
    QueryResult executeUpdate(const UpdateStatement& stmt);

private:
    TableRegistry&       registry_;
    IndexManager&        indexManager_;
    ExpressionEvaluator  evaluator_;
    JoinExecutor         joinExecutor_;
    QueryOptimizer       optimizer_;
    int                  rowsScanned_;

    // ── Index scan ──────────────────────────────────────────────
    std::vector<Row> tryIndexScan(const std::vector<Row>& rows, const Schema& schema,
                                   const ExprPtr& where, const std::string& tableName);

    // ── Filtering ───────────────────────────────────────────────
    std::vector<Row> filterRows(const std::vector<Row>& rows,
                                 const ExprPtr& predicate, const Schema& schema) const;

    // ── Aggregation ─────────────────────────────────────────────
    struct AggResult { std::vector<Row> rows; Schema schema; };

    AggResult performAggregation(const std::vector<Row>& rows, const Schema& schema,
                                  const std::vector<ExprPtr>& groupBy,
                                  const std::vector<SelectItem>& selectItems) const;

    AggResult aggregateWholeTable(const std::vector<Row>& rows, const Schema& schema,
                                   const std::vector<FunctionCallExpr*>& aggregates,
                                   const std::vector<SelectItem>& selectItems) const;

    Value computeAggregate(const FunctionCallExpr* func,
                            const std::vector<Row>& group, const Schema& schema) const;

    // ── Projection ──────────────────────────────────────────────
    std::vector<SelectItem> expandSelectItems(const std::vector<SelectItem>& items,
                                               const Schema& schema) const;

    // ── Distinct ────────────────────────────────────────────────
    std::vector<std::vector<Value>> removeDuplicates(
        std::vector<std::vector<Value>> rows) const;

    // ── Utilities ───────────────────────────────────────────────
    std::vector<FunctionCallExpr*> collectAggregates(
        const std::vector<SelectItem>& items) const;
    void collectAggregatesFromExpr(const ExprPtr& expr,
                                    std::vector<FunctionCallExpr*>& aggs) const;
    bool containsAggregates(const std::vector<SelectItem>& items) const;

    std::string expressionToColName(const ExprPtr& expr) const;
    DataType    inferOutputType(const ExprPtr& expr, const Schema& schema) const;

    // ── Subquery materialisation ──────────────────────────────
    /**
     * Recursively replaces any SubqueryExpr / InExpr(subquery) nodes in the
     * expression tree with their pre-computed results.  This is called before
     * the main pipeline so ExpressionEvaluator never sees a SUBQUERY node.
     * Subquery nodes that survive materialisation throw at evaluation time.
     */
    ExprPtr materializeSubqueries(const ExprPtr& expr);};