#include "QueryExecutor.hpp"
#include "../parser/ast/SelectStatement.hpp"
#include <algorithm>
#include <chrono>
#include <set>
#include <sstream>
#include <unordered_map>
#include <stdexcept>

QueryExecutor::QueryExecutor(TableRegistry& registry, IndexManager& indexManager)
    : registry_(registry), indexManager_(indexManager),
      evaluator_(), joinExecutor_(evaluator_), optimizer_(indexManager), rowsScanned_(0) {}

// ──────────────────────────────────────────────────────────────
//  Main execute
// ──────────────────────────────────────────────────────────────

QueryResult QueryExecutor::execute(const SelectStatement& stmt) {
    auto startMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    rowsScanned_ = 0;

    // Materialize non-correlated subqueries in WHERE and HAVING before the pipeline
    ExprPtr where  = materializeSubqueries(stmt.whereClause);
    ExprPtr having = materializeSubqueries(stmt.havingClause);

    // Step 1: Resolve primary table, or execute a subquery used as a derived table
    std::string primaryAlias = stmt.fromTable.getEffectiveName();
    Schema currentSchema;
    std::vector<Row> rows;

    if (stmt.fromTable.isSubquery()) {
        QueryResult subResult = execute(*stmt.fromTable.subquery);
        std::vector<Column> cols;
        cols.reserve(subResult.columnNames.size());
        for (size_t i = 0; i < subResult.columnNames.size(); i++)
            cols.emplace_back(subResult.columnNames[i], subResult.columnTypes[i], primaryAlias);
        currentSchema = Schema(primaryAlias, cols);
        for (auto& rv : subResult.rows)
            rows.emplace_back(std::move(rv));
    } else {
        CsvTable& primaryTable = registry_.getTable(stmt.fromTable.tableName);
        currentSchema = primaryTable.getSchema().withTableName(primaryAlias);
        rows = primaryTable.getRows();
        // Step 2: Index scan optimisation (only applies to regular table scans)
        rows = tryIndexScan(rows, currentSchema, where, primaryTable.getName());
    }
    rowsScanned_ += (int)rows.size();

    // Step 3: JOINs with predicate pushdown
    ExprPtr remainingWhere = where;
    for (const auto& join : stmt.joins) {
        CsvTable& joinTable = registry_.getTable(join.table.tableName);
        std::string joinAlias = join.table.getEffectiveName();
        Schema joinSchema = joinTable.getSchema().withTableName(joinAlias);
        std::vector<Row> joinRows = joinTable.getRows();

        // Predicate pushdown: filter right-side rows before join
        if (remainingWhere) {
            auto pd = optimizer_.pushDown(remainingWhere, joinAlias);
            if (pd.pushed) joinRows = filterRows(joinRows, pd.pushed, joinSchema);
            remainingWhere = pd.remaining;
        }

        rowsScanned_ += (int)joinTable.getRows().size();

        ExprPtr onCond = materializeSubqueries(join.onCondition);
        rows = joinExecutor_.execute(rows, currentSchema, joinRows, joinSchema,
                                     onCond, join.joinType);
        currentSchema = Schema::merge(currentSchema, joinSchema);
    }

    // Step 4: Remaining WHERE
    if (remainingWhere)
        rows = filterRows(rows, remainingWhere, currentSchema);

    // Step 5: GROUP BY + aggregation
    bool hasGroupBy    = !stmt.groupBy.empty();
    bool hasAggregates = containsAggregates(stmt.selectItems);

    if (hasGroupBy || hasAggregates) {
        auto aggResult = performAggregation(rows, currentSchema, stmt.groupBy, stmt.selectItems);
        rows          = std::move(aggResult.rows);
        currentSchema = std::move(aggResult.schema);

        // Step 6: HAVING
        if (having)
            rows = filterRows(rows, having, currentSchema);
    }

    // Step 7: Build output column metadata.
    auto expandedItems = expandSelectItems(stmt.selectItems, currentSchema);
    std::vector<std::string>        outputNames;
    std::vector<DataType>           outputTypes;
    std::vector<std::vector<Value>> outputRows;

    for (const auto& item : expandedItems) {
        outputNames.push_back(item.getOutputName());
        outputTypes.push_back(inferOutputType(item.expression, currentSchema));
    }

    // Step 7 (cont.): Project rows while pre-capturing ORDER BY sort keys in one pass.
    //
    // Combining these ensures:
    //   (a) ORDER BY aliases (e.g. AVG(salary) AS avg_salary → ORDER BY avg_salary)
    //       resolve to the already-projected value.
    //   (b) ORDER BY on non-selected columns (e.g. SELECT name ORDER BY salary)
    //       evaluate against the full pre-projection row schema.

    // Returns the output column index for an ORDER BY expression, or -1 if not found.
    auto findOutCol = [&](const ExprPtr& expr) -> int {
        if (expr->kind() != ExprKind::COLUMN_REF) return -1;
        std::string key = static_cast<const ColumnRefExpr*>(expr.get())->columnName;
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);
        for (int j = 0; j < (int)expandedItems.size(); j++) {
            if (!expandedItems[j].alias.empty()) {
                std::string al = expandedItems[j].alias;
                std::transform(al.begin(), al.end(), al.begin(), ::tolower);
                if (al == key) return j;
            }
            std::string outName = expandedItems[j].getOutputName();
            std::transform(outName.begin(), outName.end(), outName.begin(), ::tolower);
            if (outName == key) return j;
        }
        return -1;
    };

    // Project + collect sort keys in one pass.
    struct ProjRow {
        std::vector<Value> vals; // projected output columns
        std::vector<Value> keys; // ORDER BY sort keys (empty when no ORDER BY)
    };
    std::vector<ProjRow> combined;
    combined.reserve(rows.size());

    for (const auto& row : rows) {
        ProjRow pr;
        pr.vals.reserve(expandedItems.size());
        for (const auto& item : expandedItems)
            pr.vals.push_back(evaluator_.evaluate(item.expression, row, currentSchema));

        if (!stmt.orderBy.empty()) {
            pr.keys.reserve(stmt.orderBy.size());
            for (const auto& obItem : stmt.orderBy) {
                int colIdx = findOutCol(obItem.expression);
                if (colIdx >= 0)
                    pr.keys.push_back(pr.vals[colIdx]);
                else
                    pr.keys.push_back(evaluator_.evaluate(obItem.expression, row, currentSchema));
            }
        }
        combined.push_back(std::move(pr));
    }

    // Step 8: Sort (ORDER BY).
    if (!stmt.orderBy.empty()) {
        std::stable_sort(combined.begin(), combined.end(),
                         [&](const ProjRow& a, const ProjRow& b) {
            for (int i = 0; i < (int)stmt.orderBy.size(); i++) {
                int cmp = compareNullSafe(a.keys[i], b.keys[i]);
                if (cmp != 0) return stmt.orderBy[i].ascending ? cmp < 0 : cmp > 0;
            }
            return false;
        });
    }

    for (auto& pr : combined)
        outputRows.push_back(std::move(pr.vals));

    // Step 9: DISTINCT.
    if (stmt.distinct)
        outputRows = removeDuplicates(std::move(outputRows));

    // Step 10: LIMIT.
    if (stmt.limit && *stmt.limit < (int)outputRows.size())
        outputRows.resize(*stmt.limit);

    auto endMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    return QueryResult(outputNames, outputTypes, std::move(outputRows),
                       (long)(endMs - startMs), rowsScanned_);
}

// ──────────────────────────────────────────────────────────────
//  Index scan
// ──────────────────────────────────────────────────────────────

std::vector<Row> QueryExecutor::tryIndexScan(const std::vector<Row>& rows,
                                              const Schema& /*schema*/,
                                              const ExprPtr& where,
                                              const std::string& tableName) {
    if (!where || where->kind() != ExprKind::BINARY) return rows;
    auto* bin = static_cast<const BinaryExpr*>(where.get());
    if (bin->op != "=") return rows;

    const ColumnRefExpr* colRef = nullptr;
    Value lookupValue;

    if (bin->left->kind() == ExprKind::COLUMN_REF && bin->right->kind() == ExprKind::LITERAL) {
        colRef = static_cast<const ColumnRefExpr*>(bin->left.get());
        lookupValue = static_cast<const LiteralExpr*>(bin->right.get())->val;
    } else if (bin->right->kind() == ExprKind::COLUMN_REF && bin->left->kind() == ExprKind::LITERAL) {
        colRef = static_cast<const ColumnRefExpr*>(bin->right.get());
        lookupValue = static_cast<const LiteralExpr*>(bin->left.get())->val;
    }

    if (!colRef) return rows;
    if (!indexManager_.hasIndex(tableName, colRef->columnName)) return rows;

    const HashIndex* idx = indexManager_.getIndex(tableName, colRef->columnName);
    if (!idx) return rows;
    auto* indices = idx->lookup(lookupValue);
    if (!indices) return {};

    std::vector<Row> filtered;
    for (int i : *indices)
        if (i < (int)rows.size()) filtered.push_back(rows[i]);
    return filtered;
}

// ──────────────────────────────────────────────────────────────
//  Filtering
// ──────────────────────────────────────────────────────────────

std::vector<Row> QueryExecutor::filterRows(const std::vector<Row>& rows,
                                            const ExprPtr& predicate,
                                            const Schema& schema) const {
    std::vector<Row> filtered;
    for (const auto& row : rows)
        if (evaluator_.evaluateBoolean(predicate, row, schema))
            filtered.push_back(row);
    return filtered;
}

// ──────────────────────────────────────────────────────────────
//  Aggregation (GROUP BY)
// ──────────────────────────────────────────────────────────────

QueryExecutor::AggResult QueryExecutor::performAggregation(
    const std::vector<Row>&      rows,
    const Schema&                inputSchema,
    const std::vector<ExprPtr>&  groupByExprs,
    const std::vector<SelectItem>& selectItems) const
{
    auto aggregates = collectAggregates(selectItems);

    if (groupByExprs.empty())
        return aggregateWholeTable(rows, inputSchema, aggregates, selectItems);

    // Build groups (insertion-ordered by key string)
    std::vector<std::string>                              keyOrder;
    std::unordered_map<std::string, std::vector<size_t>> groups; // key → row indices

    for (size_t ri = 0; ri < rows.size(); ri++) {
        std::string keyStr;
        for (int i = 0; i < (int)groupByExprs.size(); i++) {
            if (i > 0) keyStr += '\x01';
            keyStr += valueToString(evaluator_.evaluate(groupByExprs[i], rows[ri], inputSchema));
        }
        if (!groups.count(keyStr)) keyOrder.push_back(keyStr);
        groups[keyStr].push_back(ri);
    }

    // Build aggregated schema: group-by columns + aggregate columns
    std::vector<Column> aggCols;
    for (const auto& gbExpr : groupByExprs) {
        std::string name = expressionToColName(gbExpr);
        DataType    type = inferOutputType(gbExpr, inputSchema);
        std::string tbl;
        if (gbExpr->kind() == ExprKind::COLUMN_REF)
            tbl = static_cast<const ColumnRefExpr*>(gbExpr.get())->tableName;
        aggCols.emplace_back(name, type, tbl);
    }
    for (auto* agg : aggregates) {
        DataType retType = (agg->name == "COUNT") ? DataType::INTEGER : DataType::DOUBLE;
        aggCols.emplace_back(agg->toCanonicalName(), retType);
    }

    Schema aggSchema("", aggCols);

    // Build aggregated rows
    std::vector<Row> aggRows;
    for (const auto& keyStr : keyOrder) {
        const auto& rowIndices = groups[keyStr];
        std::vector<Value> values;
        values.reserve(aggCols.size());

        // Group key values (re-evaluate from the first row in the group)
        const Row& first = rows[rowIndices[0]];
        for (const auto& gbExpr : groupByExprs)
            values.push_back(evaluator_.evaluate(gbExpr, first, inputSchema));

        // Aggregate values
        std::vector<Row> groupRows;
        groupRows.reserve(rowIndices.size());
        for (size_t ri : rowIndices) groupRows.push_back(rows[ri]);

        for (auto* agg : aggregates)
            values.push_back(computeAggregate(agg, groupRows, inputSchema));

        aggRows.emplace_back(std::move(values));
    }

    return {std::move(aggRows), std::move(aggSchema)};
}

QueryExecutor::AggResult QueryExecutor::aggregateWholeTable(
    const std::vector<Row>&              rows,
    const Schema&                        inputSchema,
    const std::vector<FunctionCallExpr*>& aggregates,
    const std::vector<SelectItem>&       /*selectItems*/) const
{
    std::vector<Column> aggCols;
    for (auto* agg : aggregates) {
        DataType retType = (agg->name == "COUNT") ? DataType::INTEGER : DataType::DOUBLE;
        aggCols.emplace_back(agg->toCanonicalName(), retType);
    }

    Schema aggSchema("", aggCols);
    std::vector<Value> values;
    values.reserve(aggregates.size());
    for (auto* agg : aggregates)
        values.push_back(computeAggregate(agg, rows, inputSchema));

    std::vector<Row> aggRows;
    aggRows.emplace_back(std::move(values));
    return {std::move(aggRows), std::move(aggSchema)};
}

Value QueryExecutor::computeAggregate(const FunctionCallExpr* func,
                                       const std::vector<Row>& group,
                                       const Schema& schema) const {
    const std::string& name = func->name;
    const auto&        args = func->arguments;

    if (name == "COUNT") {
        if (args.empty() || args[0]->kind() == ExprKind::STAR)
            return static_cast<long long>(group.size());
        if (func->distinct) {
            std::set<std::string> distinct;
            for (const auto& r : group) {
                Value v = evaluator_.evaluate(args[0], r, schema);
                if (!isNull(v)) distinct.insert(valueToString(v));
            }
            return static_cast<long long>(distinct.size());
        }
        long long cnt = 0;
        for (const auto& r : group)
            if (!isNull(evaluator_.evaluate(args[0], r, schema))) cnt++;
        return cnt;
    }

    if (name == "SUM") {
        long long intSum = 0;
        double    dblSum = 0.0;
        bool allInt = true;
        bool hasVal = false;
        for (const auto& r : group) {
            Value v = evaluator_.evaluate(args[0], r, schema);
            if (!isNull(v)) {
                if (allInt && std::holds_alternative<long long>(v)) {
                    intSum += std::get<long long>(v);
                } else {
                    if (allInt) { dblSum = static_cast<double>(intSum); allInt = false; }
                    dblSum += valueToDouble(v);
                }
                hasVal = true;
            }
        }
        if (!hasVal) return Value(std::monostate{});
        return allInt ? Value(intSum) : Value(dblSum);
    }

    if (name == "AVG") {
        double sum = 0; int cnt = 0;
        for (const auto& r : group) {
            Value v = evaluator_.evaluate(args[0], r, schema);
            if (!isNull(v)) { sum += valueToDouble(v); cnt++; }
        }
        return cnt > 0 ? Value(sum / cnt) : Value(std::monostate{});
    }

    if (name == "MIN") {
        Value mn(std::monostate{});
        for (const auto& r : group) {
            Value v = evaluator_.evaluate(args[0], r, schema);
            if (!isNull(v) && (isNull(mn) || compareValues(v, mn) < 0)) mn = v;
        }
        return mn;
    }

    if (name == "MAX") {
        Value mx(std::monostate{});
        for (const auto& r : group) {
            Value v = evaluator_.evaluate(args[0], r, schema);
            if (!isNull(v) && (isNull(mx) || compareValues(v, mx) > 0)) mx = v;
        }
        return mx;
    }

    throw std::runtime_error("Unknown aggregate: " + name);
}

// ──────────────────────────────────────────────────────────────
//  SELECT item expansion (* → columns)
// ──────────────────────────────────────────────────────────────

std::vector<SelectItem> QueryExecutor::expandSelectItems(
    const std::vector<SelectItem>& items, const Schema& schema) const
{
    // Case-insensitive string equality helper for table.* matching.
    auto iequals = [](const std::string& x, const std::string& y) -> bool {
        if (x.size() != y.size()) return false;
        for (size_t i = 0; i < x.size(); ++i)
            if (tolower((unsigned char)x[i]) != tolower((unsigned char)y[i])) return false;
        return true;
    };

    std::vector<SelectItem> expanded;
    for (const auto& item : items) {
        if (item.expression->kind() == ExprKind::STAR) {
            auto* star = static_cast<const StarExpr*>(item.expression.get());
            for (const auto& col : schema.columns)
                if (star->tableName.empty() || iequals(star->tableName, col.tableName))
                    expanded.emplace_back(
                        std::make_shared<ColumnRefExpr>(col.tableName, col.name), "");
        } else {
            expanded.push_back(item);
        }
    }
    return expanded;
}

// ──────────────────────────────────────────────────────────────
//  DISTINCT
// ──────────────────────────────────────────────────────────────

std::vector<std::vector<Value>> QueryExecutor::removeDuplicates(
    std::vector<std::vector<Value>> rows) const
{
    std::set<std::string> seen;
    std::vector<std::vector<Value>> unique;
    for (auto& row : rows) {
        std::string key;
        for (const auto& v : row) key += valueToString(v) + '\x01';
        if (seen.insert(key).second)
            unique.push_back(std::move(row));
    }
    return unique;
}

// ──────────────────────────────────────────────────────────────
//  Utility helpers
// ──────────────────────────────────────────────────────────────

std::vector<FunctionCallExpr*> QueryExecutor::collectAggregates(
    const std::vector<SelectItem>& items) const
{
    std::vector<FunctionCallExpr*> aggs;
    for (const auto& item : items)
        collectAggregatesFromExpr(item.expression, aggs);
    return aggs;
}

void QueryExecutor::collectAggregatesFromExpr(const ExprPtr& expr,
                                               std::vector<FunctionCallExpr*>& aggs) const {
    if (!expr) return;
    if (expr->kind() == ExprKind::FUNCTION_CALL) {
        auto* fc = static_cast<FunctionCallExpr*>(expr.get());
        if (fc->isAggregate()) {
            std::string canon = fc->toCanonicalName();
            bool exists = false;
            for (auto* a : aggs) if (a->toCanonicalName() == canon) { exists = true; break; }
            if (!exists) aggs.push_back(fc);
        }
        for (const auto& arg : fc->arguments) collectAggregatesFromExpr(arg, aggs);
    } else if (expr->kind() == ExprKind::BINARY) {
        auto* b = static_cast<const BinaryExpr*>(expr.get());
        collectAggregatesFromExpr(b->left,  aggs);
        collectAggregatesFromExpr(b->right, aggs);
    }
}

bool QueryExecutor::containsAggregates(const std::vector<SelectItem>& items) const {
    return !collectAggregates(items).empty();
}

std::string QueryExecutor::expressionToColName(const ExprPtr& expr) const {
    if (expr->kind() == ExprKind::COLUMN_REF)
        return static_cast<const ColumnRefExpr*>(expr.get())->columnName;
    return "?";
}

DataType QueryExecutor::inferOutputType(const ExprPtr& expr, const Schema& schema) const {
    if (!expr) return DataType::STRING;
    switch (expr->kind()) {
        case ExprKind::COLUMN_REF: {
            auto* ref = static_cast<const ColumnRefExpr*>(expr.get());
            try {
                int idx = schema.resolveColumn(ref->tableName, ref->columnName);
                return schema.getColumn(idx).type;
            } catch (...) { return DataType::STRING; }
        }
        case ExprKind::FUNCTION_CALL: {
            auto* fc = static_cast<const FunctionCallExpr*>(expr.get());
            if (fc->isAggregate()) {
                if (fc->name == "COUNT") return DataType::INTEGER;
                return DataType::DOUBLE;
            }
            if (fc->name == "LENGTH") return DataType::INTEGER;
            if (fc->name == "ABS" || fc->name == "ROUND") return DataType::DOUBLE;
            return DataType::STRING;
        }
        case ExprKind::LITERAL: {
            const Value& v = static_cast<const LiteralExpr*>(expr.get())->val;
            if (std::holds_alternative<long long>(v))    return DataType::INTEGER;
            if (std::holds_alternative<double>(v))       return DataType::DOUBLE;
            if (std::holds_alternative<bool>(v))         return DataType::BOOLEAN;
            if (std::holds_alternative<std::string>(v))  return DataType::STRING;
            return DataType::STRING;
        }
        case ExprKind::BINARY: {
            const std::string& op = static_cast<const BinaryExpr*>(expr.get())->op;
            if (op=="AND"||op=="OR"||op=="="||op=="!="||
                op=="<"||op==">"||op=="<="||op==">="||op=="LIKE")
                return DataType::BOOLEAN;
            return DataType::DOUBLE;
        }
        default: return DataType::STRING;
    }
}

// ──────────────────────────────────────────────────────────────
//  Subquery materialisation
// ──────────────────────────────────────────────────────────────

ExprPtr QueryExecutor::materializeSubqueries(const ExprPtr& expr) {
    if (!expr) return expr;
    switch (expr->kind()) {

        case ExprKind::SUBQUERY: {
            // Scalar subquery: run it and return its single cell as a literal
            auto* sq = static_cast<const SubqueryExpr*>(expr.get());
            QueryResult result = execute(*sq->stmt);
            if (result.rows.empty() || result.rows[0].empty())
                return LiteralExpr::makeNull();
            return std::make_shared<LiteralExpr>(result.rows[0][0]);
        }

        case ExprKind::IN_EXPR: {
            auto* in = static_cast<const InExpr*>(expr.get());
            if (in->subquery) {
                // Execute the subquery and convert its first column to a literal list
                QueryResult result = execute(*in->subquery);
                std::vector<ExprPtr> values;
                values.reserve(result.rows.size());
                for (auto& row : result.rows)
                    if (!row.empty())
                        values.push_back(std::make_shared<LiteralExpr>(row[0]));
                return std::make_shared<InExpr>(
                    materializeSubqueries(in->expr), std::move(values), in->negated);
            }
            // Literal list — recurse into each value in case of nested subqueries
            std::vector<ExprPtr> newVals;
            newVals.reserve(in->values.size());
            for (const auto& v : in->values) newVals.push_back(materializeSubqueries(v));
            return std::make_shared<InExpr>(
                materializeSubqueries(in->expr), std::move(newVals), in->negated);
        }

        case ExprKind::BINARY: {
            auto* b = static_cast<const BinaryExpr*>(expr.get());
            auto l = materializeSubqueries(b->left);
            auto r = materializeSubqueries(b->right);
            if (l == b->left && r == b->right) return expr;
            return std::make_shared<BinaryExpr>(std::move(l), b->op, std::move(r));
        }

        case ExprKind::NOT_EXPR: {
            auto* n = static_cast<const NotExpr*>(expr.get());
            auto inner = materializeSubqueries(n->expr);
            if (inner == n->expr) return expr;
            return std::make_shared<NotExpr>(std::move(inner));
        }

        case ExprKind::IS_NULL: {
            auto* n = static_cast<const IsNullExpr*>(expr.get());
            auto inner = materializeSubqueries(n->expr);
            if (inner == n->expr) return expr;
            return std::make_shared<IsNullExpr>(std::move(inner), n->negated);
        }

        case ExprKind::BETWEEN: {
            auto* b = static_cast<const BetweenExpr*>(expr.get());
            return std::make_shared<BetweenExpr>(
                materializeSubqueries(b->expr),
                materializeSubqueries(b->low),
                materializeSubqueries(b->high),
                b->negated);
        }

        case ExprKind::FUNCTION_CALL: {
            auto* fc = static_cast<const FunctionCallExpr*>(expr.get());
            std::vector<ExprPtr> newArgs;
            newArgs.reserve(fc->arguments.size());
            bool changed = false;
            for (const auto& arg : fc->arguments) {
                auto newArg = materializeSubqueries(arg);
                changed |= (newArg != arg);
                newArgs.push_back(std::move(newArg));
            }
            if (!changed) return expr;
            return std::make_shared<FunctionCallExpr>(
                fc->name, std::move(newArgs), fc->distinct, fc->alias);
        }

        default:
            return expr; // LITERAL, COLUMN_REF, STAR — no subqueries possible
    }
}

// ──────────────────────────────────────────────────────────────
//  UPDATE
// ──────────────────────────────────────────────────────────────

QueryResult QueryExecutor::executeUpdate(const UpdateStatement& stmt) {
    CsvTable& table = registry_.getTable(stmt.tableName);
    Schema schema   = table.getSchema().withTableName(stmt.tableName);

    // Materialise any subqueries in WHERE and SET expressions
    ExprPtr where = materializeSubqueries(stmt.whereClause);

    // Pre-resolve column indices and SET value expressions
    std::vector<std::pair<int, ExprPtr>> setOps;
    setOps.reserve(stmt.setItems.size());
    for (const auto& item : stmt.setItems) {
        int colIdx = schema.resolveColumn("", item.columnName);
        setOps.emplace_back(colIdx, materializeSubqueries(item.value));
    }

    int updated = 0;
    for (auto& row : table.getRows()) {
        if (!where || evaluator_.evaluateBoolean(where, row, schema)) {
            for (const auto& [colIdx, valueExpr] : setOps)
                row.values[colIdx] = evaluator_.evaluate(valueExpr, row, schema);
            updated++;
        }
    }

    return QueryResult(std::to_string(updated) + " row(s) updated in '" + stmt.tableName + "'.");
}
