#include "JoinExecutor.hpp"
#include <unordered_map>
#include <unordered_set>
#include <optional>

std::vector<Row> JoinExecutor::execute(
    const std::vector<Row>& leftRows,  const Schema& leftSchema,
    const std::vector<Row>& rightRows, const Schema& rightSchema,
    const ExprPtr& onCondition, JoinType joinType) const
{
    Schema merged = Schema::merge(leftSchema, rightSchema);

    // Try hash join
    auto equi = extractEquiJoin(onCondition, leftSchema, rightSchema);
    if (equi) {
        return hashJoin(leftRows, leftSchema, rightRows, rightSchema, *equi, joinType, merged);
    }
    return nestedLoopJoin(leftRows, leftSchema, rightRows, rightSchema, onCondition, joinType, merged);
}

// ──────────────────────────────────────────────────────────────
//  Hash Join
// ──────────────────────────────────────────────────────────────

std::vector<Row> JoinExecutor::hashJoin(
    const std::vector<Row>& leftRows,  const Schema& /*leftSchema*/,
    const std::vector<Row>& rightRows, const Schema& rightSchema,
    const EquiCols& eq, JoinType joinType, const Schema& /*merged*/) const
{
    std::vector<Row> result;

    // Build phase: hash right side
    std::unordered_map<std::string, std::vector<size_t>> hashTable;
    for (size_t i = 0; i < rightRows.size(); i++) {
        std::string key = valueToString(rightRows[i].get(eq.rightIdx));
        hashTable[key].push_back(i);
    }

    // Track matched right rows for RIGHT JOIN
    std::unordered_set<size_t> matchedRight;

    // Probe phase
    for (const auto& left : leftRows) {
        std::string key = valueToString(left.get(eq.leftIdx));
        auto it = hashTable.find(key);

        if (it != hashTable.end() && !it->second.empty()) {
            for (size_t ri : it->second) {
                result.push_back(Row::merge(left, rightRows[ri]));
                if (joinType == JoinType::RIGHT) matchedRight.insert(ri);
            }
        } else if (joinType == JoinType::LEFT) {
            result.push_back(Row::merge(left, Row::nullRow(rightSchema.getColumnCount())));
        }
    }

    // RIGHT JOIN: emit unmatched right rows
    if (joinType == JoinType::RIGHT) {
        for (size_t i = 0; i < rightRows.size(); i++) {
            if (!matchedRight.count(i)) {
                // We need leftSchema column count — derive from first merged row or use rightRows[i] size
                int lCols = (result.empty())
                    ? 0  // edge case: compute from merged - right
                    : (int)result[0].size() - rightSchema.getColumnCount();
                result.push_back(Row::merge(Row::nullRow(lCols), rightRows[i]));
            }
        }
    }

    return result;
}

// ──────────────────────────────────────────────────────────────
//  Nested Loop Join
// ──────────────────────────────────────────────────────────────

std::vector<Row> JoinExecutor::nestedLoopJoin(
    const std::vector<Row>& leftRows,  const Schema& /*leftSchema*/,
    const std::vector<Row>& rightRows, const Schema& rightSchema,
    const ExprPtr& onCondition, JoinType joinType,
    const Schema& merged) const
{
    std::vector<Row> result;

    for (const auto& left : leftRows) {
        bool matched = false;
        for (const auto& right : rightRows) {
            Row m = Row::merge(left, right);
            if (!onCondition || evaluator_.evaluateBoolean(onCondition, m, merged)) {
                result.push_back(std::move(m));
                matched = true;
            }
        }
        if (!matched && joinType == JoinType::LEFT) {
            result.push_back(Row::merge(left, Row::nullRow(rightSchema.getColumnCount())));
        }
    }

    return result;
}

// ──────────────────────────────────────────────────────────────
//  Equi-join detection
// ──────────────────────────────────────────────────────────────

std::optional<JoinExecutor::EquiCols>
JoinExecutor::extractEquiJoin(const ExprPtr& cond,
                               const Schema& leftSchema,
                               const Schema& rightSchema) {
    if (!cond || cond->kind() != ExprKind::BINARY) return std::nullopt;
    auto* bin = static_cast<const BinaryExpr*>(cond.get());
    if (bin->op != "=") return std::nullopt;
    if (bin->left->kind()  != ExprKind::COLUMN_REF) return std::nullopt;
    if (bin->right->kind() != ExprKind::COLUMN_REF) return std::nullopt;

    auto* lRef = static_cast<const ColumnRefExpr*>(bin->left.get());
    auto* rRef = static_cast<const ColumnRefExpr*>(bin->right.get());

    int li = tryResolve(lRef, leftSchema);
    int ri = tryResolve(rRef, rightSchema);
    if (li >= 0 && ri >= 0) return EquiCols{li, ri};

    li = tryResolve(rRef, leftSchema);
    ri = tryResolve(lRef, rightSchema);
    if (li >= 0 && ri >= 0) return EquiCols{li, ri};

    return std::nullopt;
}

int JoinExecutor::tryResolve(const ColumnRefExpr* ref, const Schema& schema) {
    try {
        return schema.resolveColumn(ref->tableName, ref->columnName);
    } catch (...) {
        return -1;
    }
}
