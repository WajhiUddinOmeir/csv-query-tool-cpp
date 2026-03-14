#pragma once
#include "parser/ast/Expr.hpp"
#include "index/IndexManager.hpp"
#include <string>
#include <vector>

/**
 * Result of a predicate push-down split.
 *
 * `pushed`    — sub-expression that applies only to `targetTable`, safe to
 *               evaluate before the JOIN (or used for index selection).
 * `remaining` — everything else that must be evaluated after the JOIN.
 *
 * Either pointer may be null (meaning "nothing to push" / "nothing left").
 */
struct PushdownResult {
    ExprPtr pushed;    ///< Expression pushed down to the base table scan
    ExprPtr remaining; ///< Expression that must stay in the outer filter
};

/**
 * Rule-based query optimizer.
 *
 * Currently implements:
 *  1. **Predicate push-down** — splits a WHERE expression into the part
 *     that references only a specific table (pushed into the scan) and the
 *     remainder that must be evaluated after JOINs.
 *  2. **Index selection** — detects `col = literal` patterns and checks
 *     whether an index exists on that column.
 */
class QueryOptimizer {
public:
    explicit QueryOptimizer(const IndexManager& indexManager)
        : indexManager_(indexManager) {}

    // -----------------------------------------------------------------------
    // Predicate push-down
    // -----------------------------------------------------------------------

    /**
     * Split `where` into (pushed, remaining) where `pushed` only references
     * columns from `targetTable` (name or alias).
     *
     * Uses AND-flattenning: top-level AND conjuncts are split independently,
     * then reconstructed. Non-AND nodes are pushed whole if they qualify.
     */
    PushdownResult pushDown(const ExprPtr& where,
                            const std::string& targetTable) const
    {
        if (!where) return {nullptr, nullptr};

        // Collect top-level AND conjuncts
        std::vector<ExprPtr> conjuncts;
        flattenAnd(where, conjuncts);

        std::vector<ExprPtr> pushedVec;
        std::vector<ExprPtr> remainingVec;

        for (const auto& conj : conjuncts) {
            if (referencesOnlyTable(conj, targetTable))
                pushedVec.push_back(conj);
            else
                remainingVec.push_back(conj);
        }

        return {combineAnd(pushedVec), combineAnd(remainingVec)};
    }

    // -----------------------------------------------------------------------
    // Index selection
    // -----------------------------------------------------------------------

    /**
     * Returns true if `where` is an equality predicate `col = literal` (or
     * `literal = col`) where `col` belongs to `tableName` and an index exists
     * on that column.
     */
    bool canUseIndex(const ExprPtr& where,
                     const std::string& tableName) const
    {
        if (!where) return false;
        if (where->kind() != ExprKind::BINARY) return false;

        auto* bin = static_cast<const BinaryExpr*>(where.get());
        if (bin->op != "=") return false;

        // col = literal  or  literal = col
        const ColumnRefExpr* colRef = nullptr;
        if (bin->left->kind()  == ExprKind::COLUMN_REF &&
            bin->right->kind() == ExprKind::LITERAL)
        {
            colRef = static_cast<const ColumnRefExpr*>(bin->left.get());
        }
        else if (bin->right->kind() == ExprKind::COLUMN_REF &&
                 bin->left->kind()  == ExprKind::LITERAL)
        {
            colRef = static_cast<const ColumnRefExpr*>(bin->right.get());
        }

        if (!colRef) return false;

        // Column must belong to this table (or have no explicit qualifier)
        if (!colRef->tableName.empty()) {
            std::string qt = colRef->tableName;
            for (auto& c : qt) c = (char)std::tolower((unsigned char)c);
            std::string tt = tableName;
            for (auto& c : tt) c = (char)std::tolower((unsigned char)c);
            if (qt != tt) return false;
        }

        return indexManager_.hasIndex(tableName, colRef->columnName);
    }

    /**
     * Extract the column name from an index-eligible predicate
     * (same pattern as canUseIndex but returns the column name).
     * Returns "" if not applicable.
     */
    std::string getIndexColumn(const ExprPtr& where) const {
        if (!where || where->kind() != ExprKind::BINARY) return "";
        auto* bin = static_cast<const BinaryExpr*>(where.get());
        if (bin->op != "=") return "";

        if (bin->left->kind() == ExprKind::COLUMN_REF &&
            bin->right->kind() == ExprKind::LITERAL)
            return static_cast<const ColumnRefExpr*>(bin->left.get())->columnName;

        if (bin->right->kind() == ExprKind::COLUMN_REF &&
            bin->left->kind()  == ExprKind::LITERAL)
            return static_cast<const ColumnRefExpr*>(bin->right.get())->columnName;

        return "";
    }

    /**
     * Extract the literal value from an index-eligible predicate.
     */
    Value getIndexValue(const ExprPtr& where) const {
        if (!where || where->kind() != ExprKind::BINARY) return std::monostate{};
        auto* bin = static_cast<const BinaryExpr*>(where.get());

        if (bin->left->kind() == ExprKind::COLUMN_REF &&
            bin->right->kind() == ExprKind::LITERAL)
            return static_cast<const LiteralExpr*>(bin->right.get())->val;

        if (bin->right->kind() == ExprKind::COLUMN_REF &&
            bin->left->kind()  == ExprKind::LITERAL)
            return static_cast<const LiteralExpr*>(bin->left.get())->val;

        return std::monostate{};
    }

private:
    const IndexManager& indexManager_;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Recursively collect top-level AND conjuncts into `out`. */
    static void flattenAnd(const ExprPtr& expr, std::vector<ExprPtr>& out) {
        if (!expr) return;
        if (expr->kind() == ExprKind::BINARY) {
            auto* bin = static_cast<const BinaryExpr*>(expr.get());
            if (bin->op == "AND") {
                flattenAnd(bin->left,  out);
                flattenAnd(bin->right, out);
                return;
            }
        }
        out.push_back(expr);
    }

    /** Re-combine a list of conjuncts into nested AND nodes. */
    static ExprPtr combineAnd(const std::vector<ExprPtr>& conjuncts) {
        if (conjuncts.empty()) return nullptr;
        ExprPtr result = conjuncts[0];
        for (size_t i = 1; i < conjuncts.size(); i++) {
            result = std::make_shared<BinaryExpr>(result, std::string("AND"), conjuncts[i]);
        }
        return result;
    }

    /**
     * Returns true if every ColumnRefExpr in `expr` either has no table
     * qualifier, or has a qualifier matching `targetTable` (case-insensitive).
     */
    static bool referencesOnlyTable(const ExprPtr& expr,
                                    const std::string& targetTable)
    {
        if (!expr) return true;
        switch (expr->kind()) {
        case ExprKind::LITERAL:
        case ExprKind::STAR:
            return true;

        case ExprKind::COLUMN_REF: {
            auto* cr = static_cast<const ColumnRefExpr*>(expr.get());
            if (cr->tableName.empty()) return true; // unqualified — optimistically push
            std::string qt = cr->tableName;
            std::string tt = targetTable;
            for (auto& c : qt) c = (char)std::tolower((unsigned char)c);
            for (auto& c : tt) c = (char)std::tolower((unsigned char)c);
            return qt == tt;
        }

        case ExprKind::BINARY: {
            auto* bin = static_cast<const BinaryExpr*>(expr.get());
            return referencesOnlyTable(bin->left,  targetTable)
                && referencesOnlyTable(bin->right, targetTable);
        }

        case ExprKind::NOT_EXPR: {
            auto* ne = static_cast<const NotExpr*>(expr.get());
            return referencesOnlyTable(ne->expr, targetTable);
        }

        case ExprKind::IS_NULL: {
            auto* isn = static_cast<const IsNullExpr*>(expr.get());
            return referencesOnlyTable(isn->expr, targetTable);
        }

        case ExprKind::BETWEEN: {
            auto* be = static_cast<const BetweenExpr*>(expr.get());
            return referencesOnlyTable(be->expr,  targetTable)
                && referencesOnlyTable(be->low,   targetTable)
                && referencesOnlyTable(be->high,  targetTable);
        }

        case ExprKind::IN_EXPR: {
            auto* ie = static_cast<const InExpr*>(expr.get());
            if (!referencesOnlyTable(ie->expr, targetTable)) return false;
            for (const auto& opt : ie->values)
                if (!referencesOnlyTable(opt, targetTable)) return false;
            return true;
        }

        case ExprKind::FUNCTION_CALL: {
            auto* fc = static_cast<const FunctionCallExpr*>(expr.get());
            for (const auto& arg : fc->arguments)
                if (!referencesOnlyTable(arg, targetTable)) return false;
            return true;
        }

        default:
            return false; // unknown — be conservative, don't push
        }
    }
};
