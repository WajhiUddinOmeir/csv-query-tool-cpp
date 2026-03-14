#include "ExpressionEvaluator.hpp"
#include <regex>
#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <sstream>

// ──────────────────────────────────────────────────────────────
//  Public API
// ──────────────────────────────────────────────────────────────

Value ExpressionEvaluator::evaluate(const ExprPtr& expr,
                                     const Row& row,
                                     const Schema& schema) const {
    switch (expr->kind()) {
        case ExprKind::LITERAL:
            return static_cast<const LiteralExpr*>(expr.get())->val;

        case ExprKind::COLUMN_REF:
            return evaluateColumnRef(static_cast<const ColumnRefExpr*>(expr.get()), row, schema);

        case ExprKind::BINARY:
            return evaluateBinary(static_cast<const BinaryExpr*>(expr.get()), row, schema);

        case ExprKind::FUNCTION_CALL:
            return evaluateFunction(static_cast<const FunctionCallExpr*>(expr.get()), row, schema);

        case ExprKind::NOT_EXPR: {
            Value val = evaluate(static_cast<const NotExpr*>(expr.get())->expr, row, schema);
            if (isNull(val)) return std::monostate{};
            return !valueToBool(val);
        }

        case ExprKind::IS_NULL: {
            auto* e = static_cast<const IsNullExpr*>(expr.get());
            Value val = evaluate(e->expr, row, schema);
            bool result = isNull(val);
            return e->negated ? !result : result;
        }

        case ExprKind::BETWEEN:
            return evaluateBetween(static_cast<const BetweenExpr*>(expr.get()), row, schema);

        case ExprKind::IN_EXPR:
            return evaluateIn(static_cast<const InExpr*>(expr.get()), row, schema);

        case ExprKind::SUBQUERY:
            throw std::runtime_error(
                "Internal error: subquery was not materialised before evaluation");

        case ExprKind::STAR:
            throw std::runtime_error("Cannot evaluate * expression directly");

        default:
            throw std::runtime_error("Unknown expression kind");
    }
}

bool ExpressionEvaluator::evaluateBoolean(const ExprPtr& expr,
                                           const Row& row,
                                           const Schema& schema) const {
    Value val = evaluate(expr, row, schema);
    if (isNull(val)) return false;
    return valueToBool(val);
}

// ──────────────────────────────────────────────────────────────
//  Column reference
// ──────────────────────────────────────────────────────────────

Value ExpressionEvaluator::evaluateColumnRef(const ColumnRefExpr* ref,
                                              const Row& row,
                                              const Schema& schema) const {
    int idx = schema.resolveColumn(ref->tableName, ref->columnName);
    return row.get(idx);
}

// ──────────────────────────────────────────────────────────────
//  Binary expression
// ──────────────────────────────────────────────────────────────

Value ExpressionEvaluator::evaluateBinary(const BinaryExpr* expr,
                                           const Row& row,
                                           const Schema& schema) const {
    const std::string& op = expr->op;

    // Short-circuit AND / OR
    if (op == "AND") {
        Value left = evaluate(expr->left, row, schema);
        if (isNull(left) || !valueToBool(left)) return false;
        Value right = evaluate(expr->right, row, schema);
        if (isNull(right)) return false;
        return valueToBool(right);
    }
    if (op == "OR") {
        Value left = evaluate(expr->left, row, schema);
        if (!isNull(left) && valueToBool(left)) return true;
        Value right = evaluate(expr->right, row, schema);
        if (isNull(right)) return false;
        return valueToBool(right);
    }

    Value left  = evaluate(expr->left,  row, schema);
    Value right = evaluate(expr->right, row, schema);

    // LIKE
    if (op == "LIKE") return evaluateLike(left, right);

    // NULL propagation
    if (isNull(left) || isNull(right)) return std::monostate{};

    // Comparisons
    if (op == "=")  return compare(left, right) == 0;
    if (op == "!=") return compare(left, right) != 0;
    if (op == "<")  return compare(left, right) <  0;
    if (op == ">")  return compare(left, right) >  0;
    if (op == "<=") return compare(left, right) <= 0;
    if (op == ">=") return compare(left, right) >= 0;

    // Arithmetic
    double l = valueToDouble(left), r = valueToDouble(right);
    if (op == "+") return smartNumericResult(l + r, left, right);
    if (op == "-") return smartNumericResult(l - r, left, right);
    if (op == "*") return smartNumericResult(l * r, left, right);
    if (op == "/") {
        if (r == 0) throw std::runtime_error("Division by zero");
        return l / r;
    }
    if (op == "%") {
        if (r == 0) throw std::runtime_error("Modulo by zero");
        return smartNumericResult(std::fmod(l, r), left, right);
    }

    throw std::runtime_error("Unknown operator: " + op);
}

// ──────────────────────────────────────────────────────────────
//  BETWEEN
// ──────────────────────────────────────────────────────────────

Value ExpressionEvaluator::evaluateBetween(const BetweenExpr* expr,
                                            const Row& row,
                                            const Schema& schema) const {
    Value val  = evaluate(expr->expr, row, schema);
    Value low  = evaluate(expr->low,  row, schema);
    Value high = evaluate(expr->high, row, schema);
    if (isNull(val) || isNull(low) || isNull(high)) return std::monostate{};
    bool result = compare(val, low) >= 0 && compare(val, high) <= 0;
    return expr->negated ? !result : result;
}

// ──────────────────────────────────────────────────────────────
//  IN
// ──────────────────────────────────────────────────────────────

Value ExpressionEvaluator::evaluateIn(const InExpr* expr,
                                       const Row& row,
                                       const Schema& schema) const {
    Value val = evaluate(expr->expr, row, schema);
    if (isNull(val)) return std::monostate{};
    for (const auto& ve : expr->values) {
        Value v = evaluate(ve, row, schema);
        if (!isNull(v) && compare(val, v) == 0)
            return !expr->negated;
    }
    return expr->negated;
}

// ──────────────────────────────────────────────────────────────
//  LIKE pattern matching
// ──────────────────────────────────────────────────────────────

Value ExpressionEvaluator::evaluateLike(const Value& value,
                                         const Value& pattern) const {
    if (isNull(value) || isNull(pattern)) return std::monostate{};
    std::string str = valueToString(value);
    std::string pat = valueToString(pattern);

    // Convert SQL LIKE pattern to regex: % → .* , _ → .
    std::string regex_str = "^";
    for (char c : pat) {
        if (c == '%') {
            regex_str += ".*";
        } else if (c == '_') {
            regex_str += '.';
        } else if (std::string("\\.[]{}()*+?^$|").find(c) != std::string::npos) {
            regex_str += '\\';
            regex_str += c;
        } else {
            regex_str += c;
        }
    }
    regex_str += '$';

    try {
        std::regex re(regex_str, std::regex_constants::icase);
        return std::regex_match(str, re);
    } catch (...) {
        return false;
    }
}

// ──────────────────────────────────────────────────────────────
//  Scalar functions
// ──────────────────────────────────────────────────────────────

Value ExpressionEvaluator::evaluateFunction(const FunctionCallExpr* func,
                                             const Row& row,
                                             const Schema& schema) const {
    // If aggregate and column pre-computed in schema, return it
    if (func->isAggregate()) {
        std::string canon = func->toCanonicalName();
        int idx = schema.findColumn(canon);
        if (idx >= 0) return row.get(idx);
        if (!func->alias.empty()) {
            idx = schema.findColumn(func->alias);
            if (idx >= 0) return row.get(idx);
        }
    }

    const auto& args = func->arguments;
    const std::string& name = func->name;

    auto arg0 = [&]() -> Value { return evaluate(args[0], row, schema); };

    if (name == "UPPER") {
        Value v = arg0(); if (isNull(v)) return v;
        std::string s = valueToString(v);
        std::transform(s.begin(), s.end(), s.begin(), ::toupper);
        return s;
    }
    if (name == "LOWER") {
        Value v = arg0(); if (isNull(v)) return v;
        std::string s = valueToString(v);
        std::transform(s.begin(), s.end(), s.begin(), ::tolower);
        return s;
    }
    if (name == "LENGTH") {
        Value v = arg0(); if (isNull(v)) return v;
        return static_cast<long long>(valueToString(v).size());
    }
    if (name == "TRIM") {
        Value v = arg0(); if (isNull(v)) return v;
        std::string s = valueToString(v);
        size_t start = s.find_first_not_of(" \t\n\r");
        if (start == std::string::npos) return std::string("");
        size_t end = s.find_last_not_of(" \t\n\r");
        return s.substr(start, end - start + 1);
    }
    if (name == "ABS") {
        Value v = arg0(); if (isNull(v)) return v;
        return std::abs(valueToDouble(v));
    }
    if (name == "ROUND") {
        Value v = arg0(); if (isNull(v)) return v;
        double d = valueToDouble(v);
        int scale = 0;
        if (args.size() > 1) {
            Value sv = evaluate(args[1], row, schema);
            if (!isNull(sv)) scale = (int)valueToDouble(sv);
        }
        double factor = std::pow(10.0, scale);
        return std::round(d * factor) / factor;
    }
    if (name == "COALESCE") {
        for (const auto& arg : args) {
            Value v = evaluate(arg, row, schema);
            if (!isNull(v)) return v;
        }
        return std::monostate{};
    }
    if (name == "CONCAT") {
        std::string result;
        for (const auto& arg : args) {
            Value v = evaluate(arg, row, schema);
            if (!isNull(v)) result += valueToString(v);
        }
        return result;
    }
    if (name == "SUBSTRING") {
        if (args.size() < 2) throw std::runtime_error("SUBSTRING requires at least 2 arguments");
        Value sv = arg0(); if (isNull(sv)) return sv;
        std::string s = valueToString(sv);
        Value startVal = evaluate(args[1], row, schema);
        int start = (int)valueToDouble(startVal) - 1; // SQL is 1-based
        start = std::max(0, start);
        if (args.size() >= 3) {
            Value lenVal = evaluate(args[2], row, schema);
            int len = (int)valueToDouble(lenVal);
            int end = std::min((int)s.size(), start + len);
            return s.substr(start, end - start);
        }
        return s.substr(start);
    }
    throw std::runtime_error("Unknown function: " + name);
}

// ──────────────────────────────────────────────────────────────
//  Type utilities
// ──────────────────────────────────────────────────────────────

int ExpressionEvaluator::compare(const Value& a, const Value& b) {
    return compareValues(a, b);
}

bool ExpressionEvaluator::toBoolean(const Value& v) {
    return valueToBool(v);
}
