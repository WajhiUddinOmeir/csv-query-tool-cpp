#pragma once
#include <variant>
#include <string>
#include <cmath>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <stdexcept>

// NULL = monostate | INTEGER = long long | DOUBLE = double | BOOLEAN = bool | STRING = string
using Value = std::variant<std::monostate, long long, double, bool, std::string>;

inline bool isNull(const Value& v) {
    return std::holds_alternative<std::monostate>(v);
}

inline std::string valueToString(const Value& v) {
    if (isNull(v)) return "NULL";
    if (std::holds_alternative<long long>(v))
        return std::to_string(std::get<long long>(v));
    if (std::holds_alternative<double>(v)) {
        double d = std::get<double>(v);
        if (!std::isfinite(d)) {
            std::ostringstream oss; oss << d; return oss.str();
        }
        if (d == std::floor(d) && std::abs(d) < 1e15)
            return std::to_string(static_cast<long long>(d));
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << d;
        return oss.str();
    }
    if (std::holds_alternative<bool>(v))
        return std::get<bool>(v) ? "true" : "false";
    return std::get<std::string>(v);
}

inline double valueToDouble(const Value& v) {
    if (std::holds_alternative<long long>(v))  return static_cast<double>(std::get<long long>(v));
    if (std::holds_alternative<double>(v))     return std::get<double>(v);
    if (std::holds_alternative<bool>(v))       return std::get<bool>(v) ? 1.0 : 0.0;
    if (std::holds_alternative<std::string>(v)) {
        try { return std::stod(std::get<std::string>(v)); }
        catch (...) { throw std::runtime_error("Cannot convert '" + std::get<std::string>(v) + "' to number"); }
    }
    throw std::runtime_error("Cannot convert NULL to number");
}

inline bool valueToBool(const Value& v) {
    if (std::holds_alternative<bool>(v))       return std::get<bool>(v);
    if (std::holds_alternative<long long>(v))  return std::get<long long>(v) != 0;
    if (std::holds_alternative<double>(v))     return std::get<double>(v) != 0.0;
    if (std::holds_alternative<std::string>(v)) return !std::get<std::string>(v).empty();
    return false; // null
}

inline int compareValues(const Value& a, const Value& b) {
    bool aNum = std::holds_alternative<long long>(a) || std::holds_alternative<double>(a);
    bool bNum = std::holds_alternative<long long>(b) || std::holds_alternative<double>(b);
    if (aNum && bNum) {
        double da = valueToDouble(a), db = valueToDouble(b);
        if (da < db) return -1;
        if (da > db) return  1;
        return 0;
    }
    // String comparison (case-insensitive)
    std::string sa = valueToString(a), sb = valueToString(b);
    std::string saL = sa, sbL = sb;
    std::transform(saL.begin(), saL.end(), saL.begin(), ::tolower);
    std::transform(sbL.begin(), sbL.end(), sbL.begin(), ::tolower);
    if (saL < sbL) return -1;
    if (saL > sbL) return  1;
    return 0;
}

inline int compareNullSafe(const Value& a, const Value& b) {
    bool aN = isNull(a), bN = isNull(b);
    if (aN && bN) return 0;
    if (aN) return -1;
    if (bN) return  1;
    return compareValues(a, b);
}

// Returns Long if both operands are integers and result is whole; otherwise Double.
inline Value smartNumericResult(double result, const Value& a, const Value& b) {
    if (std::holds_alternative<long long>(a) && std::holds_alternative<long long>(b)) {
        if (result == std::floor(result) && std::isfinite(result))
            return static_cast<long long>(result);
    }
    return result;
}
