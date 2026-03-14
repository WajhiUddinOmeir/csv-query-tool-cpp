#include "util/TablePrinter.hpp"
#include <iostream>
#include <iomanip>
#include <sstream>
#include <cmath>

// ---------------------------------------------------------------------------
// Value formatting
// ---------------------------------------------------------------------------

std::string TablePrinter::formatValue(const Value& v) {
    if (isNull(v)) return "NULL";

    if (std::holds_alternative<long long>(v))
        return std::to_string(std::get<long long>(v));

    if (std::holds_alternative<bool>(v))
        return std::get<bool>(v) ? "true" : "false";

    if (std::holds_alternative<double>(v)) {
        double d = std::get<double>(v);
        // If it is a whole number, omit the decimal part
        if (d == std::floor(d) && !std::isinf(d)) {
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(0) << d;
            return oss.str();
        }
        // Otherwise show up to 6 significant digits, strip trailing zeros
        std::ostringstream oss;
        oss << std::defaultfloat << std::setprecision(6) << d;
        return oss.str();
    }

    // std::string
    return std::get<std::string>(v);
}

// ---------------------------------------------------------------------------
// Divider row
// ---------------------------------------------------------------------------

void TablePrinter::printDivider(const std::vector<int>& widths) {
    std::cout << '+';
    for (int w : widths) {
        for (int i = 0; i < w + 2; i++) std::cout << '-';
        std::cout << '+';
    }
    std::cout << '\n';
}

// ---------------------------------------------------------------------------
// Main print
// ---------------------------------------------------------------------------

void TablePrinter::print(const QueryResult& result) const {
    // Command / error messages have no tabular data
    if (!result.message.empty()) {
        std::cout << result.message << '\n';
        return;
    }

    const auto& cols = result.columnNames;
    const auto& rows = result.rows;

    // ---- Compute column widths ----
    std::vector<int> widths(cols.size(), 0);
    for (size_t c = 0; c < cols.size(); c++)
        widths[c] = (int)cols[c].size();

    for (const auto& row : rows)
        for (size_t c = 0; c < row.size() && c < widths.size(); c++) {
            int len = (int)formatValue(row[c]).size();
            if (len > widths[c]) widths[c] = len;
        }

    // ---- Top divider ----
    printDivider(widths);

    // ---- Header row ----
    std::cout << '|';
    for (size_t c = 0; c < cols.size(); c++) {
        std::cout << ' ' << std::left << std::setw(widths[c]) << cols[c] << " |";
    }
    std::cout << '\n';

    // ---- Header / body divider ----
    printDivider(widths);

    // ---- Data rows ----
    for (const auto& row : rows) {
        std::cout << '|';
        for (size_t c = 0; c < cols.size(); c++) {
            std::string val = c < row.size() ? formatValue(row[c]) : "";
            // Right-align numbers, left-align everything else
            bool isNumeric = (c < row.size())
                && (std::holds_alternative<long long>(row[c])
                    || std::holds_alternative<double>(row[c]));
            if (isNumeric)
                std::cout << ' ' << std::right << std::setw(widths[c]) << val << " |";
            else
                std::cout << ' ' << std::left  << std::setw(widths[c]) << val << " |";
        }
        std::cout << '\n';
    }

    // ---- Bottom divider ----
    printDivider(widths);

    // ---- Stats footer ----
    if (showStats_) {
        int rowCount = (int)rows.size();
        std::cout << rowCount << (rowCount == 1 ? " row" : " rows");
        if (result.executionTimeMs >= 0.0)
            std::cout << " (" << std::fixed << std::setprecision(2)
                      << result.executionTimeMs << " ms)";
        if (result.rowsScanned > 0)
            std::cout << " [scanned: " << result.rowsScanned << "]";
        std::cout << '\n';
    }
}
