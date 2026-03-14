#pragma once
#include "executor/QueryResult.hpp"
#include <string>

/**
 * Renders a QueryResult as a formatted ASCII table to stdout.
 *
 * Example:
 *   +------+----------+--------+
 *   | id   | name     | salary |
 *   +------+----------+--------+
 *   | 1    | Alice    | 95000  |
 *   | 2    | Bob      | 87500  |
 *   +------+----------+--------+
 *   2 rows (0.3 ms)
 */
class TablePrinter {
public:
    TablePrinter() = default;

    /** Print the full result set (table + optional stats row). */
    void print(const QueryResult& result) const;

    /** Enable / disable the "N rows (X ms)" summary footer. */
    void setShowStats(bool show) { showStats_ = show; }
    bool getShowStats() const    { return showStats_; }

private:
    bool showStats_ = true;

    /** Convert a single Value to a display string. */
    static std::string formatValue(const Value& v);

    /** Print a divider row: +------+------+... */
    static void printDivider(const std::vector<int>& widths);
};
