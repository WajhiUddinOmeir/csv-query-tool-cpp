#pragma once
#include "Value.hpp"
#include <vector>
#include <stdexcept>

/**
 * A single row of data. Each value maps to a column in the row's schema.
 * Intentionally lightweight — carries values only, not schema metadata.
 */
class Row {
public:
    std::vector<Value> values;

    Row() = default;
    explicit Row(std::vector<Value> vals) : values(std::move(vals)) {}

    const Value& get(int index) const {
        if (index < 0 || index >= (int)values.size())
            throw std::out_of_range("Column index " + std::to_string(index) +
                                    " out of range [0, " + std::to_string(values.size()) + ")");
        return values[index];
    }

    int size() const { return (int)values.size(); }

    /** Merges two rows by concatenating their value arrays (used for JOINs). */
    static Row merge(const Row& left, const Row& right) {
        std::vector<Value> merged;
        merged.reserve(left.size() + right.size());
        merged.insert(merged.end(), left.values.begin(),  left.values.end());
        merged.insert(merged.end(), right.values.begin(), right.values.end());
        return Row(std::move(merged));
    }

    /** Creates a row of all NULLs (for unmatched side of LEFT/RIGHT JOINs). */
    static Row nullRow(int sz) {
        return Row(std::vector<Value>(sz, std::monostate{}));
    }
};
