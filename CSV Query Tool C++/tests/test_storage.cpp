/**
 * Storage layer tests:
 *   - CsvTable: load, type inference, quoted fields, NULL values, DDL mutations
 *   - Schema: column resolution, merge, ambiguity detection
 *   - Value: comparisons, null handling
 *   - Row: merge, nullRow
 */
#include "test_framework.hpp"
#include "storage/CsvTable.hpp"
#include "storage/Schema.hpp"
#include "storage/Row.hpp"
#include "storage/Value.hpp"

#include <fstream>
#include <cstdio>    // tmpnam / remove
#include <string>
#include <stdexcept>

// ─────────────────────────────────────────────────────────────
//  Helper: write a temp CSV file and return its path.
//  Caller must remove the file when done.
// ─────────────────────────────────────────────────────────────
static std::string writeTempCsv(const std::string& content) {
    // Use a fixed predictable path in the system temp folder
    static int counter = 0;
    std::string path = std::string(std::getenv("TEMP") ? std::getenv("TEMP") : "/tmp")
                       + "/csv_test_" + std::to_string(++counter) + ".csv";
    std::ofstream f(path);
    if (!f) throw std::runtime_error("Cannot write temp file: " + path);
    f << content;
    return path;
}

// ─────────────────────────────────────────────────────────────
//  CsvTable: basic load
// ─────────────────────────────────────────────────────────────

TEST("CsvTable: load basic CSV — row and column count") {
    auto path = writeTempCsv("id,name,active\n1,Alice,true\n2,Bob,false\n3,Charlie,true\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    ASSERT_EQ(t.getName(), std::string("test"));
    ASSERT_EQ(t.getRowCount(), 3);
    ASSERT_EQ(t.getColumnCount(), 3);
}

TEST("CsvTable: type inference — INTEGER, STRING, BOOLEAN") {
    auto path = writeTempCsv("id,name,active\n1,Alice,true\n2,Bob,false\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    ASSERT_EQ(t.getSchema().getColumn(0).type, DataType::INTEGER);
    ASSERT_EQ(t.getSchema().getColumn(1).type, DataType::STRING);
    ASSERT_EQ(t.getSchema().getColumn(2).type, DataType::BOOLEAN);
}

TEST("CsvTable: type inference — mixed int/decimal → DOUBLE") {
    auto path = writeTempCsv("value\n1\n2.5\n3\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    ASSERT_EQ(t.getSchema().getColumn(0).type, DataType::DOUBLE);
}

TEST("CsvTable: type inference — mixed int/string → STRING") {
    auto path = writeTempCsv("value\n1\nhello\n3\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    ASSERT_EQ(t.getSchema().getColumn(0).type, DataType::STRING);
}

TEST("CsvTable: empty field parsed as NULL") {
    auto path = writeTempCsv("id,name\n1,Alice\n2,\n3,Charlie\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    ASSERT_EQ(t.getRowCount(), 3);
    ASSERT_TRUE(isNull(t.getRows()[1].get(1))); // empty name → NULL
}

TEST("CsvTable: quoted CSV fields with embedded commas") {
    auto path = writeTempCsv("name,city\n\"Smith, John\",\"New York\"\nJane,Boston\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    ASSERT_EQ(t.getRowCount(), 2);
    ASSERT_EQ(std::get<std::string>(t.getRows()[0].get(0)), std::string("Smith, John"));
}

TEST("CsvTable: quoted fields with escaped double quotes") {
    auto path = writeTempCsv("name\n\"She said \"\"hello\"\"\"\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    ASSERT_EQ(std::get<std::string>(t.getRows()[0].get(0)), std::string("She said \"hello\""));
}

TEST("CsvTable: INTEGER values stored as long long") {
    auto path = writeTempCsv("id,salary\n1,85000\n2,72000\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    Value id0 = t.getRows()[0].get(0);
    ASSERT_TRUE(std::holds_alternative<long long>(id0));
    ASSERT_EQ(std::get<long long>(id0), 1LL);

    Value sal = t.getRows()[0].get(1);
    ASSERT_EQ(std::get<long long>(sal), 85000LL);
}

TEST("CsvTable: BOOLEAN values stored as bool") {
    auto path = writeTempCsv("flag\ntrue\nfalse\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    ASSERT_EQ(std::get<bool>(t.getRows()[0].get(0)), true);
    ASSERT_EQ(std::get<bool>(t.getRows()[1].get(0)), false);
}

// ─────────────────────────────────────────────────────────────
//  CsvTable: DDL mutations
// ─────────────────────────────────────────────────────────────

TEST("CsvTable: addColumn — new column is NULL for existing rows") {
    std::vector<Column> cols = { Column{"id", DataType::INTEGER},
                                  Column{"name", DataType::STRING} };
    CsvTable t = CsvTable::createEmpty("t", cols);
    t.addRow(Row(std::vector<Value>{1LL, std::string("Alice")}));
    t.addRow(Row(std::vector<Value>{2LL, std::string("Bob")}));

    t.addColumn(Column{"score", DataType::INTEGER});

    ASSERT_EQ(t.getColumnCount(), 3);
    ASSERT_TRUE(isNull(t.getRows()[0].get(2)));
    ASSERT_TRUE(isNull(t.getRows()[1].get(2)));
}

TEST("CsvTable: dropColumn removes column and its data") {
    std::vector<Column> cols = { Column{"id",    DataType::INTEGER},
                                  Column{"name",  DataType::STRING},
                                  Column{"score", DataType::INTEGER} };
    CsvTable t = CsvTable::createEmpty("t", cols);
    t.addRow(Row(std::vector<Value>{1LL, std::string("Alice"), 90LL}));

    t.dropColumn("name");

    ASSERT_EQ(t.getColumnCount(), 2);
    ASSERT_EQ(t.getSchema().getColumn(0).name, std::string("id"));
    ASSERT_EQ(t.getSchema().getColumn(1).name, std::string("score"));
    ASSERT_EQ(std::get<long long>(t.getRows()[0].get(1)), 90LL);
}

TEST("CsvTable: renameColumn — data intact, new name accessible") {
    std::vector<Column> cols = { Column{"id",   DataType::INTEGER},
                                  Column{"name", DataType::STRING} };
    CsvTable t = CsvTable::createEmpty("t", cols);
    t.addRow(Row(std::vector<Value>{1LL, std::string("Alice")}));

    t.renameColumn("name", "full_name");

    ASSERT_EQ(t.getSchema().getColumn(1).name, std::string("full_name"));
    ASSERT_EQ(std::get<std::string>(t.getRows()[0].get(1)), std::string("Alice"));
}

TEST("CsvTable: clearRows removes all rows, column structure intact") {
    std::vector<Column> cols = { Column{"val", DataType::INTEGER} };
    CsvTable t = CsvTable::createEmpty("t", cols);
    t.addRow(Row(std::vector<Value>{1LL}));
    t.addRow(Row(std::vector<Value>{2LL}));
    ASSERT_EQ(t.getRowCount(), 2);

    t.clearRows();

    ASSERT_EQ(t.getRowCount(), 0);
    ASSERT_EQ(t.getColumnCount(), 1);
}

// ─────────────────────────────────────────────────────────────
//  Schema
// ─────────────────────────────────────────────────────────────

TEST("Schema: resolveColumn — case-insensitive lookup") {
    auto path = writeTempCsv("id,name,salary\n1,Alice,85000\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    Schema& s = t.getSchema();
    ASSERT_EQ(s.resolveColumn("", "id"),     0);
    ASSERT_EQ(s.resolveColumn("", "name"),   1);
    ASSERT_EQ(s.resolveColumn("", "salary"), 2);
    ASSERT_EQ(s.resolveColumn("", "ID"),     0); // case-insensitive
    ASSERT_EQ(s.resolveColumn("", "Name"),   1);
}

TEST("Schema: resolveColumn throws on unknown column") {
    auto path = writeTempCsv("id,name\n1,Alice\n");
    CsvTable t = CsvTable::load(path, "test");
    std::remove(path.c_str());

    ASSERT_THROWS(t.getSchema().resolveColumn("", "nonexistent"));
}

TEST("Schema: merge for JOINs — column count and qualified resolution") {
    Schema left("e", { Column{"id",   DataType::INTEGER, "e"},
                       Column{"name", DataType::STRING,  "e"} });
    Schema right("d", { Column{"id",       DataType::INTEGER, "d"},
                        Column{"dept_name", DataType::STRING, "d"} });

    Schema merged = Schema::merge(left, right);

    ASSERT_EQ(merged.getColumnCount(), 4);
    ASSERT_EQ(merged.resolveColumn("e", "id"),        0);
    ASSERT_EQ(merged.resolveColumn("e", "name"),      1);
    ASSERT_EQ(merged.resolveColumn("d", "id"),        2);
    ASSERT_EQ(merged.resolveColumn("d", "dept_name"), 3);
}

TEST("Schema: ambiguous unqualified column throws after merge") {
    Schema left("e",  { Column{"id", DataType::INTEGER, "e"} });
    Schema right("d", { Column{"id", DataType::INTEGER, "d"} });
    Schema merged = Schema::merge(left, right);

    // Unqualified "id" is ambiguous — must throw
    ASSERT_THROWS(merged.resolveColumn("", "id"));
}

TEST("Schema: getColumns returns all columns") {
    Schema s("t", { Column{"a", DataType::INTEGER},
                    Column{"b", DataType::STRING} });
    ASSERT_EQ((int)s.getColumns().size(), 2);
    ASSERT_EQ(s.getColumns()[0].name, std::string("a"));
}

// ─────────────────────────────────────────────────────────────
//  Value helpers
// ─────────────────────────────────────────────────────────────

TEST("Value: isNull returns true for monostate") {
    Value v = std::monostate{};
    ASSERT_TRUE(isNull(v));
}

TEST("Value: isNull returns false for non-null variants") {
    ASSERT_FALSE(isNull(Value{1LL}));
    ASSERT_FALSE(isNull(Value{3.14}));
    ASSERT_FALSE(isNull(Value{true}));
    ASSERT_FALSE(isNull(Value{std::string("hi")}));
}

TEST("Value: compareValues — integer ordering") {
    ASSERT_TRUE(compareValues(Value{1LL}, Value{2LL}) < 0);
    ASSERT_TRUE(compareValues(Value{2LL}, Value{1LL}) > 0);
    ASSERT_EQ(compareValues(Value{5LL}, Value{5LL}), 0);
}

TEST("Value: compareValues — string lexicographic") {
    ASSERT_TRUE(compareValues(Value{std::string("apple")},
                              Value{std::string("banana")}) < 0);
}

TEST("Value: valueToString — monostate → NULL") {
    ASSERT_EQ(valueToString(Value{std::monostate{}}), std::string("NULL"));
}

TEST("Value: valueToString — long long") {
    ASSERT_EQ(valueToString(Value{42LL}), std::string("42"));
}

TEST("Row: merge combines rows correctly") {
    Row left(std::vector<Value>{1LL, std::string("Alice")});
    Row right(std::vector<Value>{10LL, std::string("Eng")});

    Row merged = Row::merge(left, right);
    ASSERT_EQ(merged.size(), 4);
    ASSERT_EQ(std::get<long long>(merged.get(0)), 1LL);
    ASSERT_EQ(std::get<std::string>(merged.get(1)), std::string("Alice"));
    ASSERT_EQ(std::get<long long>(merged.get(2)), 10LL);
}

TEST("Row: nullRow creates row of all NULLs") {
    Row r = Row::nullRow(3);
    ASSERT_EQ(r.size(), 3);
    ASSERT_TRUE(isNull(r.get(0)));
    ASSERT_TRUE(isNull(r.get(1)));
    ASSERT_TRUE(isNull(r.get(2)));
}
