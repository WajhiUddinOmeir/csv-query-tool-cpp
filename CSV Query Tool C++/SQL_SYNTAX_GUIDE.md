# SQL Syntax Reference

Quick reference for every SQL feature supported by the CSV Query Tool.

---

## Data Loading & Metadata

```sql
LOAD 'path/to/file.csv' AS table_name;
SHOW TABLES;
SHOW INDEXES;
DESCRIBE table_name;
```

---

## SELECT

```sql
SELECT [DISTINCT] <columns | * | expressions | aggregates>
FROM <table> [alias]
  [INNER | LEFT | RIGHT] JOIN <table> [alias] ON <condition>
[WHERE <condition>]
[GROUP BY <columns>]
[HAVING <aggregate_condition>]
[ORDER BY <column> [ASC|DESC] [, ...]]
[LIMIT <n>];
```

```sql
SELECT * FROM employees;
SELECT name, salary FROM employees e WHERE e.salary > 80000;
SELECT DISTINCT department_id FROM employees;
SELECT name AS employee, salary * 12 AS annual FROM employees;
```

---

## WHERE Operators

| Category | Operators |
|----------|-----------|
| Comparison | `=` `!=` `<>` `<` `>` `<=` `>=` |
| Logical | `AND` `OR` `NOT` |
| Arithmetic | `+` `-` `*` `/` `%` |
| Special | `LIKE` `BETWEEN..AND` `IN(..)` `IS [NOT] NULL` |

```sql
SELECT * FROM employees
WHERE department_id IN (1, 2)
  AND salary BETWEEN 70000 AND 95000
  AND name LIKE 'A%'
  AND is_active = true;
```

**LIKE wildcards:** `%` = any characters, `_` = one character.

---

## Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` / `COUNT(col)` / `COUNT(DISTINCT col)` | Row / non-NULL / distinct count |
| `SUM(col)` `AVG(col)` `MIN(col)` `MAX(col)` | Numeric aggregation |

```sql
SELECT department_id, COUNT(*) AS cnt, ROUND(AVG(salary), 2) AS avg_sal
FROM employees
WHERE is_active = true
GROUP BY department_id
HAVING AVG(salary) > 75000
ORDER BY avg_sal DESC;
```

---

## JOIN

| Type | Description |
|------|-------------|
| `[INNER] JOIN` | Only matching rows from both tables |
| `LEFT [OUTER] JOIN` | All left rows + matching right (NULL if no match) |
| `RIGHT [OUTER] JOIN` | All right rows + matching left (NULL if no match) |

```sql
-- Two-table join
SELECT e.name, d.name AS department
FROM employees e JOIN departments d ON e.department_id = d.id;

-- Three-table join
SELECT e.name, d.name AS dept, s.base_salary + s.bonus AS total_comp
FROM employees e
JOIN departments d ON e.department_id = d.id
JOIN salaries s ON e.id = s.employee_id;

-- LEFT JOIN to find gaps
SELECT d.name FROM departments d
LEFT JOIN employees e ON d.id = e.department_id
WHERE e.id IS NULL;
```

---

## Scalar Functions

| Function | Example |
|----------|---------|
| `UPPER(s)` `LOWER(s)` | `UPPER(name)` → `'ALICE'` |
| `LENGTH(s)` `TRIM(s)` | `LENGTH('hello')` → `5` |
| `CONCAT(s1, s2, ...)` | `CONCAT(name, ' - ', location)` |
| `SUBSTRING(s, start, len)` | `SUBSTRING(name, 1, 3)` → `'Ali'` |
| `ABS(n)` `ROUND(n [, d])` | `ROUND(salary / 12, 2)` |
| `COALESCE(v1, v2, ...)` | `COALESCE(bonus, 0)` |

---

## CREATE TABLE

```sql
CREATE TABLE products (
    id INTEGER,       -- aliases: INT
    name STRING,      -- aliases: VARCHAR, TEXT
    price DOUBLE,     -- aliases: FLOAT, DECIMAL, NUMERIC
    active BOOLEAN    -- aliases: BOOL
);
```

---

## INSERT INTO

```sql
-- Positional
INSERT INTO products VALUES (1, 'Widget', 9.99, true);

-- Named columns (omitted columns become NULL)
INSERT INTO products (id, name) VALUES (2, 'Gadget');

-- Multi-row
INSERT INTO products VALUES
    (3, 'Doohickey', 4.75, true),
    (4, 'Thingamajig', 15.00, false);
```

---

## DELETE FROM

```sql
DELETE FROM products;   -- removes all rows, schema stays intact
```

---

## UPDATE

```sql
UPDATE <table>
SET <column> = <expression> [, <column> = <expression> ...]
[WHERE <condition>];
```

```sql
-- Give everyone in Engineering a 10 % raise
UPDATE employees
SET salary = salary * 1.10
WHERE department_id = 1;

-- Update multiple columns at once
UPDATE employees
SET salary = 60000, is_active = false
WHERE name = 'Bob';

-- Update all rows (no WHERE)
UPDATE employees SET salary = 50000;
```

- All rows that satisfy `WHERE` are updated; omit `WHERE` to update every row.
- The right-hand `<expression>` is evaluated against the **current** row values (the original value, not previously updated ones in the same statement).
- Returns a message: `N row(s) updated in '<table>'.`

---

## ALTER TABLE

```sql
ALTER TABLE t ADD [COLUMN] email STRING;        -- new col = NULL for existing rows
ALTER TABLE t DROP [COLUMN] email;              -- cannot drop last column
ALTER TABLE t RENAME [COLUMN] name TO full_name;
ALTER TABLE t RENAME TO new_table_name;
```

---

## CREATE / DROP INDEX

```sql
CREATE INDEX idx_dept ON employees(department_id);  -- hash index, O(1) equality
DROP INDEX idx_dept;
```

The optimizer auto-detects indexes for `WHERE` equality and equi-join `ON` conditions.

---

## Subqueries

Subqueries are supported in three positions:

### Scalar subquery (returns one value)

```sql
-- Employees earning more than the average
SELECT name, salary FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

-- Employees earning more than Alice
SELECT name FROM employees
WHERE salary > (SELECT salary FROM employees WHERE name = 'Alice');
```

### IN / NOT IN subquery

```sql
-- Employees in departments based in New York
SELECT name FROM employees
WHERE department_id IN (
    SELECT id FROM departments WHERE location = 'NY'
);

-- Employees NOT in NY departments
SELECT name FROM employees
WHERE department_id NOT IN (
    SELECT id FROM departments WHERE location = 'NY'
);
```

### Derived table (subquery in FROM)

```sql
-- Alias the subquery result and query it
SELECT sub.name, sub.salary
FROM (SELECT name, salary FROM employees WHERE salary > 80000) AS sub
ORDER BY sub.salary DESC;

-- Aggregate over a derived table
SELECT COUNT(*) AS high_dept_count
FROM (SELECT DISTINCT department_id FROM employees WHERE salary > 75000) AS sub;
```

> **Notes:**
> - Non-correlated subqueries are evaluated once and their result inlined before the main query runs.
> - Correlated subqueries (referencing outer columns) are **not** supported.
> - Derived-table aliases are required (`AS alias`).

---

## EXPORT

```sql
EXPORT (<select_statement>)
TO '<file_path>'
[FORMAT CSV | JSON];
```

```sql
-- Export to CSV (default when FORMAT omitted and extension is .csv)
EXPORT (SELECT * FROM employees ORDER BY id)
TO 'employees.csv';

-- Explicit CSV format
EXPORT (SELECT name, salary FROM employees WHERE salary > 80000)
TO 'high_earners.csv' FORMAT CSV;

-- Export to JSON
EXPORT (SELECT name, department_id, salary FROM employees)
TO 'employees.json' FORMAT JSON;

-- Auto-detect JSON from .json extension
EXPORT (SELECT * FROM departments) TO 'departments.json';
```

**Format rules:**

| FORMAT | Output |
|--------|--------|
| `CSV` | Header row + comma-separated data rows; fields containing `,`, `"`, or newlines are quoted |
| `JSON` | A JSON array of objects: `[{"col": val, ...}, ...]`; strings are escaped, NULL → `null` |

- `FORMAT` can be omitted; the format is auto-detected from the file extension (`.csv` → CSV, `.json` → JSON). If the extension is unrecognized, CSV is used.
- Prints: `Exported N row(s) to '<file>' (FORMAT).`

---

## REPL Dot-Commands

`.help` `.tables` `.stats` `.clear` `.quit` `.exit`

---

## Combined Examples

### Department Summary (JOIN + GROUP BY + Aggregates)
```sql
SELECT d.name AS department, COUNT(*) AS headcount,
       ROUND(AVG(e.salary), 2) AS avg_salary, SUM(e.salary) AS payroll
FROM employees e
JOIN departments d ON e.department_id = d.id
GROUP BY d.name
ORDER BY payroll DESC;
```

### Full Analytics Pipeline (every major clause)
```sql
SELECT d.name AS department, COUNT(*) AS cnt,
       ROUND(AVG(e.salary), 2) AS avg_sal,
       MAX(e.salary) - MIN(e.salary) AS spread
FROM employees e
JOIN departments d ON e.department_id = d.id
WHERE e.is_active = true AND e.salary BETWEEN 65000 AND 100000
GROUP BY d.name
HAVING COUNT(*) > 1
ORDER BY avg_sal DESC
LIMIT 10;
```

### CRUD Lifecycle (CREATE → INSERT → Query → DELETE)
```sql
CREATE TABLE metrics (name STRING, value DOUBLE);
INSERT INTO metrics VALUES ('accuracy', 0.95), ('precision', 0.88);
SELECT name, ROUND(value * 100, 1) AS pct FROM metrics ORDER BY value DESC;
DELETE FROM metrics;
```

### Schema Evolution (ALTER TABLE workflow)
```sql
CREATE TABLE inventory (id INTEGER, item STRING);
INSERT INTO inventory VALUES (1, 'Widget'), (2, 'Gadget');
ALTER TABLE inventory ADD COLUMN price DOUBLE;
ALTER TABLE inventory RENAME COLUMN item TO product_name;
ALTER TABLE inventory RENAME TO products;
SELECT id, UPPER(product_name) AS product, price FROM products;
```

### UPDATE Workflow (INSERT → verify → UPDATE → verify)
```sql
CREATE TABLE prices (id INTEGER, product STRING, amount DOUBLE);
INSERT INTO prices VALUES (1, 'Widget', 9.99), (2, 'Gadget', 24.99);
SELECT * FROM prices;
UPDATE prices SET amount = amount * 1.05 WHERE amount < 20;
SELECT * FROM prices;
```

### Subquery Examples
```sql
-- Employees earning above average
SELECT name, salary FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees)
ORDER BY salary DESC;

-- Employees in Engineering or Sales (via subquery)
SELECT name FROM employees
WHERE department_id IN (
    SELECT id FROM departments WHERE name IN ('Engineering', 'Sales')
);

-- Top earners per department using a derived table
SELECT sub.name, sub.salary
FROM (SELECT name, salary, department_id FROM employees
      WHERE salary > 75000) AS sub
ORDER BY sub.department_id, sub.salary DESC;
```

### EXPORT Examples
```sql
-- Dump a full table to CSV
EXPORT (SELECT * FROM employees ORDER BY id) TO 'employees_backup.csv';

-- Export a filtered subset to JSON
EXPORT (SELECT name, salary FROM employees WHERE salary > 80000)
TO 'high_earners.json' FORMAT JSON;

-- Export a join result
EXPORT (
    SELECT e.name, d.name AS department, e.salary
    FROM employees e JOIN departments d ON e.department_id = d.id
    ORDER BY e.salary DESC
) TO 'salary_report.csv' FORMAT CSV;
```

### Multi-Table Pipeline (CSV + in-memory tables)
```sql
CREATE TABLE regions (city STRING, region STRING);
INSERT INTO regions VALUES ('San Francisco', 'West'), ('New York', 'East');

SELECT r.region, COUNT(*) AS employees, ROUND(AVG(e.salary), 2) AS avg_sal
FROM employees e
JOIN departments d ON e.department_id = d.id
JOIN regions r ON d.location = r.city
GROUP BY r.region
ORDER BY avg_sal DESC;
```

---

## Quick Cheat Sheet

```
SELECT [DISTINCT] <cols>  FROM <tbl>  [JOIN <tbl> ON <cond>]
[WHERE <cond>]  [GROUP BY <cols>]  [HAVING <cond>]
[ORDER BY <col> [ASC|DESC]]  [LIMIT <n>];

LOAD '<path>' AS <name>;         CREATE TABLE <t> (<col> <type>, ...);
INSERT INTO <t> VALUES (...);    DELETE FROM <t>;
UPDATE <t> SET col=expr [WHERE]; ALTER TABLE <t> ADD/DROP/RENAME
CREATE/DROP INDEX <name> ON <t>(<col>);
EXPORT (SELECT ...) TO '<file>' [FORMAT CSV|JSON];
SHOW TABLES; SHOW INDEXES;      DESCRIBE <t>;

Operators: = != <> < > <= >=  AND OR NOT  + - * / %
           BETWEEN..AND  IN(..)  IS [NOT] NULL  LIKE (%/_)
Aggregates: COUNT SUM AVG MIN MAX  COUNT(DISTINCT)
Functions:  UPPER LOWER LENGTH TRIM CONCAT SUBSTRING ABS ROUND COALESCE
Subqueries: scalar (SELECT ...) in WHERE; IN (SELECT ...); FROM (SELECT ...) AS alias
```
