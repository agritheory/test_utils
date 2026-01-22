# SQL Registry

A tool for scanning, analyzing, and converting `frappe.db.sql` calls to Frappe Query Builder or ORM equivalents.

## Overview

The SQL Registry scans Python files for `frappe.db.sql` calls, analyzes the SQL queries, and generates equivalent Query Builder or ORM code. This helps with:

- **Auditing** - Understanding SQL usage across a codebase
- **Migration** - Converting raw SQL to Query Builder for better security and maintainability
- **Quick Wins** - Identifying simple queries that can use `frappe.get_all` instead

## Installation

The `sql_registry` command is included with test_utils:

```bash
pip install test_utils
# or
poetry add test_utils
```

## CLI Commands

### Scan a Directory

Scan Python files for SQL operations:

```bash
sql_registry scan --directory /path/to/app
```

This creates a `.sql_registry.pkl` file containing all discovered SQL calls.

### List SQL Calls

List all SQL calls in the registry:

```bash
sql_registry list
sql_registry list --file-filter myfile.py
```

### Show Call Details

Show detailed information about a specific SQL call:

```bash
sql_registry show <call_id>
```

Output includes:
- Original SQL query
- Extracted parameters
- Query Builder equivalent
- Status indicators (`💡 ORM-ELIGIBLE`, `⚠️ HAS TODO`)

### List TODOs

List calls that need manual review:

```bash
sql_registry todos
```

These are typically:
- Dynamic SQL with f-string placeholders (`{}`)
- Complex queries the converter couldn't fully handle

### List ORM-Eligible Calls

List simple queries that can use `frappe.get_all`:

```bash
sql_registry orm
```

These are quick wins - simple SELECT queries on single tables that don't need Query Builder.

### Generate Report

Generate a markdown report of all SQL operations:

```bash
sql_registry report --output sql_report.md
```

The report includes:
- Conversion status summary (Query Builder, ORM-eligible, Needs Review)
- Per-file breakdown with status indicators
- SQL preview for each call

### Rewrite SQL Call

Preview or apply Query Builder conversion:

```bash
# Preview changes (dry run)
sql_registry rewrite <call_id>

# Apply changes to file
sql_registry rewrite <call_id> --apply
```

## Conversion Status

| Status | Meaning |
|--------|---------|
| ✅ Query Builder | Converted to `frappe.qb` Query Builder |
| 💡 ORM-eligible | Simple query, can use `frappe.get_all` |
| ⚠️ Needs Review | Has TODO, requires manual intervention |

## Supported SQL Patterns

The converter handles:

| SQL Pattern | Query Builder Equivalent |
|-------------|--------------------------|
| `SELECT ... FROM` | `frappe.qb.from_(...).select(...)` |
| `WHERE field = %s` | `.where(table.field == value)` |
| `IS NULL` / `IS NOT NULL` | `.isnull()` / `.isnotnull()` |
| `IFNULL()` / `COALESCE()` | `fn.Coalesce(field, value)` |
| `IN (subquery)` | `.isin(SubQuery(...))` |
| `NOT IN (subquery)` | `.notin(SubQuery(...))` |
| `EXISTS (subquery)` | `ExistsCriterion(...)` |
| `DATEDIFF()` | `CustomFunction('DATEDIFF', ...)` |
| `COUNT()`, `SUM()`, `AVG()` | `fn.Count()`, `fn.Sum()`, `fn.Avg()` |
| `GROUP BY` | `.groupby(...)` |
| `HAVING` | `.having(...)` |
| `ORDER BY` | `.orderby(..., order=frappe.qb.desc)` |
| `LIMIT` | `.limit(n)` |
| `BETWEEN` | `field[start:end]` |
| Arithmetic (`+`, `-`, `*`, `/`) | Native operators |

## ORM-Eligible Queries

Simple queries are automatically converted to `frappe.get_all`:

**Original:**
```python
frappe.db.sql("""
    SELECT name, status FROM `tabSales Order`
    WHERE customer = %s AND docstatus = 1
""", (customer,), as_dict=True)
```

**Converted:**
```python
frappe.get_all(
    "Sales Order",
    filters={"customer": customer, "docstatus": 1},
    fields=["name", "status"]
)
```

A query is ORM-eligible when:
- Single table (no JOINs)
- No aggregations (COUNT, SUM, etc.)
- No GROUP BY or HAVING
- No subqueries
- Simple WHERE conditions

## Frappe v16 Compatibility

The converter generates v16-compatible ORM syntax for:

### IFNULL/COALESCE in Filters

**Original SQL:**
```python
frappe.db.sql("""
    SELECT name FROM tabDepartment
    WHERE ifnull(company, '') != ''
""")
```

**v16 ORM Output:**
```python
from frappe.query_builder import Field
from frappe.query_builder.functions import IfNull

frappe.get_all(
    "Department",
    filters=[[IfNull(Field("company"), ""), "!=", ""]]
)
```

### DISTINCT Queries

**Original SQL:**
```python
frappe.db.sql("""
    SELECT DISTINCT parent FROM `tabBOM Operation`
    WHERE workstation = %s
""", (workstation,))
```

**v16 ORM Output:**
```python
frappe.get_all(
    "BOM Operation",
    filters={"workstation": workstation},
    fields=["parent"],
    distinct=True
)
```

### v16 Breaking Changes

Frappe v16 changed how `frappe.get_all()` handles fields and filters internally (now using Pypika Query Builder). Raw SQL strings for functions are **no longer supported**:

```python
# OLD (broken in v16)
frappe.get_all("DocType", fields=["sum(qty) as total"])

# NEW (v16 compatible)
frappe.get_all("DocType", fields=[{"SUM": "qty", "as": "total"}])
```

The converter automatically uses the Query Builder path for aggregate queries, which generates valid pypika syntax that works in both v15 and v16.

## Example Workflow

```bash
# 1. Scan your app
sql_registry scan --directory ~/frappe-bench/apps/myapp

# 2. Check for quick wins (ORM-eligible)
sql_registry orm

# 3. Check for issues needing manual review
sql_registry todos

# 4. Generate a report for the team
sql_registry report --output myapp_sql_audit.md

# 5. Preview a conversion
sql_registry show abc12345

# 6. Apply a conversion
sql_registry rewrite abc12345 --apply
```

## Full Codebase Audit

To audit an entire Frappe/ERPNext installation:

```bash
cd /path/to/test_utils
rm -f .sql_registry.pkl
poetry run sql_registry scan --directory ~/frappe-bench/apps/erpnext/erpnext
poetry run sql_registry report --output erpnext_sql_report.md
```

## Known Limitations

The following patterns require manual review:

1. **Dynamic SQL** - Queries with `{}` f-string placeholders for dynamic conditions
2. **Complex Subqueries** - Deeply nested or correlated subqueries
3. **Database-specific Functions** - Some MySQL-specific functions may not have direct equivalents

## See Also

- [Frappe Query Builder Documentation](https://frappeframework.com/docs/user/en/api/query-builder)
- [pypika Documentation](https://pypika.readthedocs.io/)
