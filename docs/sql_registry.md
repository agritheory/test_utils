# SQL Registry

A tool for scanning, analyzing, and converting `frappe.db.sql` calls to Frappe Query Builder or ORM equivalents.

## Overview

The SQL Registry scans Python files for `frappe.db.sql` calls, analyzes the SQL queries, and generates equivalent Query Builder or ORM code. This helps with:

- **Auditing** - Understanding SQL usage across a codebase
- **Migration** - Converting raw SQL to Query Builder for better security and maintainability
- **Quick Wins** - Identifying simple queries that can use `frappe.get_all` instead

Registry data is stored as **JSON** (`.sql_registry.json`), which is human-readable, diffable, and portable. If an existing `.sql_registry.pkl` (pickle) file is found and no JSON registry exists, it is automatically migrated on first load.

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
sql_registry scan --directory /path/to/app [--registry .sql_registry.json] [--include-patches]
```

Options:
- `--directory` - Directory to scan (default: current directory)
- `--registry` - Registry file path (default: `.sql_registry.json`)
- `--include-patches` - Include SQL in patch/migration files. **Patches are excluded by default** since they are one-time migrations and typically don't need conversion.

This creates a `.sql_registry.json` file containing all discovered SQL calls.

### List SQL Calls

List all SQL calls in the registry:

```bash
sql_registry list [--registry .sql_registry.json] [--file-filter myfile.py]
```

### Show Call Details

Show detailed information about a specific SQL call:

```bash
sql_registry show <call_id> [--registry .sql_registry.json]
```

Output includes:
- Original SQL query
- Extracted parameters
- Query Builder equivalent
- Status indicators (`✅ Query Builder`, `💡 ORM-ELIGIBLE`, `🔧 Manual Review`, `⚠️ HAS TODO`)

### List TODOs

List calls that need manual review (have TODO comments in the conversion):

```bash
sql_registry todos [--registry .sql_registry.json]
```

### List ORM-Eligible Calls

List simple queries that can use `frappe.get_all`:

```bash
sql_registry orm [--registry .sql_registry.json]
```

These are quick wins - simple SELECT queries on single tables that don't need Query Builder.

### Generate Report

Generate a markdown report of all SQL operations:

```bash
sql_registry report [--registry .sql_registry.json] [--output sql_report.md]
```

The report includes:
- Conversion status summary (Query Builder, ORM-eligible, Manual Review, TODOs)
- Implementation distribution (frappe_db_sql, query_builder, mixed)
- Per-file breakdown with status indicators and SQL preview for each call

### Rewrite SQL Call

Preview or apply Query Builder conversion for a single call:

```bash
# Preview changes (dry run)
sql_registry rewrite <call_id> [--registry .sql_registry.json]

# Apply changes to file
sql_registry rewrite <call_id> --apply [--registry .sql_registry.json]
```

Only calls with **✅ Query Builder** or **💡 ORM-eligible** status can be rewritten. Calls marked **🔧 Manual Review** require manual conversion.

## Conversion Status

| Status | Meaning |
|--------|---------|
| ✅ Query Builder | Validated conversion ready to apply via `rewrite --apply` |
| 💡 ORM-eligible | Simple query, can use `frappe.get_all` instead |
| 🔧 Manual Review | Validation failed - complex query needs manual conversion |
| ⚠️ Has TODOs | Conversion has TODO comments requiring attention |

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
# 1. Scan your app (from test_utils or with test_utils installed)
poetry run sql_registry scan --directory ~/frappe-bench/apps/myapp

# 2. Check for quick wins (ORM-eligible)
poetry run sql_registry orm

# 3. Check for issues needing manual review
poetry run sql_registry todos

# 4. Generate a report for the team
poetry run sql_registry report --output myapp_sql_audit.md

# 5. Preview a conversion
poetry run sql_registry show abc12345

# 6. Apply a conversion (only for ✅ or 💡 status)
poetry run sql_registry rewrite abc12345 --apply
```

The registry file (`.sql_registry.json`) is typically added to `.gitignore` since it's project-local scan data.

## Scanning Multiple Codebases

Use `--registry` to maintain separate registries per app:

```bash
# Scan each app with its own registry
poetry run sql_registry scan --directory ~/bench/apps/erpnext --registry .sql_registry_erpnext.json
poetry run sql_registry scan --directory ~/bench/apps/hrms --registry .sql_registry_hrms.json
poetry run sql_registry scan --directory ~/bench/apps/myapp --registry .sql_registry_myapp.json

# Generate reports
poetry run sql_registry report --registry .sql_registry_erpnext.json --output erpnext_sql_report.md
poetry run sql_registry report --registry .sql_registry_hrms.json --output hrms_sql_report.md
```

## Full Codebase Audit

To audit an entire Frappe/ERPNext installation:

```bash
cd /path/to/test_utils
rm -f .sql_registry.json
poetry run sql_registry scan --directory ~/frappe-bench/apps/erpnext/erpnext
poetry run sql_registry report --output erpnext_sql_report.md
```

Use `poetry run sql_registry` (or `python -m sql_registry`) when running from the test_utils repo.

## Known Limitations (Manual Review Required)

The following patterns require **🔧 Manual Review** and cannot be auto-rewritten:

1. **Dynamic SQL** - Queries with `{}` f-string placeholders for dynamic conditions
2. **Complex Subqueries** - Deeply nested or correlated subqueries
3. **Database-specific Functions** - MySQL-specific functions (e.g. `ROW_COUNT()`, `information_schema` queries) without direct Query Builder equivalents
4. **Multi-table JOINs** - Some JOIN patterns may not validate correctly
5. **Patches/Migrations** - SQL in patch files is excluded by default; use `--include-patches` to include them

## Package Structure

The sql_registry is implemented as a package (`test_utils/utils/sql_registry/`):

| Module | Purpose |
|--------|---------|
| `models.py` | `SQLCall`, `SQLStructure`, `UnresolvedParameterError` |
| `scanner.py` | AST scanning, param extraction, ORM conversion (`frappe.get_all`) |
| `converter.py` | `SQLToQBConverter` - SQL-to-Query-Builder conversion logic |
| `registry.py` | `SQLRegistry` - load/save, scan orchestration, report generation |
| `cli.py` | `main()` - argparse and command dispatch |

Backward-compatible imports remain unchanged:

```python
from test_utils.utils.sql_registry import SQLRegistry, SQLCall, main
```

## See Also

- [Frappe Query Builder Documentation](https://frappeframework.com/docs/user/en/api/query-builder)
- [pypika Documentation](https://pypika.readthedocs.io/)
