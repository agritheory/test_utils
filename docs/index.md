# AgriTheory Test Utils — Documentation Index

Test Utils is a collection of development tools for [Frappe](https://frappeframework.com) apps: test fixtures, pre-commit hooks, GitHub Action templates, and static analysis. It is maintained by [AgriTheory](https://agritheory.dev) and published at [github.com/agritheory/test_utils](https://github.com/agritheory/test_utils).

---

## Tools

### Pre-commit Hooks

[`pre_commit_hooks.md`](pre_commit_hooks.md) documents all available hooks. A summary:

| Hook | Purpose |
|------|---------|
| `validate_copyright` | Add/check copyright headers in `.py`, `.js`, `.ts`, `.md` |
| `clean_customized_doctypes` | Remove null values from customization JSON files |
| `validate_javascript_dependencies` | Detect `package.json` version mismatches across installed apps |
| `validate_python_dependencies` | Detect `pyproject.toml` version mismatches across installed apps |
| `validate_customizations` | Validate Custom Fields and Property Setters ([details](validate_customizations.md)) |
| `update_pre_commit_config` | Keep pre-commit config pinned to the latest test_utils revision |
| `mypy` | Run mypy with a preset configuration |
| `track_overrides` | Track changes to override methods across branches |
| `add_translations` | Extract untranslated strings and translate them |
| `sql_docstring_formatter` | Format SQL in Python docstrings via sqlglot |
| `sql_registry` | Register SQL operations for tracking |
| `bylines` | Add git-history-derived bylines to Markdown files |
| `validate_patches` | Validate `patches.txt` ordering and file presence |
| `validate_frappe_project` | Validate `pyproject.toml` structure for Frappe apps |
| `check_code_duplication` | Detect copy-paste duplication in Python and JS/TS |
| `static_analysis` | Full static analysis suite (see above) |

---

### Customization Validation

[`validate_customizations.md`](validate_customizations.md) covers the `validate_customizations` hook in depth, including why to avoid fixtures, how to organize customization files, and all validation checks (custom permissions, module attribution, system-generated fields, duplicate detection).

---

### SQL Registry

[`sql_registry.md`](sql_registry.md) documents the SQL scanning, registration, and Query Builder rewriting tools.

---

### Code Duplication

[`code_duplication.md`](code_duplication.md) documents the copy-paste duplication checker.

---

### Partitions

[`partitions.md`](partitions.md) documents the `create_partition` utility for MariaDB table partitioning.

---


### Static Analysis

[`static_analysis`](static_analysis.md) validates the internal consistency of a Frappe app's configuration and source code without importing any Python or running a bench. It catches broken references before they reach a server.

---

## Test Fixtures

Test Utils ships reference fixture data for bootstrapping Frappe/ERPNext test environments:

- Companies and Chart of Accounts
- Customers, Suppliers, Employees, Tax Authorities
- Items, BOMs, Workstations, Operations

---

## GitHub Action Templates

Reusable GitHub Action workflows are in the `actions/` directory:

- Lint
- Python semantic release
- Synchronization report with this library
- Translated documentation pull requests
