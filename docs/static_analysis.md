# Frappe Static Analysis

`static_analysis` is a suite of static checks for Frappe apps that runs entirely from the filesystem — no running bench, no imports. It validates that the paths and references scattered across a Frappe app's configuration files, frontend code, and Python source actually point to real, correctly-decorated code.

## Checks

### hooks

Parses `hooks.py` with the AST and validates every dotted Python path it finds. Keys whose values are never Python paths (asset bundles, URL rules, app metadata, fixture lists, etc.) are excluded automatically.

Errors when a referenced function, class, or attribute does not exist in the resolved file.
Warns when a path resolves to a module rather than a callable.

### patches

Parses `patches.txt` — including the `execute:` prefix format introduced in Frappe v14 and lines with trailing reload-doc suffixes — and validates each path.

Frappe's convention of pointing a patch at a plain module (where `execute()` is called automatically) is handled: if the path resolves to a module, the validator checks that `execute()` exists inside it.

### frontend

Scans all `.js`, `.ts`, `.vue`, `.jsx`, and `.tsx` files for `frappe.call(...)` and `frappe.xcall(...)` invocations and validates the referenced Python path.

Errors when:
- The target function does not exist
- The function exists but is not decorated with `@frappe.whitelist()`

The `method: "..."` dict-key pattern is also matched when the file already contains a `frappe.call` or `frappe.xcall` call.

### python_calls

The same check as **frontend**, applied to Python files. Uses the AST to find `frappe.call` / `frappe.xcall` invocations and supports positional string args, `{"method": "..."}` dicts, and `method="..."` keyword arguments.

### jinja

Validates template paths referenced in:
- `frappe.get_template("path")`
- `frappe.render_template("path", ...)` (when the first argument looks like a file path)
- `{% include "path" %}` and `{% extends "path" %}` in HTML/Jinja files

Searches for templates in the current app and all dependency apps, so references to base templates in `frappe` (e.g. `templates/web.html`) resolve correctly.

### reports

Scans every Python file inside any `report/` directory in the app (including nested module structures like `app/module/report/`). Flags top-level functions that are:
- Not a known Frappe report entry point (`execute`, `get_data`, `get_columns`, `get_filters`, `get_chart_data`, `get_summary`)
- Not decorated with `@frappe.whitelist()`
- Not called directly by `execute()`

### orphans

Runs [Vulture](https://github.com/jendrikseipp/vulture) against the app to detect unused imports, variables, and functions. Before running, the analyzer seeds a Vulture whitelist with all discovered entry points (whitelisted functions, hooks paths, doctype controllers) so they are not incorrectly flagged.

Vulture exit code 3 (dead code found alongside syntax errors) is handled gracefully — results are still shown.

## Dependency resolution

When the app lives inside a standard Frappe bench `apps/` directory, dependency apps are discovered automatically:

1. `frappe` is always included as an implicit dependency
2. Apps listed in `required_apps` in `hooks.py` are resolved from the sibling `apps/` directory
3. Frappe's `"owner/app"` format in `required_apps` is handled — only the app name portion is used

This means cross-app dotted paths (e.g. calling `erpnext.setup.utils.get_exchange_rate` from `hrms`) resolve correctly without any extra configuration.

## Configuration

### Whitelisting paths

Add a `[tool.test_utils.static-analysis]` section to the app's `pyproject.toml` to permanently skip specific paths:

```toml
[tool.test_utils.static-analysis]
whitelist = [
    "my_app.legacy.old_module.some_function",
    "my_app.external.*",
]
```

Glob-style `.*` suffixes whitelist an entire module subtree.

### Suppressing individual lines

Add `frappe-vulture:ignore` as a comment on any line to skip it across all passes:

```python
frappe.call("my_app.some.path")  # frappe-vulture:ignore
```

```javascript
frappe.call("my_app.some.path");  // frappe-vulture:ignore
```

## Pre-commit integration

Add the hook to your app's `.pre-commit-config.yaml`:

```yaml
- repo: https://github.com/agritheory/test_utils
  rev: v0.17.0  # use the current release
  hooks:
    - id: static_analysis
      args: ['--no-orphans']  # remove if vulture is available in the hook env
```

The hook is configured with `always_run: true` and `pass_filenames: false` — it analyses the whole app on every commit regardless of which files changed.

### Enabling orphan detection in pre-commit

Orphan detection requires Vulture. The hook definition in this repo already declares it as an `additional_dependency`, so it will be installed automatically in the pre-commit environment:

```yaml
- repo: https://github.com/agritheory/test_utils
  rev: v0.17.0
  hooks:
    - id: static_analysis
```

To set a custom confidence threshold:

```yaml
    - id: static_analysis
      args: ['--min-confidence', '90']
```

## GitHub Actions / CI integration

### Standalone workflow

Copy `.github/workflows/static-analysis.yml` from this repo into your app, or add the following job to an existing workflow:

```yaml
jobs:
  static-analysis:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install
        run: pip install "git+https://github.com/agritheory/test_utils.git" vulture

      - name: Run
          run: static_analysis . --no-orphans
```

`--no-orphans` is recommended for standalone jobs because orphan detection produces false positives when the app's dependencies (`frappe`, `erpnext`, etc.) are not present on the filesystem. Remove it in full-bench CI environments.

### Inside a full-bench CI job

If your CI workflow already sets up a complete bench (e.g. using `frappe/frappe-docker` or a custom install script), run with all passes enabled:

```yaml
      - name: Static analysis
        run: |
          pip install "git+https://github.com/agritheory/test_utils.git" vulture
          static_analysis apps/my_app
```

Because the app is inside `apps/`, dependency auto-discovery will find `frappe` and any other installed apps automatically.

## CLI reference

```
static_analysis APP_PATH [options]

Arguments:
  APP_PATH                  Path to the Frappe app root

Options:
  --no-hooks                Skip hooks.py validation
  --no-patches              Skip patches.txt validation
  --no-frontend             Skip JS/TS/Vue frappe.call validation
  --no-python-calls         Skip Python frappe.call validation
  --no-jinja                Skip Jinja template path validation
  --no-reports              Skip report directory function validation
  --no-orphans              Skip dead-code detection (Vulture)
  --min-confidence N        Vulture confidence threshold (default: 80)
  --dependency-paths PATH   Extra app directories for path resolution
                            (auto-discovered when inside a bench apps/ dir)
  --json                    Output results as JSON (machine-readable)
```

## Python API

```python
from test_utils.utils.static_analysis import analyze_app, StaticAnalysisConfig

result = analyze_app(
    "/path/to/my_app",
    config=StaticAnalysisConfig(detect_orphans=False),
)

if result.has_errors:
    for msg in result.all_errors:
        print(msg)

for msg in result.all_warnings:
    print(msg)

# Machine-readable
print(result.to_json())
```
