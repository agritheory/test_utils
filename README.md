# AgriTheory Test Utils

Development tools for [Frappe](https://frappeframework.com) apps: test fixtures, pre-commit hooks, GitHub Action templates, and static analysis.

See [docs/index.md](docs/index.md) for a full overview of every tool.

---

## Installation

### Python

```bash
pip install "git+https://github.com/agritheory/test_utils.git@v0.17.0"
```

Or with [Poetry](https://python-poetry.org/):

```bash
poetry add "git+https://github.com/agritheory/test_utils.git@v0.17.0"
```

### Pre-commit

Add to your app's `.pre-commit-config.yaml`:

```yaml
- repo: https://github.com/agritheory/test_utils
  rev: v0.17.0
  hooks:
    - id: static_analysis
    # - id: validate_customizations
    #   args: ['--app', 'your_app_name']
    # ... see docs/index.md for all available hooks
```

---

## Programmatic use

### Static analysis

```python
from test_utils.utils.static_analysis import analyze_app, StaticAnalysisConfig

result = analyze_app("/path/to/my_app")

if result.has_errors:
    for msg in result.all_errors:
        print(msg)

for msg in result.all_warnings:
    print(msg)
```

Selective passes:

```python
result = analyze_app(
    "/path/to/my_app",
    config=StaticAnalysisConfig(
        detect_orphans=False,
        validate_reports=False,
    ),
)
```

JSON output:

```python
print(result.to_json())
```

Path resolution only:

```python
from test_utils.utils.static_analysis.path_resolver import PathResolver

resolver = PathResolver.from_app("/path/to/my_app")
resolved = resolver.resolve("my_app.api.create_order")
print(resolved.exists, resolved.kind, resolved.is_whitelisted)
```

### CLI

```bash
static_analysis /path/to/my_app
static_analysis /path/to/my_app --no-orphans --json
```

Full CLI reference in [docs/static_analysis.md](docs/static_analysis.md).
