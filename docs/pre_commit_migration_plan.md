# Pre-commit Migration Plan (version-15 apps)

Standardize pre-commit hooks and pyproject.toml across Frappe apps.

## Standard Configuration

### Pre-commit hooks (order)
1. **pre-commit-hooks** (v5.0.0): trailing-whitespace, check-yaml, no-commit-to-branch (version-15, version-16), check-merge-conflict, check-ast, check-json, check-toml, debug-statements
2. **codespell** (v2.4.1): args `--ignore-words-list notin`, exclude `yarn.lock|poetry.lock`
3. **pyupgrade** (v3.19.1): args `--py310-plus`
4. **black** (agritheory/black): standard
5. **flake8** (7.2.0): flake8-bugbear
6. **test_utils** (v1.15.3):
   - update_pre_commit_config
   - validate_frappe_project
   - validate_copyright (files `\.(js|ts|py|md)$`, args `--app {app_name}`)
   - bylines (exclude README.md, CHANGELOG.md)
   - clean_customized_doctypes (args `--app {app_name}`)
   - validate_customizations
   - patch_linters (args `--app {app_name}`)
   - track_overrides (args `--app {app_name}`, `--base-branch version-15`)
7. **prettier** (mirrors-prettier v3.1.0 or local npx) - after test_utils

### pyproject.toml requirements
- `[project]` with name, version, authors, description, readme, requires-python
- `[tool.bench.frappe-dependencies]` with bounded frappe (and erpnext if used)
- No hybrid config: Flit OR Poetry, not both
- Version consistency: `[project.version]` as source of truth; remove duplicate `[tool.poetry]` metadata
- `[project.dynamic] = ["dependencies"]` when using `[tool.poetry.dependencies]` exclusively

## Progress Summary

- **Complete:** 7 apps — fleet, approvals, saml, fab, greensight, cloud_storage (+ time_and_expense poetry only)
- **WIP:** shipstation_integration (poetry lock blocked by test_utils Python \<3.14)
- **Remaining:** ~9 apps

## App Status

| App | Bench | Pre-commit | pyproject.toml | poetry check | Notes |
|-----|-------|------------|----------------|--------------|-------|
| fleet | hawthorn | ✓ | ✓ | ✓ | Complete; added track_overrides, patches/ dir |
| shipstation_integration | quercus | ✓ | ✓ | ✗ | WIP: test_utils requires Python <3.14 |
| approvals | cranberry | ✓ | ✓ | ✓ | Complete; removed duplicate [tool.poetry] metadata |
| saml | cranberry | ✓ | ✓ | ✓ | Done |
| fab | cranberry | ✓ | ✓ | ✓ | Complete; removed duplicate [tool.poetry] metadata |
| greensight | cranberry | ✓ | ✓ | ✓ | Complete; added project.version |
| time_and_expense | cranberry | ? | ✓ | ✓ | poetry lock OK; pre-commit needs review |
| cloud_storage | uhdei | ✓ | ✓ | ✓ | WIP: track_overrides needs attention |
| inventory_tools | salix | | | | Pending |
| frappe_vault | uhdei | | | | Pending |
| electronic_payments | poplar | | | | Pending |
| check_run | cedar | | | | Pending |
| beam | | | | | Pending |
| upro_erp | | | | | Pending |
| upro_erp_integ | | | | | Pending |
| washmoreerp | | | | | Pending |
| communications | | | | | Pending |
| taxjar_erpnext | | | | | Create config from scratch |

## GitHub Actions (GHA) Inventory

| App | lint | pytest | release | generate-changelog | backport | overrides | Notes |
|-----|------|--------|---------|-------------------|----------|-----------|-------|
| fleet | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (agritheory) | Full set |
| shipstation_integration | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | Refactored to AgriTheory standard |
| approvals | ✓ | ✓ | ✓ | ✓ | ✓ | — | |
| saml | ✓ | ✓ | ✓ | ✓ | — | ✓ (agritheory) | |
| fab | ✓ | ✓ | ✓ | ✓ | — | — | Added generate-changelog |
| greensight | ✓ | — | ✓ | ✓ | — | — | No tests (customer app, mostly customizations); added changelog |
| cloud_storage | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (agritheory) | WIP: track_overrides needs attention; added changelog |
| time_and_expense | — | — | — | — | — | — | Customer GitLab; no GHAs required |

### Standard set
- **lint**: pre-commit/action or equivalent (black, mypy, prettier)
- **pytest**: Frappe bench + MariaDB (skip if app has no tests, e.g. greensight)
- **release**: python-semantic-release on version-15 (and version-14 if applicable)
- **generate-changelog**: agritheory/test_utils/actions/generate_changelog
- **backport**: tibdex/backport workflow
- **overrides**: agritheory/test_utils/actions/track_overrides
