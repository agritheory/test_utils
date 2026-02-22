# Pre-commit Migration Plan (version-15 apps)

Standardize pre-commit hooks and pyproject.toml across Frappe apps.

## Standard Configuration

### Pre-commit hooks (order)
1. **pre-commit-hooks** (v5.0.0): trailing-whitespace, check-yaml, no-commit-to-branch (version-15, version-16), check-merge-conflict, check-ast, check-json, check-toml, debug-statements
2. **codespell** (v2.4.1): args `--ignore-words-list notin`, exclude `yarn.lock|poetry.lock`
3. **pyupgrade** (v3.19.1): args `--py310-plus`
4. **black** (agritheory/black): standard
5. **flake8** (7.2.0): flake8-bugbear
6. **test_utils** (v1.15.4):
   - update_pre_commit_config
   - validate_frappe_project
   - validate_copyright (files `\.(js|ts|py|md)$`, args `--app {app_name}`)
   - bylines (exclude README.md, CHANGELOG.md)
   - clean_customized_doctypes (args `--app {app_name}`)
   - validate_customizations
   - patch_linters (args `--app {app_name}`)
   - track_overrides (args `--directory .`, `--app {app_name}`, `--base-branch version-15`; set `GH_TOKEN` or `GITHUB_TOKEN` for GitHub API auth)
   - check_code_duplication (args `--max-clones 60`, `--max-percentage 5.0`)
7. **prettier** (mirrors-prettier v3.1.0 or local npx) - after test_utils

### pyproject.toml requirements
- `[project]` with name, version, authors, description, readme, requires-python
- `[tool.bench.frappe-dependencies]` with bounded frappe (and erpnext if used)
- No hybrid config: Flit OR Poetry, not both
- Version consistency: `[project.version]` as source of truth; remove duplicate `[tool.poetry]` metadata
- `[project.dynamic] = ["dependencies"]` when using `[tool.poetry.dependencies]` exclusively

### pytest CI (apps with tests)
- Use bench’s `[tool.bench.dev-dependencies]` for test tools (pytest, pytest-cov, pytest-order, etc.)
- Use pip-style version specifiers (`~=x.y.z`, `>=x.y.z`), not Poetry `^` — bench passes these directly to pip
- CI helper scripts must run `bench setup requirements --dev` after `bench setup requirements --python`
- Bench reads only `[tool.bench.dev-dependencies]`; Poetry’s `[tool.poetry.group.dev.dependencies]` is ignored by bench

### Pytest CI audit (2025-02)

| App | Had `[tool.bench.dev-dependencies]` | Ran `--dev` in CI | Fixed |
|-----|------------------------------------|-------------------|-------|
| check_run | ✗ | ✗ | ✓ |
| electronic_payments | ✗ | ✗ | ✓ |
| taxjar_erpnext | ✗ | ✗ | ✓ |
| inventory_tools | ✗ | ✗ | ✓ |
| beam | ✗ | ✓ | ✓ (added bench dev-deps) |
| fleet | ✗ | ✓ | ✓ (added bench dev-deps) |
| cloud_storage | ✗ | ✓ | ✓ (added bench dev-deps) |
| approvals | ✗ | ✓ | ✓ (added bench dev-deps) |
| saml | ✗ | ✓ + `bench pip install` hack | ✓ (replaced with bench dev-deps) |
| shipstation_integration | ✓ | ✓ | — |
| fab | — | ✓ | — (uses `run-tests`, not pytest) |

## Progress Summary

- **Complete:** 12 apps — fleet, approvals, saml, fab, greensight, cloud_storage, inventory_tools, check_run, electronic_payments, beam, taxjar_erpnext (+ time_and_expense poetry only)
- **WIP:** shipstation_integration (poetry lock blocked by test_utils Python \<3.14)
- **Remaining:** ~8 apps

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
| cloud_storage | uhdei | ✓ | ✓ | ✓ | Complete |
| inventory_tools | salix | ✓ | ✓ | ✓ | Complete |
| frappe_vault | uhdei | | | | Pending |
| electronic_payments | poplar | ✓ | ✓ | ✓ | Complete |
| check_run | cedar | ✓ | ✓ | ✓ | Complete |
| beam | pinyon | ✓ | ✓ | ✓ | Complete |
| upro_erp | | | | | Pending |
| upro_erp_integ | | | | | Pending |
| washmoreerp | | | | | Pending |
| communications | | | | | Pending |
| taxjar_erpnext | magnolia | ✓ | ✓ | ✓ | Complete; created from scratch |

## GitHub Actions (GHA) Inventory

| App | lint | pytest | release | generate-changelog | backport | code-dup | overrides | Notes |
|-----|------|--------|---------|-------------------|----------|----------|-----------|-------|
| fleet | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (agritheory) | Full set |
| shipstation_integration | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | Refactored to AgriTheory standard |
| approvals | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | |
| saml | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (agritheory) | Added backport |
| fab | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | Added backport |
| greensight | ✓ | — | ✓ | ✓ | — | ✓ | — | No tests (customer app, mostly customizations); added changelog |
| cloud_storage | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (agritheory) | Complete; added changelog |
| inventory_tools | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (agritheory) | Added track_overrides |
| check_run | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (agritheory) | Added overrides |
| electronic_payments | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (agritheory) | Complete; added backport |
| beam | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (agritheory) | Complete; added overrides, changelog |
| taxjar_erpnext | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (agritheory) | Created from scratch; added backport |
| time_and_expense | — | — | — | — | — | — | — | Customer GitLab; no GHAs required |

### Code duplication audit (2025-02)

Added `agritheory/test_utils/actions/code_duplication` to: check_run, electronic_payments, beam, taxjar_erpnext, fleet, cloud_storage, approvals, saml, fab, shipstation_integration, greensight (inventory_tools and test_utils had it already).

### Backport audit (2025-02)

| App | Had backport | Fixed |
|-----|--------------|-------|
| electronic_payments | ✗ | ✓ |
| taxjar_erpnext | ✗ | ✓ |
| saml | ✗ | ✓ |
| fab | ✗ | ✓ |
| test_utils | ✗ | ✓ |
| check_run, beam, fleet, cloud_storage, approvals, inventory_tools, shipstation_integration | ✓ | — |

### Standard set
- **lint**: pre-commit/action or equivalent (black, mypy, prettier)
- **pytest**: Frappe bench + MariaDB (skip if app has no tests, e.g. greensight)
- **release**: python-semantic-release on version-15 (and version-14 if applicable)
- **generate-changelog**: agritheory/test_utils/actions/generate_changelog
- **backport**: tibdex/backport workflow
- **overrides**: agritheory/test_utils/actions/track_overrides (with `app:` input)
- **code-duplication**: agritheory/test_utils/actions/code_duplication (see docs/actions/code-duplication.md)
