# Track Overrides

`track-overrides` is a GitHub Action designed to track method overrides in Python projects.
It automatically detects the base branch from the pull request that triggers the action and compares the code changes in the pull request against the corresponding branch in the repository specified in the method annotations.

This comparison is performed using the commit hash associated with each method. If the overridden method and the upstream method have different commit hashes, the action will indicate that the overridden method has changed. This seamless comparison capability enables efficient tracking of method overrides across different branches and repositories.


## Method Annotation

To track the overrides, the methods must have a comment with the following structure:

```
"""
APP: <app_name>
HASH: <commit_hash>
REPO: <repository_url>
PATH: <file_path>
METHOD: <method_name>
"""
```

- **APP**: The app that contains this override (e.g. `frappe`, `inventory_tools`). Required for pre-commit to correctly scope checks when using `--app` or `--directory`.
- **HASH**: Commit hash of the upstream version when this override was last validated.
- **REPO**: Upstream repository URL.
- **PATH**: Path to the file in the upstream repo.
- **METHOD**: Method name being overridden.

### Example
```python
def is_system_manager_disabled(user):
    """
    APP: frappe
    HASH: 171e1d0159cda3b8d9415527590c9c3ca0c827be
    REPO: https://github.com/frappe/frappe/
    PATH: frappe/core/doctype/user/user.py
    METHOD: is_system_manager_disabled
    """
    # Method implementation here
    pass
```

## Pre-commit Usage

For **monorepos** (bench structure with apps/frappe, apps/erpnext, etc.), use `--directory .` to scan
the entire workspace—matching GHA behavior. Optionally add `--app` to limit which apps are checked:

```yaml
# Check all apps
- id: track_overrides
  args: ['--directory', '.', '--base-branch', 'version-15']

# Or only frappe and erpnext (requires APP in annotations)
- id: track_overrides
  args: ['--directory', '.', '--app', 'frappe', '--app', 'erpnext', '--base-branch', 'version-15']
```

For **single-app repos**, use `--app`:

```yaml
- id: track_overrides
  args: ['--app', 'inventory_tools', '--base-branch', 'version-15']
```

## GitHub Action Usage

To use the `track-overrides` GitHub Action in your workflow, follow these steps:

### 1. Create a Workflow File

Create a new GitHub Actions workflow file in your repository, e.g., `.github/workflows/overrides.yml`, and add the following content:

```yaml
name: Track Overrides

on:
  pull_request:

jobs:
  track_overrides:
    runs-on: ubuntu-latest
    name: Track Overrides
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Track Overrides
        uses: agritheory/test_utils/actions/track_overrides@main
        with:
          post-comment: false
```