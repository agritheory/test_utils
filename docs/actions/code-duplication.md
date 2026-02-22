# Code Duplication

The code duplication action runs [jscpd](https://jscpd.dev/) to detect copy-paste in Python, JavaScript, and TypeScript. It reports duplications on PRs, uploads HTML/JSON artifacts when issues are found, and fails when clone/percentage thresholds are exceeded.

When **no duplications are detected**, the action does nothing: no summary, no artifact upload, no PR comment.

## Usage

Add `.github/workflows/code-duplication.yml` to your repository:

```yaml
name: Code Duplication

on:
  push:
    branches: ["*"]
  pull_request:
    branches: ["*"]

permissions:
  contents: read
  pull-requests: write
  issues: write

jobs:
  duplication:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-node@v4
        with:
          node-version: '18'

      - uses: agritheory/test_utils/actions/code_duplication@main
        with:
          max_clones: 60      # Fail if clones exceed this (default: 60)
          max_percentage: 5.0 # Warn if duplication % exceeds this (default: 5.0)
```

### Optional inputs

| Input | Default | Description |
|-------|---------|-------------|
| `max_clones` | 60 | Fail the job if clone count exceeds this |
| `max_percentage` | 5.0 | Warn if duplication percentage exceeds this |

## Ignoring false positives

For configuration arrays (report columns, dialog fields) that jscpd flags as duplicated:

```javascript
/* jscpd:ignore-start */
const dialogFields = [...];
/* jscpd:ignore-end */
```

```python
# jscpd:ignore-start
columns = [...]
# jscpd:ignore-end
```

## App detection

The workflow detects the app name from:
1. A directory matching the repository name (e.g. `inventory_tools/` for `agritheory/inventory_tools`)
2. The `name` in `setup.py` if no such directory exists
3. The repository name as fallback
