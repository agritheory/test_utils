# Code Duplication Reporting Setup

This guide covers setting up automated code duplication detection using `jscpd` for Frappe applications.

## Overview

The duplication check system detects copy-paste problems and duplicated code across Python and JavaScript/TypeScript files. It automatically:
- Ignores tests, fixtures, and JSON files
- Generates HTML and JSON reports
- Provides thresholds for CI/CD
- Works with both GitHub Actions and pre-commit hooks
- Auto-detects app name from repository structure

## Quick Start

### Option 1: GitHub Actions (Recommended)

1. Copy the workflow file to `.github/workflows/code-duplication.yml`
2. Push to your repository
3. The workflow runs automatically on push/PR

**View reports:**
- GitHub Actions Summary tab shows a formatted table
- Download HTML report from workflow artifacts
- Reports are saved to `{app_name}/tests/jscpd/`

### Option 2: Pre-commit Hook

1. Add to your `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: local
    hooks:
      - id: jscpd
        name: Check for code duplication
        entry: bash -c 'npx jscpd@4 . --format "python,javascript,typescript" --ignore "**/node_modules/**,**/.venv/**,**/venv/**,**/__pycache__/**,**/dist/**,**/build/**,**/*.bundle.js,**/tests/**,**/test_*.py,**/*_test.py,**/*.test.js,**/*.spec.js,**/fixtures/**,**/*fixtures.py" --min-lines 20 --min-tokens 150 --threshold 6 --exitCode 1 --reporters "console" --silent'
        language: system
        pass_filenames: false
        files: \.(py|js|ts)$
        stages: [commit]
```

2. Install pre-commit hooks:
```bash
pre-commit install
```

### Option 3: Standalone Script

1. Make the script executable:
```bash
chmod +x check_duplication.sh
```

2. Run the check:
```bash
./check_duplication.sh
```

3. With custom options:
```bash
./check_duplication.sh --max-clones 50 --max-percentage 4.0 --exit-on-fail
```

## Configuration

### Thresholds

**Default values:**
- Maximum clones: 60
- Maximum percentage: 5.0%
- Minimum lines for detection: 20
- Minimum tokens for detection: 150

**Customize in GitHub Actions:**

Edit the workflow file:
```yaml
- name: Check clone threshold
  run: |
    MAX_CLONES=50
    MAX_PERCENTAGE=4.0
```

**Customize in standalone script:**
```bash
./check_duplication.sh --max-clones 50 --max-percentage 4.0
```

### Ignored Patterns

The following are automatically ignored:
- `**/node_modules/**` - Node.js dependencies
- `**/.venv/**, **/venv/**` - Python virtual environments
- `**/__pycache__/**` - Python cache
- `**/dist/**, **/build/**` - Build outputs
- `**/*.bundle.js` - Bundled JavaScript
- `**/tests/**, **/test_*.py, **/*_test.py` - Test files
- `**/*.test.js, **/*.spec.js` - JavaScript tests
- `**/fixtures/**, **/*fixtures.py` - Fixture files
- `**/*.min.js, **/*.min.css` - Minified files
- `**/migrations/**` - Database migrations

### Ignoring Legitimate Duplications

For configuration arrays, dialog fields, and report columns that are intentionally similar:

**JavaScript/TypeScript:**
```javascript
/* jscpd:ignore-start */
const dialogFields = [
  { fieldname: 'item_code', label: 'Item', fieldtype: 'Link' },
  { fieldname: 'qty', label: 'Quantity', fieldtype: 'Float' },
  { fieldname: 'rate', label: 'Rate', fieldtype: 'Currency' },
];
/* jscpd:ignore-end */
```

**Python:**
```python
# jscpd:ignore-start
columns = [
    {"fieldname": "item_code", "label": "Item", "fieldtype": "Link"},
    {"fieldname": "qty", "label": "Quantity", "fieldtype": "Float"},
    {"fieldname": "rate", "label": "Rate", "fieldtype": "Currency"},
]
# jscpd:ignore-end
```

## Understanding Reports

### Console Output

```
Found 12 clones (3.2% duplication)
Thresholds: 60 clones, 5.0% duplication
✅ Clone count within threshold (60)
✅ Duplication percentage within threshold (5.0%)
```

### GitHub Actions Summary

Shows a formatted table with:
- Clones per file format (Python, JavaScript)
- Total lines and duplicated lines
- Duplication percentage
- Top 5 duplications with locations

### HTML Report

Provides:
- Interactive visualization
- Side-by-side code comparison
- Clone statistics by file
- Exact line numbers
- Visual code highlighting

**Location:** `{app_name}/tests/jscpd/jscpd-report.html`

### JSON Report Structure

```json
{
  "statistics": {
    "total": {
      "clones": 12,
      "duplicatedLines": 156,
      "percentage": 0.032
    },
    "formats": {
      "python": { "clones": 8, "duplicatedLines": 120 },
      "javascript": { "clones": 4, "duplicatedLines": 36 }
    }
  },
  "duplicates": [...]
}
```

## Troubleshooting

### App Name Not Detected

The script tries to detect app name from:
1. GitHub repository name
2. Frappe app directory structure
3. `setup.py` name field
4. Current directory name

To manually specify:
```bash
# In standalone script
APP_NAME="my_custom_app" ./check_duplication.sh

# In GitHub Actions (automatically detected)
```

### False Positives

**Problem:** Configuration arrays flagged as duplicates

**Solution:** Add ignore comments around legitimate duplications:
```python
# jscpd:ignore-start
columns = [...]
# jscpd:ignore-end
```

**Problem:** Similar but distinct code blocks

**Solution:** Increase minimum lines/tokens:
```bash
./check_duplication.sh --min-lines 30 --min-tokens 200
```

### No Report Generated

**Possible causes:**
1. No duplications found (good!)
2. All files are in ignored patterns
3. `jscpd` installation failed

**Solution:**
```bash
# Install jscpd globally
npm install -g jscpd

# Run with verbose output
npx jscpd@4 . --reporters console
```

### Workflow Fails on Threshold

**Problem:** Clone count exceeds threshold

**Options:**
1. Refactor duplicate code
2. Add ignore comments for legitimate cases
3. Increase threshold (not recommended)

```yaml
# Temporary: Increase threshold during refactoring
- name: Check clone threshold
  run: |
    MAX_CLONES=80  # Increase temporarily
```

## Best Practices

### 1. Set Appropriate Thresholds

Start with default values and adjust based on your codebase:
- **New projects:** Strict thresholds (30 clones, 3%)
- **Legacy projects:** Gradual reduction (start at 100, reduce quarterly)

### 2. Review Reports Regularly

- Check HTML reports weekly
- Address new duplications in PRs
- Track duplication trends over time

### 3. Refactor Strategically

**Extract to functions:**
```python
# Before
def report_a():
    data = frappe.db.get_all("Item", filters={...}, fields=["name", "item_code"])
    # ... 20 lines of processing

def report_b():
    data = frappe.db.get_all("Customer", filters={...}, fields=["name", "customer_code"])
    # ... 20 lines of similar processing

# After
def get_and_process_data(doctype, code_field):
    data = frappe.db.get_all(doctype, filters={...}, fields=["name", code_field])
    # ... shared processing logic
    return data
```

**Use inheritance:**
```javascript
// Before: Two similar classes with duplicate methods

// After: Extract common functionality to base class
class BaseController {
  validate() { /* common validation */ }
  onSubmit() { /* common submission */ }
}

class CustomController extends BaseController {
  // Only unique methods
}
```

### 4. Ignore Appropriately

**Do ignore:**
- Configuration arrays
- Report column definitions
- Dialog field lists
- Fixed data structures

**Don't ignore:**
- Business logic
- Data processing
- Complex algorithms
- Validation functions

## Integration with CI/CD

### Require Checks to Pass

```yaml
# .github/workflows/code-duplication.yml
- name: Check clone threshold
  run: |
    # ... threshold check ...
    exit 1  # Fail the build
```

### Branch Protection

In GitHub repository settings:
1. Go to Settings → Branches
2. Add rule for main/master
3. Require status checks: "duplication"

### Pull Request Comments

The workflow automatically comments on PRs with:
- Duplication statistics
- Pass/fail status
- Link to detailed report
- Remediation instructions

## Advanced Usage

### Custom Ignore Patterns

Edit the workflow or script to add patterns:
```yaml
--ignore "**/custom_dir/**,**/*_generated.py"
```

### Multiple Report Formats

```bash
./check_duplication.sh --format "console,json,html,xml"
```

### Integration with Other Tools

**Combine with SonarQube:**
```yaml
- name: Upload to SonarQube
  run: |
    sonar-scanner \
      -Dsonar.sources=. \
      -Dsonar.cpd.exclusions=**/tests/**
```

**Export metrics:**
```bash
# Extract metrics for tracking
jq '.statistics.total' ./app/tests/jscpd/jscpd-report.json
```

## Support

For issues or questions:
1. Check the HTML report for detailed information
2. Review ignore patterns for false positives
3. Adjust thresholds if needed
4. Add ignore comments for legitimate cases

## References

- [jscpd documentation](https://github.com/kucherenko/jscpd)
- [Frappe Framework](https://frappeframework.com/)
- [GitHub Actions](https://docs.github.com/en/actions)
- [pre-commit](https://pre-commit.com/)