## Using test_utils with pre-commit

Add this to your .pre-commit-config.yaml

### Hooks available

### Validate Copyright - `validate_copyright` 

Check all *.js, *.ts, *.py, and *.md files and add copyright in these files if copyright doesn't exist.

```
  - repo: https://github.com/agritheory/test_utils/
    rev: {rev}
    hooks:
      - id: validate_copyright
        files: '\.(js|ts|py|md)$'
        args: ["--app", "{app_name}"]
```

### Validate javascript dependencies - `validate_javascript_dependencies` 

Examine package.json across the installed apps on the site to detect any version mismatches

```
  - repo: https://github.com/agritheory/test_utils/
    rev: {rev}
    hooks:
      - id: validate_javascript_dependencies
```