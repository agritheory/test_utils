## Using test_utils with pre-commit

Add this to your .pre-commit-config.yaml

### Hooks available

### Validate Copyright - `validate_copyright`

Check all *.js, *.ts, *.py, and *.md files and add copyright in these files if copyright doesn't exist.

```
  - repo: https://github.com/agritheory/test_utils/
    rev: {rev} // The revision or tag to clone. Example: rev: v0.11.0
    hooks:
      - id: validate_copyright
        files: '\.(js|ts|py|md)$'
        args: ["--app", "{app_name}"]
```

### Clean customized doctypes - `clean_customized_doctypes`

Remove unused keys in customizations.

```
  - repo: https://github.com/agritheory/test_utils/
    rev: {rev} // The revision or tag to clone. Example: rev: v0.11.0
    hooks:
      - id: clean_customized_doctypes
        args: ["--app", "{app_name}"]
```

### Validate javascript dependencies - `validate_javascript_dependencies`

Examine package.json across the installed apps on the site to detect any version mismatches

```
  - repo: https://github.com/agritheory/test_utils/
    rev: {rev} // The revision or tag to clone. Example: rev: v0.11.0
    hooks:
      - id: validate_javascript_dependencies
```

### Validate python dependencies - `validate_python_dependencies`

Examine pyproject.toml across the installed apps on the site to detect any version mismatches

```
  - repo: https://github.com/agritheory/test_utils/
    rev: {rev} // The revision or tag to clone. Example: rev: v0.11.0
    hooks:
      - id: validate_python_dependencies
```

### Validate customizations - `validate_customizations`

Validate Customizations

```
  - repo: https://github.com/agritheory/test_utils/
    rev: {rev} // The revision or tag to clone. Example: rev: v0.11.0
    hooks:
      - id: validate_customizations
```

### Update pre-commit config - `update_pre_commit_config`

Update test_utils pre-commit config to latest

```
  - repo: https://github.com/agritheory/test_utils/
    rev: {rev} // The revision or tag to clone. Example: rev: v0.11.0
    hooks:
      - id: update_pre_commit_config
```

### Mypy - `mypy`

Run mypy on the codebase using a preset configuration

```
  - repo: https://github.com/agritheory/test_utils/
    rev: {rev} // The revision or tag to clone. Example: rev: v0.11.0
    hooks:
      - id: mypy
```
