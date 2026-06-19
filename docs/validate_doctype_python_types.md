# DocType Python types — `validate_doctype_python_types`

Frappe can [auto-generate static type hints](https://frappeframework.com/docs/user/en/basics/doctypes/controllers) on DocType controllers when `export_python_type_annotations = True` is set in `hooks.py`. Saving a DocType in Desk (or calling `export_types_to_controller` on the DocType document) writes a `TYPE_CHECKING` block delimited by:

- `# begin: auto-generated types`
- `# end: auto-generated types`

This matches [`frappe.types.exporter`](https://github.com/frappe/frappe/blob/develop/frappe/types/exporter.py). The pre-commit hook checks that every DocType controller either contains that block or an explicit opt-out comment, so teams using exported types do not accidentally commit controllers that fell out of sync with the schema.

---

## When the hook runs

| Situation | Behavior |
|-----------|----------|
| `export_python_type_annotations` is **not** `True` in `hooks.py` | Exit **0**, message on stderr that the check was skipped. Safe for apps that do not use Frappe’s exporter yet. |
| Flag is **True** (or you pass `--force`) | All matching controllers are scanned; missing block + opt-out → exit **1** with a file list. |

`hooks.py` is resolved as `<app_root>/<package>/hooks.py` first (e.g. `myapp/myapp/hooks.py`), then `<app_root>/hooks.py`.

---

## Which files are checked

Controllers matching:

`<app_root>/<package>/**/doctype/*/<module_name>.py`

where the Python file name equals its parent directory name (Frappe’s usual layout, including nested modules such as `accounts/doctype/sales_invoice/sales_invoice.py`).

---

## Satisfying the check

**Option A — Frappe-generated block**

Enable `export_python_type_annotations = True`, then for each listed DocType either re-save it in Desk or run `export_types_to_controller` (e.g. from a bench console). The controller must contain `# begin: auto-generated types`.

**Option B — Opt-out (documented exception)**

Add a *whole-line* comment near the top of the controller (after copyright / module docstring is fine). Accepted forms (case-insensitive prefix after strip):

- `# frappe: skip-python-type-annotations` (preferred in this hook’s interactive fixer)
- `# frappe types: ignore` (convention discussed on Frappe’s exporter PR)

---

## pre-commit configuration

`--app` must be the **Frappe app repository root**: the directory that contains the inner package folder (same as `static_analysis` / most other test_utils hooks).

```yaml
- repo: https://github.com/agritheory/test_utils
  rev: v1.25.1  # or newer tag containing this hook
  hooks:
    - id: validate_doctype_python_types
      args: ["--app", "."]
```

**CI / non-interactive** — use the snippet above. No TTY prompts; failures list paths and exit 1.

**Local interactive** — optionally add `--interactive` so each violation can be handled with:

- **[a]** run `frappe.get_doc("DocType", …).export_types_to_controller()` via the bench’s `env/bin/python` (requires a discoverable site),
- **[s]** insert `# frappe: skip-python-type-annotations` after the file header,
- **[n]** leave unchanged (hook still fails),
- **[q]** abort.

```yaml
    - id: validate_doctype_python_types
      args: ["--app", ".", "--interactive"]
```

**Rollout without enabling hooks first** — enforce scanning even when `export_python_type_annotations` is still off:

```yaml
    - id: validate_doctype_python_types
      args: ["--app", ".", "--force"]
```

---

## CLI flags

| Flag | Meaning |
|------|---------|
| `--app PATH` | App repo root. Default: `.` |
| `--force` | Run checks even if `export_python_type_annotations` is not enabled. |
| `--interactive` | Prompt per violation (only when stdin is a TTY). |
| `--site NAME` | Bench site for interactive export; overrides `FRAPPE_SITE` / config defaults. |

### Site resolution for interactive export

Interactive **[a]** needs a bench layout (`sites/`, `apps/`, `env/`) reachable by walking up from `--app`, and a site name from, in order:

1. `--site`
2. Environment variable `FRAPPE_SITE`
3. `default_site` in `sites/common_site_config.json`
4. `sites/currentsite.txt`

---

## Running without pre-commit

From a machine that has `test_utils` installed (or a Poetry shell inside this repo):

```bash
validate_doctype_python_types --app /path/to/bench/apps/myapp
```

Or:

```bash
cd /path/to/test_utils && poetry run python -m test_utils.pre_commit.validate_doctype_python_types \
  --app /path/to/bench/apps/myapp
```

Exit codes: `0` ok / skipped, `1` violations (or abort in interactive mode).

---

## Testing on an existing Frappe app (local `test_utils` clone)

Use this while the hook is only on a branch or not yet released.

### 1. One-off run from your app repo

From the **app root** (directory containing `myapp/myapp/`):

```bash
cd /path/to/bench/apps/myapp
/path/to/test_utils/.venv/bin/python -m test_utils.pre_commit.validate_doctype_python_types --app .
```

Or after `pip install /path/to/test_utils` (or `poetry add --editable`), run `validate_doctype_python_types --app .`.

Add `--force` if you have not enabled `export_python_type_annotations` yet. Add `--interactive` in a real terminal to try export or auto-insert skip comments.

### 2. Through pre-commit using your clone

Point `repo` at your local git checkout and pin `rev` to a commit or branch tip that exists **in that repo**:

```yaml
- repo: /home/you/projects/test_utils
  rev: your-branch-name
  hooks:
    - id: validate_doctype_python_types
      args: ["--app", "."]
```

Then:

```bash
cd /path/to/bench/apps/myapp
pre-commit autoupdate --repo /home/you/projects/test_utils   # optional, refreshes cache
pre-commit run validate_doctype_python_types --all-files
```

### 3. `pre-commit try-repo` (quick smoke test)

From the **target app** root:

```bash
cd /path/to/bench/apps/myapp
pre-commit try-repo /path/to/test_utils validate_doctype_python_types --all-files -v
```

Note: `try-repo` uses the hook defaults (`--app` defaults to `.`), which is correct when you run the command from the app root.

---

## Troubleshooting

- **Hook always skips** — Ensure `export_python_type_annotations = True` is a literal assignment in `hooks.py`, or pass `--force`.
- **Interactive export does nothing** — Confirm `export_python_type_annotations` is enabled for that app; `export_types_to_controller` returns early otherwise. Confirm site and bench paths.
- **False “missing types” after export** — Run formatter (Black, etc.); the block must still contain the exact marker line `# begin: auto-generated types`.

---

## See also

- [Controllers — Frappe docs](https://frappeframework.com/docs/user/en/basics/doctypes/controllers)
- [`pre_commit_hooks.md`](pre_commit_hooks.md) for the full hook list
