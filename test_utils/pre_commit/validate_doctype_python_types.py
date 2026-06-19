"""
Ensure DocType controllers include Frappe auto-generated TYPE_CHECKING blocks when
`export_python_type_annotations` is enabled, or an explicit opt-out comment.

Detection matches Frappe's exporter markers (see frappe.types.exporter).
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import pathlib
import subprocess
import sys
from collections.abc import Sequence

AUTO_TYPES_BEGIN = "# begin: auto-generated types"

# Recognised opt-outs (Frappe PR #21776 suggested a file-level ignore; we accept both.)
SKIP_COMMENT_CANONICAL = "# frappe: skip-python-type-annotations"
SKIP_COMMENT_ALT = "# frappe types: ignore"


def find_bench_root(start: pathlib.Path) -> pathlib.Path | None:
	for p in [start] + list(start.parents):
		if (p / "sites").is_dir() and (p / "apps").is_dir() and (p / "env").is_dir():
			return p
	return None


def resolve_hooks_path(app_path: pathlib.Path) -> pathlib.Path | None:
	h = app_path / app_path.name / "hooks.py"
	if h.exists():
		return h
	h2 = app_path / "hooks.py"
	if h2.exists():
		return h2
	return None


def hooks_enables_export_python_types(hooks_file: pathlib.Path) -> bool:
	try:
		tree = ast.parse(hooks_file.read_text(encoding="utf-8"))
	except (OSError, SyntaxError):
		return False
	for node in ast.walk(tree):
		if not isinstance(node, ast.Assign):
			continue
		for target in node.targets:
			if isinstance(target, ast.Name) and target.id == "export_python_type_annotations":
				val = node.value
				if isinstance(val, ast.Constant):
					return bool(val.value)
				if isinstance(val, ast.Name) and val.id == "True":
					return True
	return False


def line_is_skip_comment(line: str) -> bool:
	s = line.strip()
	low = s.lower()
	if low.startswith(SKIP_COMMENT_CANONICAL.lower()):
		return True
	if low.startswith(SKIP_COMMENT_ALT.lower()):
		return True
	return False


def source_has_auto_types_or_skip(text: str) -> bool:
	if AUTO_TYPES_BEGIN in text:
		return True
	return any(line_is_skip_comment(ln) for ln in text.splitlines())


def insert_skip_comment_after_header(content: str, comment: str) -> str:
	lines = content.splitlines(keepends=True)
	idx = 0
	if lines and lines[0].startswith("#!"):
		idx = 1
	while idx < len(lines):
		line = lines[idx]
		stripped = line.strip()
		if stripped.startswith("#") and not line_is_skip_comment(line):
			idx += 1
			continue
		if stripped.startswith('"""'):
			delim = '"""'
			if stripped.count(delim) >= 2:
				idx += 1
				continue
			idx += 1
			while idx < len(lines) and delim not in lines[idx]:
				idx += 1
			idx += 1
			continue
		if stripped.startswith("'''"):
			delim = "'''"
			if stripped.count(delim) >= 2:
				idx += 1
				continue
			idx += 1
			while idx < len(lines) and delim not in lines[idx]:
				idx += 1
			idx += 1
			continue
		break
	block = f"{comment}\n\n"
	if idx == 0:
		return block + content
	return "".join(lines[:idx]) + block + "".join(lines[idx:])


def apply_skip_comment(controller_path: pathlib.Path) -> None:
	text = controller_path.read_text(encoding="utf-8")
	if source_has_auto_types_or_skip(text):
		return
	updated = insert_skip_comment_after_header(text, SKIP_COMMENT_CANONICAL)
	controller_path.write_text(updated, encoding="utf-8")
	print(f"Wrote opt-out comment in {controller_path}", file=sys.stderr)


def read_doctype_name(doctype_dir: pathlib.Path) -> str | None:
	json_path = doctype_dir / f"{doctype_dir.name}.json"
	if not json_path.exists():
		return None
	try:
		data = json.loads(json_path.read_text(encoding="utf-8"))
	except (OSError, json.JSONDecodeError):
		return None
	name = data.get("name")
	return name if isinstance(name, str) else None


def iter_doctype_controllers(app_path: pathlib.Path):
	app_pkg = app_path / app_path.name
	if not app_pkg.is_dir():
		return
	for py in sorted(app_pkg.rglob("doctype/*/*.py")):
		if py.name == "__init__.py":
			continue
		parent = py.parent
		if py.name != f"{parent.name}.py":
			continue
		yield parent.name, py


def collect_violations(app_path: pathlib.Path) -> list[tuple[str, pathlib.Path]]:
	out: list[tuple[str, pathlib.Path]] = []
	for scrubbed, py_path in iter_doctype_controllers(app_path):
		try:
			text = py_path.read_text(encoding="utf-8")
		except OSError:
			continue
		if source_has_auto_types_or_skip(text):
			continue
		out.append((scrubbed, py_path))
	return out


def resolve_site(bench_root: pathlib.Path, site_arg: str | None) -> str | None:
	if site_arg:
		return site_arg
	env_site = os.environ.get("FRAPPE_SITE")
	if env_site:
		return env_site
	cfg = bench_root / "sites" / "common_site_config.json"
	if cfg.exists():
		try:
			site = json.loads(cfg.read_text(encoding="utf-8")).get("default_site")
			if isinstance(site, str) and site:
				return site
		except (OSError, json.JSONDecodeError):
			return None
	cur = bench_root / "sites" / "currentsite.txt"
	if cur.is_file():
		raw = cur.read_text(encoding="utf-8").strip()
		if raw:
			return raw
	return None


def bench_export_types(
	bench_root: pathlib.Path, site: str, doctype_record_name: str
) -> tuple[bool, str]:
	py_bin = bench_root / "env" / "bin" / "python"
	if not py_bin.is_file():
		return False, f"no interpreter at {py_bin}"
	sites_path = str(bench_root / "sites")
	code = (
		"import frappe\n"
		f"frappe.init(site={site!r}, sites_path={sites_path!r})\n"
		"frappe.connect()\n"
		f'frappe.get_doc("DocType", {doctype_record_name!r}).export_types_to_controller()\n'
	)
	proc = subprocess.run(
		[str(py_bin), "-c", code],
		cwd=str(bench_root),
		capture_output=True,
		text=True,
		timeout=120,
	)
	if proc.returncode != 0:
		err = (proc.stderr or proc.stdout or "").strip()
		return False, err or f"exit {proc.returncode}"
	return True, ""


def prompt_for_violation(doctype_scrubbed: str, py_path: pathlib.Path) -> str:
	print(
		f"\nDocType controller missing Frappe auto-generated types: {doctype_scrubbed}",
		file=sys.stderr,
	)
	print(f"  File: {py_path}", file=sys.stderr)
	print(
		"  [a] run export_types_to_controller via bench env (needs site; "
		"set FRAPPE_SITE or ensure sites/common_site_config.json has default_site)",
		file=sys.stderr,
	)
	print(f"  [s] insert opt-out line: {SKIP_COMMENT_CANONICAL}", file=sys.stderr)
	print("  [n] leave as-is (hook will fail)", file=sys.stderr)
	print("  [q] abort", file=sys.stderr)
	while True:
		try:
			c = input("Choice [a/s/n/q]: ").strip().lower()
		except EOFError:
			return "n"
		if c in ("a", "s", "n", "q"):
			return c


def main(argv: Sequence[str] | None = None) -> int:
	parser = argparse.ArgumentParser(
		description=(
			"Require Frappe auto-generated controller types (or an opt-out comment) "
			"when export_python_type_annotations is enabled in hooks.py."
		)
	)
	parser.add_argument(
		"--app",
		default=".",
		help="Frappe app root (repository root containing the inner package directory). Default: cwd.",
	)
	parser.add_argument(
		"--force",
		action="store_true",
		help="Check controllers even if export_python_type_annotations is not set in hooks.py.",
	)
	parser.add_argument(
		"--interactive",
		action="store_true",
		help="Prompt per violation (export via bench, add skip comment, or fail). Requires a TTY.",
	)
	parser.add_argument(
		"--site",
		default=None,
		help="Bench site name for --interactive export (overrides FRAPPE_SITE / default_site).",
	)
	args = parser.parse_args(list(argv) if argv is not None else None)

	app_path = pathlib.Path(args.app).resolve()
	if not app_path.is_dir():
		print(f"Error: --app path does not exist: {app_path}", file=sys.stderr)
		return 1

	hooks_path = resolve_hooks_path(app_path)
	hook_on = hooks_path is not None and hooks_enables_export_python_types(hooks_path)

	if not args.force and not hook_on:
		if hooks_path is None:
			print(
				"validate_doctype_python_types: no hooks.py found; use --force to check anyway.",
				file=sys.stderr,
			)
		else:
			print(
				"validate_doctype_python_types: export_python_type_annotations is not enabled in "
				f"{hooks_path}; skipping. Enable it or pass --force.",
				file=sys.stderr,
			)
		return 0

	violations = collect_violations(app_path)
	if not violations:
		return 0

	bench_root = find_bench_root(app_path)
	site = resolve_site(bench_root, args.site) if bench_root else None

	if args.interactive and sys.stdin.isatty():
		still_failed: list[tuple[str, pathlib.Path]] = []
		for scrubbed, py_path in violations:
			choice = prompt_for_violation(scrubbed, py_path)
			if choice == "q":
				print("Aborted.", file=sys.stderr)
				return 1
			if choice == "s":
				apply_skip_comment(py_path)
				continue
			if choice == "a":
				if not bench_root:
					print("No bench root found (need sites/, apps/, env/).", file=sys.stderr)
					still_failed.append((scrubbed, py_path))
					continue
				if not site:
					print(
						"No site resolved; set FRAPPE_SITE or default_site in sites/common_site_config.json.",
						file=sys.stderr,
					)
					still_failed.append((scrubbed, py_path))
					continue
				dt_name = read_doctype_name(py_path.parent) or scrubbed.replace("_", " ").title()
				ok, err = bench_export_types(bench_root, site, dt_name)
				if not ok:
					print(f"export_types_to_controller failed: {err}", file=sys.stderr)
					still_failed.append((scrubbed, py_path))
					continue
				text = py_path.read_text(encoding="utf-8")
				if AUTO_TYPES_BEGIN not in text:
					print(
						"export ran but controller still has no auto-generated block; "
						"ensure export_python_type_annotations is True for this app.",
						file=sys.stderr,
					)
					still_failed.append((scrubbed, py_path))
				continue
			still_failed.append((scrubbed, py_path))
		violations = still_failed

	if not violations:
		return 0

	print(
		"DocType controllers without Frappe auto-generated types "
		f"(expected `{AUTO_TYPES_BEGIN}` or `{SKIP_COMMENT_CANONICAL}` / `{SKIP_COMMENT_ALT}`):",
		file=sys.stderr,
	)
	for scrubbed, py_path in violations:
		print(f"  {scrubbed}: {py_path}", file=sys.stderr)
	print(
		"\nFix: enable export_python_type_annotations in hooks.py, re-save the DocType in Desk "
		"(or run export_types_to_controller), or add an opt-out comment at the top of the controller.",
		file=sys.stderr,
	)
	if bench_root and site and (bench_root / "env" / "bin" / "python").is_file():
		print(
			f"\nBench detected at {bench_root} (site {site!r}). "
			"You can run this hook with --interactive to export or add the skip comment.",
			file=sys.stderr,
		)
	return 1


if __name__ == "__main__":
	sys.exit(main())
