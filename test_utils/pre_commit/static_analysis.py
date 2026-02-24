"""
Frappe Static Analysis — pre-commit / CI entry point.

Usage (from bench root or app root):

    static_analysis <app_path> [options]

Exit codes:
    0  No errors found
    1  One or more errors found
"""

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from test_utils.utils.static_analysis import StaticAnalysisConfig, StaticAnalyzer


def _print_section(title: str, items: list[str], prefix: str = "  ") -> None:
	if not items:
		return
	print(f"\n{title}")
	for item in items:
		print(f"{prefix}{item}")


def _run(args: argparse.Namespace) -> int:
	app_path = Path(args.app_path).resolve()
	if not app_path.exists():
		print(f"Error: app path '{app_path}' does not exist", file=sys.stderr)
		return 1

	dependency_paths = [Path(p).resolve() for p in (args.dependency_paths or [])]

	config = StaticAnalysisConfig(
		validate_hooks=not args.no_hooks,
		validate_patches=not args.no_patches,
		validate_frontend=not args.no_frontend,
		validate_python_calls=not args.no_python_calls,
		validate_jinja=not args.no_jinja,
		validate_reports=not args.no_reports,
		detect_orphans=not args.no_orphans,
		min_confidence=args.min_confidence,
	)

	analyzer = StaticAnalyzer(app_path, dependency_paths=dependency_paths, config=config)
	result = analyzer.analyze()

	if args.json:
		print(result.to_json())
		return 1 if result.has_errors else 0

	# --- Human-readable output ---
	app_name = app_path.name
	print(f"\nFrappe Static Analysis: {app_name}")
	print("=" * (24 + len(app_name)))

	any_output = False

	# Hooks
	if result.hooks_result is not None:
		hr = result.hooks_result
		label = f"Hooks ({hr.paths_checked} paths checked)"
		if hr.errors or hr.warnings:
			any_output = True
			_print_section(f"[hooks] {label}", hr.errors, prefix="  ERROR: ")
			_print_section("", hr.warnings, prefix="  WARN:  ")
		else:
			print(f"\n[hooks] {label} — OK")

	# Patches
	if result.patches_result is not None:
		pr = result.patches_result
		label = f"Patches ({pr.paths_checked} paths checked)"
		if pr.errors or pr.warnings:
			any_output = True
			_print_section(f"[patches] {label}", pr.errors, prefix="  ERROR: ")
			_print_section("", pr.warnings, prefix="  WARN:  ")
		else:
			print(f"\n[patches] {label} — OK")

	# Frontend
	if result.frontend_result is not None:
		fr = result.frontend_result
		label = f"Frontend ({fr.calls_checked} calls checked)"
		if fr.errors or fr.warnings:
			any_output = True
			_print_section(f"[frontend] {label}", fr.errors, prefix="  ERROR: ")
			_print_section("", fr.warnings, prefix="  WARN:  ")
		else:
			print(f"\n[frontend] {label} — OK")

	# Python calls
	if result.python_call_result is not None:
		pcr = result.python_call_result
		label = f"Python frappe.call ({pcr.calls_checked} calls checked)"
		if pcr.errors or pcr.warnings:
			any_output = True
			_print_section(f"[python_calls] {label}", pcr.errors, prefix="  ERROR: ")
			_print_section("", pcr.warnings, prefix="  WARN:  ")
		else:
			print(f"\n[python_calls] {label} — OK")

	# Jinja
	if result.jinja_result is not None:
		jr = result.jinja_result
		label = f"Jinja templates ({jr.calls_checked} references checked)"
		if jr.errors or jr.warnings:
			any_output = True
			_print_section(f"[jinja] {label}", jr.errors, prefix="  ERROR: ")
			_print_section("", jr.warnings, prefix="  WARN:  ")
		else:
			print(f"\n[jinja] {label} — OK")

	# Reports
	if result.report_result is not None:
		rr = result.report_result
		label = f"Reports ({rr.files_checked} files checked)"
		if rr.warnings:
			any_output = True
			_print_section(f"[reports] {label}", rr.warnings, prefix="  WARN:  ")
		else:
			print(f"\n[reports] {label} — OK")

	# Orphans
	if result.orphan_result is not None:
		orph = result.orphan_result
		label = f"Orphan detection ({orph.entry_points_found} entry points)"
		if orph.error:
			any_output = True
			print(f"\n[orphans] {label}")
			print(f"  ERROR: {orph.error}")
		elif orph.unreachable:
			any_output = True
			_print_section(
				f"[orphans] {label} — {len(orph.unreachable)} unused item(s)",
				[str(item) for item in orph.unreachable],
				prefix="  ",
			)
		else:
			print(f"\n[orphans] {label} — OK")

	print()
	if result.has_errors:
		total = len(result.all_errors)
		print(f"Result: FAIL — {total} error(s) found\n")
		return 1

	print("Result: PASS\n")
	return 0


def main(argv: Sequence[str] | None = None) -> None:
	parser = argparse.ArgumentParser(
		description=(
			"Run static analysis on a Frappe app: validate hooks, patches, "
			"frontend calls, Python calls, Jinja templates, and detect orphaned code."
		)
	)
	parser.add_argument(
		"app_path",
		help="Path to the Frappe app root (directory containing pyproject.toml)",
	)
	parser.add_argument(
		"--dependency-paths",
		nargs="*",
		metavar="PATH",
		dest="dependency_paths",
		help="Additional app directories to include when resolving paths (e.g. frappe, erpnext)",
	)
	parser.add_argument(
		"--no-hooks",
		action="store_true",
		default=False,
		help="Skip hooks.py path validation",
	)
	parser.add_argument(
		"--no-patches",
		action="store_true",
		default=False,
		help="Skip patches.txt path validation",
	)
	parser.add_argument(
		"--no-frontend",
		action="store_true",
		default=False,
		help="Skip JS/TS/Vue frappe.call validation",
	)
	parser.add_argument(
		"--no-python-calls",
		action="store_true",
		default=False,
		dest="no_python_calls",
		help="Skip Python frappe.call validation",
	)
	parser.add_argument(
		"--no-jinja",
		action="store_true",
		default=False,
		help="Skip Jinja template path validation",
	)
	parser.add_argument(
		"--no-reports",
		action="store_true",
		default=False,
		help="Skip report directory function validation",
	)
	parser.add_argument(
		"--no-orphans",
		action="store_true",
		default=False,
		help="Skip orphan/dead-code detection (Vulture)",
	)
	parser.add_argument(
		"--min-confidence",
		type=int,
		default=80,
		metavar="N",
		dest="min_confidence",
		help="Vulture minimum confidence threshold (default: 80)",
	)
	parser.add_argument(
		"--json",
		action="store_true",
		default=False,
		help="Output results as JSON (machine-readable)",
	)

	args = parser.parse_args(argv)
	sys.exit(_run(args))
