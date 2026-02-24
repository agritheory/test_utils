"""
Validate Python files inside Frappe report directories.

Frappe report controllers follow a specific convention:
  - ``execute(filters)``    — the Frappe-called entry point
  - ``@frappe.whitelist()`` — functions callable from the frontend

Any other top-level function is unreachable and flagged as a warning.
"""

import ast
from dataclasses import dataclass, field
from pathlib import Path

FRAPPE_REPORT_ENTRY_POINTS = frozenset(
	{"execute", "get_data", "get_columns", "get_filters", "get_chart_data", "get_summary"}
)


@dataclass
class ReportIssue:
	file: str
	line: int
	function: str
	message: str

	def __str__(self) -> str:
		return f"{self.file}:{self.line}: [report] '{self.function}' — {self.message}"

	def to_dict(self) -> dict:
		return {
			"file": self.file,
			"line": self.line,
			"function": self.function,
			"message": self.message,
		}


@dataclass
class ReportValidationResult:
	errors: list[str] = field(default_factory=list)
	warnings: list[str] = field(default_factory=list)
	files_checked: int = 0
	issues: list[ReportIssue] = field(default_factory=list)

	def to_dict(self) -> dict:
		return {
			"errors": self.errors,
			"warnings": self.warnings,
			"files_checked": self.files_checked,
			"issues": [i.to_dict() for i in self.issues],
		}


def has_whitelist_decorator(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
	for dec in node.decorator_list:
		if isinstance(dec, ast.Call):
			func = dec.func
			if isinstance(func, ast.Attribute) and func.attr == "whitelist":
				return True
			if isinstance(func, ast.Name) and func.id == "whitelist":
				return True
		elif isinstance(dec, ast.Attribute) and dec.attr == "whitelist":
			return True
		elif isinstance(dec, ast.Name) and dec.id == "whitelist":
			return True
	return False


def functions_called_by_execute(tree: ast.Module) -> set[str]:
	"""Return names called directly inside any ``execute`` function (single-level)."""
	called: set[str] = set()
	for node in ast.walk(tree):
		if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
			continue
		if node.name != "execute":
			continue
		for child in ast.walk(node):
			if isinstance(child, ast.Call):
				if isinstance(child.func, ast.Name):
					called.add(child.func.id)
				elif isinstance(child.func, ast.Attribute):
					called.add(child.func.attr)
	return called


def check_report_file(filepath: Path) -> list[ReportIssue]:
	issues: list[ReportIssue] = []
	try:
		source = filepath.read_text(encoding="utf-8", errors="replace")
		tree = ast.parse(source, filename=str(filepath))
	except SyntaxError as exc:
		issues.append(
			ReportIssue(
				file=str(filepath), line=1, function="<module>", message=f"syntax error: {exc}"
			)
		)
		return issues

	called = functions_called_by_execute(tree)

	for node in tree.body:
		if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
			continue
		name = node.name
		if (
			name in FRAPPE_REPORT_ENTRY_POINTS or has_whitelist_decorator(node) or name in called
		):
			continue
		issues.append(
			ReportIssue(
				file=str(filepath),
				line=node.lineno,
				function=name,
				message="not a report entry point, not @frappe.whitelist(), and not called by execute() — unreachable from Frappe",
			)
		)
	return issues


class ReportValidator:
	"""
	Validate Python files inside all ``report/`` directories in the app.

	Flags top-level functions that are not a known Frappe entry point, not
	``@frappe.whitelist()``, and not called directly by ``execute()``.
	"""

	def __init__(self, app_path: Path) -> None:
		self.app_path = app_path

	def validate(self) -> ReportValidationResult:
		result = ReportValidationResult()
		app_name = self.app_path.name

		app_pkg = self.app_path / app_name
		if not app_pkg.is_dir():
			app_pkg = self.app_path

		seen: set[Path] = set()
		for report_base in app_pkg.rglob("report"):
			if not report_base.is_dir() or "__pycache__" in report_base.parts:
				continue
			if report_base in seen:
				continue
			seen.add(report_base)

			for py_file in report_base.rglob("*.py"):
				if py_file.name == "__init__.py" or "__pycache__" in py_file.parts:
					continue
				result.files_checked += 1
				for issue in check_report_file(py_file):
					result.issues.append(issue)
					result.warnings.append(str(issue))

		return result
