"""Validate frappe.call / frappe.xcall paths found in Python source files."""

import ast
from dataclasses import dataclass, field
from pathlib import Path

from .path_resolver import PathResolver, is_potential_dotted_path


@dataclass
class PythonCallValidationResult:
	errors: list[str] = field(default_factory=list)
	warnings: list[str] = field(default_factory=list)
	calls_checked: int = 0

	def to_dict(self) -> dict:
		return {
			"errors": self.errors,
			"warnings": self.warnings,
			"calls_checked": self.calls_checked,
		}


IGNORE_MARKER = "frappe-vulture:ignore"


class FrappeCallVisitor(ast.NodeVisitor):
	"""Collect (dotted_path, lineno) pairs from frappe.call / frappe.xcall."""

	def __init__(self) -> None:
		self.found: list[tuple[str, int]] = []

	def visit_Call(self, node: ast.Call) -> None:
		if self.is_frappe_call(node.func):
			self.extract_path(node)
		self.generic_visit(node)

	def is_frappe_call(self, node: ast.expr) -> bool:
		return (
			isinstance(node, ast.Attribute)
			and isinstance(node.value, ast.Name)
			and node.value.id == "frappe"
			and node.attr in ("call", "xcall")
		)

	def extract_path(self, node: ast.Call) -> None:
		if node.args:
			first = node.args[0]
			if isinstance(first, ast.Constant) and isinstance(first.value, str):
				self.add(first.value, node.lineno)
				return
			if isinstance(first, ast.Dict):
				self.extract_from_dict(first, node.lineno)
				return
		for kw in node.keywords:
			if kw.arg == "method" and isinstance(kw.value, ast.Constant):
				self.add(kw.value.value, node.lineno)

	def extract_from_dict(self, d: ast.Dict, lineno: int) -> None:
		for key, val in zip(d.keys, d.values):
			if (
				isinstance(key, ast.Constant)
				and key.value == "method"
				and isinstance(val, ast.Constant)
				and isinstance(val.value, str)
			):
				self.add(val.value, lineno)

	def add(self, path: str, lineno: int) -> None:
		if is_potential_dotted_path(path):
			self.found.append((path, lineno))


def has_ignore_comment(source: str, lineno: int) -> bool:
	lines = source.splitlines()
	if 1 <= lineno <= len(lines):
		return IGNORE_MARKER in lines[lineno - 1]
	return False


def collect_calls(source: str, filepath: Path) -> list[tuple[str, int]]:
	try:
		tree = ast.parse(source, filename=str(filepath))
	except SyntaxError:
		return []
	visitor = FrappeCallVisitor()
	visitor.visit(tree)
	return [
		(path, lineno)
		for path, lineno in visitor.found
		if not has_ignore_comment(source, lineno)
	]


class PythonCallValidator:
	"""
	Validate every ``frappe.call`` / ``frappe.xcall`` invocation in Python source.

	Errors when the target function does not exist or lacks ``@frappe.whitelist()``.
	"""

	def __init__(self, resolver: PathResolver) -> None:
		self.resolver = resolver

	def validate(self, app_path: Path) -> PythonCallValidationResult:
		result = PythonCallValidationResult()

		for filepath in app_path.rglob("*.py"):
			if any(part in filepath.parts for part in ("node_modules", "__pycache__")):
				continue

			try:
				source = filepath.read_text(encoding="utf-8", errors="replace")
			except OSError:
				continue

			if "frappe.call" not in source and "frappe.xcall" not in source:
				continue

			for path, lineno in collect_calls(source, filepath):
				resolved = self.resolver.resolve(path)
				result.calls_checked += 1
				loc = f"{filepath}:{lineno}"

				if not resolved.exists:
					result.errors.append(f"{loc}: '{path}' — {resolved.error}")
				elif resolved.kind == "whitelisted":
					pass
				elif not resolved.is_whitelisted:
					result.errors.append(
						f"{loc}: '{path}' exists but is not decorated with @frappe.whitelist()"
					)

		return result
