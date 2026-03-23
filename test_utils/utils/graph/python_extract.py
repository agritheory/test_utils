"""Extract Python function metadata from a Frappe app tree (AST, no import)."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from ..static_analysis.path_resolver import PathResolver
from .constants import SKIP_DIR_PARTS


def module_dotted_path(py_file: Path, app_root: Path) -> str | None:
	"""Dotted module path for *py_file* relative to *app_root* (Frappe app layout)."""
	try:
		rel = py_file.relative_to(app_root)
	except ValueError:
		return None
	parts = list(rel.with_suffix("").parts)
	if parts and parts[-1] == "__init__":
		parts = parts[:-1]
	if not parts:
		return None
	return ".".join(parts)


def iter_python_files(app_root: Path):
	"""Yield ``*.py`` files under *app_root*, skipping caches and virtualenvs."""
	for p in app_root.rglob("*.py"):
		if p.parts and any(part in SKIP_DIR_PARTS for part in p.parts):
			continue
		yield p


def cyclomatic_complexity(node: ast.FunctionDef | ast.AsyncFunctionDef) -> int:
	"""Simplified McCabe complexity for *node* (includes nested closures in walk)."""
	cc = 1
	for child in ast.walk(node):
		if child is node:
			continue
		if isinstance(child, (ast.If, ast.While, ast.For, ast.AsyncFor, ast.With)):
			cc += 1
		elif isinstance(child, ast.ExceptHandler):
			cc += 1
		elif isinstance(child, ast.BoolOp):
			cc += len(child.values) - 1
	return cc


def max_ast_depth(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> int:
	"""Maximum nesting depth of compound statements in *fn* body (0 = flat)."""

	def depth_stmt(stmt: ast.stmt, d: int) -> int:
		m = d
		if isinstance(stmt, (ast.If, ast.For, ast.AsyncFor, ast.While, ast.With)):
			nd = d + 1
			m = max(m, nd)
			if isinstance(stmt, ast.If):
				for s in stmt.body:
					m = max(m, depth_stmt(s, nd))
				for s in stmt.orelse:
					m = max(m, depth_stmt(s, nd))
			elif isinstance(stmt, (ast.For, ast.AsyncFor, ast.While)):
				for s in stmt.body:
					m = max(m, depth_stmt(s, nd))
				for s in stmt.orelse:
					m = max(m, depth_stmt(s, d))
			elif isinstance(stmt, ast.With):
				for s in stmt.body:
					m = max(m, depth_stmt(s, nd))
		elif isinstance(stmt, ast.Try):
			nd = d + 1
			m = max(m, nd)
			for s in stmt.body:
				m = max(m, depth_stmt(s, nd))
			for h in stmt.handlers:
				for s in h.body:
					m = max(m, depth_stmt(s, nd))
			for s in stmt.orelse:
				m = max(m, depth_stmt(s, nd))
			for s in stmt.finalbody:
				m = max(m, depth_stmt(s, nd))
		elif isinstance(stmt, ast.Match):
			nd = d + 1
			m = max(m, nd)
			for case in stmt.cases:
				for s in case.body:
					m = max(m, depth_stmt(s, nd))
		return m

	best = 0
	for stmt in fn.body:
		best = max(best, depth_stmt(stmt, 0))
	return best


@dataclass(slots=True)
class PythonFunctionRow:
	dotted_path: str
	file_path: str
	line: int
	is_whitelisted: bool
	is_async: bool
	function_name: str
	cyclomatic: int
	max_ast_depth: int


class _FunctionCollector(ast.NodeVisitor):
	def __init__(self, module: str, file_relpath: str, resolver: PathResolver) -> None:
		self.module = module
		self.file_relpath = file_relpath
		self.resolver = resolver
		self.class_stack: list[str] = []
		self.function_stack: list[str] = []
		self.rows: list[PythonFunctionRow] = []

	def visit_ClassDef(self, node: ast.ClassDef) -> None:
		self.class_stack.append(node.name)
		self.generic_visit(node)
		self.class_stack.pop()

	def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
		self._function(node)

	def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
		self._function(node)

	def _function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
		parts = [self.module, *self.class_stack, *self.function_stack, node.name]
		dotted = ".".join(parts)
		is_wl = self.resolver.has_whitelist_decorator(node) or self.resolver.is_whitelisted(
			dotted
		)
		self.rows.append(
			PythonFunctionRow(
				dotted_path=dotted,
				file_path=self.file_relpath,
				line=node.lineno,
				is_whitelisted=is_wl,
				is_async=isinstance(node, ast.AsyncFunctionDef),
				function_name=node.name,
				cyclomatic=cyclomatic_complexity(node),
				max_ast_depth=max_ast_depth(node),
			)
		)
		self.function_stack.append(node.name)
		self.generic_visit(node)
		self.function_stack.pop()


def extract_python_functions(
	app_root: Path, resolver: PathResolver
) -> list[PythonFunctionRow]:
	"""Collect one row per function/async def under *app_root*."""
	all_rows: list[PythonFunctionRow] = []
	for py_file in iter_python_files(app_root):
		mod = module_dotted_path(py_file, app_root)
		if not mod:
			continue
		try:
			src = py_file.read_text(encoding="utf-8", errors="replace")
			tree = ast.parse(src, filename=str(py_file))
		except (OSError, SyntaxError):
			continue
		relpath = py_file.relative_to(app_root).as_posix()
		collector = _FunctionCollector(mod, relpath, resolver)
		collector.visit(tree)
		all_rows.extend(collector.rows)
	return all_rows
