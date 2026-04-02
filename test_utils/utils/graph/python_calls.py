"""Extract Python call edges via AST: same-module + import-resolved cross-module calls."""

from __future__ import annotations

import ast
from pathlib import Path

from .python_extract import iter_python_files, module_dotted_path
from .python_imports import build_import_map


def _collect_qualnames_in_module(
	tree: ast.Module, module: str
) -> list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef, str | None]]:
	"""Return ``(dotted_qualname, node, current_class_or_none)`` for every function."""
	out: list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef, str | None]] = []

	class V(ast.NodeVisitor):
		def __init__(self) -> None:
			self.class_stack: list[str] = []
			self.fn_stack: list[str] = []

		def visit_ClassDef(self, node: ast.ClassDef) -> None:
			self.class_stack.append(node.name)
			self.generic_visit(node)
			self.class_stack.pop()

		def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
			self._fn(node)

		def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
			self._fn(node)

		def _fn(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
			parts = [module, *self.class_stack, *self.fn_stack, node.name]
			qual = ".".join(parts)
			cur_cls = self.class_stack[-1] if self.class_stack else None
			out.append((qual, node, cur_cls))
			self.fn_stack.append(node.name)
			self.generic_visit(node)
			self.fn_stack.pop()

	V().visit(tree)
	return out


def collect_all_function_qualnames(
	app_root: Path,
	exclude_globs: list[str] | None = None,
) -> set[str]:
	"""All function dotted paths under *app_root*."""
	globs = exclude_globs or []
	quals: set[str] = set()
	for py_file in iter_python_files(app_root, exclude_globs=globs):
		mod = module_dotted_path(py_file, app_root)
		if not mod:
			continue
		try:
			src = py_file.read_text(encoding="utf-8", errors="replace")
			tree = ast.parse(src, filename=str(py_file))
		except (OSError, SyntaxError):
			continue
		for q, _, _ in _collect_qualnames_in_module(tree, mod):
			quals.add(q)
	return quals


def _load_attr_chain(expr: ast.expr) -> list[str] | None:
	"""``a.b.c`` -> ``['a','b','c']`` for load context; else None."""
	if isinstance(expr, ast.Name):
		return [expr.id]
	if isinstance(expr, ast.Attribute) and isinstance(expr.ctx, ast.Load):
		left = _load_attr_chain(expr.value)
		if left is None:
			return None
		return left + [expr.attr]
	return None


def _resolve_name_call(
	caller_qual: str,
	name: str,
	all_quals: set[str],
	module: str,
) -> str | None:
	"""Resolve bare name to a qualname in *all_quals* (same-module heuristic)."""
	cands = [q for q in all_quals if q == f"{module}.{name}" or q.endswith(f".{name}")]
	if not cands:
		return None
	if len(cands) == 1:
		return cands[0]

	def score(callee: str) -> int:
		if callee == caller_qual:
			return -1
		callee_parent = callee.rsplit(".", 1)[0]
		caller_parent = caller_qual.rsplit(".", 1)[0]
		if caller_qual.startswith(callee_parent + ".") and callee_parent != caller_qual:
			return len(callee_parent) + 1000
		if callee_parent == caller_parent:
			return len(callee_parent) + 500
		common = 0
		a, b = caller_qual.split("."), callee.split(".")
		for x, y in zip(a, b):
			if x == y:
				common += 1
			else:
				break
		return common

	best = max(cands, key=score)
	return best if score(best) >= 0 else None


def _resolve_attr_call(
	caller_qual: str,
	base: ast.expr,
	attr: str,
	all_quals: set[str],
	module: str,
	current_class: str | None,
) -> str | None:
	if isinstance(base, ast.Name) and base.id in ("self", "cls") and current_class:
		target = f"{module}.{current_class}.{attr}"
		if target in all_quals:
			return target
	return None


class _CallScanner(ast.NodeVisitor):
	def __init__(
		self,
		root_fn: ast.FunctionDef | ast.AsyncFunctionDef,
		caller_qual: str,
		all_quals: set[str],
		module: str,
		current_class: str | None,
		import_map: dict[str, tuple[str, str]],
	) -> None:
		self._root_fn = root_fn
		self.caller_qual = caller_qual
		self.all_quals = all_quals
		self.module = module
		self.current_class = current_class
		self.import_map = import_map
		self.edges: list[tuple[str, str, int]] = []

	def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
		if node is not self._root_fn:
			return
		self.generic_visit(node)

	def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
		if node is not self._root_fn:
			return
		self.generic_visit(node)

	def visit_ClassDef(self, _node: ast.ClassDef) -> None:
		# Nested classes get their own qualnames from the outer collector; skip here.
		return

	def _resolve_callee(self, func: ast.expr) -> str | None:
		parts = _load_attr_chain(func)
		if not parts:
			return None

		if len(parts) == 1:
			name = parts[0]
			if name in self.import_map:
				kind, dotted = self.import_map[name]
				if kind == "name":
					return dotted if dotted in self.all_quals else None
			return _resolve_name_call(self.caller_qual, name, self.all_quals, self.module)

		root, *attrs = parts

		if root in self.import_map:
			kind, base = self.import_map[root]
			if kind == "mod":
				cand = ".".join([base, *attrs])
				if cand in self.all_quals:
					return cand
			elif kind == "name":
				cand = ".".join([base, *attrs])
				if cand in self.all_quals:
					return cand

		if len(parts) == 2 and isinstance(func, ast.Attribute):
			return _resolve_attr_call(
				self.caller_qual,
				func.value,
				func.attr,
				self.all_quals,
				self.module,
				self.current_class,
			)

		return None

	def visit_Call(self, node: ast.Call) -> None:
		callee = self._resolve_callee(node.func)
		if callee and callee != self.caller_qual:
			self.edges.append((self.caller_qual, callee, node.lineno))
		self.generic_visit(node)


def extract_python_call_edges(
	app_root: Path,
	exclude_globs: list[str] | None = None,
) -> list[tuple[str, str, str, int]]:
	"""Return ``(caller_dotted, callee_dotted, file_relpath, line)`` for resolved calls."""
	globs = exclude_globs or []
	all_quals = collect_all_function_qualnames(app_root, exclude_globs=globs)
	edges: list[tuple[str, str, str, int]] = []

	for py_file in iter_python_files(app_root, exclude_globs=globs):
		mod = module_dotted_path(py_file, app_root)
		if not mod:
			continue
		relpath = py_file.relative_to(app_root).as_posix()
		try:
			src = py_file.read_text(encoding="utf-8", errors="replace")
			tree = ast.parse(src, filename=str(py_file))
		except (OSError, SyntaxError):
			continue

		imp_map = build_import_map(mod, tree)
		pairs = _collect_qualnames_in_module(tree, mod)

		for caller_qual, fn_node, current_class in pairs:
			scanner = _CallScanner(
				fn_node,
				caller_qual,
				all_quals,
				mod,
				current_class,
				imp_map,
			)
			scanner.visit(fn_node)
			for c, t, line in scanner.edges:
				edges.append((c, t, relpath, line))

	# One row per (caller, callee, file, line); duplicates can occur on a single source line.
	return list(dict.fromkeys(edges))
