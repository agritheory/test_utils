"""Resolve Python import statements to absolute module paths (AST, no import execution)."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Literal

from .python_extract import iter_python_files, module_dotted_path

ImportMapEntry = tuple[Literal["mod", "name"], str]
# "mod" = local name is a package/module prefix (import x / import x.y)
# "name" = local name refers to symbol x.y from "from ... import y"


def resolve_relative_module(current_module: str, level: int, module: str | None) -> str:
	"""Resolve ``from x import`` / ``from .x import`` to an absolute dotted module."""
	if level == 0:
		return module or ""
	parts = current_module.split(".")
	if level > len(parts):
		return ""
	base = parts[:-level]
	if module:
		return ".".join([*base, *module.split(".")])
	return ".".join(base)


def build_import_map(module: str, tree: ast.Module) -> dict[str, ImportMapEntry]:
	"""Map local names from imports to ``("mod", prefix)`` or ``("name", dotted_symbol)``."""
	out: dict[str, ImportMapEntry] = {}

	for node in tree.body:
		if isinstance(node, ast.Import):
			for alias in node.names:
				# ``import a.b.c`` binds only the first component as a local name.
				first = alias.name.split(".")[0]
				local = alias.asname or first
				out[local] = ("mod", first)
		elif isinstance(node, ast.ImportFrom):
			if node.level == 0 and not node.module:
				continue
			base = resolve_relative_module(module, node.level, node.module)
			if not base:
				continue
			for alias in node.names:
				if alias.name == "*":
					continue
				local = alias.asname or alias.name
				out[local] = ("name", f"{base}.{alias.name}")

	return out


def extract_python_import_edges(
	app_root: Path,
	exclude_globs: list[str] | None = None,
) -> list[tuple[str, str, str, int, str, str]]:
	"""Return rows for ``python_import_edges``.

	Each row: ``(importer_module, target_module, file_path, line, import_kind, names_csv)``.
	*import_kind* is ``import`` or ``from``. *names_csv* lists imported bare names (may be empty).
	"""
	globs = exclude_globs or []
	rows: list[tuple[str, str, str, int, str, str]] = []

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

		for node in tree.body:
			if isinstance(node, ast.Import):
				for alias in node.names:
					target = alias.name.split(".")[0]
					nm = f"{alias.name} as {alias.asname}" if alias.asname else alias.name
					rows.append((mod, target, relpath, node.lineno, "import", nm))
			elif isinstance(node, ast.ImportFrom):
				if node.level == 0 and not node.module:
					continue
				base = resolve_relative_module(mod, node.level, node.module)
				if not base:
					continue
				names_list = [a.name for a in node.names if a.name != "*"]
				names_csv = ",".join(names_list)
				if any(a.name == "*" for a in node.names):
					rows.append((mod, base, relpath, node.lineno, "from", "*"))
				else:
					rows.append((mod, base, relpath, node.lineno, "from", names_csv))

	return rows
