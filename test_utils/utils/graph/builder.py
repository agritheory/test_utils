"""Build a DuckDB code graph artifact for one Frappe app."""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from ..static_analysis import discover_dependency_paths
from ..static_analysis.hooks_validator import PathExtractor, find_hooks_file
from ..static_analysis.path_resolver import PathResolver
from .js_callsites import iter_js_callsites_in_app
from .python_extract import extract_python_functions
from .schema import apply_schema, clear_data_tables


def _require_duckdb():
	try:
		import duckdb
	except ImportError as e:  # pragma: no cover - optional dependency
		raise ImportError(
			"duckdb is required to build the code graph. Install with: "
			"poetry install --with graph"
		) from e
	return duckdb


def build_graph(
	app_path: Path,
	output_path: Path,
	*,
	dependency_paths: list[Path] | None = None,
) -> Path:
	"""Scan *app_path* and write a DuckDB graph to *output_path*.

	Parameters
	----------
	app_path:
	    Root of the Frappe app (directory containing ``pyproject.toml`` / package).
	output_path:
	    Path to the ``.duckdb`` file to create (parent directories must exist).
	dependency_paths:
	    Extra app roots for :class:`PathResolver` (e.g. frappe, erpnext). When
	    omitted, :func:`discover_dependency_paths` is used.
	"""
	duckdb = _require_duckdb()
	app_path = app_path.resolve()
	output_path = output_path.resolve()
	output_path.parent.mkdir(parents=True, exist_ok=True)

	deps = discover_dependency_paths(
		app_path, [Path(p).resolve() for p in (dependency_paths or [])]
	)
	resolver = PathResolver.from_app(app_path, deps)

	con = duckdb.connect(str(output_path))
	try:
		apply_schema(con)
		clear_data_tables(con)

		now = datetime.now(timezone.utc).isoformat()
		con.execute("INSERT INTO meta VALUES ('app_name', ?)", [app_path.name])
		con.execute("INSERT INTO meta VALUES ('built_at', ?)", [now])
		con.execute("INSERT INTO meta VALUES ('app_root', ?)", [str(app_path)])

		fn_rows = extract_python_functions(app_path, resolver)
		if fn_rows:
			con.executemany(
				"""
				INSERT INTO functions (
					dotted_path, file_path, line, is_whitelisted, is_async,
					function_name, cyclomatic, max_ast_depth
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				""",
				[
					(
						r.dotted_path,
						r.file_path,
						r.line,
						r.is_whitelisted,
						r.is_async,
						r.function_name,
						r.cyclomatic,
						r.max_ast_depth,
					)
					for r in fn_rows
				],
			)

		hooks_file = find_hooks_file(app_path)
		if hooks_file and hooks_file.exists():
			try:
				src = hooks_file.read_text(encoding="utf-8", errors="replace")
				tree = ast.parse(src, filename=str(hooks_file))
			except (OSError, SyntaxError):
				tree = None
			if tree is not None:
				ex = PathExtractor()
				ex.visit(tree)
				hooks_relpath = hooks_file.relative_to(app_path).as_posix()
				for path, lineno, ctx in ex.found:
					con.execute(
						"""
						INSERT INTO hook_registrations (
							source_file, line, hook_variable, target_path
						) VALUES (?, ?, ?, ?)
						""",
						[hooks_relpath, lineno, ctx or "hooks", path],
					)

		for site in iter_js_callsites_in_app(app_path):
			tp = site.target_path or ""
			con.execute(
				"""
				INSERT INTO js_callsites (
					file_path, line, target_path, loop_context, loop_type
				) VALUES (?, ?, ?, ?, ?)
				""",
				[
					site.file_path,
					site.line,
					tp,
					site.loop_context,
					site.loop_type,
				],
			)
	finally:
		con.close()

	return output_path
