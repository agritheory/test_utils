"""Read-only queries against a code graph DuckDB file."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _require_duckdb():
	try:
		import duckdb
	except ImportError as e:  # pragma: no cover
		raise ImportError(
			"duckdb is required. Install with: poetry install --with graph"
		) from e
	return duckdb


def complexity_leaderboard(
	db_path: Path | str,
	*,
	limit: int = 100,
	min_cyclomatic: int | None = None,
) -> list[dict[str, Any]]:
	"""Return top functions by a simple composite score (cyclomatic + depth).

	Score = ``cyclomatic * 2 + max_ast_depth`` (NULL-safe).
	"""
	duckdb = _require_duckdb()
	path = Path(db_path)
	con = duckdb.connect(str(path), read_only=True)
	try:
		where = ""
		params: list[Any] = []
		if min_cyclomatic is not None:
			where = " WHERE cyclomatic >= ?"
			params.append(min_cyclomatic)
		params.append(limit)
		sql = f"""
			SELECT
				dotted_path,
				file_path,
				line,
				is_whitelisted,
				is_async,
				function_name,
				cyclomatic,
				max_ast_depth,
				(COALESCE(cyclomatic, 0) * 2 + COALESCE(max_ast_depth, 0)) AS complexity_score
			FROM functions
			{where}
			ORDER BY complexity_score DESC NULLS LAST, dotted_path
			LIMIT ?
		"""
		rows = con.execute(sql, params).fetchall()
		cols = [
			"dotted_path",
			"file_path",
			"line",
			"is_whitelisted",
			"is_async",
			"function_name",
			"cyclomatic",
			"max_ast_depth",
			"complexity_score",
		]
		return [dict(zip(cols, r)) for r in rows]
	finally:
		con.close()


def js_callsites_in_loops(db_path: Path | str) -> list[dict[str, Any]]:
	"""Return JS ``frappe.call`` / ``xcall`` rows where ``loop_context`` is true."""
	duckdb = _require_duckdb()
	con = duckdb.connect(str(Path(db_path)), read_only=True)
	try:
		rows = con.execute(
			"""
			SELECT file_path, line, target_path, loop_type
			FROM js_callsites
			WHERE loop_context = TRUE
			ORDER BY file_path, line
			"""
		).fetchall()
		cols = ["file_path", "line", "target_path", "loop_type"]
		return [dict(zip(cols, r)) for r in rows]
	finally:
		con.close()


def graph_meta(db_path: Path | str) -> dict[str, str]:
	"""Return ``meta`` key/value pairs for the graph file."""
	duckdb = _require_duckdb()
	con = duckdb.connect(str(Path(db_path)), read_only=True)
	try:
		rows = con.execute("SELECT key, value FROM meta").fetchall()
		return {k: v for k, v in rows}
	finally:
		con.close()
