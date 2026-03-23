"""DuckDB schema for the Frappe code graph."""

from __future__ import annotations

from typing import Any

SCHEMA_STATEMENTS: tuple[str, ...] = (
	"""
	CREATE TABLE IF NOT EXISTS meta (
		key VARCHAR PRIMARY KEY,
		value VARCHAR NOT NULL
	)
	""",
	"""
	CREATE TABLE IF NOT EXISTS functions (
		dotted_path VARCHAR PRIMARY KEY,
		file_path VARCHAR NOT NULL,
		line INTEGER NOT NULL,
		is_whitelisted BOOLEAN NOT NULL,
		is_async BOOLEAN NOT NULL DEFAULT FALSE,
		function_name VARCHAR NOT NULL,
		cyclomatic INTEGER,
		max_ast_depth INTEGER
	)
	""",
	"""
	CREATE TABLE IF NOT EXISTS hook_registrations (
		source_file VARCHAR NOT NULL,
		line INTEGER NOT NULL,
		hook_variable VARCHAR NOT NULL,
		target_path VARCHAR NOT NULL,
		PRIMARY KEY (source_file, line, target_path)
	)
	""",
	"""
	CREATE TABLE IF NOT EXISTS js_callsites (
		file_path VARCHAR NOT NULL,
		line INTEGER NOT NULL,
		target_path VARCHAR NOT NULL,
		loop_context BOOLEAN NOT NULL,
		loop_type VARCHAR,
		PRIMARY KEY (file_path, line, target_path)
	)
	""",
)


def apply_schema(con: Any) -> None:
	"""Create all graph tables if they do not exist."""
	for stmt in SCHEMA_STATEMENTS:
		con.execute(stmt)


def clear_data_tables(con: Any) -> None:
	"""Remove all rows from data tables (keeps schema)."""
	for table in ("js_callsites", "hook_registrations", "functions", "meta"):
		con.execute(f"DELETE FROM {table}")
