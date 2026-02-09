import ast
import argparse
import tokenize
import os
import re
import sys
import traceback

from io import StringIO
from typing import List, Tuple, Optional

from collections.abc import Sequence
from pathlib import Path
from sqlglot import parse, transpile, ErrorLevel, exp


RED = "\033[91m"
BLUE = "\033[94m"
RESET = "\033[0m"
ORANGE = "\033[38;5;208m"


def find_python_files(app: str) -> list[str]:
	files = []
	app_dir = Path().resolve().parent / "apps" / app
	if Path.exists(app_dir / app):
		for file_path in app_dir.glob("**/*.py"):
			files.append(str(file_path))

	return sorted(files)


def detect_indentation(source: str) -> tuple[str, int]:
	"""
	Detects the indentation style from the first function in the file.
	Returns a tuple of (char, count) where char is ' ' or '\t'
	"""
	tree = ast.parse(source)

	# Find first function
	for node in ast.walk(tree):
		if isinstance(node, ast.FunctionDef):
			# Get the line before the function body starts
			lines = source.split("\n")
			# Look at the first line of the function body
			for body_node in node.body:
				body_line = lines[body_node.lineno - 1]
				whitespace = body_line[: len(body_line) - len(body_line.lstrip())]
				if whitespace:
					# Count tabs vs spaces
					if "\t" in whitespace:
						return ("\t", 1)
					else:
						return (" ", len(whitespace))

	# Default to 2 spaces if no indentation found
	return (" ", 2)


def is_likely_sql(text: str) -> bool:
	sql_keywords = {
		"SELECT",
		"FROM",
		"WHERE",
		"JOIN",
		"GROUP BY",
		"ORDER BY",
		"INSERT",
		"UPDATE",
		"DELETE",
		"CREATE",
		"ALTER",
		"DROP",
		"TABLE",
		"INDEX",
		"VIEW",
		"HAVING",
		"UNION",
		"TAB",
	}

	# Replace Python string formatting patterns to not interfere with SQL detection
	cleaned_text = " ".join(line.strip() for line in text.splitlines())
	# Handle %(key)s style
	cleaned_text = re.sub(r"%\([^)]+\)s", "placeholder", cleaned_text)
	# Handle f-string {blocks}
	cleaned_text = re.sub(r"{[^}]+}", "placeholder", cleaned_text)
	text_upper = cleaned_text.upper()
	sql_chars = ";", "(", ")", ","
	has_sql_chars = any(char in text for char in sql_chars)
	keyword_count = sum(1 for keyword in sql_keywords if keyword in text_upper)

	return has_sql_chars and keyword_count >= 2


def check_format_patterns(sql: str, file_path: str, line_num: int) -> list[str]:
	"""Returns warning message for %s patterns with formatted SQL"""
	if "%s" in sql:
		return [
			f"\n{BLUE}Recommendation: Use %(key)s format to make SQL queries more readable{RESET}",
			f"{file_path}:{line_num}",
			f"{ORANGE}{sql.strip()}{RESET}",
		]
	return []


def extract_docstrings(code: str) -> str:
	docstrings = []
	tokens = tokenize.generate_tokens(StringIO(code).readline)

	try:
		for token in tokens:
			if token.type == 3:
				string_val = token.string.strip()
				if (string_val.startswith('"""') or string_val.startswith("'''")) and (
					string_val.endswith('"""') or string_val.endswith("'''")
				):
					content = string_val[3:-3].strip()
					docstrings.append((token.start[0], content))
	except tokenize.TokenError:
		pass

	return docstrings


def find_sql_docstrings(code: str) -> list[tuple[int, str]]:
	sql_strings = []
	tree = ast.parse(code)

	for node in ast.walk(tree):
		# Look for any string constant in the AST
		if isinstance(node, ast.Constant) and isinstance(node.value, str):
			if node.value.strip() and is_likely_sql(node.value):
				# Get the line number from the node
				sql_strings.append((node.lineno, node.value))
		# Also check for joined strings
		elif isinstance(node, ast.JoinedStr):
			# Handle f-strings
			if hasattr(node, "values"):
				full_str = "".join(
					val.value if isinstance(val, ast.Constant) else "{...}" for val in node.values
				)
				if is_likely_sql(full_str):
					sql_strings.append((node.lineno, full_str))

	return sql_strings


def get_table_mapping(node):
	"""Get dict of tables and their aliases and resolve to full names"""
	tables = {}
	default_table = None

	for table in node.find_all(exp.Table):
		if isinstance(table, str):
			table_name = table
		elif hasattr(table, "name") and isinstance(table.name, str):
			table_name = table.name
		elif hasattr(table, "name") and hasattr(table.name, "this"):
			table_name = str(table.name.this)
		else:
			print(table, type(table))
			table_name = None

		if table_name:
			alias = str(table.alias or table_name)
			tables[alias] = table_name

			# Set default table from first table in FROM clause
			if not default_table and hasattr(node, "args"):
				from_tables = node.args.get("from", {}).expressions
				if from_tables:
					if isinstance(from_tables[0].this, str):
						default_table = exp.Table(this=from_tables[0].this, quoted=True)
					elif hasattr(from_tables[0].this, "this"):
						default_table = from_tables[0]
					else:
						default_table = exp.Table(this=list(tables.values())[0], quoted=True)

	if not default_table and tables:
		default_table = exp.Table(this=list(tables.values())[0], quoted=True)

	# Use the mapping to resolve aliases in column references
	for column in node.find_all(exp.Column):
		if column.table and str(column.table) in tables:
			table_name = tables[str(column.table)]
			column.set(
				"table",
				exp.Table(
					this=table_name,
					quoted=True,
					args={"this": exp.Identifier(this=table_name, quoted=True)},
				),
			)

	# Remove aliases from table definitions
	for table in node.find_all(exp.Table):
		if table.alias:
			table.set("alias", None)
			table.set("quoted", True)

	return tables, default_table


def qualify_outputs(node):
	"""Ensure all columns are qualified with a table name based on query context"""
	tables, default_table = get_table_mapping(node)

	# For SELECT expressions, unqualified columns belong to first FROM table
	if hasattr(node, "expressions"):
		for expr in node.expressions:
			for column in expr.find_all(exp.Column):
				if not column.table and not column.name.startswith("__PH"):
					column.set("table", exp.Table(this=default_table, quoted=True))

	# For JOIN conditions, unqualified columns belong to the JOIN table
	if hasattr(node, "joins"):
		for join in node.joins:
			join_table = str(join.this)
			if hasattr(join, "on"):
				for column in join.on.find_all(exp.Column):
					if not column.table and not column.name.startswith("__PH"):
						column.set("table", exp.Table(this=join_table, quoted=True))

	return node


def ensure_quoting(node):
	"""Ensure all identifiers are properly quoted for the current dialect"""
	# Handle table references
	for table in node.find_all(exp.Table):
		table.set("quoted", True)
		if hasattr(table, "this"):
			if isinstance(table.this, str):
				table.set("this", exp.Identifier(this=table.this, quoted=True))
			else:
				table.this.set("quoted", True)

	# Handle column references
	for column in node.find_all(exp.Column):
		if column.table:
			column.table.set("quoted", True)
			if hasattr(column.table, "this"):
				if isinstance(column.table.this, str):
					column.table.set("this", exp.Identifier(this=column.table.this, quoted=True))
				else:
					column.table.this.set("quoted", True)

	return node


def format_sql_content(
	sql: str, file_path: str, line_num: int, dialect: str = "mysql"
) -> tuple[str, bool, list[str]]:
	warnings = []
	try:
		sql_for_format, replacements = replace_sql_patterns(sql)
		print(f"\nProcessing {file_path}:{line_num}")

		parsed = parse(sql_for_format, read=dialect)[0]
		resolved = resolve_columns(parsed)
		qualified = qualify_outputs(parsed)
		quoted = ensure_quoting(qualified)

		formatted = quoted.sql(
			dialect=dialect,
			pretty=True,
			indent=2,
			normalize=True,
		)
		print("\nFormatted SQL:", formatted)
		replaced = formatted
		for placeholder, original in replacements:
			print("replacing", placeholder.lower(), original)
			print(placeholder.lower() in replaced)
			replaced = replaced.replace(placeholder.lower(), original)

		print("\nFinal SQL:", replaced)
		warnings.extend(check_format_patterns(replaced, file_path, line_num))
		return replaced, True, warnings

	except Exception as e:
		print(f"\nError at {file_path}:{line_num}")
		print(f"Exception type: {type(e)}")
		print(f"Exception args: {e.args}")
		print(f"Exception: {str(e)}")
		print("Traceback:", traceback.format_exc())
		warnings.append(f"{RED}Could not format SQL: {str(e)}{RESET}")
		return sql, True, warnings


def resolve_columns(node):
	tables = {}

	for table in node.find_all(exp.Table):
		tables[str(table.alias or table.name)] = str(table.name)

	fallback_table = list(tables.values())[0]

	for column in node.find_all(exp.Column):
		if not column.table:
			if len(tables) == 1:
				column.set("table", exp.Table(this=list(tables.values())[0]))
			elif len(tables) > 1:
				# Try to infer table ownership
				for alias, table_name in tables.items():
					if column.name in ["name", "parent", "doctype", "parenttype"]:
						column.set("table", exp.Table(this=table_name))
						break

	return node


def replace_sql_patterns(sql: str) -> tuple[str, list[tuple[str, str]]]:
	replacements = []
	patterns = [
		(r"(%\([^)]+\)s)", "named parameter"),  # %(key)s patterns
		(r"({[^}]+})", "f-string block"),  # {condition} patterns
		(r"(?<!%)%s", "positional parameter"),  # %s patterns
	]

	for pattern, _ in patterns:
		for idx, match in enumerate(re.finditer(pattern, sql)):
			placeholder = f"__PH{idx}__"
			print(idx, placeholder)
			replacements.append((placeholder, match.group(0)))
			sql = sql.replace(match.group(0), placeholder)

	return sql, list(set(replacements))


def update_file_content(
	file_path: str, replacements: list[tuple[int, str, str]]
) -> bool:
	try:
		with open(file_path, encoding="utf-8") as f:
			source = f.read()

		indent_char, indent_count = detect_indentation(source)
		tree = ast.parse(source)
		lines = source.split("\n")
		offsets = [0]
		for line in lines:
			offsets.append(offsets[-1] + len(line) + 1)

		for line_num, original, formatted in sorted(replacements, reverse=True):
			for node in ast.walk(tree):
				if isinstance(node, ast.Constant) and isinstance(node.value, str):
					if node.value == original:
						start_pos = offsets[node.lineno - 1] + node.col_offset
						end_pos = start_pos + len(original) + 6  # +6 for quotes
						# Use detected indentation style
						base_indent = indent_char * (node.col_offset // indent_count * indent_count)
						sql_indent = base_indent + indent_char * indent_count
						formatted_lines = []
						for i, line in enumerate(formatted.split("\n")):
							if i == 0:
								formatted_lines.append(line.strip())
							else:
								formatted_lines.append(sql_indent + line.strip())
						formatted_sql = "\n".join(formatted_lines)
						source = source[:start_pos] + f'"""{formatted_sql}"""' + source[end_pos:]
						break

		with open(file_path, "w", encoding="utf-8") as f:
			f.write(source)

		return True
	except Exception as e:
		print(f"{RED}Error updating file {file_path}: {str(e)}{RESET}", file=sys.stderr)
		return False


def process_file(
	file_path: str, dialect: str
) -> list[tuple[int, str, str, bool, list[str]]]:
	try:
		content = Path(file_path).read_text()
		sql_docstrings = find_sql_docstrings(content)
		results = []

		for line_num, sql in sql_docstrings:
			formatted_sql, is_valid, warnings = format_sql_content(
				sql, file_path, line_num, dialect
			)
			results.append((line_num, sql, formatted_sql, is_valid, warnings))

		return results
	except Exception as e:
		print(f"{RED}Error processing {file_path}: {str(e)}{RESET}", file=sys.stderr)
		return []


def format_files(
	filenames: list[str], write: bool = False, dialect: str = "mysql"
) -> bool:
	total_formatted = 0
	has_warnings = False
	shown_recommendation = False

	for file_path in filenames:
		if not file_path.endswith(".py"):
			continue

		results = process_file(file_path, dialect)
		if results:
			valid_replacements = []

			for line_num, original, formatted, is_valid, warnings in results:
				if not is_valid:
					print(f"\nFile: {file_path}")
					print(f"Could not format SQL: {formatted}")  # formatted contains error message
					continue

				if "%s" in original and not shown_recommendation:
					# Show recommendation only once
					print(
						f"\n{BLUE}Recommendation: Use %(key)s format to make SQL queries more readable{RESET}"
					)
					shown_recommendation = True

				if warnings:
					print(f"\nFile: {file_path}")
					print(f"{file_path}:{line_num}")
					# Show the formatted version since that's what they'll be working with
					print(f"{ORANGE}{formatted.strip()}{RESET}")

				if original != formatted:
					total_formatted += 1
				if write:
					valid_replacements.append((line_num, original, formatted))

			if write and valid_replacements:
				update_file_content(file_path, valid_replacements)

	print(
		f"\n{BLUE}Formatted {total_formatted} SQL statement{'s' if total_formatted != 1 else ''}{RESET}"
	)
	return True


def main():
	parser = argparse.ArgumentParser(description="Format SQL in Python docstrings")
	parser.add_argument("filenames", nargs="*")
	parser.add_argument("--app", help="Application name to process")
	parser.add_argument("--write", action="store_true", help="Write changes to files")
	parser.add_argument(
		"--dialect",
		default="mysql",
		choices=["mysql", "postgres", "sqlite"],
		help="SQL dialect (default: mysql)",
	)
	args = parser.parse_args()
	app = args.app[0]
	files = args.filenames
	if not files:
		files = find_python_files(app)
	success = format_files(files, args.write, args.dialect)
	sys.exit(0 if success else 1)


if __name__ == "__main__":
	main()
