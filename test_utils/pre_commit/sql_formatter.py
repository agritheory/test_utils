import ast
import argparse
import tokenize
import os
import re
import sys
from io import StringIO
from typing import Sequence, List, Tuple, Optional
from pathlib import Path
from sqlglot import parse, transpile, ErrorLevel
from sqlglot.errors import ParseError

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


def format_sql_content(
	sql: str, file_path: str, line_num: int, dialect: str = "mysql"
) -> tuple[str, bool, list[str]]:
	warnings = []
	try:
		sql_for_format, replacements = replace_sql_patterns(sql)

		formatted = transpile(
			sql_for_format,
			read=dialect,
			write=dialect,
			pretty=True,
			indent=2,
			# identify=True, # use table names not aliases
		)[0]

		for placeholder, original in replacements:
			formatted = formatted.replace(placeholder, original)
		warnings.extend(check_format_patterns(formatted, file_path, line_num))
		return formatted, True, warnings

	except Exception as e:
		warnings.append(f"{RED}Could not format SQL: {str(e)}{RESET}")
		return sql, True, warnings


def replace_sql_patterns(sql: str) -> tuple[str, list[tuple[str, str]]]:
	"""
	Replace Python string formatting patterns with placeholders in SQL.
	Returns the modified SQL and a list of (placeholder, original) pairs.
	"""
	replacements = []
	patterns = [
		(r"(%\([^)]+\)s)", "named parameter"),  # %(key)s patterns
		(r"({[^}]+})", "f-string block"),  # {condition} patterns
		(r"(?<!%)%s", "positional parameter"),  # %s patterns
	]

	for pattern, _ in patterns:
		for match in re.finditer(pattern, sql):
			placeholder = f"__PH{len(replacements)}__"
			replacements.append((placeholder, match.group(0)))
			sql = sql.replace(match.group(0), placeholder)

	return sql, replacements


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
