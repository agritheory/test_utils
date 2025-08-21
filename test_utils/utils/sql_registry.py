import ast
import hashlib
import json
import pickle
import sys
import argparse
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import subprocess

try:
	import sqlglot
	from sqlglot import exp
except ImportError:
	print("Error: sqlglot package required. Install with: pip install sqlglot")
	sys.exit(1)


@dataclass
class SQLCall:
	call_id: str
	file_path: str
	line_number: int
	function_context: str
	sql_query: str
	ast_object: str | None  # Store as string since sqlglot objects aren't picklable
	ast_normalized: str
	query_builder_equivalent: str
	implementation_type: str  # frappe_db_sql|query_builder|mixed
	semantic_signature: str
	notes: str | None
	created_at: datetime
	updated_at: datetime


class SQLRegistry:
	"""Registry for SQL operations to enable programmatic rewriting and tracking"""

	def __init__(self, registry_file: str = ".sql_registry.pkl"):
		self.registry_file = Path(registry_file)
		self.data = self._load_registry()

	def _load_registry(self) -> dict:
		"""Load existing registry or create new one"""
		if self.registry_file.exists():
			try:
				with open(self.registry_file, "rb") as f:
					return pickle.load(f)
			except Exception as e:
				print(f"Warning: Error loading registry ({e}), creating new one")
				return self._create_empty_registry()
		return self._create_empty_registry()

	def _create_empty_registry(self) -> dict:
		"""Create empty registry structure"""
		return {
			"metadata": {
				"version": "1.0",
				"last_scan": None,
				"repository": self._get_repo_name(),
				"total_calls": 0,
				"commit_hash": self._get_commit_hash(),
			},
			"calls": {},
		}

	def _get_repo_name(self) -> str:
		"""Get repository name from git remote"""
		try:
			result = subprocess.run(
				["git", "config", "--get", "remote.origin.url"],
				capture_output=True,
				text=True,
				check=True,
			)
			url = result.stdout.strip()
			# Extract org/repo from URL
			if "github.com" in url:
				return url.split("github.com")[-1].strip("/:").replace(".git", "")
			return "unknown/repo"
		except Exception:
			return "unknown/repo"

	def _get_commit_hash(self) -> str:
		"""Get current git commit hash"""
		try:
			result = subprocess.run(
				["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
			)
			return result.stdout.strip()[:7]
		except Exception:
			return "unknown"

	def save_registry(self):
		"""Save registry to pickle file"""
		self.data["metadata"]["last_scan"] = datetime.now()
		self.data["metadata"]["total_calls"] = len(self.data["calls"])
		self.data["metadata"]["commit_hash"] = self._get_commit_hash()

		with open(self.registry_file, "wb") as f:
			pickle.dump(self.data, f)

	def generate_call_id(self, file_path: str, line_num: int, sql_query: str) -> str:
		"""Generate unique call ID based on file, line, and SQL content"""
		content = f"{file_path}:{line_num}:{sql_query[:100]}"  # Limit SQL length for ID
		return hashlib.md5(content.encode()).hexdigest()[:12]

	def register_sql_call(
		self, file_path: str, line_num: int, sql_query: str, function_context: str = ""
	) -> str:
		"""Register a SQL call in the registry"""
		call_id = self.generate_call_id(file_path, line_num, sql_query)

		# Check if already exists
		if call_id in self.data["calls"]:
			# Update existing entry
			existing = self.data["calls"][call_id]
			existing.updated_at = datetime.now()
			existing.sql_query = sql_query  # Update in case SQL changed
			return call_id

		# Analyze SQL
		ast_str, semantic_sig, qb_equivalent = self._analyze_sql(sql_query)

		sql_call = SQLCall(
			call_id=call_id,
			file_path=file_path,
			line_number=line_num,
			function_context=function_context,
			sql_query=sql_query,
			ast_object=ast_str,
			ast_normalized=semantic_sig,
			query_builder_equivalent=qb_equivalent,
			implementation_type="frappe_db_sql",  # Default, can be updated
			semantic_signature=semantic_sig,
			notes=None,
			created_at=datetime.now(),
			updated_at=datetime.now(),
		)

		self.data["calls"][call_id] = sql_call
		return call_id

	def _analyze_sql(self, sql_query: str) -> tuple[str, str, str]:
		"""Analyze SQL query to generate semantic signature and Query Builder equivalent"""
		try:
			# Clean and parse SQL
			sql_cleaned, replacements = self._replace_sql_patterns(sql_query)
			parsed = sqlglot.parse(sql_cleaned, dialect="mysql")

			if not parsed or not parsed[0]:
				return "", "UNPARSABLE", "# Could not parse SQL"

			ast_object = parsed[0]

			# Generate semantic signature for stability tracking
			semantic_sig = self._generate_semantic_signature(ast_object)

			# Convert to Query Builder equivalent for reference
			qb_equivalent = self._ast_to_query_builder(ast_object, replacements)

			return str(ast_object), semantic_sig, qb_equivalent

		except Exception as e:
			return "", f"ERROR: {str(e)}", f"# Error analyzing SQL: {str(e)}"

	def _replace_sql_patterns(self, sql: str) -> tuple[str, list[tuple[str, str]]]:
		"""Replace Python format patterns with placeholders"""
		import re

		replacements = []
		patterns = [
			(r"(%\([^)]+\)s)", "named parameter"),
			(r"({[^}]+})", "f-string block"),
			(r"(?<!%)%s", "positional parameter"),
		]

		for pattern, desc in patterns:
			for idx, match in enumerate(re.finditer(pattern, sql)):
				placeholder = f"__PH{len(replacements)}__"
				replacements.append((placeholder, match.group(0)))
				sql = sql.replace(match.group(0), placeholder, 1)

		return sql, replacements

	def _generate_semantic_signature(self, ast_object: sqlglot.Expression) -> str:
		"""Generate semantic signature for tracking"""
		try:
			# Create a semantic fingerprint that captures query structure
			normalized = ast_object.copy()

			# Normalize literals while preserving structure
			for node in normalized.walk():
				if isinstance(node, (exp.Literal, exp.Placeholder)):
					node.this = "VALUE"
				elif isinstance(node, exp.Identifier):
					# Keep table/column structure for semantic meaning
					pass

			# Create signature from normalized structure
			return str(normalized).replace(" ", "_").replace("(", "").replace(")", "")[:80]
		except Exception:
			return "UNKNOWN_SIGNATURE"

	def _ast_to_query_builder(
		self, ast_object: sqlglot.Expression, replacements: list[tuple[str, str]]
	) -> str:
		"""Convert AST to Query Builder equivalent for reference and potential refactoring"""
		try:
			if isinstance(ast_object, exp.Select):
				return self._convert_select_to_qb(ast_object, replacements)
			elif isinstance(ast_object, exp.Insert):
				return self._convert_insert_to_qb(ast_object, replacements)
			elif isinstance(ast_object, exp.Update):
				return self._convert_update_to_qb(ast_object, replacements)
			elif isinstance(ast_object, exp.Delete):
				return self._convert_delete_to_qb(ast_object, replacements)
			else:
				return f"# Unsupported query type: {type(ast_object).__name__}"
		except Exception as e:
			return f"# Error converting to Query Builder: {str(e)}"

	def _convert_select_to_qb(
		self, select: exp.Select, replacements: list[tuple[str, str]]
	) -> str:
		"""Convert SELECT statement to Query Builder"""
		lines = []

		# Start with from clause
		from_table = None
		if select.find(exp.From):
			from_clause = select.find(exp.From)
			if from_clause.this:
				from_table = str(from_clause.this).strip("`\"'")
				lines.append(f"qb = frappe.qb.from_('{from_table}')")

		# Add select fields
		if select.expressions:
			fields = []
			for expr in select.expressions:
				if isinstance(expr, exp.Star):
					fields.append("'*'")
				else:
					field_name = str(expr).strip("`\"'")
					fields.append(f"'{field_name}'")

			if len(fields) == 1:
				lines.append(f"qb = qb.select({fields[0]})")
			else:
				lines.append(f"qb = qb.select({', '.join(fields)})")

		# Add WHERE clause
		where_clause = select.find(exp.Where)
		if where_clause:
			condition = self._convert_condition_to_qb(where_clause.this, replacements)
			lines.append(f"qb = qb.where({condition})")

		# Add ORDER BY
		order_by = select.find(exp.Order)
		if order_by:
			for ordered in order_by.expressions:
				field = str(ordered.this).strip("`\"'")
				direction = "desc" if ordered.args.get("desc") else "asc"
				lines.append(f"qb = qb.orderby('{field}', order=frappe.qb.{direction})")

		# Add LIMIT
		limit = select.find(exp.Limit)
		if limit:
			lines.append(f"qb = qb.limit({limit.expression})")

		# Final execution
		lines.append("result = qb.run(as_dict=True)")

		return "\n".join(lines)

	def _convert_condition_to_qb(
		self, condition: exp.Expression, replacements: list[tuple[str, str]]
	) -> str:
		"""Convert WHERE condition to Query Builder format"""
		try:
			if isinstance(condition, exp.EQ):
				left = str(condition.left).strip("`\"'")
				right = str(condition.right)

				# Handle replacements
				for placeholder, original in replacements:
					if placeholder in right:
						right = right.replace(placeholder, original)

				return f"frappe.qb.Field('{left}') == {right}"

			elif isinstance(condition, exp.And):
				left_cond = self._convert_condition_to_qb(condition.left, replacements)
				right_cond = self._convert_condition_to_qb(condition.right, replacements)
				return f"({left_cond}) & ({right_cond})"

			elif isinstance(condition, exp.Or):
				left_cond = self._convert_condition_to_qb(condition.left, replacements)
				right_cond = self._convert_condition_to_qb(condition.right, replacements)
				return f"({left_cond}) | ({right_cond})"

			else:
				return f"# TODO: Handle {type(condition).__name__}"

		except Exception as e:
			return f"# Error converting condition: {str(e)}"

	def _convert_insert_to_qb(
		self, insert: exp.Insert, replacements: list[tuple[str, str]]
	) -> str:
		"""Convert INSERT to Query Builder"""
		table = str(insert.this).strip("`\"'")
		return f"""# INSERT operations typically use frappe.get_doc() in Frappe:
doc = frappe.get_doc({{
    'doctype': '{table}',
    # Add field mappings here
}})
doc.insert()"""

	def _convert_update_to_qb(
		self, update: exp.Update, replacements: list[tuple[str, str]]
	) -> str:
		"""Convert UPDATE to Query Builder"""
		return "# UPDATE: Use frappe.db.set_value() or doc.save() for single records"

	def _convert_delete_to_qb(
		self, delete: exp.Delete, replacements: list[tuple[str, str]]
	) -> str:
		"""Convert DELETE to Query Builder"""
		return "# DELETE: Use frappe.delete_doc() for single records or qb.delete() for bulk"

	def scan_directory(self, directory: Path, pattern: str = "**/*.py") -> int:
		"""Scan directory for SQL calls"""
		count = 0

		for file_path in directory.glob(pattern):
			if file_path.is_file():
				count += self._scan_file(file_path)

		return count

	def _scan_file(self, file_path: Path) -> int:
		"""Scan single file for SQL calls"""
		count = 0

		try:
			content = file_path.read_text(encoding="utf-8")
			tree = ast.parse(content)

			# Find frappe.db.sql calls
			for node in ast.walk(tree):
				if isinstance(node, ast.Call):
					if self._is_frappe_db_sql_call(node):
						sql_query = self._extract_sql_from_call(node)
						if sql_query:
							function_context = self._get_function_context(tree, node)
							self.register_sql_call(str(file_path), node.lineno, sql_query, function_context)
							count += 1

			# Also scan for SQL in docstrings
			sql_strings = self._find_sql_docstrings(content)
			for line_num, sql_query in sql_strings:
				function_context = f"docstring at line {line_num}"
				self.register_sql_call(str(file_path), line_num, sql_query, function_context)
				count += 1

		except Exception as e:
			print(f"Error scanning {file_path}: {e}")

		return count

	def _is_frappe_db_sql_call(self, node: ast.Call) -> bool:
		"""Check if AST node is a frappe.db.sql call"""
		try:
			if isinstance(node.func, ast.Attribute):
				if (
					isinstance(node.func.value, ast.Attribute)
					and isinstance(node.func.value.value, ast.Name)
					and node.func.value.value.id == "frappe"
					and node.func.value.attr == "db"
					and node.func.attr == "sql"
				):
					return True
			return False
		except Exception:
			return False

	def _extract_sql_from_call(self, node: ast.Call) -> str | None:
		"""Extract SQL query from frappe.db.sql call"""
		try:
			if node.args and len(node.args) > 0:
				first_arg = node.args[0]
				if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
					return first_arg.value
		except Exception:
			pass
		return None

	def _get_function_context(self, tree: ast.AST, target_node: ast.AST) -> str:
		"""Get the function context where the SQL call appears"""
		for node in ast.walk(tree):
			if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
				if self._node_contains(node, target_node):
					return f"def {node.name}():"
		return "global scope"

	def _node_contains(self, parent: ast.AST, child: ast.AST) -> bool:
		"""Check if parent AST node contains child node"""
		for node in ast.walk(parent):
			if node is child:
				return True
		return False

	def _find_sql_docstrings(self, content: str) -> list[tuple[int, str]]:
		"""Find SQL in docstrings"""
		sql_strings = []
		try:
			tree = ast.parse(content)
			for node in ast.walk(tree):
				if isinstance(node, ast.Constant) and isinstance(node.value, str):
					if self._is_likely_sql(node.value):
						sql_strings.append((node.lineno, node.value))
		except Exception:
			pass
		return sql_strings

	def _is_likely_sql(self, text: str) -> bool:
		"""Check if text is likely SQL"""
		sql_keywords = {"SELECT", "FROM", "WHERE", "JOIN", "INSERT", "UPDATE", "DELETE"}
		text_upper = text.upper()
		keyword_count = sum(1 for keyword in sql_keywords if keyword in text_upper)
		return keyword_count >= 2 and any(char in text for char in ";(),")

	def generate_report(self) -> str:
		"""Generate SQL usage report"""
		metadata = self.data["metadata"]
		calls = self.data["calls"]

		# Calculate statistics
		total = len(calls)
		by_type = {}

		for call in calls.values():
			impl_type = call.implementation_type
			by_type[impl_type] = by_type.get(impl_type, 0) + 1

		# Generate report
		report = f"""# SQL Operations Registry Report

**Repository**: {metadata.get('repository', 'N/A')}
**Last Updated**: {metadata.get('last_scan', 'Never')}
**Commit**: {metadata.get('commit_hash', 'N/A')}
**Total SQL Operations**: {total}

## Implementation Distribution
| Type | Count | Percentage |
|------|-------|------------|
| frappe_db_sql | {by_type.get('frappe_db_sql', 0)} | {(by_type.get('frappe_db_sql', 0) / max(total, 1) * 100):.1f}% |
| query_builder | {by_type.get('query_builder', 0)} | {(by_type.get('query_builder', 0) / max(total, 1) * 100):.1f}% |
| mixed | {by_type.get('mixed', 0)} | {(by_type.get('mixed', 0) / max(total, 1) * 100):.1f}% |

## Operations by File
"""

		# Group by file
		by_file = {}
		for call in calls.values():
			file_path = call.file_path
			if file_path not in by_file:
				by_file[file_path] = []
			by_file[file_path].append(call)

		for file_path, file_calls in sorted(by_file.items()):
			total_file = len(file_calls)
			report += f"\n**{Path(file_path).name}**: {total_file} operations\n"

		report += f"""
## Summary
- **Unique Query Patterns**: {len(set(c.semantic_signature for c in calls.values()))}
- **Total Operations Tracked**: {total}
"""

		return report


def main():
	parser = argparse.ArgumentParser(description="SQL Operations Registry")

	subparsers = parser.add_subparsers(dest="command", help="Available commands")

	# Scan command
	scan_parser = subparsers.add_parser("scan", help="Scan directory for SQL operations")
	scan_parser.add_argument("--directory", default=".", help="Directory to scan")
	scan_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)

	# Report command
	report_parser = subparsers.add_parser("report", help="Generate usage report")
	report_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	report_parser.add_argument("--output", help="Output file for report")

	# List command
	list_parser = subparsers.add_parser("list", help="List SQL calls")
	list_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	list_parser.add_argument("--file-filter", help="Filter by file path")

	# Show command
	show_parser = subparsers.add_parser("show", help="Show details for specific call")
	show_parser.add_argument("call_id", help="Call ID to show details for")
	show_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)

	# Rewrite command
	rewrite_parser = subparsers.add_parser(
		"rewrite", help="Rewrite SQL call to Query Builder"
	)
	rewrite_parser.add_argument("call_id", help="Call ID to rewrite")
	rewrite_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	rewrite_parser.add_argument(
		"--apply", action="store_true", help="Apply changes to file"
	)
	rewrite_parser.add_argument(
		"--no-backup", action="store_true", help="Skip backup creation"
	)

	args = parser.parse_args()

	if not args.command:
		parser.print_help()
		return

	registry = SQLRegistry(args.registry)

	if args.command == "scan":
		print("Scanning for SQL operations...")
		count = registry.scan_directory(Path(args.directory))
		registry.save_registry()
		print(f"Found and registered {count} SQL operations")

	elif args.command == "report":
		report = registry.generate_report()
		if args.output:
			Path(args.output).write_text(report)
			print(f"Report saved to {args.output}")
		else:
			print(report)

	elif args.command == "list":
		calls = registry.data["calls"]
		if not calls:
			print("No SQL calls found in registry.")
			return

		filtered_calls = []
		for call in calls.values():
			if args.file_filter and not call.file_path.endswith(args.file_filter):
				continue
			filtered_calls.append(call)

		print(f"\n📋 Found {len(filtered_calls)} SQL calls:")
		print("=" * 80)

		for call in sorted(filtered_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			print(f"\n{call.call_id[:8]}  {file_name}:{call.line_number}")
			print(f"   Function: {call.function_context}")
			print(f"   SQL: {call.sql_query[:100]}{'...' if len(call.sql_query) > 100 else ''}")

	elif args.command == "show":
		# Find call by ID (support partial IDs)
		matching_calls = [
			call
			for call in registry.data["calls"].values()
			if call.call_id.startswith(args.call_id)
		]

		if not matching_calls:
			print(f"No SQL call found with ID starting with '{args.call_id}'")
			return

		if len(matching_calls) > 1:
			print(f"Multiple calls match '{args.call_id}'. Please be more specific:")
			for call in matching_calls:
				print(f"  {call.call_id[:12]} - {Path(call.file_path).name}:{call.line_number}")
			return

		call = matching_calls[0]
		print(f"\n📝 SQL Call Details: {call.call_id}")
		print("=" * 60)
		print(f"File: {call.file_path}")
		print(f"Line: {call.line_number}")
		print(f"Function: {call.function_context}")
		print(f"Implementation: {call.implementation_type}")
		print(f"Created: {call.created_at}")
		print(f"Updated: {call.updated_at}")

		print("\n🔍 Original SQL:")
		print("-" * 40)
		print(call.sql_query)

		print("\n🏗️ Query Builder Equivalent:")
		print("-" * 40)
		print(call.query_builder_equivalent)

		if call.notes:
			print("\n📋 Notes:")
			print("-" * 40)
			print(call.notes)

	elif args.command == "rewrite":
		from sql_rewriter_functions import SQLRewriter

		rewriter = SQLRewriter(args.registry)
		dry_run = not args.apply
		backup = not args.no_backup

		success = rewriter.rewrite_sql(args.call_id, dry_run, backup)
		if not success:
			sys.exit(1)


if __name__ == "__main__":
	main()
