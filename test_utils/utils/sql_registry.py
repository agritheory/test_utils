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
	complexity_score: float
	notes: str | None
	created_at: datetime
	updated_at: datetime


class SQLRegistry:
	"""Comprehensive registry for all SQL operations to ensure long-term database stability"""

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
		except Exception as e:
			return "unknown/repo"

	def _get_commit_hash(self) -> str:
		"""Get current git commit hash"""
		try:
			result = subprocess.run(
				["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
			)
			return result.stdout.strip()[:7]
		except Exception as e:
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
		ast_str, semantic_sig, qb_equivalent, complexity = self._analyze_sql(sql_query)

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
			complexity_score=complexity,
			notes=None,
			created_at=datetime.now(),
			updated_at=datetime.now(),
		)

		self.data["calls"][call_id] = sql_call
		return call_id

	def _analyze_sql(self, sql_query: str) -> tuple[str, str, str, float]:
		"""Analyze SQL query to generate semantic signature and Query Builder equivalent"""
		try:
			# Clean and parse SQL
			sql_cleaned, replacements = self._replace_sql_patterns(sql_query)
			parsed = sqlglot.parse(sql_cleaned, dialect="mysql")

			if not parsed or not parsed[0]:
				return "", "UNPARSABLE", "# Could not parse SQL", 1.0

			ast_object = parsed[0]

			# Generate semantic signature for stability tracking
			semantic_sig = self._generate_semantic_signature(ast_object)

			# Convert to Query Builder equivalent for reference
			qb_equivalent = self._ast_to_query_builder(ast_object, replacements)

			# Calculate complexity score for risk assessment
			complexity = self._calculate_complexity_score(ast_object, replacements)

			return str(ast_object), semantic_sig, qb_equivalent, complexity

		except Exception as e:
			return "", f"ERROR: {str(e)}", f"# Error analyzing SQL: {str(e)}", 1.0

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
		"""Generate semantic signature for long-term stability tracking"""
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
		except Exception as e:
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

	def _calculate_complexity_score(
		self, ast_object: sqlglot.Expression, replacements: list[tuple[str, str]]
	) -> float:
		"""Calculate complexity score for risk assessment (0.0 = simple, 1.0 = complex)"""
		complexity = 0.0

		# Base complexity factors
		if ast_object.find_all(exp.Join):
			complexity += 0.3

		if ast_object.find_all(exp.Subquery):
			complexity += 0.4

		if ast_object.find_all(exp.Union):
			complexity += 0.3

		if len(replacements) > 5:
			complexity += 0.2

		# Complex functions increase risk
		complex_functions = ast_object.find_all(exp.Anonymous)
		complexity += min(len(complex_functions) * 0.1, 0.3)

		return min(1.0, complexity)

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

			# Also scan for SQL in docstrings (from existing functionality)
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
		except Exception as e:
			return False

	def _extract_sql_from_call(self, node: ast.Call) -> str | None:
		"""Extract SQL query from frappe.db.sql call"""
		try:
			if node.args and len(node.args) > 0:
				first_arg = node.args[0]
				if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
					return first_arg.value
		except Exception as e:
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
		"""Find SQL in docstrings (from existing code)"""
		sql_strings = []
		try:
			tree = ast.parse(content)
			for node in ast.walk(tree):
				if isinstance(node, ast.Constant) and isinstance(node.value, str):
					if self._is_likely_sql(node.value):
						sql_strings.append((node.lineno, node.value))
		except Exception as e:
			pass
		return sql_strings

	def _is_likely_sql(self, text: str) -> bool:
		"""Check if text is likely SQL"""
		sql_keywords = {"SELECT", "FROM", "WHERE", "JOIN", "INSERT", "UPDATE", "DELETE"}
		text_upper = text.upper()
		keyword_count = sum(1 for keyword in sql_keywords if keyword in text_upper)
		return keyword_count >= 2 and any(char in text for char in ";(),")

	def generate_stability_report(self) -> str:
		"""Generate comprehensive database stability and SQL usage report"""
		metadata = self.data["metadata"]
		calls = self.data["calls"]

		# Calculate statistics
		total = len(calls)
		by_type = {}
		by_complexity = {"simple": 0, "moderate": 0, "complex": 0}

		for call in calls.values():
			impl_type = call.implementation_type
			by_type[impl_type] = by_type.get(impl_type, 0) + 1

			if call.complexity_score <= 0.3:
				by_complexity["simple"] += 1
			elif call.complexity_score <= 0.7:
				by_complexity["moderate"] += 1
			else:
				by_complexity["complex"] += 1

		# Generate report
		report = f"""# Database Operations Registry & Stability Report

**Repository**: {metadata.get('repository', 'N/A')}
**Last Updated**: {metadata.get('last_scan', 'Never')}
**Commit**: {metadata.get('commit_hash', 'N/A')}
**Total Database Operations**: {total}

## Implementation Distribution
| Type | Count | Percentage | Description |
|------|-------|------------|-------------|
| frappe_db_sql | {by_type.get('frappe_db_sql', 0)} | {(by_type.get('frappe_db_sql', 0) / max(total, 1) * 100):.1f}% | Raw SQL operations |
| query_builder | {by_type.get('query_builder', 0)} | {(by_type.get('query_builder', 0) / max(total, 1) * 100):.1f}% | Query Builder operations |
| mixed | {by_type.get('mixed', 0)} | {(by_type.get('mixed', 0) / max(total, 1) * 100):.1f}% | Mixed implementation |

## Complexity Analysis
| Level | Count | Risk Assessment |
|--------|-------|-----------------|
| Simple (≤30%) | {by_complexity['simple']} | Low maintenance risk |
| Moderate (31-70%) | {by_complexity['moderate']} | Regular review recommended |
| Complex (>70%) | {by_complexity['complex']} | High attention required |

## Database Operations by File
"""

		# Group by file
		by_file = {}
		for call in calls.values():
			file_path = call.file_path
			if file_path not in by_file:
				by_file[file_path] = []
			by_file[file_path].append(call)

		for file_path, file_calls in sorted(by_file.items()):
			complex_calls = sum(1 for c in file_calls if c.complexity_score > 0.7)
			total_file = len(file_calls)

			report += f"\n**{Path(file_path).name}**: {total_file} operations"
			if complex_calls > 0:
				report += f", {complex_calls} complex"
			report += "\n"

		report += f"""

## Summary
- **Semantic Signatures**: {len(set(c.semantic_signature for c in calls.values()))} unique query patterns tracked
- **Risk Assessment**: {by_complexity['complex']} high-complexity operations require attention
- **Stability Score**: {((by_complexity['simple'] + by_complexity['moderate'] * 0.5) / max(total, 1) * 100):.1f}% operations are low-to-moderate risk

---
*This registry enables proactive database stability management by cataloging all SQL operations and their complexity patterns.*
"""

		return report


def main():
	parser = argparse.ArgumentParser(
		description="Database Operations Registry & Stability Tracker"
	)
	parser.add_argument("command", choices=["scan", "report", "status"])
	parser.add_argument("--directory", default=".", help="Directory to scan")
	parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	parser.add_argument("--output", help="Output file for report")

	args = parser.parse_args()

	registry = SQLRegistry(args.registry)

	if args.command == "scan":
		print("Scanning for database operations...")
		count = registry.scan_directory(Path(args.directory))
		registry.save_registry()
		print(f"Found and registered {count} database operations")

	elif args.command == "report":
		report = registry.generate_stability_report()
		if args.output:
			Path(args.output).write_text(report)
			print(f"Stability report saved to {args.output}")
		else:
			print(report)

	elif args.command == "status":
		total = len(registry.data["calls"])
		complex_ops = sum(
			1 for c in registry.data["calls"].values() if c.complexity_score > 0.7
		)
		print(
			f"Registry: {total} total operations, {complex_ops} complex operations requiring attention"
		)


if __name__ == "__main__":
	main()
