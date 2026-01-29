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
import re
import sqlglot
from sqlglot import exp


class UnresolvedParameterError(Exception):
	"""Raised when a SQL parameter cannot be resolved to a Python variable."""

	pass


class ConversionIneligible(Exception):
	"""Raised when SQL cannot be automatically converted."""

	pass


@dataclass
class SQLCall:
	call_id: str
	file_path: str
	line_number: int
	function_context: str
	sql_query: str
	sql_params: dict | None
	sql_kwargs: dict | None
	variable_name: str | None
	ast_object: str | None
	ast_normalized: str
	query_builder_equivalent: str
	implementation_type: str
	semantic_signature: str
	notes: str | None
	created_at: datetime
	updated_at: datetime
	# Validation fields
	conversion_eligible: bool = True
	conversion_validated: bool = False
	ineligibility_reason: str | None = None


@dataclass
class SQLStructure:
	"""Normalized structure extracted from SQL for comparison."""

	query_type: str  # SELECT, INSERT, UPDATE, DELETE
	tables: list[str]
	fields: list[str]
	conditions: list[str]  # Normalized condition strings
	joins: list[str]
	group_by: list[str]
	order_by: list[str]
	limit: int | None
	has_aggregation: bool


class SQLRegistry:
	def __init__(self, registry_file: str = ".sql_registry.pkl"):
		self.registry_file = Path(registry_file)
		self.data = self.load_registry()

	def load_registry(self) -> dict:
		if self.registry_file.exists():
			try:
				with open(self.registry_file, "rb") as f:
					return pickle.load(f)
			except Exception as e:
				print(f"Warning: Error loading registry ({e}), creating new one")
				return self.create_empty_registry()
		return self.create_empty_registry()

	def create_empty_registry(self) -> dict:
		return {
			"metadata": {
				"version": "1.0",
				"last_scan": None,
				"repository": self.get_repo_name(),
				"total_calls": 0,
				"commit_hash": self.get_commit_hash(),
			},
			"calls": {},
		}

	def get_repo_name(self) -> str:
		try:
			result = subprocess.run(
				["git", "config", "--get", "remote.origin.url"],
				capture_output=True,
				text=True,
				check=True,
			)
			url = result.stdout.strip()
			if "github.com" in url:
				return url.split("github.com")[-1].strip("/:").replace(".git", "")
			return "unknown/repo"
		except Exception:
			return "unknown/repo"

	def get_commit_hash(self) -> str:
		try:
			result = subprocess.run(
				["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
			)
			return result.stdout.strip()[:7]
		except Exception:
			return "unknown"

	def save_registry(self):
		self.data["metadata"]["last_scan"] = datetime.now()
		self.data["metadata"]["total_calls"] = len(self.data["calls"])
		self.data["metadata"]["commit_hash"] = self.get_commit_hash()

		with open(self.registry_file, "wb") as f:
			pickle.dump(self.data, f)

	def generate_call_id(self, file_path: str, line_num: int, sql_query: str) -> str:
		content = f"{file_path}:{line_num}:{sql_query[:100]}"
		return hashlib.md5(content.encode()).hexdigest()[:12]

	def register_sql_call(
		self,
		file_path: str,
		line_num: int,
		sql_query: str,
		function_context: str = "",
		sql_params: dict = None,
		sql_kwargs: dict = None,
		variable_name: str = None,
	) -> str:
		call_id = self.generate_call_id(file_path, line_num, sql_query)

		if call_id in self.data["calls"]:
			existing = self.data["calls"][call_id]
			existing.updated_at = datetime.now()
			existing.sql_query = sql_query
			existing.sql_params = sql_params
			existing.sql_kwargs = sql_kwargs
			existing.variable_name = variable_name
			return call_id

		# Check conversion eligibility first
		sql_cleaned, replacements = self.replace_sql_patterns(sql_query)
		is_eligible, ineligibility_reason = self.check_conversion_eligibility(
			sql_query, replacements, sql_params
		)

		if not is_eligible:
			# Mark as ineligible with reason
			sql_call = SQLCall(
				call_id=call_id,
				file_path=file_path,
				line_number=line_num,
				function_context=function_context,
				sql_query=sql_query,
				sql_params=sql_params,
				sql_kwargs=sql_kwargs,
				variable_name=variable_name,
				ast_object="",
				ast_normalized="INELIGIBLE",
				query_builder_equivalent=f"# MANUAL: {ineligibility_reason}",
				implementation_type="frappe_db_sql",
				semantic_signature="INELIGIBLE",
				notes=ineligibility_reason,
				created_at=datetime.now(),
				updated_at=datetime.now(),
				conversion_eligible=False,
				conversion_validated=False,
				ineligibility_reason=ineligibility_reason,
			)
			self.data["calls"][call_id] = sql_call
			return call_id

		ast_str, semantic_sig, qb_equivalent = self.analyze_sql(
			sql_query, sql_params, variable_name
		)

		# Validate conversion if we have a valid QB output
		conversion_validated = False
		validation_notes = None
		if not qb_equivalent.startswith("#"):
			try:
				parsed = sqlglot.parse(sql_cleaned, dialect="mysql")
				if parsed and parsed[0]:
					is_valid, validation_error = self.validate_conversion(parsed[0], qb_equivalent)
					conversion_validated = is_valid
					if not is_valid:
						validation_notes = f"Validation failed: {validation_error}"
						# Mark as needing manual review
						qb_equivalent = f"# MANUAL: Validation failed - {validation_error}\n# Generated (unvalidated):\n# {qb_equivalent.replace(chr(10), chr(10) + '# ')}"
			except Exception as e:
				validation_notes = f"Validation error: {str(e)}"

		sql_call = SQLCall(
			call_id=call_id,
			file_path=file_path,
			line_number=line_num,
			function_context=function_context,
			sql_query=sql_query,
			sql_params=sql_params,
			sql_kwargs=sql_kwargs,
			variable_name=variable_name,
			ast_object=ast_str,
			ast_normalized=semantic_sig,
			query_builder_equivalent=qb_equivalent,
			implementation_type="frappe_db_sql",
			semantic_signature=semantic_sig,
			notes=validation_notes,
			created_at=datetime.now(),
			updated_at=datetime.now(),
			conversion_eligible=True,
			conversion_validated=conversion_validated,
			ineligibility_reason=None,
		)

		self.data["calls"][call_id] = sql_call
		return call_id

	def replace_sql_patterns(self, sql: str) -> tuple[str, list[tuple[str, str]]]:
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

	def generate_semantic_signature(self, ast_object: sqlglot.Expression) -> str:
		try:
			normalized = ast_object.copy()

			for node in normalized.walk():
				if isinstance(node, (exp.Literal, exp.Placeholder)):
					node.this = "VALUE"
				elif isinstance(node, exp.Identifier):
					pass

			return str(normalized).replace(" ", "_").replace("(", "").replace(")", "")[:80]
		except Exception:
			return "UNKNOWN_SIGNATURE"

	def ast_to_query_builder(
		self,
		ast_object: sqlglot.Expression,
		replacements: list[tuple[str, str]],
		sql_params: dict = None,
		variable_name: str = None,
	) -> str:
		try:
			if isinstance(ast_object, exp.Select):
				# Check if this is a simple query that can use Frappe ORM
				if self.can_use_frappe_orm(ast_object):
					return self.convert_select_to_orm(
						ast_object, replacements, sql_params, variable_name
					)
				else:
					return self.convert_select_to_qb(
						ast_object, replacements, sql_params, variable_name
					)
			elif isinstance(ast_object, exp.Insert):
				return self.convert_insert_to_qb(ast_object, replacements)
			elif isinstance(ast_object, exp.Update):
				return self.convert_update_to_qb(ast_object, replacements, sql_params)
			elif isinstance(ast_object, exp.Delete):
				return self.convert_delete_to_qb(ast_object, replacements, sql_params)
			else:
				return f"# Unsupported query type: {type(ast_object).__name__}"
		except UnresolvedParameterError as e:
			return f"# MANUAL: {str(e)} - needs manual conversion"
		except Exception as e:
			return f"# Error converting to Query Builder: {str(e)}"

	def can_use_frappe_orm(self, select: exp.Select) -> bool:
		"""
		Determine if a SELECT query can be converted to Frappe ORM instead of Query Builder.

		Frappe ORM is suitable when:
		- Single table query (no JOINs)
		- No complex aggregations
		- No GROUP BY
		- No HAVING
		- Simple WHERE conditions
		"""
		tables = set()

		from_clause = select.find(exp.From)
		if from_clause and from_clause.this:
			if isinstance(from_clause.this, exp.Table):
				tables.add(str(from_clause.this).strip("`\"'"))
			else:
				tables.add(str(from_clause.this).strip("`\"'"))

		if select.find(exp.Join):
			return False

		for node in select.walk():
			if isinstance(node, exp.Table):
				table_name = str(node).strip("`\"'")
				parent = getattr(node, "parent", None)
				if not isinstance(parent, exp.Column):
					tables.add(table_name)

		if from_clause:
			from_str = str(from_clause).upper()
			if "," in from_str and "FROM" in from_str:
				from_part = from_str.split("WHERE")[0] if "WHERE" in from_str else from_str
				from_part = from_part.split("ORDER")[0] if "ORDER" in from_part else from_part
				from_part = from_part.split("GROUP")[0] if "GROUP" in from_part else from_part
				if from_part.count(",") > 0:
					return False

		if (
			len(tables) > 1
			or select.find(exp.Group)
			or select.find(exp.Having)
			or select.find(exp.Union)
		):
			return False

		for expr in select.expressions:
			expr_str = str(expr).upper()
			if any(
				agg in expr_str
				for agg in ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "GROUP_CONCAT("]
			):
				return False

		for node in select.walk():
			if isinstance(node, exp.Subquery):
				return False

		return True

	def analyze_sql(
		self, sql_query: str, sql_params: dict = None, variable_name: str = None
	) -> tuple[str, str, str]:
		"""Analyze SQL and determine the best conversion approach"""
		try:
			sql_cleaned, replacements = self.replace_sql_patterns(sql_query)
			parsed = sqlglot.parse(sql_cleaned, dialect="mysql")

			if not parsed or not parsed[0]:
				return "", "UNPARSABLE", "# Could not parse SQL"

			ast_object = parsed[0]
			semantic_sig = self.generate_semantic_signature(ast_object)
			qb_equivalent = self.ast_to_query_builder(
				ast_object, replacements, sql_params, variable_name
			)
			return str(ast_object), semantic_sig, qb_equivalent

		except Exception as e:
			return "", f"ERROR: {str(e)}", f"# Error analyzing SQL: {str(e)}"

	def check_conversion_eligibility(
		self,
		sql_query: str,
		replacements: list[tuple[str, str]],
		sql_params: dict | None = None,
	) -> tuple[bool, str | None]:
		"""
		Check if a SQL query is eligible for automatic conversion.

		Returns: (is_eligible, reason_if_not)

		Ineligible cases:
		- F-strings with complex expressions (not simple variable references)
		- String concatenation that can't be statically resolved
		- Dynamic table names
		- SQL parameters (%(name)s, %s) without corresponding values in sql_params
		"""
		# Track parameter requirements
		named_params_needed = []
		positional_params_count = 0

		for placeholder, original in replacements:
			# Check for f-string blocks with complex expressions
			if original.startswith("{") and original.endswith("}"):
				inner = original[1:-1]
				# Simple variable references are OK: {var}, {self.var}, {obj.attr}
				# Complex expressions are NOT OK: {func()}, {a + b}, {x[0]}
				if re.search(r"[\[\]()+=\-*/]", inner):
					return False, f"Complex f-string expression: {original}"
				if "(" in inner:
					return False, f"F-string with function call: {original}"

			# Track named parameters like %(name)s
			elif original.startswith("%(") and original.endswith(")s"):
				param_name = original[2:-2]  # Extract 'name' from '%(name)s'
				named_params_needed.append(param_name)

			# Track positional parameters %s
			elif original == "%s":
				positional_params_count += 1

		# Check if named parameters are provided
		if named_params_needed:
			if sql_params is None:
				return (
					False,
					f"Named parameters {named_params_needed} used but no params dict provided",
				)
			missing = [p for p in named_params_needed if p not in sql_params]
			if missing:
				return False, f"Named parameters {missing} not found in params dict"

		# Check if positional parameters are provided
		if positional_params_count > 0:
			if sql_params is None:
				return (
					False,
					f"{positional_params_count} positional parameter(s) used but no params provided",
				)
			# Check if we have enough positional params
			# Positional params are stored as __pos_0__, __pos_1__, etc.
			provided_positional = sum(1 for k in sql_params.keys() if k.startswith("__pos_"))
			if provided_positional < positional_params_count:
				return (
					False,
					f"{positional_params_count} positional parameter(s) needed but only {provided_positional} provided",
				)

		# Check for dynamic table names (table name is a placeholder)
		try:
			sql_cleaned, _ = self.replace_sql_patterns(sql_query)
			parsed = sqlglot.parse(sql_cleaned, dialect="mysql")
			if parsed and parsed[0]:
				ast = parsed[0]
				# Check if FROM clause contains a placeholder
				from_clause = ast.find(exp.From)
				if from_clause:
					table_str = str(from_clause.this) if from_clause.this else ""
					if "__PH" in table_str:
						return False, "Dynamic table name"
		except Exception:
			pass

		return True, None

	def extract_sql_structure(self, ast_object: exp.Expression) -> SQLStructure:
		"""Extract normalized structure from SQL AST for comparison."""
		query_type = type(ast_object).__name__.upper()
		tables = []
		fields = []
		conditions = []
		joins = []
		group_by = []
		order_by = []
		limit_val = None
		has_aggregation = False

		if isinstance(ast_object, exp.Select):
			# Extract tables
			from_clause = ast_object.find(exp.From)
			if from_clause and from_clause.this:
				tables.append(self._normalize_table_name(str(from_clause.this)))

			# Extract joins
			for join in ast_object.find_all(exp.Join):
				if join.this:
					tables.append(self._normalize_table_name(str(join.this)))
					joins.append(f"JOIN {self._normalize_table_name(str(join.this))}")

			# Extract fields
			for expr in ast_object.expressions:
				field_str = self._normalize_field(str(expr))
				fields.append(field_str)
				# Check for aggregations
				if any(
					agg in str(expr).upper() for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("]
				):
					has_aggregation = True

			# Extract conditions
			where = ast_object.find(exp.Where)
			if where:
				conditions.append(self._normalize_condition(str(where.this)))

			# Extract GROUP BY
			group = ast_object.find(exp.Group)
			if group:
				for expr in group.expressions:
					group_by.append(self._normalize_field(str(expr)))

			# Extract ORDER BY
			order = ast_object.find(exp.Order)
			if order:
				for expr in order.expressions:
					order_by.append(self._normalize_field(str(expr)))

			# Extract LIMIT
			limit = ast_object.find(exp.Limit)
			if limit and limit.expression:
				try:
					limit_val = int(str(limit.expression))
				except ValueError:
					pass

		elif isinstance(ast_object, exp.Delete):
			table = ast_object.this
			if table:
				tables.append(self._normalize_table_name(str(table)))
			where = ast_object.find(exp.Where)
			if where:
				conditions.append(self._normalize_condition(str(where.this)))

		elif isinstance(ast_object, exp.Update):
			table = ast_object.this
			if table:
				tables.append(self._normalize_table_name(str(table)))
			# Extract SET fields
			for expr in ast_object.expressions:
				if isinstance(expr, exp.EQ):
					field_name = str(expr.left).strip("`\"'")
					fields.append(field_name.lower())
			where = ast_object.find(exp.Where)
			if where:
				conditions.append(self._normalize_condition(str(where.this)))

		return SQLStructure(
			query_type=query_type,
			tables=tables,
			fields=fields,
			conditions=conditions,
			joins=joins,
			group_by=group_by,
			order_by=order_by,
			limit=limit_val,
			has_aggregation=has_aggregation,
		)

	def _normalize_table_name(self, name: str) -> str:
		"""Normalize table name by removing quotes, aliases, and 'tab' prefix.

		Frappe table names can have spaces (e.g., 'tabEmail Queue'), so we must
		be careful to distinguish between spaces in names vs implicit aliases.
		"""
		name = name.strip("`\"'")

		# Remove explicit alias (AS clause)
		if " AS " in name.upper():
			name = name.upper().split(" AS ")[0].strip()
		# Check for implicit alias - but only if pattern looks like: `tabName` alias
		# where alias is a single lowercase word at the end
		elif " " in name:
			# Split into parts
			parts = name.split()
			# If last part is a simple lowercase identifier (alias), remove it
			# Frappe table names start with 'tab' and use Title Case for spaces
			if len(parts) >= 2 and parts[-1].islower() and parts[-1].isalpha():
				# Likely an alias - check if remaining parts form a valid Frappe table
				potential_table = " ".join(parts[:-1])
				if potential_table.startswith("`tab") or potential_table.lower().startswith("tab"):
					name = potential_table
			# Otherwise, keep the full name (it's a multi-word table name like "Email Queue")

		name = name.strip("`\"'")
		if name.lower().startswith("tab"):
			name = name[3:]
		return name.lower()

	def _normalize_field(self, field: str) -> str:
		"""Normalize field reference."""
		# Remove backticks, quotes, and standardize
		field = field.strip("`\"'")
		# Replace placeholders with generic marker
		field = re.sub(r"__PH\d+__", "PARAM", field)
		return field.lower()

	def _normalize_condition(self, condition: str) -> str:
		"""Normalize condition for comparison."""
		# Replace placeholders with generic marker
		condition = re.sub(r"__PH\d+__", "PARAM", condition)
		# Remove extra whitespace
		condition = " ".join(condition.split())
		return condition.lower()

	def extract_qb_structure(self, qb_code: str) -> SQLStructure | None:
		"""
		Extract structure from generated Query Builder code.

		Parses the QB code as Python AST and extracts the query structure.
		"""
		try:
			tree = ast.parse(qb_code)
		except SyntaxError:
			return None

		tables = []
		fields = []
		conditions = []
		joins = []
		group_by = []
		order_by = []
		limit_val = None
		has_aggregation = False
		query_type = "SELECT"  # Default

		for node in ast.walk(tree):
			# Look for frappe.get_all / frappe.get_list calls (ORM)
			if isinstance(node, ast.Call):
				func = node.func
				if isinstance(func, ast.Attribute):
					method_name = func.attr

					# ORM methods
					if method_name in ("get_all", "get_list", "get_value"):
						if node.args:
							first_arg = node.args[0]
							if isinstance(first_arg, ast.Constant):
								tables.append(first_arg.value.lower())
						# Extract fields from 'fields' kwarg
						for kw in node.keywords:
							if kw.arg == "fields":
								fields.extend(self._extract_list_values(kw.value))
							elif kw.arg == "filters":
								conditions.append("HAS_FILTERS")
							elif kw.arg == "limit":
								if isinstance(kw.value, ast.Constant):
									limit_val = kw.value.value

					# Query Builder methods
					elif method_name == "delete":
						query_type = "DELETE"
					elif method_name == "select":
						fields.extend(self._extract_qb_fields(node))
					elif method_name == "where":
						conditions.append("HAS_WHERE")
					elif method_name == "groupby":
						group_by.append("HAS_GROUPBY")
					elif method_name == "orderby":
						order_by.append("HAS_ORDERBY")
					elif method_name == "limit":
						if node.args and isinstance(node.args[0], ast.Constant):
							limit_val = node.args[0].value

				# Look for frappe.qb.DocType calls
				if isinstance(func, ast.Attribute) and func.attr == "DocType":
					if node.args and isinstance(node.args[0], ast.Constant):
						tables.append(node.args[0].value.lower())

			# Check for db.delete calls
			if isinstance(node, ast.Call):
				func = node.func
				if isinstance(func, ast.Attribute) and func.attr == "delete":
					# Check if it's frappe.db.delete
					if isinstance(func.value, ast.Attribute) and func.value.attr == "db":
						query_type = "DELETE"
						if node.args and isinstance(node.args[0], ast.Constant):
							tables.append(node.args[0].value.lower())
						# Check for filters (second argument = WHERE equivalent)
						if len(node.args) >= 2 or any(kw.arg == "filters" for kw in node.keywords):
							conditions.append("HAS_FILTERS")

			# Check for db.set_value calls (UPDATE)
			if isinstance(node, ast.Call):
				func = node.func
				if isinstance(func, ast.Attribute) and func.attr == "set_value":
					# Check if it's frappe.db.set_value
					if isinstance(func.value, ast.Attribute) and func.value.attr == "db":
						query_type = "UPDATE"
						if node.args and isinstance(node.args[0], ast.Constant):
							tables.append(node.args[0].value.lower())
						# set_value always has a name/filter (WHERE equivalent)
						if len(node.args) >= 2:
							conditions.append("HAS_NAME_FILTER")
						# Extract fields from 3rd argument (field name) or 3rd argument dict
						if len(node.args) >= 3:
							third_arg = node.args[2]
							if isinstance(third_arg, ast.Constant):
								fields.append(str(third_arg.value).lower())
							elif isinstance(third_arg, ast.Dict):
								for key in third_arg.keys:
									if isinstance(key, ast.Constant):
										fields.append(str(key.value).lower())

			# Check for QB update operations
			if isinstance(node, ast.Call):
				func = node.func
				if isinstance(func, ast.Attribute):
					if func.attr == "update":
						query_type = "UPDATE"
					elif func.attr == "set" and query_type == "UPDATE":
						# Extract field from .set(field, value)
						if node.args:
							first_arg = node.args[0]
							if isinstance(first_arg, ast.Attribute):
								fields.append(first_arg.attr.lower())

		return SQLStructure(
			query_type=query_type,
			tables=tables,
			fields=[f.lower() for f in fields],
			conditions=conditions,
			joins=joins,
			group_by=group_by,
			order_by=order_by,
			limit=limit_val,
			has_aggregation=has_aggregation,
		)

	def _extract_list_values(self, node: ast.AST) -> list[str]:
		"""Extract string values from a list AST node."""
		values = []
		if isinstance(node, ast.List):
			for elt in node.elts:
				if isinstance(elt, ast.Constant):
					values.append(str(elt.value))
		return values

	def _extract_qb_fields(self, call_node: ast.Call) -> list[str]:
		"""Extract field names from QB select() call."""
		fields = []
		for arg in call_node.args:
			if isinstance(arg, ast.Constant):
				fields.append(str(arg.value))
			elif isinstance(arg, ast.Attribute):
				fields.append(arg.attr)
			elif isinstance(arg, ast.Call):
				# Handle function calls like fn.Count(), fn.Sum(), etc.
				# These count as a field (aggregation result)
				func = arg.func
				if isinstance(func, ast.Attribute):
					func_name = func.attr.lower()
					if func_name in ("count", "sum", "avg", "min", "max", "coalesce", "ifnull", "abs"):
						fields.append(f"__{func_name}__")
					elif func_name == "as_":
						# field.as_("alias") - count as 1 field
						fields.append("__aliased__")
					else:
						fields.append("__func__")
				else:
					fields.append("__func__")
			elif isinstance(arg, ast.BinOp):
				# Arithmetic expressions like (fn.Sum(x) - fn.Sum(y)) count as 1 field
				fields.append("__expr__")
			elif isinstance(arg, ast.Tuple):
				# Multiple fields as a tuple - count each
				fields.extend(["__tuple_elem__"] * len(arg.elts))
		return fields

	def validate_conversion(
		self, original_ast: exp.Expression, qb_code: str
	) -> tuple[bool, str | None]:
		"""
		Validate that QB code produces equivalent query structure.

		Returns: (is_valid, error_message_if_not)
		"""
		# Skip validation for manual/error conversions
		if qb_code.startswith("#"):
			return True, None

		original_struct = self.extract_sql_structure(original_ast)
		qb_struct = self.extract_qb_structure(qb_code)

		if qb_struct is None:
			return False, "Could not parse QB code"

		errors = []

		# Compare query types
		if original_struct.query_type != qb_struct.query_type:
			errors.append(
				f"Query type mismatch: {original_struct.query_type} vs {qb_struct.query_type}"
			)

		# Compare tables
		orig_tables = set(original_struct.tables)
		qb_tables = set(qb_struct.tables)
		if orig_tables != qb_tables:
			# QB may have MORE tables than original (subquery tables need DocType declarations)
			# Only error if original has tables that QB doesn't have
			missing_in_qb = orig_tables - qb_tables
			if missing_in_qb:
				errors.append(f"Table mismatch: {orig_tables} vs {qb_tables}")

		# For SELECT, compare field count (not exact match due to normalization differences)
		if original_struct.query_type == "SELECT":
			# Check if both have fields or both use *
			orig_has_star = "*" in str(original_struct.fields)
			qb_has_star = "*" in str(qb_struct.fields)
			if not orig_has_star and not qb_has_star:
				if len(original_struct.fields) != len(qb_struct.fields):
					errors.append(
						f"Field count mismatch: {len(original_struct.fields)} vs {len(qb_struct.fields)}"
					)

		# Compare condition presence
		orig_has_where = bool(original_struct.conditions)
		qb_has_where = bool(qb_struct.conditions)
		if orig_has_where != qb_has_where:
			errors.append(f"WHERE clause mismatch: original={orig_has_where}, qb={qb_has_where}")

		# Compare LIMIT
		if original_struct.limit != qb_struct.limit:
			errors.append(f"LIMIT mismatch: {original_struct.limit} vs {qb_struct.limit}")

		if errors:
			return False, "; ".join(errors)

		return True, None

	def convert_select_to_orm(
		self,
		select: exp.Select,
		replacements: list[tuple[str, str]],
		sql_params: dict = None,
		variable_name: str = None,
	) -> str:
		"""Convert a simple SELECT to Frappe ORM calls (v16 compatible)"""
		lines = []
		needs_imports = set()  # Track required imports for v16

		# Determine how to handle the result
		if variable_name == "__return__":
			# Direct return statement
			result_prefix = "return "
		elif variable_name == "__yield__":
			# Yield statement
			result_prefix = "yield "
		elif variable_name == "__expr__":
			# Expression statement (no assignment)
			result_prefix = ""
		elif variable_name:
			# Regular variable assignment
			result_prefix = f"{variable_name} = "
		else:
			# Fallback
			result_prefix = "result = "

		# Get the table name
		from_clause = select.find(exp.From)
		if not from_clause or not from_clause.this:
			return "# Error: No FROM clause found"

		# Extract table name, ignoring alias if present
		table_node = from_clause.this
		if hasattr(table_node, "this") and table_node.this:
			# Table has alias, get actual table name
			table_name = str(table_node.this).strip("`\"'")
		else:
			table_name = str(table_node).strip("`\"'")
		# Convert 'tabDocType' to 'DocType'
		doctype = (
			table_name.replace("tab", "") if table_name.startswith("tab") else table_name
		)

		# Extract fields - check for DISTINCT modifier
		fields = []
		is_single_field = False
		has_distinct = False

		# Check for SELECT DISTINCT
		if select.args.get("distinct"):
			has_distinct = True

		if select.expressions:
			for expr in select.expressions:
				if isinstance(expr, exp.Star):
					fields = None  # Get all fields
					break
				elif isinstance(expr, exp.Distinct):
					# Handle DISTINCT wrapper
					has_distinct = True
					inner_exprs = expr.expressions if hasattr(expr, "expressions") else [expr.this]
					for inner in inner_exprs:
						field_name = (
							str(inner.this).strip("`\"'")
							if hasattr(inner, "this")
							else str(inner).strip("`\"'")
						)
						if "." in field_name:
							_, field_name = field_name.rsplit(".", 1)
						fields.append(field_name)
				else:
					expr_str = str(expr)
					# Check for DISTINCT keyword in expression string
					if "DISTINCT" in expr_str.upper():
						has_distinct = True
						field_name = (
							str(expr.this).strip("`\"'")
							if hasattr(expr, "this")
							else expr_str.upper().replace("DISTINCT", "").strip("`\"' ")
						)
					else:
						field_name = (
							str(expr.this).strip("`\"'") if hasattr(expr, "this") else expr_str.strip("`\"'")
						)

					# Remove table prefix if present
					if "." in field_name:
						_, field_name = field_name.rsplit(".", 1)

					fields.append(field_name)

			is_single_field = len(fields) == 1 if fields else False

		# Extract filters from WHERE clause
		filters = {}
		where_clause = select.find(exp.Where)
		if where_clause:
			filters = self.convert_where_to_filters(where_clause.this, replacements, sql_params)

		# Extract ORDER BY
		order_by = None
		order_clause = select.find(exp.Order)
		if order_clause:
			order_parts = []
			for ordered in order_clause.expressions:
				field = str(ordered.this).strip("`\"'")
				if "." in field:
					_, field = field.rsplit(".", 1)
				# Check for DESC
				if hasattr(ordered, "args") and ordered.args.get("desc"):
					order_parts.append(f"{field} desc")
				else:
					order_parts.append(f"{field} asc")

			if order_parts:
				order_by = ", ".join(order_parts)

		# Extract LIMIT
		limit = None
		limit_clause = select.find(exp.Limit)
		if limit_clause:
			limit = str(limit_clause.expression)

		# Build the Frappe ORM call
		lines.append(f"{result_prefix}frappe.get_all(")
		lines.append(f'\t"{doctype}",')

		# Add filters if present
		if filters:
			formatted_filters = self.format_filters_for_orm(filters, needs_imports)
			lines.append(f"\tfilters={formatted_filters},")

		# Add fields - use ["*"] for SELECT *, otherwise list specific fields
		if fields is None:
			# SELECT * - need to explicitly request all fields
			lines.append('\tfields=["*"],')
		elif is_single_field:
			lines.append(f'\tfields=["{fields[0]}"],')
		else:
			fields_str = ", ".join(f'"{f}"' for f in fields)
			lines.append(f"\tfields=[{fields_str}],")

		# Add order_by if present
		if order_by:
			lines.append(f'\torder_by="{order_by}",')

		# Add limit if present
		if limit:
			lines.append(f"\tlimit={limit},")

		# v16: Handle DISTINCT with distinct=True parameter
		if has_distinct:
			lines.append("\tdistinct=True,")

		# Close the function call
		if lines[-1].endswith(","):
			lines[-1] = lines[-1][:-1]  # Remove trailing comma
		lines.append(")")

		# Generate imports if needed (v16 features)
		import_lines = []
		if "Field" in needs_imports or "IfNull" in needs_imports:
			import_lines.append("from frappe.query_builder import Field")
		if "IfNull" in needs_imports:
			import_lines.append("from frappe.query_builder.functions import IfNull")

		if import_lines:
			return "\n".join(import_lines) + "\n\n" + "\n".join(lines)

		return "\n".join(lines)

	def convert_where_to_filters(
		self,
		condition: exp.Expression,
		replacements: list[tuple[str, str]],
		sql_params: dict = None,
	):
		"""Convert WHERE clause to Frappe ORM filters format"""
		try:
			if isinstance(condition, exp.EQ):
				field = self.extract_field_name(condition.left)
				value = self.extract_value(condition.right, replacements, sql_params)
				return {field: value}

			elif isinstance(condition, exp.And):
				# Combine filters from both sides
				left_filters = self.convert_where_to_filters(
					condition.left, replacements, sql_params
				)
				right_filters = self.convert_where_to_filters(
					condition.right, replacements, sql_params
				)

				if isinstance(left_filters, dict) and isinstance(right_filters, dict):
					# Merge dictionaries
					return {**left_filters, **right_filters}
				elif isinstance(left_filters, list) or isinstance(right_filters, list):
					# If either is a list, we need to use list format
					filters = []
					if isinstance(left_filters, dict):
						filters.extend([[k, "=", v] for k, v in left_filters.items()])
					else:
						filters.extend(left_filters)

					if isinstance(right_filters, dict):
						filters.extend([[k, "=", v] for k, v in right_filters.items()])
					else:
						filters.extend(right_filters)
					return filters

			elif isinstance(condition, exp.Or):
				# OR conditions require list format
				left_filters = self.convert_where_to_filters(
					condition.left, replacements, sql_params
				)
				right_filters = self.convert_where_to_filters(
					condition.right, replacements, sql_params
				)

				# Convert to list format with OR
				filters = ["or"]
				if isinstance(left_filters, dict):
					filters.extend([[k, "=", v] for k, v in left_filters.items()])
				else:
					filters.extend(left_filters)

				if isinstance(right_filters, dict):
					filters.extend([[k, "=", v] for k, v in right_filters.items()])
				else:
					filters.extend(right_filters)

				return filters

			elif isinstance(condition, exp.GT):
				field = self.extract_field_name(condition.left)
				value = self.extract_value(condition.right, replacements, sql_params)
				return [[field, ">", value]]

			elif isinstance(condition, exp.GTE):
				field = self.extract_field_name(condition.left)
				value = self.extract_value(condition.right, replacements, sql_params)
				return [[field, ">=", value]]

			elif isinstance(condition, exp.LT):
				field = self.extract_field_name(condition.left)
				value = self.extract_value(condition.right, replacements, sql_params)
				return [[field, "<", value]]

			elif isinstance(condition, exp.LTE):
				field = self.extract_field_name(condition.left)
				value = self.extract_value(condition.right, replacements, sql_params)
				return [[field, "<=", value]]

			elif isinstance(condition, exp.In):
				field = self.extract_field_name(condition.this)
				values = [
					self.extract_value(v, replacements, sql_params) for v in condition.expressions
				]
				return [[field, "in", values]]

			elif isinstance(condition, exp.Like):
				field = self.extract_field_name(condition.this)
				pattern = self.extract_value(condition.expression, replacements, sql_params)
				return [[field, "like", pattern]]

			elif isinstance(condition, exp.Is):
				# IS NULL
				field = self.extract_field_name(condition.this)
				return [[field, "is", "null"]]

			elif isinstance(condition, exp.Not):
				# Check if this is IS NOT NULL
				if isinstance(condition.this, exp.Is):
					field = self.extract_field_name(condition.this.this)
					return [[field, "is", "set"]]
				# Otherwise it's a NOT of something else
				inner = self.convert_where_to_filters(condition.this, replacements, sql_params)
				# Can't easily negate arbitrary filters in ORM
				return {}

			elif isinstance(condition, exp.Between):
				# BETWEEN low AND high
				field = self.extract_field_name(condition.this)
				low = self.extract_value(condition.args.get("low"), replacements, sql_params)
				high = self.extract_value(condition.args.get("high"), replacements, sql_params)
				# Frappe uses ["between", [low, high]]
				return [[field, "between", [low, high]]]

			elif isinstance(condition, exp.NEQ):
				# Handle != conditions (v16 compatible)
				left_expr = condition.left
				right_value = self.extract_value(condition.right, replacements, sql_params)

				# Check if left side is a function call (IFNULL/COALESCE)
				if isinstance(left_expr, exp.Coalesce):
					# v16: Use IfNull(Field("field"), default) format
					field_name = self.extract_field_name(left_expr.this)
					default_val = (
						self.extract_value(left_expr.expressions[0], replacements, sql_params)
						if left_expr.expressions
						else '""'
					)
					# Return special marker for v16 IFNULL filter
					return [["__ifnull__", field_name, default_val, "!=", right_value]]
				else:
					field = self.extract_field_name(left_expr)
					return [[field, "!=", right_value]]

			elif isinstance(condition, exp.EQ) and isinstance(condition.left, exp.Coalesce):
				# Handle IFNULL(field, '') = '' pattern (v16 compatible)
				left_expr = condition.left
				right_value = self.extract_value(condition.right, replacements, sql_params)
				field_name = self.extract_field_name(left_expr.this)
				default_val = (
					self.extract_value(left_expr.expressions[0], replacements, sql_params)
					if left_expr.expressions
					else '""'
				)
				return [["__ifnull__", field_name, default_val, "=", right_value]]

			else:
				return {}

		except Exception as e:
			return {}

	def extract_field_name(self, expr: exp.Expression) -> str:
		"""Extract field name from expression"""
		field_str = str(expr).strip("`\"'")
		if "." in field_str:
			_, field_name = field_str.rsplit(".", 1)
			return field_name.strip("`\"'")
		return field_str

	def extract_value(
		self,
		expr: exp.Expression,
		replacements: list[tuple[str, str]],
		sql_params: dict = None,
	):
		"""Extract value from expression, handling parameters"""
		value_str = str(expr).strip("`\"'")

		for placeholder, original in replacements:
			if placeholder == value_str:
				param_match = re.match(r"%\((\w+)\)s", original)
				if param_match:
					param_name = param_match.group(1)
					if sql_params:
						if param_name in sql_params:
							param_value = sql_params[param_name]
							if isinstance(param_value, str) and not param_value.startswith("doc."):
								if param_value.isidentifier():
									return param_value
								return f'"{param_value}"'
							return param_value
					return param_name

				if original == "%s":
					for key, value in (sql_params or {}).items():
						if key.startswith("__pos_"):
							return value

				return original

		if value_str.startswith("'") or value_str.startswith('"'):
			return value_str.strip("'\"")

		return value_str

	def format_filters_for_orm(self, filters, needs_imports: set = None) -> str:
		"""Format filters for Frappe ORM calls (v16 compatible)

		Args:
		        filters: The filters to format
		        needs_imports: Set to track required imports (modified in place)
		"""
		if needs_imports is None:
			needs_imports = set()

		if isinstance(filters, dict):
			if len(filters) == 1 and "name" in filters:
				# Single name filter
				return filters["name"] if isinstance(filters["name"], str) else str(filters["name"])
			else:
				# Dictionary filters
				items = []
				for key, value in filters.items():
					if isinstance(value, str):
						# Check if value is already quoted or is a variable reference
						if value.startswith('"') or value.startswith("'"):
							# Already quoted, use as-is
							items.append(f'"{key}": {value}')
						elif value.startswith("doc.") or "." in value or value.isidentifier():
							# Variable reference - don't quote
							items.append(f'"{key}": {value}')
						else:
							# String literal - add quotes
							items.append(f'"{key}": "{value}"')
					else:
						items.append(f'"{key}": {value}')
				return "{" + ", ".join(items) + "}"
		elif isinstance(filters, list):
			# List format filters - check for v16 IFNULL markers
			formatted = []
			for f in filters:
				if isinstance(f, list) and len(f) >= 5 and f[0] == "__ifnull__":
					# v16 IFNULL filter: ["__ifnull__", field, default, op, value]
					_, field_name, default_val, op, value = f
					needs_imports.add("Field")
					needs_imports.add("IfNull")
					# Format: [IfNull(Field("field"), default), op, value]
					if isinstance(default_val, str) and default_val not in ('""', "''"):
						default_repr = (
							f'"{default_val}"' if not default_val.startswith('"') else default_val
						)
					else:
						default_repr = '""'
					if isinstance(value, str) and value not in ('""', "''"):
						value_repr = f'"{value}"' if not value.startswith('"') else value
					else:
						value_repr = '""'
					formatted.append(
						f'[IfNull(Field("{field_name}"), {default_repr}), "{op}", {value_repr}]'
					)
				elif isinstance(f, list):
					# Regular list filter
					formatted.append(repr(f))
				else:
					formatted.append(repr(f))
			return "[" + ", ".join(formatted) + "]"
		else:
			return str(filters)

	def format_fields_for_orm(self, fields: list) -> str:
		"""Format fields list for Frappe ORM calls"""
		if len(fields) == 1:
			return f'"{fields[0]}"'
		else:
			return "[" + ", ".join(f'"{f}"' for f in fields) + "]"

	def convert_select_to_qb(
		self,
		select: exp.Select,
		replacements: list[tuple[str, str]],
		sql_params: dict = None,
		variable_name: str = None,
	) -> str:
		"""Convert SELECT statement to Query Builder"""
		lines = []
		table_vars = {}  # Maps table name/alias to variable name
		alias_to_table = {}  # Maps alias to actual table name

		if variable_name == "__return__":
			result_prefix = "return "
		elif variable_name == "__yield__":
			result_prefix = "yield "
		elif variable_name == "__expr__":
			result_prefix = ""
		elif variable_name:
			result_prefix = f"{variable_name} = "
		else:
			result_prefix = "result = "

		tables = []  # List of (table_name, alias_or_none) tuples

		# Extract tables and their aliases properly
		for node in select.walk():
			if isinstance(node, exp.Table):
				# Get the actual table name (not including alias)
				if hasattr(node, "this") and node.this:
					table_name = str(node.this).strip("`\"'")
				else:
					table_name = (
						str(node.name).strip("`\"'") if hasattr(node, "name") else str(node).strip("`\"'")
					)

				# Get alias if present
				alias = None
				if hasattr(node, "alias") and node.alias:
					alias = str(node.alias).strip("`\"'")

				# Only add if we haven't seen this table/alias combo
				table_key = alias if alias else table_name
				if table_key not in [t[1] if t[1] else t[0] for t in tables]:
					tables.append((table_name, alias))
					if alias:
						alias_to_table[alias] = table_name

		# Check if we need to import functions (for aggregates, etc.)
		needs_fn_import = any(
			isinstance(
				expr,
				(
					exp.Count,
					exp.Avg,
					exp.Sum,
					exp.Max,
					exp.Min,
					exp.Date,
					exp.Cast,
					exp.Coalesce,
					exp.Abs,
				),
			)
			for expr in select.walk()
		)

		# Check if we need Case import
		needs_case_import = any(isinstance(expr, exp.Case) for expr in select.walk())
		if needs_fn_import:
			lines.append("from frappe.query_builder import functions as fn")
			lines.append("")

		if needs_case_import:
			lines.append("from pypika import Case")
			lines.append("")

		# Check if we need SubQuery import (for IN/NOT IN with subqueries)
		def has_subquery_in(expr):
			if isinstance(expr, exp.In) and expr.args.get("query"):
				return True
			if (
				isinstance(expr, exp.Not)
				and isinstance(expr.this, exp.In)
				and expr.this.args.get("query")
			):
				return True
			return False

		needs_subquery_import = any(has_subquery_in(expr) for expr in select.walk())
		if needs_subquery_import:
			lines.append("from frappe.query_builder.terms import SubQuery")
			lines.append("")

		# Check if we need CustomFunction import (for DATEDIFF, etc.)
		needs_custom_function = any(isinstance(expr, exp.DateDiff) for expr in select.walk())
		if needs_custom_function:
			lines.append("from frappe.query_builder import CustomFunction")
			lines.append("")

		# Check if we need ExistsCriterion import (for EXISTS subqueries)
		needs_exists_import = any(isinstance(expr, exp.Exists) for expr in select.walk())
		if needs_exists_import:
			lines.append("from pypika.terms import ExistsCriterion")
			lines.append("")

		# Generate DocType declarations
		for table_name, alias in tables:
			doctype_name = (
				table_name.replace("tab", "") if table_name.startswith("tab") else table_name
			)
			# Use alias for variable name if present, otherwise use table name
			var_base = alias if alias else doctype_name
			var_name_local = var_base.lower().replace(" ", "_").replace("-", "_")

			# Map both the alias and full table name to this variable
			if alias:
				table_vars[alias] = var_name_local
				table_vars[table_name] = var_name_local
			else:
				table_vars[table_name] = var_name_local

			lines.append(f'{var_name_local} = frappe.qb.DocType("{doctype_name}")')

		lines.append("")

		main_table_tuple = tables[0] if tables else None
		if main_table_tuple:
			main_table_name, main_alias = main_table_tuple
			main_key = main_alias if main_alias else main_table_name
			chain_parts = []

			# Start with FROM
			main_var = table_vars[main_key]
			chain_parts.append(f"frappe.qb.from_({main_var})")

			# Add additional FROM tables for implicit joins
			for table_name, alias in tables[1:]:
				table_key = alias if alias else table_name
				chain_parts.append(f"	.from_({table_vars[table_key]})")

			# Add SELECT fields
			if select.expressions:
				select_fields = []
				for expr in select.expressions:
					field_ref = self.format_select_field(
						expr, table_vars, main_var, replacements, sql_params
					)
					select_fields.append(field_ref)

				if len(select_fields) == 1:
					chain_parts.append(f"	.select({select_fields[0]})")
				else:
					chain_parts.append("	.select(")
					for i, field in enumerate(select_fields):
						if i < len(select_fields) - 1:
							chain_parts.append(f"		{field},")
						else:
							chain_parts.append(f"		{field}")
					chain_parts.append("	)")

			# Add WHERE clause
			where_clause = select.find(exp.Where)
			if where_clause:
				condition = self.convert_condition_to_qb(
					where_clause.this, replacements, table_vars, sql_params
				)
				chain_parts.append(f"	.where({condition})")

			# Add GROUP BY
			group_by = select.find(exp.Group)
			if group_by:
				group_fields = []
				for group_expr in group_by.expressions:
					field = str(group_expr).strip("`\"'")
					if "." in field:
						table_part, field_part = field.rsplit(".", 1)
						table_part = table_part.strip("`\"'")
						if table_part in table_vars:
							group_fields.append(f"{table_vars[table_part]}.{field_part}")
						else:
							group_fields.append(f"{main_var}.{field_part}")
					else:
						group_fields.append(f"{main_var}.{field}")
				if group_fields:
					chain_parts.append(f"\t.groupby({', '.join(group_fields)})")

			# Add HAVING
			having = select.find(exp.Having)
			if having:
				having_condition = self.convert_condition_to_qb(
					having.this, replacements, table_vars, sql_params
				)
				chain_parts.append(f"\t.having({having_condition})")

			# Add ORDER BY
			order_by = select.find(exp.Order)
			if order_by:
				for ordered in order_by.expressions:
					field = str(ordered.this).strip("`\"'")
					direction = "desc" if ordered.args.get("desc") else "asc"

					if "." in field:
						table_part, field_part = field.rsplit(".", 1)
						table_part = table_part.strip("`\"'")
						if table_part in table_vars:
							chain_parts.append(
								f"\t.orderby({table_vars[table_part]}.{field_part}, order=frappe.qb.{direction})"
							)
					else:
						chain_parts.append(f"\t.orderby({main_var}.{field}, order=frappe.qb.{direction})")

			# Add LIMIT
			limit = select.find(exp.Limit)
			if limit:
				chain_parts.append(f"\t.limit({limit.expression})")

			# Add final execution
			chain_parts.append("\t.run(as_dict=True)")

			# Join all parts with the correct prefix - indent the chain for proper formatting
			# Add a tab to the first line so it aligns with the continuation
			chain_str = "\n".join(chain_parts)
			lines.append(f"{result_prefix}(\n\t{chain_str}\n)")

		return "\n".join(lines)

	def format_select_field(
		self,
		expr,
		table_vars: dict,
		main_var: str,
		replacements: list = None,
		sql_params: dict = None,
	) -> str:
		if isinstance(expr, exp.Star):
			return "'*'"

		# Handle placeholder resolution for parameter substitution
		if replacements and sql_params:
			expr_str = str(expr).strip()
			for placeholder, original in replacements:
				if placeholder == expr_str:
					# Extract parameter name from %(param)s format
					param_match = re.match(r"%\((\w+)\)s", original)
					if param_match:
						param_name = param_match.group(1)
						if param_name in sql_params:
							return str(sql_params[param_name])
					# Positional parameter
					if original == "%s":
						pos_key = f"__pos_{replacements.index((placeholder, original))}__"
						if pos_key in sql_params:
							return str(sql_params[pos_key])

		# Handle literal values (numbers, strings)
		if isinstance(expr, exp.Literal):
			if expr.is_string:
				return f'"{expr.this}"'
			else:
				return str(expr.this)

		# Handle parenthesized expressions
		if isinstance(expr, exp.Paren):
			inner = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			return f"({inner})"

		# Handle Alias expressions (e.g., COUNT(*) AS total)
		if isinstance(expr, exp.Alias):
			inner = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			alias_name = str(expr.alias).strip("`\"'")
			return f'{inner}.as_("{alias_name}")'

		# Handle aggregate functions - use pypika functions from Frappe QB
		if isinstance(expr, exp.Count):
			if isinstance(expr.this, exp.Star):
				return 'fn.Count("*")'
			else:
				inner_field = self.format_select_field(
					expr.this, table_vars, main_var, replacements, sql_params
				)
				return f"fn.Count({inner_field})"

		if isinstance(expr, exp.Avg):
			inner_field = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			return f"fn.Avg({inner_field})"

		if isinstance(expr, exp.Sum):
			inner_field = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			return f"fn.Sum({inner_field})"

		if isinstance(expr, exp.Max):
			inner_field = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			return f"fn.Max({inner_field})"

		if isinstance(expr, exp.Min):
			inner_field = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			return f"fn.Min({inner_field})"

		# Handle Date/Cast functions
		if isinstance(expr, (exp.Date, exp.TsOrDsToDate, exp.Cast)):
			inner_field = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			if isinstance(expr, exp.Cast) and hasattr(expr, "to"):
				cast_type = str(expr.to).upper()
				return f'fn.Cast({inner_field}, "{cast_type}")'
			return f"fn.Date({inner_field})"

		# Handle DateDiff function - use CustomFunction since pypika doesn't have DateDiff
		if isinstance(expr, exp.DateDiff):
			# DATEDIFF(end, start) -> difference in days
			end_field = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			start_field = self.format_select_field(
				expr.expression, table_vars, main_var, replacements, sql_params
			)
			return f"CustomFunction('DATEDIFF', ['end', 'start'])({end_field}, {start_field})"

		# Handle Coalesce/IfNull
		if isinstance(expr, exp.Coalesce):
			inner = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			if expr.expressions:
				fallback = self.format_select_field(
					expr.expressions[0], table_vars, main_var, replacements, sql_params
				)
			else:
				fallback = "0"
			return f"fn.Coalesce({inner}, {fallback})"

		# Handle CASE expressions - complex, produce QB Case syntax
		if isinstance(expr, exp.Case):
			# CASE WHEN cond1 THEN val1 WHEN cond2 THEN val2 ELSE default END
			# -> Case().when(cond1, val1).when(cond2, val2).else_(default)
			parts = ["Case()"]
			ifs = expr.args.get("ifs", [])
			for if_clause in ifs:
				condition = self.convert_condition_to_qb(
					if_clause.this, replacements, table_vars, sql_params
				)
				result_val = self.format_select_field(
					if_clause.args.get("true"), table_vars, main_var, replacements, sql_params
				)
				parts.append(f".when({condition}, {result_val})")
			default = expr.args.get("default")
			if default:
				default_val = self.format_select_field(
					default, table_vars, main_var, replacements, sql_params
				)
				parts.append(f".else_({default_val})")
			return "".join(parts)

		# Handle ABS function
		if isinstance(expr, exp.Abs):
			inner = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			return f"fn.Abs({inner})"

		# Handle arithmetic expressions (Sub, Add, Mul, Div)
		if isinstance(expr, exp.Sub):
			left = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			right = self.format_select_field(
				expr.expression, table_vars, main_var, replacements, sql_params
			)
			return f"({left} - {right})"

		if isinstance(expr, exp.Add):
			left = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			right = self.format_select_field(
				expr.expression, table_vars, main_var, replacements, sql_params
			)
			return f"({left} + {right})"

		if isinstance(expr, exp.Mul):
			left = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			right = self.format_select_field(
				expr.expression, table_vars, main_var, replacements, sql_params
			)
			return f"({left} * {right})"

		if isinstance(expr, exp.Div):
			left = self.format_select_field(
				expr.this, table_vars, main_var, replacements, sql_params
			)
			right = self.format_select_field(
				expr.expression, table_vars, main_var, replacements, sql_params
			)
			return f"({left} / {right})"

		expr_str = str(expr)
		is_distinct = "DISTINCT" in expr_str.upper()

		# Handle Column expressions - check for table qualifier
		if isinstance(expr, exp.Column):
			column_name = str(expr.this).strip("`\"'") if expr.this else ""

			# Check if there's a table qualifier
			if hasattr(expr, "table") and expr.table:
				table_ref = str(expr.table).strip("`\"'")
				if table_ref in table_vars:
					field_ref = f"{table_vars[table_ref]}.{column_name}"
				else:
					field_ref = f"{main_var}.{column_name}"
			else:
				field_ref = f"{main_var}.{column_name}"
		else:
			# Get the actual field name from other expression types
			if hasattr(expr, "this"):
				field_str = str(expr.this).strip("`\"'")
			else:
				field_str = expr_str.replace("DISTINCT", "").strip("`\"' ")

			# Handle table.field notation in string
			if "." in field_str:
				table_part, field_part = field_str.rsplit(".", 1)
				table_part = table_part.strip("`\"'")
				field_part = field_part.strip("`\"'")

				if table_part in table_vars:
					field_ref = f"{table_vars[table_part]}.{field_part}"
				else:
					field_ref = f"{main_var}.{field_part}"
			else:
				# No table specified, use main table
				field_ref = f"{main_var}.{field_str}"

		if is_distinct:
			field_ref += ".distinct()"

		return field_ref

	def format_value_or_field(
		self,
		expr: exp.Expression,
		replacements: list[tuple[str, str]],
		table_vars: dict,
		sql_params: dict = None,
	) -> str:
		"""Format a value expression, handling parameter placeholders and literal values."""
		# Handle literal values first
		if isinstance(expr, exp.Literal):
			if expr.is_string:
				return f'"{expr.this}"'
			else:
				return str(expr.this)

		expr_str = str(expr).strip("`\"'")

		# Check if this is a parameter placeholder
		for placeholder, original in replacements:
			if placeholder == expr_str:
				# Extract parameter name from %(param)s format
				param_match = re.match(r"%\((\w+)\)s", original)
				if param_match:
					param_name = param_match.group(1)
					if sql_params and param_name in sql_params:
						param_value = sql_params[param_name]
						# Check if it's a doc reference like "doc.doctype"
						if isinstance(param_value, str) and param_value.startswith("doc."):
							return param_value
						elif isinstance(param_value, str) and "." in param_value:
							# It's already a reference like "doc.something"
							return param_value
						else:
							# It's a literal value, wrap in quotes if string
							return f'"{param_value}"' if isinstance(param_value, str) else str(param_value)
					else:
						# No params dict or param not found, assume doc.param_name pattern
						return param_name
				elif original == "%s":
					# Positional parameter - extract index from placeholder name
					# Placeholder is like __PH0__, __PH1__, etc.
					ph_match = re.match(r"__PH(\d+)__", placeholder)
					if ph_match and sql_params:
						pos_index = ph_match.group(1)
						pos_key = f"__pos_{pos_index}__"
						if pos_key in sql_params:
							value = sql_params[pos_key]
							if isinstance(value, str) and ("." in value or value.isidentifier()):
								return value
							return f'"{value}"' if isinstance(value, str) else str(value)
					# Cannot resolve %s parameter - flag for manual review
					raise UnresolvedParameterError("Cannot resolve positional parameter %s")
				else:
					# Not a parameter pattern, keep original
					return original

		# Not a placeholder, format as field or literal
		return self.format_field(expr, table_vars)

	def convert_condition_to_qb(
		self,
		condition: exp.Expression,
		replacements: list[tuple[str, str]],
		table_vars: dict,
		sql_params: dict = None,
	) -> str:
		try:
			if isinstance(condition, exp.EQ):
				left = self.format_field(condition.left, table_vars)
				right = self.format_value_or_field(
					condition.right, replacements, table_vars, sql_params
				)
				return f"({left} == {right})"

			elif isinstance(condition, exp.And):
				left_cond = self.convert_condition_to_qb(
					condition.left, replacements, table_vars, sql_params
				)
				right_cond = self.convert_condition_to_qb(
					condition.right, replacements, table_vars, sql_params
				)
				# Remove extra parentheses if both sides are already wrapped
				if (
					left_cond.startswith("(")
					and left_cond.endswith(")")
					and right_cond.startswith("(")
					and right_cond.endswith(")")
				):
					return f"{left_cond} & {right_cond}"
				return f"({left_cond} & {right_cond})"

			elif isinstance(condition, exp.Or):
				left_cond = self.convert_condition_to_qb(
					condition.left, replacements, table_vars, sql_params
				)
				right_cond = self.convert_condition_to_qb(
					condition.right, replacements, table_vars, sql_params
				)
				if (
					left_cond.startswith("(")
					and left_cond.endswith(")")
					and right_cond.startswith("(")
					and right_cond.endswith(")")
				):
					return f"{left_cond} | {right_cond}"
				return f"({left_cond} | {right_cond})"

			elif isinstance(condition, exp.GT):
				left = self.format_field(condition.left, table_vars)
				right = self.format_value_or_field(
					condition.right, replacements, table_vars, sql_params
				)
				return f"({left} > {right})"

			elif isinstance(condition, exp.GTE):
				left = self.format_field(condition.left, table_vars)
				right = self.format_value_or_field(
					condition.right, replacements, table_vars, sql_params
				)
				return f"({left} >= {right})"

			elif isinstance(condition, exp.LT):
				left = self.format_field(condition.left, table_vars)
				right = self.format_value_or_field(
					condition.right, replacements, table_vars, sql_params
				)
				return f"({left} < {right})"

			elif isinstance(condition, exp.LTE):
				left = self.format_field(condition.left, table_vars)
				right = self.format_value_or_field(
					condition.right, replacements, table_vars, sql_params
				)
				return f"({left} <= {right})"

			elif isinstance(condition, exp.In):
				left = self.format_field(condition.this, table_vars)

				# Check if this is a subquery IN clause
				subquery = condition.args.get("query")
				if subquery:
					# Handle subquery - extract the inner SELECT
					inner_select = subquery.this if isinstance(subquery, exp.Subquery) else subquery
					if isinstance(inner_select, exp.Select):
						subquery_str = self.convert_subquery_to_qb(inner_select, replacements, sql_params)
						return f"{left}.isin(SubQuery({subquery_str}))"
					else:
						return "# TODO: Complex subquery"
				else:
					# Regular IN with values list
					values = [
						self.format_value_or_field(v, replacements, table_vars, sql_params)
						for v in condition.expressions
					]
					return f"{left}.isin([{', '.join(values)}])"

			elif isinstance(condition, exp.Like):
				left = self.format_field(condition.this, table_vars)
				right = self.format_value_or_field(
					condition.expression, replacements, table_vars, sql_params
				)
				return f"{left}.like({right})"

			elif isinstance(condition, exp.Between):
				# Frappe QB uses slice syntax for BETWEEN: field[start:end]
				left = self.format_field(condition.this, table_vars)
				low = self.format_value_or_field(
					condition.args["low"], replacements, table_vars, sql_params
				)
				high = self.format_value_or_field(
					condition.args["high"], replacements, table_vars, sql_params
				)
				return f"{left}[{low}:{high}]"

			elif isinstance(condition, exp.NEQ):
				left = self.format_field(condition.left, table_vars)
				right = self.format_value_or_field(
					condition.right, replacements, table_vars, sql_params
				)
				return f"({left} != {right})"

			elif isinstance(condition, exp.Paren):
				# Parenthesized expression - recursively convert the inner expression
				inner = self.convert_condition_to_qb(
					condition.this, replacements, table_vars, sql_params
				)
				return f"({inner})"

			elif isinstance(condition, exp.Not):
				# Handle NOT - check if it's wrapping IS NULL (for IS NOT NULL) or IN (for NOT IN)
				inner = condition.this
				if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
					# IS NOT NULL
					left = self.format_field(inner.this, table_vars)
					return f"{left}.isnotnull()"
				elif isinstance(inner, exp.In):
					# NOT IN - use .notin() method
					left = self.format_field(inner.this, table_vars)
					subquery = inner.args.get("query")
					if subquery:
						inner_select = subquery.this if isinstance(subquery, exp.Subquery) else subquery
						if isinstance(inner_select, exp.Select):
							subquery_str = self.convert_subquery_to_qb(
								inner_select, replacements, sql_params
							)
							return f"{left}.notin(SubQuery({subquery_str}))"
						else:
							return "# TODO: Complex NOT IN subquery"
					else:
						values = [
							self.format_value_or_field(v, replacements, table_vars, sql_params)
							for v in inner.expressions
						]
						return f"{left}.notin([{', '.join(values)}])"
				else:
					# General NOT
					inner_cond = self.convert_condition_to_qb(
						inner, replacements, table_vars, sql_params
					)
					return f"~({inner_cond})"

			elif isinstance(condition, exp.Is):
				# Handle IS NULL
				if isinstance(condition.expression, exp.Null):
					left = self.format_field(condition.this, table_vars)
					return f"{left}.isnull()"
				else:
					# Other IS comparisons (rare)
					left = self.format_field(condition.this, table_vars)
					right = self.format_field(condition.expression, table_vars)
					return f"({left} == {right})"

			elif isinstance(condition, exp.Exists):
				# Handle EXISTS subquery
				inner_select = condition.this
				if isinstance(inner_select, exp.Select):
					subquery_str = self.convert_subquery_to_qb(inner_select, replacements, sql_params)
					return f"ExistsCriterion({subquery_str})"
				else:
					return "# TODO: Complex EXISTS subquery"

			else:
				return f"# TODO: Handle {type(condition).__name__}"

		except Exception as e:
			return f"# Error converting condition: {str(e)}"

	def convert_subquery_to_qb(
		self,
		select: exp.Select,
		replacements: list[tuple[str, str]],
		sql_params: dict = None,
	) -> str:
		"""Convert a subquery SELECT to an inline Query Builder expression."""
		# Extract tables with aliases
		tables = []
		table_vars = {}

		for node in select.walk():
			if isinstance(node, exp.Table):
				if hasattr(node, "this") and node.this:
					table_name = str(node.this).strip("`\"'")
				else:
					table_name = (
						str(node.name).strip("`\"'") if hasattr(node, "name") else str(node).strip("`\"'")
					)

				alias = None
				if hasattr(node, "alias") and node.alias:
					alias = str(node.alias).strip("`\"'")

				table_key = alias if alias else table_name
				if table_key not in [t[1] if t[1] else t[0] for t in tables]:
					tables.append((table_name, alias))
					if alias:
						table_vars[alias] = alias.lower()
						table_vars[table_name] = alias.lower()
					else:
						doctype_name = (
							table_name.replace("tab", "") if table_name.startswith("tab") else table_name
						)
						var_name = doctype_name.lower().replace(" ", "_").replace("-", "_")
						table_vars[table_name] = var_name

		if not tables:
			return "# Error: No tables found in subquery"

		main_table_name, main_alias = tables[0]
		main_key = main_alias if main_alias else main_table_name
		main_var = table_vars[main_key]
		doctype_name = (
			main_table_name.replace("tab", "")
			if main_table_name.startswith("tab")
			else main_table_name
		)

		# Build the query chain
		parts = [f'frappe.qb.from_(frappe.qb.DocType("{doctype_name}"))']

		# Add SELECT fields
		if select.expressions:
			select_fields = []
			for expr in select.expressions:
				field_ref = self.format_select_field(
					expr, table_vars, main_var, replacements, sql_params
				)
				select_fields.append(field_ref)
			parts.append(f".select({', '.join(select_fields)})")

		# Add WHERE clause
		where_clause = select.find(exp.Where)
		if where_clause:
			condition = self.convert_condition_to_qb(
				where_clause.this, replacements, table_vars, sql_params
			)
			parts.append(f".where({condition})")

		return "".join(parts)

	def format_field(self, field: exp.Expression, table_vars: dict) -> str:
		# Handle literal values (strings, numbers)
		if isinstance(field, exp.Literal):
			if field.is_string:
				return f'"{field.this}"'
			else:
				# Numeric literal
				return str(field.this)

		# Handle function expressions (Date, Cast, etc.) - use the main table var
		if isinstance(field, (exp.Date, exp.TsOrDsToDate)):
			main_var = list(table_vars.values())[0] if table_vars else "doc"
			inner = self.format_field(field.this, table_vars)
			return f"fn.Date({inner})"

		if isinstance(field, exp.Cast):
			inner = self.format_field(field.this, table_vars)
			cast_type = str(field.to).upper() if hasattr(field, "to") else "VARCHAR"
			return f'fn.Cast({inner}, "{cast_type}")'

		# Handle Coalesce/IfNull
		if isinstance(field, exp.Coalesce):
			inner = self.format_field(field.this, table_vars)
			# Get the fallback value from expressions
			if field.expressions:
				fallback = self.format_field(field.expressions[0], table_vars)
			else:
				fallback = '""'
			return f"fn.Coalesce({inner}, {fallback})"

		# Handle aggregation functions
		if isinstance(field, exp.Count):
			if field.this and str(field.this) == "*":
				return 'fn.Count("*")'
			elif field.this:
				inner = self.format_field(field.this, table_vars)
				return f"fn.Count({inner})"
			return 'fn.Count("*")'

		if isinstance(field, exp.Sum):
			inner = self.format_field(field.this, table_vars) if field.this else '"*"'
			return f"fn.Sum({inner})"

		if isinstance(field, exp.Avg):
			inner = self.format_field(field.this, table_vars) if field.this else '"*"'
			return f"fn.Avg({inner})"

		if isinstance(field, exp.Max):
			inner = self.format_field(field.this, table_vars) if field.this else '"*"'
			return f"fn.Max({inner})"

		if isinstance(field, exp.Min):
			inner = self.format_field(field.this, table_vars) if field.this else '"*"'
			return f"fn.Min({inner})"

		# Handle arithmetic operations (Add, Sub, Mul, Div)
		if isinstance(field, exp.Add):
			left = self.format_field(field.left, table_vars)
			right = self.format_field(field.right, table_vars)
			return f"({left} + {right})"

		if isinstance(field, exp.Sub):
			left = self.format_field(field.left, table_vars)
			right = self.format_field(field.right, table_vars)
			return f"({left} - {right})"

		if isinstance(field, exp.Mul):
			left = self.format_field(field.left, table_vars)
			right = self.format_field(field.right, table_vars)
			return f"({left} * {right})"

		if isinstance(field, exp.Div):
			left = self.format_field(field.left, table_vars)
			right = self.format_field(field.right, table_vars)
			return f"({left} / {right})"

		# Handle Paren (parenthesized expressions)
		if isinstance(field, exp.Paren):
			inner = self.format_field(field.this, table_vars)
			return f"({inner})"

		# Handle Column expressions
		if isinstance(field, exp.Column):
			column_name = str(field.this).strip("`\"'") if field.this else ""
			if hasattr(field, "table") and field.table:
				table_ref = str(field.table).strip("`\"'")
				if table_ref in table_vars:
					return f"{table_vars[table_ref]}.{column_name}"
			if table_vars:
				main_var = list(table_vars.values())[0]
				return f"{main_var}.{column_name}"
			return f'"{column_name}"'

		field_str = str(field).strip("`\"'")

		# Handle table.column notation
		if "." in field_str:
			parts = field_str.split(".")
			if len(parts) == 2:
				table, column = parts
				table = table.strip("`\"'")
				column = column.strip("`\"'")

				if table in table_vars:
					return f"{table_vars[table]}.{column}"
				else:
					return f'"{column}"'

		# Handle parameter placeholders
		if field_str.startswith("%") and field_str.endswith("s"):
			return field_str

		# Handle already quoted strings
		if field_str.startswith("'") or field_str.startswith('"'):
			cleaned = field_str.strip("\"'")
			return f'"{cleaned}"'

		# Handle internal placeholders
		if field_str.startswith("__PH") and field_str.endswith("__"):
			return field_str

		# Check if it looks like a number
		try:
			float(field_str)
			return field_str  # Return numeric literals as-is
		except ValueError:
			pass

		# Default: treat as field reference from main table
		if table_vars:
			main_table_var = list(table_vars.values())[0]
			return f"{main_table_var}.{field_str}"

		return f'"{field_str}"'

	def convert_insert_to_qb(
		self, insert: exp.Insert, replacements: list[tuple[str, str]]
	) -> str:
		table = str(insert.this).strip("`\"'")
		return f"""# INSERT operations typically use frappe.get_doc() in Frappe:
doc = frappe.get_doc({{
	'doctype': '{table}',
	# Add field mappings here
}})
doc.insert()"""

	def convert_update_to_qb(
		self, update: exp.Update, replacements: list[tuple[str, str]], sql_params: dict = None
	) -> str:
		"""Convert UPDATE statement to frappe.db.set_value() or Query Builder.

		UPDATE tabX SET field = value WHERE name = %s
		        -> frappe.db.set_value("X", name_var, "field", value)
		UPDATE tabX SET f1 = v1, f2 = v2 WHERE name = %s
		        -> frappe.db.set_value("X", name_var, {"f1": v1, "f2": v2})
		UPDATE with complex WHERE -> Query Builder
		"""
		# Extract table name
		table = update.this
		if not table:
			return "# MANUAL: UPDATE - Could not determine table"

		table_name = str(table).strip("`\"'")

		# Check for multi-table UPDATE (MySQL-specific, not supported)
		if "," in table_name or " AS " in table_name.upper():
			return "# MANUAL: Multi-table UPDATE - requires manual conversion"
		if table_name.startswith("tab"):
			doctype = table_name[3:]
		else:
			doctype = table_name

		# Extract SET assignments
		set_fields = {}
		for expr in update.expressions:
			if isinstance(expr, exp.EQ):
				field_name = str(expr.left).strip("`\"'")
				value_expr = expr.right

				# Resolve the value
				value_str = str(value_expr).strip()
				resolved_value = self._resolve_update_value(value_str, replacements, sql_params)
				set_fields[field_name] = resolved_value

		if not set_fields:
			return "# MANUAL: UPDATE - Could not extract SET fields"

		# Extract WHERE clause
		where_clause = update.find(exp.Where)

		if not where_clause:
			# UPDATE without WHERE - dangerous, flag for manual review
			return f"# MANUAL: UPDATE without WHERE on {doctype} - needs manual review"

		# Check if WHERE is simple "name = value" pattern
		where_cond = where_clause.this
		is_name_filter, name_value = self._is_simple_name_filter(
			where_cond, replacements, sql_params
		)

		if is_name_filter and name_value:
			# Use frappe.db.set_value with name
			if len(set_fields) == 1:
				field, value = list(set_fields.items())[0]
				return f'frappe.db.set_value("{doctype}", {name_value}, "{field}", {value})'
			else:
				# Multiple fields - use dict
				fields_dict = self._format_update_fields_dict(set_fields)
				return f'frappe.db.set_value("{doctype}", {name_value}, {fields_dict})'

		# Complex WHERE - try Query Builder
		try:
			var_name = doctype.replace(" ", "").replace("-", "")
			table_vars = {table_name: var_name}
			condition = self.convert_condition_to_qb(
				where_cond, replacements, table_vars, sql_params
			)

			# Check for unresolved placeholders
			if "__PH" in condition or "%s" in condition:
				raise UnresolvedParameterError("Unresolved parameter in WHERE")

			lines = [f'{var_name} = frappe.qb.DocType("{doctype}")']

			# Build the update chain
			update_parts = [f"frappe.qb.update({var_name})"]
			for field, value in set_fields.items():
				update_parts.append(f".set({var_name}.{field}, {value})")
			update_parts.append(f".where({condition})")
			update_parts.append(".run()")

			lines.append("".join(update_parts))
			return "\n".join(lines)

		except (UnresolvedParameterError, Exception) as e:
			# Fall back to manual review with helpful template
			var_name = doctype.replace(" ", "").replace("-", "")
			return f'# MANUAL: UPDATE with complex WHERE on {doctype}\n# frappe.db.set_value("{doctype}", filters, fields_dict) or use Query Builder'

	def _resolve_update_value(
		self, value_str: str, replacements: list, sql_params: dict
	) -> str:
		"""Resolve a value from UPDATE SET clause."""
		# Check if it's a placeholder
		for placeholder, original in replacements:
			if placeholder in value_str:
				# Try to resolve the parameter
				if original.startswith("%(") and original.endswith(")s"):
					param_name = original[2:-2]
					if sql_params and param_name in sql_params:
						resolved = sql_params[param_name]
						if isinstance(resolved, str) and ("." in resolved or resolved.isidentifier()):
							return resolved
						return f'"{resolved}"' if isinstance(resolved, str) else str(resolved)
					return param_name  # Return param name as variable
				elif original == "%s":
					# Positional parameter
					ph_match = re.match(r"__PH(\d+)__", placeholder)
					if ph_match and sql_params:
						pos_key = f"__pos_{ph_match.group(1)}__"
						if pos_key in sql_params:
							resolved = sql_params[pos_key]
							if isinstance(resolved, str) and ("." in resolved or resolved.isidentifier()):
								return resolved
							return f'"{resolved}"' if isinstance(resolved, str) else str(resolved)
					raise UnresolvedParameterError("Cannot resolve positional parameter")

		# Literal value
		if value_str.isdigit():
			return value_str
		elif value_str.upper() in ("NULL", "TRUE", "FALSE"):
			return value_str.capitalize() if value_str.upper() != "NULL" else "None"
		elif value_str.startswith("'") and value_str.endswith("'"):
			return f'"{value_str[1:-1]}"'
		else:
			return f'"{value_str}"'

	def _is_simple_name_filter(
		self, where_cond, replacements: list, sql_params: dict
	) -> tuple[bool, str | None]:
		"""Check if WHERE is simple 'name = value' pattern."""
		if isinstance(where_cond, exp.EQ):
			left = str(where_cond.left).strip("`\"'").lower()
			if left == "name":
				right_str = str(where_cond.right).strip()
				# Try to resolve the value
				for placeholder, original in replacements:
					if placeholder in right_str:
						if original.startswith("%(") and original.endswith(")s"):
							param_name = original[2:-2]
							if sql_params and param_name in sql_params:
								return True, sql_params[param_name]
							return True, param_name
						elif original == "%s":
							ph_match = re.match(r"__PH(\d+)__", placeholder)
							if ph_match and sql_params:
								pos_key = f"__pos_{ph_match.group(1)}__"
								if pos_key in sql_params:
									return True, sql_params[pos_key]
							raise UnresolvedParameterError("Cannot resolve name parameter")
				# Literal value
				if right_str.startswith("'") and right_str.endswith("'"):
					return True, f'"{right_str[1:-1]}"'
				return True, right_str
		return False, None

	def _format_update_fields_dict(self, fields: dict) -> str:
		"""Format fields dict for frappe.db.set_value."""
		items = []
		for key, value in fields.items():
			items.append(f'"{key}": {value}')
		return "{" + ", ".join(items) + "}"

	def convert_delete_to_qb(
		self, delete: exp.Delete, replacements: list[tuple[str, str]], sql_params: dict = None
	) -> str:
		"""Convert DELETE statement to frappe.db.delete() or Query Builder.

		Simple DELETE FROM `tabX` -> frappe.db.delete("X")
		DELETE with WHERE -> frappe.db.delete("X", filters) or Query Builder
		"""
		# Extract table name
		table = delete.this
		if not table:
			return "# MANUAL: DELETE - Could not determine table"

		table_name = table.name
		# Remove 'tab' prefix if present
		if table_name.startswith("tab"):
			doctype = table_name[3:]
		else:
			doctype = table_name

		# Check for WHERE clause
		where_clause = delete.args.get("where")

		if not where_clause:
			# Simple DELETE without WHERE - use frappe.db.delete()
			return f'frappe.db.delete("{doctype}")'

		# Try to convert WHERE to filters for frappe.db.delete()
		try:
			filters = self.convert_where_to_filters(where_clause.this, replacements, sql_params)
			if filters and not any(
				isinstance(v, str) and v.startswith("__PH") for v in filters.values()
			):
				# Successfully converted to simple filters
				filter_str = self._format_filters_dict(filters)
				return f'frappe.db.delete("{doctype}", {filter_str})'
		except Exception:
			pass

		# Try Query Builder approach for more complex WHERE
		try:
			var_name = doctype.replace(" ", "").replace("-", "")
			table_vars = {table_name: var_name}
			condition = self.convert_condition_to_qb(
				where_clause.this, replacements, table_vars, sql_params
			)
			# Check if condition contains unresolved placeholders
			if "__PH" not in condition and "%s" not in condition:
				lines = [
					f'{var_name} = frappe.qb.DocType("{doctype}")',
					f"frappe.qb.from_({var_name}).delete().where({condition}).run()",
				]
				return "\n".join(lines)
		except (UnresolvedParameterError, Exception):
			pass

		# Fall back to manual review
		return f'# MANUAL: DELETE with WHERE on {doctype} - convert to frappe.db.delete("{doctype}", filters) or Query Builder'

	def _format_filters_dict(self, filters: dict) -> str:
		"""Format a filters dict as a Python dict literal."""
		items = []
		for key, value in filters.items():
			if isinstance(value, str):
				# Check if it's a variable reference (like doc.name, self.name, etc.)
				if "." in value or value.isidentifier():
					items.append(f'"{key}": {value}')
				else:
					items.append(f'"{key}": "{value}"')
			elif isinstance(value, (int, float)):
				items.append(f'"{key}": {value}')
			elif isinstance(value, list):
				# List of values for IN clause
				items.append(f'"{key}": {value}')
			else:
				items.append(f'"{key}": {repr(value)}')
		return "{" + ", ".join(items) + "}"

	def scan_directory(self, directory: Path, pattern: str = "**/*.py") -> int:
		count = 0
		for file_path in directory.glob(pattern):
			if file_path.is_file():
				count += self.scan_file(file_path)
		return count

	def scan_file(self, file_path: Path) -> int:
		count = 0
		# Track SQL queries we've already registered from frappe.db.sql calls
		# to avoid duplicate registration from docstring scanning
		registered_sql_hashes = set()

		try:
			content = file_path.read_text(encoding="utf-8")
			tree = ast.parse(content)

			for node in ast.walk(tree):
				if isinstance(node, ast.Call):
					if self.is_frappe_db_sql_call(node):
						sql_query = self.extract_sql_from_call(node)
						if sql_query:
							sql_params = self.extract_params_from_call(node)
							sql_kwargs = self.extract_kwargs_from_call(node)
							variable_name = self.extract_variable_name(tree, node)
							function_context = self.get_function_context(tree, node)

							self.register_sql_call(
								str(file_path),
								node.lineno,
								sql_query,
								function_context,
								sql_params,
								sql_kwargs,
								variable_name,
							)
							count += 1
							# Track this SQL to avoid duplicate from docstring scan
							sql_hash = hashlib.md5(sql_query.encode()).hexdigest()
							registered_sql_hashes.add(sql_hash)

			# Skip docstring scanning - it creates duplicates and the AST-based
			# detection is more accurate for frappe.db.sql calls
			# sql_strings = self.find_sql_docstrings(content)
			# for line_num, sql_query in sql_strings:
			# 	sql_hash = hashlib.md5(sql_query.encode()).hexdigest()
			# 	if sql_hash not in registered_sql_hashes:
			# 		function_context = f"docstring at line {line_num}"
			# 		self.register_sql_call(str(file_path), line_num, sql_query, function_context)
			# 		count += 1

		except Exception as e:
			print(f"Error scanning {file_path}: {e}")

		return count

	def is_frappe_db_sql_call(self, node: ast.Call) -> bool:
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

	def extract_sql_from_call(self, node: ast.Call) -> str | None:
		try:
			if node.args and len(node.args) > 0:
				first_arg = node.args[0]
				if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
					return first_arg.value
		except Exception:
			pass
		return None

	def extract_params_from_call(self, node: ast.Call) -> dict | None:
		"""Extract parameters from frappe.db.sql call (second argument)"""
		try:
			if len(node.args) > 1:
				param_node = node.args[1]

				if isinstance(param_node, ast.Dict):
					# Dictionary literal like {"user": user}
					params = {}
					for key, value in zip(param_node.keys, param_node.values):
						if isinstance(key, ast.Constant):
							key_str = key.value
							if isinstance(value, ast.Attribute):
								# Handle doc.doctype style
								if isinstance(value.value, ast.Name):
									params[key_str] = f"{value.value.id}.{value.attr}"
							elif isinstance(value, ast.Constant):
								params[key_str] = value.value
							elif isinstance(value, ast.Name):
								# Variable reference like user
								params[key_str] = value.id
							else:
								params[key_str] = ast.unparse(value) if hasattr(ast, "unparse") else str(value)
					return params

				elif isinstance(param_node, ast.Name):
					# Single variable passed like: frappe.db.sql(sql, filters)
					# The variable is a dict containing all named parameters
					sql_query = self.extract_sql_from_call(node)
					if sql_query:
						param_matches = re.findall(r"%\((\w+)\)s", sql_query)
						if param_matches:
							# Map ALL named parameters to the variable
							# At runtime, filters[param_name] will be used
							return {param: param_node.id for param in param_matches}
						# Check for %s positional parameter
						if "%s" in sql_query and "%" not in sql_query.replace("%s", ""):
							return {"__pos_0__": param_node.id}

					# Fallback: store as variable reference
					return {"__var_ref__": param_node.id}

				elif isinstance(param_node, ast.Attribute):
					# Single attribute access like: frappe.db.sql(sql, d.sales_order)
					if isinstance(param_node.value, ast.Name):
						attr_value = f"{param_node.value.id}.{param_node.attr}"
					else:
						attr_value = (
							ast.unparse(param_node) if hasattr(ast, "unparse") else str(param_node)
						)

					sql_query = self.extract_sql_from_call(node)
					if sql_query:
						param_matches = re.findall(r"%\((\w+)\)s", sql_query)
						if param_matches:
							# Map ALL named parameters to the attribute
							return {param: attr_value for param in param_matches}
						# Check for %s positional parameter
						if "%s" in sql_query:
							return {"__pos_0__": attr_value}

					return {"__var_ref__": attr_value}

				elif isinstance(param_node, ast.Tuple) or isinstance(param_node, ast.List):
					# Positional parameters like (value1, value2)
					# Map to %s placeholders in order
					sql_query = self.extract_sql_from_call(node)
					if sql_query:
						placeholders = re.findall(r"(?<!%)%s", sql_query)
						params = {}
						elements = param_node.elts if hasattr(param_node, "elts") else []
						for i, (_, elem) in enumerate(zip(placeholders, elements)):
							param_key = f"__pos_{i}__"
							if isinstance(elem, ast.Name):
								params[param_key] = elem.id
							elif isinstance(elem, ast.Constant):
								params[param_key] = elem.value
							elif isinstance(elem, ast.Attribute):
								# Use ast.unparse for any attribute, handles complex chains
								if hasattr(ast, "unparse"):
									params[param_key] = ast.unparse(elem)
								elif isinstance(elem.value, ast.Name):
									params[param_key] = f"{elem.value.id}.{elem.attr}"
								else:
									params[param_key] = str(elem)
							elif isinstance(elem, ast.Call):
								# Function call like nowdate()
								if hasattr(ast, "unparse"):
									params[param_key] = ast.unparse(elem)
								else:
									params[param_key] = "__call__"
							else:
								params[param_key] = ast.unparse(elem) if hasattr(ast, "unparse") else str(elem)
						return params if params else None

				elif isinstance(param_node, ast.Call):
					# Function call like dict(key=value, ...) or nowdate()
					func = param_node.func
					func_name = ""
					if isinstance(func, ast.Name):
						func_name = func.id
					elif isinstance(func, ast.Attribute):
						func_name = func.attr

					if func_name == "dict" and param_node.keywords:
						# dict(item_name=self.item_name, ...) - extract keyword args
						params = {}
						for kw in param_node.keywords:
							if kw.arg:
								if hasattr(ast, "unparse"):
									params[kw.arg] = ast.unparse(kw.value)
								elif isinstance(kw.value, ast.Attribute):
									if isinstance(kw.value.value, ast.Name):
										params[kw.arg] = f"{kw.value.value.id}.{kw.value.attr}"
								elif isinstance(kw.value, ast.Name):
									params[kw.arg] = kw.value.id
						return params if params else None
					else:
						# Other function call like nowdate() - check for params in SQL
						sql_query = self.extract_sql_from_call(node)
						if sql_query:
							# Check named params first
							param_matches = re.findall(r"%\((\w+)\)s", sql_query)
							if param_matches:
								return {param: f"__from_{func_name}__" for param in param_matches}
							# Check for single positional param (function returns one value)
							positional_count = len(re.findall(r"(?<!%)%s", sql_query))
							if positional_count == 1:
								func_repr = (
									ast.unparse(param_node) if hasattr(ast, "unparse") else func_name + "()"
								)
								return {"__pos_0__": func_repr}

		except Exception as e:
			print(f"Error extracting params: {e}")

		return None

	def extract_kwargs_from_call(self, node: ast.Call) -> dict | None:
		try:
			if node.keywords:
				kwargs = {}
				for keyword in node.keywords:
					if keyword.arg:
						if isinstance(keyword.value, ast.Constant):
							kwargs[keyword.arg] = keyword.value.value
						elif isinstance(keyword.value, ast.Name):
							kwargs[keyword.arg] = keyword.value.id
						else:
							kwargs[keyword.arg] = (
								ast.unparse(keyword.value) if hasattr(ast, "unparse") else str(keyword.value)
							)
				return kwargs
		except Exception as e:
			print(f"Error extracting kwargs: {e}")

		return None

	def extract_variable_name(self, tree: ast.AST, call_node: ast.Call) -> str | None:
		"""Extract the variable name that the SQL result is assigned to, or detect if it's returned directly"""

		# First check if it's part of an assignment
		for node in ast.walk(tree):
			if isinstance(node, ast.Assign):
				# Check if this assignment contains our call
				if self.node_contains(node.value, call_node):
					# Get the target variable name
					if node.targets and isinstance(node.targets[0], ast.Name):
						return node.targets[0].id
					elif node.targets and isinstance(node.targets[0], ast.Attribute):
						# Handle self.var = frappe.db.sql(...)
						return f"self.{node.targets[0].attr}"

		# Check if it's part of a return statement
		for node in ast.walk(tree):
			if isinstance(node, ast.Return):
				# Check if the return value contains our call
				if node.value and self.node_contains(node.value, call_node):
					# Mark this as a direct return
					return "__return__"

		# Check if it's part of a yield statement
		for node in ast.walk(tree):
			if isinstance(node, ast.Yield):
				if node.value and self.node_contains(node.value, call_node):
					return "__yield__"

		# Check if it's part of an expression statement (called but not assigned)
		for node in ast.walk(tree):
			if isinstance(node, ast.Expr):
				if self.node_contains(node.value, call_node):
					return "__expr__"

		return None

	def get_function_context(self, tree: ast.AST, target_node: ast.AST) -> str:
		for node in ast.walk(tree):
			if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
				if self.node_contains(node, target_node):
					return f"def {node.name}():"
		return "global scope"

	def node_contains(self, parent: ast.AST, child: ast.AST) -> bool:
		if parent is child:
			return True
		for node in ast.walk(parent):
			if node is child:
				return True
		return False

	def find_sql_docstrings(self, content: str) -> list[tuple[int, str]]:
		sql_strings = []
		try:
			tree = ast.parse(content)
			for node in ast.walk(tree):
				if isinstance(node, ast.Constant) and isinstance(node.value, str):
					if self.is_likely_sql(node.value):
						sql_strings.append((node.lineno, node.value))
		except Exception:
			pass
		return sql_strings

	def is_likely_sql(self, text: str) -> bool:
		sql_keywords = {"SELECT", "FROM", "WHERE", "JOIN", "INSERT", "UPDATE", "DELETE"}
		text_upper = text.upper()
		keyword_count = sum(1 for keyword in sql_keywords if keyword in text_upper)
		return keyword_count >= 2 and any(char in text for char in ";(),")

	def generate_report(self) -> str:
		metadata = self.data["metadata"]
		calls = self.data["calls"]

		total = len(calls)
		by_type = {}
		orm_count = 0
		todo_count = 0

		for call in calls.values():
			impl_type = call.implementation_type
			by_type[impl_type] = by_type.get(impl_type, 0) + 1
			if call.query_builder_equivalent:
				if "frappe.get_all(" in call.query_builder_equivalent:
					orm_count += 1
				if "# TODO" in call.query_builder_equivalent:
					todo_count += 1

		report = f"""# SQL Operations Registry Report

**Repository**: {metadata.get('repository', 'N/A')}
**Last Updated**: {metadata.get('last_scan', 'Never')}
**Commit**: {metadata.get('commit_hash', 'N/A')}
**Total SQL Operations**: {total}

## Conversion Status
| Status | Count | Percentage |
|--------|-------|------------|
| ✅ Query Builder | {total - orm_count - todo_count} | {((total - orm_count - todo_count) / max(total, 1) * 100):.1f}% |
| 💡 ORM-eligible | {orm_count} | {(orm_count / max(total, 1) * 100):.1f}% |
| ⚠️ Needs Review | {todo_count} | {(todo_count / max(total, 1) * 100):.1f}% |

## Implementation Distribution
| Type | Count | Percentage |
|------|-------|------------|
| frappe_db_sql | {by_type.get('frappe_db_sql', 0)} | {(by_type.get('frappe_db_sql', 0) / max(total, 1) * 100):.1f}% |
| query_builder | {by_type.get('query_builder', 0)} | {(by_type.get('query_builder', 0) / max(total, 1) * 100):.1f}% |
| mixed | {by_type.get('mixed', 0)} | {(by_type.get('mixed', 0) / max(total, 1) * 100):.1f}% |

## Operations by File
"""

		by_file = {}
		for call in calls.values():
			file_path = call.file_path
			if file_path not in by_file:
				by_file[file_path] = []
			by_file[file_path].append(call)

		sorted_files = sorted(by_file.items(), key=lambda x: len(x[1]), reverse=True)

		for file_path, file_calls in sorted_files:
			total_file = len(file_calls)
			file_name = Path(file_path).name

			try:
				relative_path = Path(file_path).relative_to(Path.cwd())
			except Exception:
				relative_path = file_path

			report += f"\n### {file_name} ({total_file} operations)\n"
			report += f"**Path**: `{relative_path}`\n\n"

			sorted_calls = sorted(file_calls, key=lambda x: x.line_number)

			report += "| Call ID | Status | Line | Function | SQL Preview |\n"
			report += "|---------|--------|------|----------|-------------|\n"

			for call in sorted_calls:
				# Determine status
				status = "✅"
				if call.query_builder_equivalent:
					if "frappe.get_all(" in call.query_builder_equivalent:
						status = "💡"
					elif "# TODO" in call.query_builder_equivalent:
						status = "⚠️"

				sql_preview = call.sql_query.replace("\n", " ").strip()[:50]
				if len(call.sql_query) > 50:
					sql_preview += "..."
				sql_preview = sql_preview.replace("|", "\\|")
				func_name = call.function_context[:25] if call.function_context else ""
				report += f"| `{call.call_id[:8]}` | {status} | {call.line_number} | {func_name} | {sql_preview} |\n"

			report += "\n"

		report += f"""
## Summary
- **Files with SQL Operations**: {len(by_file)}
- **Total Operations Tracked**: {total}
- **Unique Query Patterns**: {len(set(c.semantic_signature for c in calls.values()))}
"""

		return report


def main():
	parser = argparse.ArgumentParser(description="SQL Operations Registry")

	subparsers = parser.add_subparsers(dest="command", help="Available commands")

	scan_parser = subparsers.add_parser("scan", help="Scan directory for SQL operations")
	scan_parser.add_argument("--directory", default=".", help="Directory to scan")
	scan_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)

	report_parser = subparsers.add_parser("report", help="Generate usage report")
	report_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	report_parser.add_argument("--output", help="Output file for report")

	list_parser = subparsers.add_parser("list", help="List SQL calls")
	list_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	list_parser.add_argument("--file-filter", help="Filter by file path")

	show_parser = subparsers.add_parser("show", help="Show details for specific call")
	show_parser.add_argument("call_id", help="Call ID to show details for")
	show_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)

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

	todos_parser = subparsers.add_parser(
		"todos", help="List calls with TODO in conversion"
	)
	todos_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)

	orm_parser = subparsers.add_parser(
		"orm", help="List calls converted to simple ORM (frappe.get_all)"
	)
	orm_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
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

		print(f"\nFound {len(filtered_calls)} SQL calls:")
		print("=" * 80)

		for call in sorted(filtered_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			print(f"\n{call.call_id[:8]}  {file_name}:{call.line_number}")
			print(f"   Function: {call.function_context}")
			print(f"   SQL: {call.sql_query[:100]}{'...' if len(call.sql_query) > 100 else ''}")

	elif args.command == "show":
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
		print(f"\nSQL Call Details: {call.call_id}")
		print("=" * 60)
		print(f"File: {call.file_path}")
		print(f"Line: {call.line_number}")
		print(f"Function: {call.function_context}")
		print(f"Variable: {call.variable_name or 'None'}")
		print(f"Implementation: {call.implementation_type}")
		print(f"Created: {call.created_at}")
		print(f"Updated: {call.updated_at}")

		if call.sql_params:
			print("\nSQL Parameters:")
			print("-" * 40)
			for key, value in call.sql_params.items():
				print(f"  {key}: {value}")

		if call.sql_kwargs:
			print("\nSQL Kwargs:")
			print("-" * 40)
			for key, value in call.sql_kwargs.items():
				print(f"  {key}: {value}")

		print("\nOriginal SQL:")
		print("-" * 40)
		print(call.sql_query)

		# Check if this is ORM-eligible
		is_orm = "frappe.get_all(" in call.query_builder_equivalent
		has_todo = "# TODO" in call.query_builder_equivalent

		print("\nQuery Builder Equivalent:")
		print("-" * 40)
		if is_orm:
			print("💡 [ORM-ELIGIBLE] Can use frappe.get_all instead of frappe.db.sql")
		if has_todo:
			print("⚠️  [HAS TODO] Needs manual review")
		print(call.query_builder_equivalent)

		if call.notes:
			print("\nNotes:")
			print("-" * 40)
			print(call.notes)

	elif args.command == "rewrite":
		from test_utils.pre_commit.sql_rewriter_functions import SQLRewriter

		rewriter = SQLRewriter(args.registry)
		dry_run = not args.apply

		success = rewriter.rewrite_sql(args.call_id, dry_run)
		if not success:
			sys.exit(1)

	elif args.command == "todos":
		calls = registry.data["calls"]
		if not calls:
			print("No SQL calls found in registry.")
			return

		todo_calls = []
		for call in calls.values():
			if call.query_builder_equivalent and "# TODO" in call.query_builder_equivalent:
				todo_calls.append(call)

		if not todo_calls:
			print("✅ No TODOs found - all conversions complete!")
			return

		print(f"\n⚠️  Found {len(todo_calls)} calls with TODOs:")
		print("=" * 80)

		for call in sorted(todo_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			print(f"\n{call.call_id[:8]}  {file_name}:{call.line_number}")
			print(f"   Function: {call.function_context}")
			# Extract the TODO line(s)
			for line in call.query_builder_equivalent.split("\n"):
				if "# TODO" in line:
					print(f"   {line.strip()}")

	elif args.command == "orm":
		calls = registry.data["calls"]
		if not calls:
			print("No SQL calls found in registry.")
			return

		orm_calls = []
		for call in calls.values():
			if (
				call.query_builder_equivalent and "frappe.get_all(" in call.query_builder_equivalent
			):
				orm_calls.append(call)

		if not orm_calls:
			print("No ORM-eligible calls found.")
			return

		print(f"\n🔄 Found {len(orm_calls)} calls that can use simple ORM (frappe.get_all):")
		print("=" * 80)
		print(
			"These are simple queries that could be refactored to use frappe.get_all/get_value"
		)
		print("instead of frappe.db.sql - often quick wins!\n")

		for call in sorted(orm_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			print(f"{call.call_id[:8]}  {file_name}:{call.line_number}")
			print(f"   Function: {call.function_context}")
			print(f"   SQL: {call.sql_query[:80]}{'...' if len(call.sql_query) > 80 else ''}")


if __name__ == "__main__":
	main()
