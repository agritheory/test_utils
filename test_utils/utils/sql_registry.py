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

		ast_str, semantic_sig, qb_equivalent = self.analyze_sql(
			sql_query, sql_params, variable_name
		)

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
			notes=None,
			created_at=datetime.now(),
			updated_at=datetime.now(),
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
				return self.convert_update_to_qb(ast_object, replacements)
			elif isinstance(ast_object, exp.Delete):
				return self.convert_delete_to_qb(ast_object, replacements)
			else:
				return f"# Unsupported query type: {type(ast_object).__name__}"
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

	def convert_select_to_orm(
		self,
		select: exp.Select,
		replacements: list[tuple[str, str]],
		sql_params: dict = None,
		variable_name: str = None,
	) -> str:
		"""Convert a simple SELECT to Frappe ORM calls"""
		lines = []

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

		table_name = str(from_clause.this).strip("`\"'")
		# Convert 'tabDocType' to 'DocType'
		doctype = (
			table_name.replace("tab", "") if table_name.startswith("tab") else table_name
		)

		# Extract fields
		fields = []
		is_single_field = False
		has_distinct = False

		if select.expressions:
			for expr in select.expressions:
				if isinstance(expr, exp.Star):
					fields = None  # Get all fields
					break
				else:
					expr_str = str(expr)
					if "DISTINCT" in expr_str.upper():
						has_distinct = True
						field_name = expr_str.replace("DISTINCT", "").strip("`\"' ")
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
			lines.append(f"\tfilters={self.format_filters_for_orm(filters)},")

		# Add fields if specified (not *)
		if fields:
			if is_single_field:
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

		# Handle DISTINCT on single field (use pluck)
		if is_single_field and has_distinct:
			lines.append(f'\tpluck="{fields[0]}"')

		# Close the function call
		if lines[-1].endswith(","):
			lines[-1] = lines[-1][:-1]  # Remove trailing comma
		lines.append(")")

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

	def format_filters_for_orm(self, filters) -> str:
		"""Format filters for Frappe ORM calls"""
		if isinstance(filters, dict):
			if len(filters) == 1 and "name" in filters:
				# Single name filter
				return filters["name"] if isinstance(filters["name"], str) else str(filters["name"])
			else:
				# Dictionary filters
				items = []
				for key, value in filters.items():
					if isinstance(value, str) and not value.startswith("doc."):
						items.append(f'"{key}": "{value}"')
					else:
						items.append(f'"{key}": {value}')
				return "{" + ", ".join(items) + "}"
		elif isinstance(filters, list):
			# List format filters
			return repr(filters)
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
				(exp.Count, exp.Avg, exp.Sum, exp.Max, exp.Min, exp.Date, exp.Cast, exp.Coalesce),
			)
			for expr in select.walk()
		)
		if needs_fn_import:
			lines.append("from frappe.query_builder import functions as fn")
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
					field_ref = self.format_select_field(expr, table_vars, main_var)
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

	def format_select_field(self, expr, table_vars: dict, main_var: str) -> str:
		if isinstance(expr, exp.Star):
			return "'*'"

		# Handle literal values (numbers, strings)
		if isinstance(expr, exp.Literal):
			if expr.is_string:
				return f'"{expr.this}"'
			else:
				return str(expr.this)

		# Handle parenthesized expressions
		if isinstance(expr, exp.Paren):
			inner = self.format_select_field(expr.this, table_vars, main_var)
			return f"({inner})"

		# Handle Alias expressions (e.g., COUNT(*) AS total)
		if isinstance(expr, exp.Alias):
			inner = self.format_select_field(expr.this, table_vars, main_var)
			alias_name = str(expr.alias).strip("`\"'")
			return f'{inner}.as_("{alias_name}")'

		# Handle aggregate functions - use pypika functions from Frappe QB
		if isinstance(expr, exp.Count):
			if isinstance(expr.this, exp.Star):
				return 'fn.Count("*")'
			else:
				inner_field = self.format_select_field(expr.this, table_vars, main_var)
				return f"fn.Count({inner_field})"

		if isinstance(expr, exp.Avg):
			inner_field = self.format_select_field(expr.this, table_vars, main_var)
			return f"fn.Avg({inner_field})"

		if isinstance(expr, exp.Sum):
			inner_field = self.format_select_field(expr.this, table_vars, main_var)
			return f"fn.Sum({inner_field})"

		if isinstance(expr, exp.Max):
			inner_field = self.format_select_field(expr.this, table_vars, main_var)
			return f"fn.Max({inner_field})"

		if isinstance(expr, exp.Min):
			inner_field = self.format_select_field(expr.this, table_vars, main_var)
			return f"fn.Min({inner_field})"

		# Handle Date/Cast functions
		if isinstance(expr, (exp.Date, exp.TsOrDsToDate, exp.Cast)):
			inner_field = self.format_select_field(expr.this, table_vars, main_var)
			if isinstance(expr, exp.Cast) and hasattr(expr, "to"):
				cast_type = str(expr.to).upper()
				return f'fn.Cast({inner_field}, "{cast_type}")'
			return f"fn.Date({inner_field})"

		# Handle arithmetic expressions (Sub, Add, Mul, Div)
		if isinstance(expr, exp.Sub):
			left = self.format_select_field(expr.this, table_vars, main_var)
			right = self.format_select_field(expr.expression, table_vars, main_var)
			return f"({left} - {right})"

		if isinstance(expr, exp.Add):
			left = self.format_select_field(expr.this, table_vars, main_var)
			right = self.format_select_field(expr.expression, table_vars, main_var)
			return f"({left} + {right})"

		if isinstance(expr, exp.Mul):
			left = self.format_select_field(expr.this, table_vars, main_var)
			right = self.format_select_field(expr.expression, table_vars, main_var)
			return f"({left} * {right})"

		if isinstance(expr, exp.Div):
			left = self.format_select_field(expr.this, table_vars, main_var)
			right = self.format_select_field(expr.expression, table_vars, main_var)
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
					return original
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
					# NOT IN - use .notin() method  # codespell:ignore notin
					left = self.format_field(inner.this, table_vars)
					subquery = inner.args.get("query")
					if subquery:
						inner_select = subquery.this if isinstance(subquery, exp.Subquery) else subquery
						if isinstance(inner_select, exp.Select):
							subquery_str = self.convert_subquery_to_qb(
								inner_select, replacements, sql_params
							)
							return f"{left}.notin(SubQuery({subquery_str}))"  # codespell:ignore notin
						else:
							return "# TODO: Complex NOT IN subquery"
					else:
						values = [
							self.format_value_or_field(v, replacements, table_vars, sql_params)
							for v in inner.expressions
						]
						return f"{left}.notin([{', '.join(values)}])"  # codespell:ignore notin
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
				field_ref = self.format_select_field(expr, table_vars, main_var)
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
		self, update: exp.Update, replacements: list[tuple[str, str]]
	) -> str:
		return "# UPDATE: Use frappe.db.set_value() or doc.save() for single records"

	def convert_delete_to_qb(
		self, delete: exp.Delete, replacements: list[tuple[str, str]]
	) -> str:
		return "# DELETE: Use frappe.delete_doc() for single records or qb.delete() for bulk"

	def scan_directory(self, directory: Path, pattern: str = "**/*.py") -> int:
		count = 0
		for file_path in directory.glob(pattern):
			if file_path.is_file():
				count += self.scan_file(file_path)
		return count

	def scan_file(self, file_path: Path) -> int:
		count = 0

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

			sql_strings = self.find_sql_docstrings(content)
			for line_num, sql_query in sql_strings:
				function_context = f"docstring at line {line_num}"
				self.register_sql_call(str(file_path), line_num, sql_query, function_context)
				count += 1

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
					# Single variable passed like: frappe.db.sql(sql, user)
					# We need to infer the parameter name from the SQL
					# Look for %(param_name)s in the SQL
					sql_query = self.extract_sql_from_call(node)
					if sql_query:
						param_matches = re.findall(r"%\((\w+)\)s", sql_query)
						if param_matches:
							# Assume the variable maps to the first parameter found
							# Return a dict mapping param name to variable name
							return {param_matches[0]: param_node.id}
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
							return {param_matches[0]: attr_value}
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
								if isinstance(elem.value, ast.Name):
									params[param_key] = f"{elem.value.id}.{elem.attr}"
							else:
								params[param_key] = ast.unparse(elem) if hasattr(ast, "unparse") else str(elem)
						return params if params else None

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

		for call in calls.values():
			impl_type = call.implementation_type
			by_type[impl_type] = by_type.get(impl_type, 0) + 1

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

			report += "| Call ID | Line | Function | SQL Preview |\n"
			report += "|---------|------|----------|-------------|\n"

			for call in sorted_calls:
				sql_preview = call.sql_query.replace("\n", " ").strip()[:60]
				if len(call.sql_query) > 60:
					sql_preview += "..."
				sql_preview = sql_preview.replace("|", "\\|")
				report += f"| `{call.call_id[:8]}` | {call.line_number} | {call.function_context[:30]} | {sql_preview} |\n"

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

		print("\nQuery Builder Equivalent:")
		print("-" * 40)
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


if __name__ == "__main__":
	main()
