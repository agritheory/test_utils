"""SQL to Query Builder conversion."""

import ast
import re
import sqlglot
from sqlglot import exp

from test_utils.utils.sql_registry.models import SQLStructure, UnresolvedParameterError
from test_utils.utils.sql_registry.scanner import (
	can_use_frappe_orm,
	convert_select_to_orm,
	convert_where_to_filters,
)


class SQLToQBConverter:
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
				if can_use_frappe_orm(ast_object):
					return convert_select_to_orm(ast_object, replacements, sql_params, variable_name)
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
				tables.append(self.normalize_table_name(str(from_clause.this)))

			# Extract joins
			for join in ast_object.find_all(exp.Join):
				if join.this:
					tables.append(self.normalize_table_name(str(join.this)))
					joins.append(f"JOIN {self.normalize_table_name(str(join.this))}")

			# Extract fields
			for expr in ast_object.expressions:
				field_str = self.normalize_field(str(expr))
				fields.append(field_str)
				# Check for aggregations
				if any(
					agg in str(expr).upper() for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("]
				):
					has_aggregation = True

			# Extract conditions
			where = ast_object.find(exp.Where)
			if where:
				conditions.append(self.normalize_condition(str(where.this)))

			# Extract GROUP BY
			group = ast_object.find(exp.Group)
			if group:
				for expr in group.expressions:
					group_by.append(self.normalize_field(str(expr)))

			# Extract ORDER BY
			order = ast_object.find(exp.Order)
			if order:
				for expr in order.expressions:
					order_by.append(self.normalize_field(str(expr)))

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
				tables.append(self.normalize_table_name(str(table)))
			where = ast_object.find(exp.Where)
			if where:
				conditions.append(self.normalize_condition(str(where.this)))

		elif isinstance(ast_object, exp.Update):
			table = ast_object.this
			if table:
				tables.append(self.normalize_table_name(str(table)))
			# Extract SET fields
			for expr in ast_object.expressions:
				if isinstance(expr, exp.EQ):
					field_name = str(expr.left).strip("`\"'")
					fields.append(field_name.lower())
			where = ast_object.find(exp.Where)
			if where:
				conditions.append(self.normalize_condition(str(where.this)))

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

	def normalize_table_name(self, name: str) -> str:
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

	def normalize_field(self, field: str) -> str:
		"""Normalize field reference."""
		# Remove backticks, quotes, and standardize
		field = field.strip("`\"'")
		# Replace placeholders with generic marker
		field = re.sub(r"__PH\d+__", "PARAM", field)
		return field.lower()

	def normalize_condition(self, condition: str) -> str:
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
								fields.extend(self.extract_list_values(kw.value))
							elif kw.arg == "filters":
								conditions.append("HAS_FILTERS")
							elif kw.arg == "limit":
								if isinstance(kw.value, ast.Constant):
									limit_val = kw.value.value

					# Query Builder methods
					elif method_name == "delete":
						query_type = "DELETE"
					elif method_name == "select":
						fields.extend(self.extract_qb_fields(node))
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

	def extract_list_values(self, node: ast.AST) -> list[str]:
		"""Extract string values from a list AST node."""
		values = []
		if isinstance(node, ast.List):
			for elt in node.elts:
				if isinstance(elt, ast.Constant):
					values.append(str(elt.value))
		return values

	def extract_qb_fields(self, call_node: ast.Call) -> list[str]:
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
				orig_count = len(original_struct.fields)
				qb_count = len(qb_struct.fields)
				# Allow QB to have 1 extra field (common when SubQuery/JOIN adds DocType refs)
				if qb_count < orig_count or qb_count > orig_count + 1:
					errors.append(f"Field count mismatch: {orig_count} vs {qb_count}")

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
		elif variable_name and variable_name.startswith("__for_"):
			result_prefix = "result = "
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
				if condition.startswith("# MANUAL:"):
					return condition
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
				if having_condition.startswith("# MANUAL:"):
					return having_condition
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
						elif isinstance(param_value, str) and param_value.isidentifier():
							# It's a variable name reference, return without quotes
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
				if left_cond.startswith("# MANUAL:"):
					return left_cond
				right_cond = self.convert_condition_to_qb(
					condition.right, replacements, table_vars, sql_params
				)
				if right_cond.startswith("# MANUAL:"):
					return right_cond
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
				if left_cond.startswith("# MANUAL:"):
					return left_cond
				right_cond = self.convert_condition_to_qb(
					condition.right, replacements, table_vars, sql_params
				)
				if right_cond.startswith("# MANUAL:"):
					return right_cond
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
				# (col1, col2) IN values - pypika/Frappe QB has limited support; avoid invalid code
				if isinstance(condition.this, exp.Tuple):
					return (
						"# MANUAL: (col1, col2) IN list_of_tuples - pypika lacks native support. "
						"Use manual OR conditions or raw SQL"
					)

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
				# Check if expressions is empty and there's a 'field' arg (parameter placeholder case)
				elif not condition.expressions and condition.args.get("field"):
					# Single parameter placeholder like IN %(param)s
					param_value = self.format_value_or_field(
						condition.args["field"], replacements, table_vars, sql_params
					)
					return f"{left}.isin({param_value})"
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
				if inner.startswith("# MANUAL:"):
					return inner
				return f"({inner})"

			elif isinstance(condition, exp.Not):
				# Handle NOT - check if it's wrapping IS NULL (for IS NOT NULL) or IN (for NOT IN)
				inner = condition.this
				if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
					# IS NOT NULL
					left = self.format_field(inner.this, table_vars)
					return f"{left}.isnotnull()"
				elif isinstance(inner, exp.In):
					# NOT IN - use .notin() method (tuple IN not supported)
					if isinstance(inner.this, exp.Tuple):
						return (
							"# MANUAL: (col1, col2) NOT IN list_of_tuples - pypika lacks native support. "
							"Use manual OR conditions or raw SQL"
						)
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
				resolved_value = self.resolve_update_value(value_str, replacements, sql_params)
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
		is_name_filter, name_value = self.is_simple_name_filter(
			where_cond, replacements, sql_params
		)

		if is_name_filter and name_value:
			# Use frappe.db.set_value with name
			if len(set_fields) == 1:
				field, value = list(set_fields.items())[0]
				return f'frappe.db.set_value("{doctype}", {name_value}, "{field}", {value})'
			else:
				# Multiple fields - use dict
				fields_dict = self.format_update_fields_dict(set_fields)
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

	def resolve_update_value(
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

	def is_simple_name_filter(
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

	def format_update_fields_dict(self, fields: dict) -> str:
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
			filters = convert_where_to_filters(where_clause.this, replacements, sql_params)
			if filters and not any(
				isinstance(v, str) and v.startswith("__PH") for v in filters.values()
			):
				# Successfully converted to simple filters
				filter_str = self.format_filters_dict(filters)
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

	def format_filters_dict(self, filters: dict) -> str:
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
