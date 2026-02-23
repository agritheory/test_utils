"""AST-based SQL scanning and ORM conversion (frappe.get_all)."""

import ast
import re
from sqlglot import exp


def is_frappe_db_sql_call(node: ast.Call) -> bool:
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


def extract_sql_from_call(node: ast.Call) -> str | None:
	try:
		if node.args and len(node.args) > 0:
			first_arg = node.args[0]
			if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
				return first_arg.value
	except Exception:
		pass
	return None


def extract_params_from_call(node: ast.Call) -> dict | None:
	try:
		if len(node.args) > 1:
			param_node = node.args[1]

			if isinstance(param_node, ast.Dict):
				params = {}
				for key, value in zip(param_node.keys, param_node.values):
					if isinstance(key, ast.Constant):
						key_str = key.value
						if isinstance(value, ast.Attribute):
							if isinstance(value.value, ast.Name):
								params[key_str] = f"{value.value.id}.{value.attr}"
						elif isinstance(value, ast.Constant):
							params[key_str] = value.value
						elif isinstance(value, ast.Name):
							params[key_str] = value.id
						else:
							params[key_str] = ast.unparse(value) if hasattr(ast, "unparse") else str(value)
				return params

			elif isinstance(param_node, ast.Name):
				sql_query = extract_sql_from_call(node)
				if sql_query:
					param_matches = re.findall(r"%\((\w+)\)s", sql_query)
					if param_matches:
						return {param: param_node.id for param in param_matches}
					if "%s" in sql_query and "%" not in sql_query.replace("%s", ""):
						return {"__pos_0__": param_node.id}
				return {"__var_ref__": param_node.id}

			elif isinstance(param_node, ast.Attribute):
				if isinstance(param_node.value, ast.Name):
					attr_value = f"{param_node.value.id}.{param_node.attr}"
				else:
					attr_value = (
						ast.unparse(param_node) if hasattr(ast, "unparse") else str(param_node)
					)
				sql_query = extract_sql_from_call(node)
				if sql_query:
					param_matches = re.findall(r"%\((\w+)\)s", sql_query)
					if param_matches:
						return {param: attr_value for param in param_matches}
					if "%s" in sql_query:
						return {"__pos_0__": attr_value}
				return {"__var_ref__": attr_value}

			elif isinstance(param_node, ast.Tuple) or isinstance(param_node, ast.List):
				sql_query = extract_sql_from_call(node)
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
							if hasattr(ast, "unparse"):
								params[param_key] = ast.unparse(elem)
							elif isinstance(elem.value, ast.Name):
								params[param_key] = f"{elem.value.id}.{elem.attr}"
							else:
								params[param_key] = str(elem)
						elif isinstance(elem, ast.Call):
							params[param_key] = ast.unparse(elem) if hasattr(ast, "unparse") else "__call__"
						else:
							params[param_key] = ast.unparse(elem) if hasattr(ast, "unparse") else str(elem)
					return params if params else None

			elif isinstance(param_node, ast.Call):
				func = param_node.func
				func_name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
				if func_name == "dict" and param_node.keywords:
					params = {}
					for kw in param_node.keywords:
						if kw.arg:
							if hasattr(ast, "unparse"):
								params[kw.arg] = ast.unparse(kw.value)
							elif isinstance(kw.value, ast.Attribute) and isinstance(
								kw.value.value, ast.Name
							):
								params[kw.arg] = f"{kw.value.value.id}.{kw.value.attr}"
							elif isinstance(kw.value, ast.Name):
								params[kw.arg] = kw.value.id
					return params if params else None
				else:
					sql_query = extract_sql_from_call(node)
					if sql_query:
						param_matches = re.findall(r"%\((\w+)\)s", sql_query)
						if param_matches:
							return {param: f"__from_{func_name}__" for param in param_matches}
						if len(re.findall(r"(?<!%)%s", sql_query)) == 1:
							func_repr = (
								ast.unparse(param_node) if hasattr(ast, "unparse") else func_name + "()"
							)
							return {"__pos_0__": func_repr}

	except Exception as e:
		print(f"Error extracting params: {e}")
	return None


def extract_kwargs_from_call(node: ast.Call) -> dict | None:
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


def node_contains(parent: ast.AST, child: ast.AST) -> bool:
	if parent is child:
		return True
	for node in ast.walk(parent):
		if node is child:
			return True
	return False


def extract_variable_name(tree: ast.AST, call_node: ast.Call) -> str | None:
	for node in ast.walk(tree):
		if isinstance(node, ast.Assign):
			if node_contains(node.value, call_node):
				if node.targets and isinstance(node.targets[0], ast.Name):
					return node.targets[0].id
				elif node.targets and isinstance(node.targets[0], ast.Attribute):
					return f"self.{node.targets[0].attr}"

	for node in ast.walk(tree):
		if isinstance(node, ast.Return):
			if node.value and node_contains(node.value, call_node):
				return "__return__"

	for node in ast.walk(tree):
		if isinstance(node, ast.Yield):
			if node.value and node_contains(node.value, call_node):
				return "__yield__"

	for node in ast.walk(tree):
		if isinstance(node, ast.For):
			if node_contains(node.iter, call_node):
				if isinstance(node.target, ast.Name):
					return f"__for_{node.target.id}__"
				return "__for_iter__"

	for node in ast.walk(tree):
		if isinstance(node, ast.Expr):
			if node_contains(node.value, call_node):
				return "__expr__"

	return None


def get_function_context(tree: ast.AST, target_node: ast.AST) -> str:
	for node in ast.walk(tree):
		if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
			if node_contains(node, target_node):
				return f"def {node.name}():"
	return "global scope"


def can_use_frappe_orm(select: exp.Select) -> bool:
	tables = set()
	from_clause = select.find(exp.From)
	if from_clause and from_clause.this:
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


def extract_field_name(expr: exp.Expression) -> str:
	field_str = str(expr).strip("`\"'")
	if "." in field_str:
		_, field_name = field_str.rsplit(".", 1)
		return field_name.strip("`\"'")
	return field_str


def extract_value(expr: exp.Expression, replacements: list, sql_params: dict = None):
	value_str = str(expr).strip("`\"'")
	for placeholder, original in replacements:
		if placeholder == value_str:
			param_match = re.match(r"%\((\w+)\)s", original)
			if param_match:
				param_name = param_match.group(1)
				if sql_params and param_name in sql_params:
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


def convert_where_to_filters(
	condition: exp.Expression, replacements: list, sql_params: dict = None
):
	try:
		if isinstance(condition, exp.EQ):
			field = extract_field_name(condition.left)
			value = extract_value(condition.right, replacements, sql_params)
			return {field: value}
		elif isinstance(condition, exp.And):
			left_filters = convert_where_to_filters(condition.left, replacements, sql_params)
			right_filters = convert_where_to_filters(condition.right, replacements, sql_params)
			if isinstance(left_filters, dict) and isinstance(right_filters, dict):
				return {**left_filters, **right_filters}
			elif isinstance(left_filters, list) or isinstance(right_filters, list):
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
			left_filters = convert_where_to_filters(condition.left, replacements, sql_params)
			right_filters = convert_where_to_filters(condition.right, replacements, sql_params)
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
			field = extract_field_name(condition.left)
			value = extract_value(condition.right, replacements, sql_params)
			return [[field, ">", value]]
		elif isinstance(condition, exp.GTE):
			field = extract_field_name(condition.left)
			value = extract_value(condition.right, replacements, sql_params)
			return [[field, ">=", value]]
		elif isinstance(condition, exp.LT):
			field = extract_field_name(condition.left)
			value = extract_value(condition.right, replacements, sql_params)
			return [[field, "<", value]]
		elif isinstance(condition, exp.LTE):
			field = extract_field_name(condition.left)
			value = extract_value(condition.right, replacements, sql_params)
			return [[field, "<=", value]]
		elif isinstance(condition, exp.In):
			field = extract_field_name(condition.this)
			values = [extract_value(v, replacements, sql_params) for v in condition.expressions]
			return [[field, "in", values]]
		elif isinstance(condition, exp.Like):
			field = extract_field_name(condition.this)
			pattern = extract_value(condition.expression, replacements, sql_params)
			return [[field, "like", pattern]]
		elif isinstance(condition, exp.Is):
			field = extract_field_name(condition.this)
			return [[field, "is", "null"]]
		elif isinstance(condition, exp.Not):
			if isinstance(condition.this, exp.Is):
				field = extract_field_name(condition.this.this)
				return [[field, "is", "set"]]
			return {}
		elif isinstance(condition, exp.Between):
			field = extract_field_name(condition.this)
			low = extract_value(condition.args.get("low"), replacements, sql_params)
			high = extract_value(condition.args.get("high"), replacements, sql_params)
			return [[field, "between", [low, high]]]
		elif isinstance(condition, exp.NEQ):
			left_expr = condition.left
			right_value = extract_value(condition.right, replacements, sql_params)
			if isinstance(left_expr, exp.Coalesce):
				field_name = extract_field_name(left_expr.this)
				default_val = (
					extract_value(left_expr.expressions[0], replacements, sql_params)
					if left_expr.expressions
					else '""'
				)
				return [["__ifnull__", field_name, default_val, "!=", right_value]]
			else:
				field = extract_field_name(left_expr)
				return [[field, "!=", right_value]]
		elif isinstance(condition, exp.EQ) and isinstance(condition.left, exp.Coalesce):
			left_expr = condition.left
			right_value = extract_value(condition.right, replacements, sql_params)
			field_name = extract_field_name(left_expr.this)
			default_val = (
				extract_value(left_expr.expressions[0], replacements, sql_params)
				if left_expr.expressions
				else '""'
			)
			return [["__ifnull__", field_name, default_val, "=", right_value]]
		else:
			return {}
	except Exception:
		return {}


def format_filters_for_orm(filters, needs_imports: set = None) -> str:
	if needs_imports is None:
		needs_imports = set()
	if isinstance(filters, dict):
		if len(filters) == 1 and "name" in filters:
			return filters["name"] if isinstance(filters["name"], str) else str(filters["name"])
		else:
			items = []
			for key, value in filters.items():
				if isinstance(value, str):
					if value.startswith('"') or value.startswith("'"):
						items.append(f'"{key}": {value}')
					elif value.startswith("doc.") or "." in value or value.isidentifier():
						items.append(f'"{key}": {value}')
					else:
						items.append(f'"{key}": "{value}"')
				else:
					items.append(f'"{key}": {value}')
			return "{" + ", ".join(items) + "}"
	elif isinstance(filters, list):
		formatted = []
		for f in filters:
			if isinstance(f, list) and len(f) >= 5 and f[0] == "__ifnull__":
				_, field_name, default_val, op, value = f
				needs_imports.add("Field")
				needs_imports.add("IfNull")
				default_repr = (
					f'"{default_val}"'
					if (isinstance(default_val, str) and default_val not in ('""', "''"))
					else '""'
				)
				value_repr = (
					f'"{value}"' if (isinstance(value, str) and value not in ('""', "''")) else '""'
				)
				formatted.append(
					f'[IfNull(Field("{field_name}"), {default_repr}), "{op}", {value_repr}]'
				)
			elif isinstance(f, list):
				formatted.append(repr(f))
			else:
				formatted.append(repr(f))
		return "[" + ", ".join(formatted) + "]"
	else:
		return str(filters)


def format_fields_for_orm(fields: list) -> str:
	if len(fields) == 1:
		return f'"{fields[0]}"'
	return "[" + ", ".join(f'"{f}"' for f in fields) + "]"


def convert_select_to_orm(
	select: exp.Select,
	replacements: list,
	sql_params: dict = None,
	variable_name: str = None,
) -> str:
	lines = []
	needs_imports = set()
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

	from_clause = select.find(exp.From)
	if not from_clause or not from_clause.this:
		return "# Error: No FROM clause found"

	table_node = from_clause.this
	table_name = (
		str(table_node.this).strip("`\"'")
		if (hasattr(table_node, "this") and table_node.this)
		else str(table_node).strip("`\"'")
	)
	doctype = table_name.replace("tab", "") if table_name.startswith("tab") else table_name

	fields = []
	is_single_field = False
	has_distinct = False

	if select.args.get("distinct"):
		has_distinct = True

	if select.expressions:
		for expr in select.expressions:
			if isinstance(expr, exp.Star):
				fields = None
				break
			elif isinstance(expr, exp.Distinct):
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
				if "." in field_name:
					_, field_name = field_name.rsplit(".", 1)
				fields.append(field_name)
		is_single_field = len(fields) == 1 if fields else False

	filters = {}
	where_clause = select.find(exp.Where)
	if where_clause:
		filters = convert_where_to_filters(where_clause.this, replacements, sql_params)

	order_by = None
	order_clause = select.find(exp.Order)
	if order_clause:
		order_parts = []
		for ordered in order_clause.expressions:
			field = str(ordered.this).strip("`\"'")
			if "." in field:
				_, field = field.rsplit(".", 1)
			if hasattr(ordered, "args") and ordered.args.get("desc"):
				order_parts.append(f"{field} desc")
			else:
				order_parts.append(f"{field} asc")
		if order_parts:
			order_by = ", ".join(order_parts)

	limit_clause = select.find(exp.Limit)
	limit = (
		str(limit_clause.expression) if limit_clause and limit_clause.expression else None
	)

	lines.append(f"{result_prefix}frappe.get_all(")
	lines.append(f'\t"{doctype}",')
	if filters:
		lines.append(f"\tfilters={format_filters_for_orm(filters, needs_imports)},")
	if fields is None:
		lines.append('\tfields=["*"],')
	elif is_single_field:
		lines.append(f'\tfields=["{fields[0]}"],')
	else:
		field_str = ", ".join(f'"{f}"' for f in fields)
		lines.append(f"\tfields=[{field_str}],")
	if order_by:
		lines.append(f'\torder_by="{order_by}",')
	if limit:
		lines.append(f"\tlimit={limit},")
	if has_distinct:
		lines.append("\tdistinct=True,")

	if lines[-1].endswith(","):
		lines[-1] = lines[-1][:-1]
	lines.append(")")

	if "Field" in needs_imports or "IfNull" in needs_imports:
		import_lines = ["from frappe.query_builder import Field"]
		if "IfNull" in needs_imports:
			import_lines.append("from frappe.query_builder.functions import IfNull")
		return "\n".join(import_lines) + "\n\n" + "\n".join(lines)
	return "\n".join(lines)
