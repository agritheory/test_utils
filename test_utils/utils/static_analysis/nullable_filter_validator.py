"""
Flag range filters on nullable Date/Datetime/Time/Data fields with no "is set" guard.

Frappe wraps filters on nullable columns in ``ifnull(col, '')``. That makes
``''`` a valid operand for comparison operators — ``'' < '2026-01-01'`` is
string-true on both MariaDB and Postgres — so ``["due_date", "<", nowdate()]``
also matches rows where ``due_date`` is NULL. Pairing the comparison with
``["field", "is", "set"]`` (Frappe allows two conditions per fieldname) avoids it.
"""

import ast
import json
from dataclasses import dataclass, field
from pathlib import Path

RANGE_OPS = frozenset({"<", ">", "<=", ">="})
NULLABLE_FIELDTYPES = frozenset({"Date", "Datetime", "Data", "Time"})

GET_ALL_FUNCS = frozenset({"get_all", "get_list", "get_count", "exists"})


@dataclass
class NullableFilterValidationResult:
	errors: list[str] = field(default_factory=list)
	warnings: list[str] = field(default_factory=list)
	calls_checked: int = 0

	def to_dict(self) -> dict:
		return {
			"errors": self.errors,
			"warnings": self.warnings,
			"calls_checked": self.calls_checked,
		}


def _load_nullable_fields(app_path: Path) -> dict[str, set[str]]:
	"""Map DocType name -> set of non-mandatory Date/Datetime/Data/Time fieldnames."""
	fields_by_doctype: dict[str, set[str]] = {}
	for json_file in app_path.rglob("*.json"):
		if "doctype" not in json_file.parts:
			continue
		try:
			data = json.loads(json_file.read_text(encoding="utf-8"))
		except (OSError, json.JSONDecodeError):
			continue
		if data.get("doctype") != "DocType" or "fields" not in data:
			continue
		name = data.get("name") or json_file.stem
		nullable = {
			f["fieldname"]
			for f in data["fields"]
			if f.get("fieldtype") in NULLABLE_FIELDTYPES and not f.get("reqd") and not f.get("default")
		}
		if nullable:
			fields_by_doctype[name] = nullable
	return fields_by_doctype


def _filter_entries(node: ast.AST) -> list[tuple[ast.AST, int]]:
	"""Extract literal filter clauses (as AST list/tuple nodes) from a filters argument."""
	entries: list[tuple[ast.AST, int]] = []
	if isinstance(node, (ast.List, ast.Tuple)):
		# Either a single ["field", "op", value] clause or a list of clauses.
		if node.elts and all(isinstance(e, ast.Constant) for e in node.elts[:2]):
			entries.append((node, node.lineno))
		else:
			for elt in node.elts:
				entries.extend(_filter_entries(elt))
	return entries


def _clause_field_and_op(clause: ast.AST) -> tuple[str, str] | None:
	if not isinstance(clause, (ast.List, ast.Tuple)):
		return None
	elts = clause.elts
	if len(elts) == 3 and all(isinstance(e, ast.Constant) for e in (elts[0], elts[1])):
		return elts[0].value, elts[1].value
	return None


class NullableFilterValidator:
	def validate(self, app_path: Path) -> NullableFilterValidationResult:
		result = NullableFilterValidationResult()
		nullable_fields = _load_nullable_fields(app_path)
		if not nullable_fields:
			return result

		for py_file in app_path.rglob("*.py"):
			if any(p in py_file.parts for p in ("__pycache__", "node_modules", "test", "tests")):
				continue
			try:
				source = py_file.read_text(encoding="utf-8", errors="replace")
			except OSError:
				continue
			try:
				tree = ast.parse(source, filename=str(py_file))
			except SyntaxError:
				continue

			lines = source.splitlines()

			for node in ast.walk(tree):
				if not isinstance(node, ast.Call):
					continue
				func = node.func
				func_name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
				if func_name not in GET_ALL_FUNCS:
					continue

				# doctype is first positional arg or `doctype=` kwarg.
				doctype = None
				if node.args and isinstance(node.args[0], ast.Constant):
					doctype = node.args[0].value
				else:
					for kw in node.keywords:
						if kw.arg == "doctype" and isinstance(kw.value, ast.Constant):
							doctype = kw.value.value
				if doctype not in nullable_fields:
					continue

				# filters is the 2nd positional arg or `filters=`/`filter=` kwarg.
				filters_node = None
				if len(node.args) > 1:
					filters_node = node.args[1]
				else:
					for kw in node.keywords:
						if kw.arg in ("filters", "filter"):
							filters_node = kw.value
				if filters_node is None:
					continue

				clauses = _filter_entries(filters_node)
				fields_with_range: dict[str, int] = {}
				fields_with_guard: set[str] = set()
				for clause, lineno in clauses:
					parsed = _clause_field_and_op(clause)
					if not parsed:
						continue
					fname, op = parsed
					if fname not in nullable_fields[doctype]:
						continue
					if op in RANGE_OPS:
						fields_with_range.setdefault(fname, lineno)
					elif op == "is":
						fields_with_guard.add(fname)

				result.calls_checked += 1
				for fname, lineno in fields_with_range.items():
					if fname in fields_with_guard:
						continue
					line_text = lines[lineno - 1] if 0 < lineno <= len(lines) else ""
					if "frappe-vulture:ignore" in line_text:
						continue
					result.warnings.append(
						f"{py_file}:{lineno}: {doctype}.{fname} compared with a range operator "
						f"but not guarded with [\"{fname}\", \"is\", \"set\"] — Frappe wraps "
						f"nullable columns in ifnull(col, ''), so NULL rows can silently match "
						f"the comparison"
					)

		return result
