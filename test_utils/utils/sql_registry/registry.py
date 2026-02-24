"""SQL Registry - load/save, scan, register, report."""

import ast
import hashlib
import json
import os
import pickle
import re
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import sqlglot
from sqlglot import exp

from test_utils.utils.sql_registry.models import SQLCall
from test_utils.utils.sql_registry.converter import SQLToQBConverter
from test_utils.utils.sql_registry import scanner


def _extract_sql_from_file(
	file_path_str: str, force: bool = False
) -> list[dict] | None:
	"""Extract raw SQL call data from a single file.

	Module-level so it's picklable for ProcessPoolExecutor.
	Returns plain dicts — no registry state involved.

	Return value semantics:
	  None  — file was *skipped* (compile error, read error).  The caller must
	           NOT purge existing registry entries for this file.
	  []    — file was scanned successfully but contained no frappe.db.sql calls.
	  [...] — file was scanned successfully; list contains discovered calls.

	When *force* is True a best-effort regex scan is attempted for files that
	fail to parse, so that at least the line numbers are visible in the registry.
	The resulting entries have an empty sql_query and no parameters, and are
	tagged so the report can surface them.
	"""
	file_path = Path(file_path_str)
	try:
		content = file_path.read_text(encoding="utf-8")
	except Exception as e:
		print(f"Error reading {file_path}: {e}")
		return None

	try:
		tree = ast.parse(content)
	except SyntaxError as e:
		if force:
			print(
				f"Warning: {file_path} does not compile ({e}); "
				"using regex fallback — SQL content may be incomplete"
			)
			return _regex_extract_sql_from_content(content, file_path_str)
		print(f"Skipping {file_path} (does not compile): {e}")
		return None

	# Build the lxml XML representation so we can compute a stable XPath address
	# for each frappe.db.sql call node.
	try:
		from lxml import etree as lxml_etree
		from astpath.asts import convert_to_xml

		node_mappings: dict = {}  # xml_elem → ast_node
		xml_tree = convert_to_xml(tree, node_mappings=node_mappings)
		lxml_et = lxml_etree.ElementTree(xml_tree)
		# Reverse: id(ast_node) → xml_elem
		ast_id_to_xml: dict[int, object] = {id(v): k for k, v in node_mappings.items()}
	except Exception as e:
		print(
			f"Warning: astpath/lxml unavailable for {file_path}: {e}; ast_path will be None"
		)
		lxml_et = None
		ast_id_to_xml = {}

	# Collect all frappe.db.sql calls in source order so occurrence indices are
	# assigned consistently regardless of ast.walk traversal order.
	sql_nodes: list[ast.Call] = []
	try:
		for node in ast.walk(tree):
			if isinstance(node, ast.Call) and scanner.is_frappe_db_sql_call(node):
				if scanner.extract_sql_from_call(node):
					sql_nodes.append(node)
	except Exception as e:
		print(f"Error scanning {file_path}: {e}")
		return []

	sql_nodes.sort(key=lambda n: (n.lineno, n.col_offset))

	# Track (function_context, sql_key) → next occurrence index as fallback.
	occurrence_counter: dict[tuple[str, str], int] = {}
	results = []
	try:
		for node in sql_nodes:
			sql_query = scanner.extract_sql_from_call(node)
			function_context = scanner.get_function_context(tree, node)
			occ_key = (function_context, sql_query[:100])
			occ = occurrence_counter.get(occ_key, 0)
			occurrence_counter[occ_key] = occ + 1

			# Compute the stable XPath structural address via lxml.
			ast_path: str | None = None
			if lxml_et is not None:
				xml_elem = ast_id_to_xml.get(id(node))
				if xml_elem is not None:
					try:
						ast_path = lxml_et.getpath(xml_elem)
					except Exception:
						pass

			results.append(
				{
					"file_path": file_path_str,
					"line_num": node.lineno,
					"sql_query": sql_query,
					"sql_params": scanner.extract_params_from_call(node),
					"sql_kwargs": scanner.extract_kwargs_from_call(node),
					"variable_name": scanner.extract_variable_name(tree, node),
					"function_context": function_context,
					"occurrence": occ,
					"ast_path": ast_path,
				}
			)
	except Exception as e:
		print(f"Error scanning {file_path}: {e}")
	return results


# ---------------------------------------------------------------------------
# Regex fallback used when a file does not compile (--force scan)
# ---------------------------------------------------------------------------

_FRAPPE_DB_SQL_RE = re.compile(r"frappe\.db\.sql\s*\(")


def _regex_extract_sql_from_content(content: str, file_path_str: str) -> list[dict]:
	"""Best-effort, line-number-only scan for frappe.db.sql calls.

	Used as a fallback when ast.parse fails.  The sql_query field is left
	empty because we cannot reliably extract it without a valid AST.
	"""
	results = []
	for lineno, line in enumerate(content.splitlines(), start=1):
		if _FRAPPE_DB_SQL_RE.search(line):
			results.append(
				{
					"file_path": file_path_str,
					"line_num": lineno,
					"sql_query": "",
					"sql_params": None,
					"sql_kwargs": None,
					"variable_name": None,
					"function_context": "(broken source — regex only)",
				}
			)
	return results


def _json_path(path: Path) -> Path:
	return path.with_suffix(".json") if path.suffix == ".pkl" else path


class SQLRegistry:
	def __init__(self, registry_file: str = ".sql_registry.json"):
		self.registry_file = Path(registry_file)
		self.converter = SQLToQBConverter()
		self.data = self.load_registry()

	def load_registry(self) -> dict:
		path = self.registry_file
		json_path = Path(str(path).replace(".pkl", ".json"))
		pkl_path = Path(str(path).replace(".json", ".pkl"))

		if path.suffix == ".pkl":
			pkl_path = path
			json_path = path.with_suffix(".json")

		if json_path.exists():
			try:
				with open(json_path, encoding="utf-8") as f:
					raw = json.load(f)
				calls = {}
				for cid, d in raw.get("calls", {}).items():
					calls[cid] = SQLCall.from_dict(d)
				raw["calls"] = calls
				return raw
			except Exception as e:
				print(f"Warning: Error loading registry ({e}), creating new one")
				return self.create_empty_registry()

		if pkl_path.exists() and pkl_path != json_path:
			try:
				with open(pkl_path, "rb") as f:
					data = pickle.load(f)
				self.registry_file = json_path
				self.data = data
				self.save_registry()
				try:
					pkl_path.unlink()
				except OSError:
					pass
				return self.data
			except Exception as e:
				print(f"Warning: Error migrating pkl ({e}), creating new one")

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

		path = self.registry_file
		if path.suffix == ".pkl":
			path = path.with_suffix(".json")

		calls_serializable = {
			cid: c.to_dict() if hasattr(c, "to_dict") else c
			for cid, c in self.data["calls"].items()
		}
		metadata = self.data["metadata"].copy()
		if isinstance(metadata.get("last_scan"), datetime):
			metadata["last_scan"] = metadata["last_scan"].isoformat()

		path.parent.mkdir(parents=True, exist_ok=True)
		with open(path, "w", encoding="utf-8") as f:
			json.dump({"metadata": metadata, "calls": calls_serializable}, f, indent=2)

	def generate_call_id(
		self,
		file_path: str,
		function_context: str,
		sql_query: str,
		occurrence: int,
		line_num: int | None = None,  # kept for signature compat; no longer used in hash
		ast_path: str | None = None,
	) -> str:
		"""Stable identifier based on structural location, not line number.

		When *ast_path* is provided (the lxml XPath address from scan time) it is
		used as the primary key — it encodes the exact position of the Call node in
		the AST tree and is unique within a file even for duplicate SQL strings.

		Falls back to (file, function, sql_prefix, occurrence) for registries that
		pre-date ast_path storage.
		"""
		if ast_path:
			content = f"{file_path}:{ast_path}"
		else:
			content = f"{file_path}:{function_context}:{sql_query[:100]}:{occurrence}"
		return hashlib.md5(content.encode()).hexdigest()[:12]

	def replace_sql_patterns(self, sql: str) -> tuple[str, list[tuple[str, str]]]:
		replacements = []
		patterns = [
			(r"(%\([^)]+\)s)", "named parameter"),
			(r"({[^}]+})", "f-string block"),
			(r"(?<!%)%s", "positional parameter"),
		]
		for pattern, _ in patterns:
			for match in re.finditer(pattern, sql):
				placeholder = f"__PH{len(replacements)}__"
				replacements.append((placeholder, match.group(0)))
				sql = sql.replace(match.group(0), placeholder, 1)
		return sql, replacements

	def check_conversion_eligibility(
		self,
		sql_query: str,
		replacements: list[tuple[str, str]],
		sql_params: dict | None = None,
	) -> tuple[bool, str | None]:
		named_params_needed = []
		positional_params_count = 0

		for placeholder, original in replacements:
			if original.startswith("{") and original.endswith("}"):
				inner = original[1:-1]
				if re.search(r"[\[\]()+=\-*/]", inner):
					return False, f"Complex f-string expression: {original}"
				if "(" in inner:
					return False, f"F-string with function call: {original}"
			elif original.startswith("%(") and original.endswith(")s"):
				named_params_needed.append(original[2:-2])
			elif original == "%s":
				positional_params_count += 1

		if named_params_needed:
			if sql_params is None:
				return (
					False,
					f"Named parameters {named_params_needed} used but no params dict provided",
				)
			missing = [p for p in named_params_needed if p not in sql_params]
			if missing:
				return False, f"Named parameters {missing} not found in params dict"

		if positional_params_count > 0:
			if sql_params is None:
				return (
					False,
					f"{positional_params_count} positional parameter(s) used but no params provided",
				)
			provided = sum(1 for k in sql_params.keys() if k.startswith("__pos_"))
			if provided < positional_params_count:
				return (
					False,
					f"{positional_params_count} positional parameter(s) needed but only {provided} provided",
				)

		try:
			sql_cleaned, _ = self.replace_sql_patterns(sql_query)
			parsed = sqlglot.parse(sql_cleaned, dialect="mysql")
			if parsed and parsed[0]:
				ast_obj = parsed[0]
				from_clause = ast_obj.find(exp.From)
				if from_clause:
					table_str = str(from_clause.this) if from_clause.this else ""
					if "__PH" in table_str:
						return False, "Dynamic table name"
		except Exception:
			pass

		return True, None

	def analyze_sql(
		self, sql_query: str, sql_params: dict = None, variable_name: str = None
	) -> tuple[str, str, str]:
		try:
			sql_cleaned, replacements = self.replace_sql_patterns(sql_query)
			parsed = sqlglot.parse(sql_cleaned, dialect="mysql")
			if not parsed or not parsed[0]:
				return "", "UNPARSABLE", "# Could not parse SQL"
			ast_object = parsed[0]
			semantic_sig = self.converter.generate_semantic_signature(ast_object)
			qb_equivalent = self.converter.ast_to_query_builder(
				ast_object, replacements, sql_params, variable_name
			)
			return str(ast_object), semantic_sig, qb_equivalent
		except Exception as e:
			return "", f"ERROR: {str(e)}", f"# Error analyzing SQL: {str(e)}"

	def validate_conversion(self, original_ast, qb_code: str) -> tuple[bool, str | None]:
		return self.converter.validate_conversion(original_ast, qb_code)

	def register_sql_call(
		self,
		file_path: str,
		line_num: int,
		sql_query: str,
		function_context: str = "",
		sql_params: dict = None,
		sql_kwargs: dict = None,
		variable_name: str = None,
		occurrence: int = 0,
		ast_path: str | None = None,
	) -> str:
		call_id = self.generate_call_id(
			file_path, function_context, sql_query, occurrence, ast_path=ast_path
		)

		if call_id in self.data["calls"]:
			existing = self.data["calls"][call_id]
			existing.updated_at = datetime.now()
			existing.sql_query = sql_query
			existing.sql_params = sql_params
			existing.sql_kwargs = sql_kwargs
			existing.variable_name = variable_name
			return call_id

		sql_cleaned, replacements = self.replace_sql_patterns(sql_query)
		is_eligible, ineligibility_reason = self.check_conversion_eligibility(
			sql_query, replacements, sql_params
		)

		if not is_eligible:
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
				occurrence_in_function=occurrence,
				ast_path=ast_path,
			)
			self.data["calls"][call_id] = sql_call
			return call_id

		ast_str, semantic_sig, qb_equivalent = self.analyze_sql(
			sql_query, sql_params, variable_name
		)

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
			occurrence_in_function=occurrence,
			ast_path=ast_path,
		)

		self.data["calls"][call_id] = sql_call
		return call_id

	def scan_directory(
		self,
		directory: Path,
		pattern: str = "**/*.py",
		include_patches: bool = False,
		force: bool = False,
	) -> int:
		files = []
		for file_path in directory.glob(pattern):
			if not file_path.is_file():
				continue
			if not include_patches:
				try:
					rel = file_path.relative_to(directory)
				except ValueError:
					pass
				else:
					if "patches" in rel.parts or "patch" in rel.parts:
						continue
			files.append(file_path)

		# All files that were passed to the scanner.  We only purge stale entries
		# for files that were *successfully* processed (result != None).  Files
		# skipped due to compile errors keep their existing entries so that a
		# temporarily-broken file does not lose its registry data.
		candidate_file_strs = {str(f) for f in files}

		workers = min(os.cpu_count() or 1, len(files)) if files else 1
		all_raw: list[dict] = []
		skipped_file_strs: set[str] = set()
		with ProcessPoolExecutor(max_workers=workers) as pool:
			futures = {pool.submit(_extract_sql_from_file, str(f), force): f for f in files}
			for future in as_completed(futures):
				result = future.result()
				if result is None:
					skipped_file_strs.add(str(futures[future]))
				else:
					all_raw.extend(result)

		# Only purge entries for files we actually processed.
		purgeable_file_strs = candidate_file_strs - skipped_file_strs

		found_call_ids: set[str] = set()
		count = 0
		for raw in all_raw:
			if not raw.get("sql_query"):
				# Regex-fallback entries have no SQL — skip registration but
				# don't purge the file's existing entries either.
				skipped_file_strs.add(raw["file_path"])
				purgeable_file_strs.discard(raw["file_path"])
				continue
			call_id = self.register_sql_call(
				raw["file_path"],
				raw["line_num"],
				raw["sql_query"],
				raw["function_context"],
				raw["sql_params"],
				raw["sql_kwargs"],
				raw["variable_name"],
				occurrence=raw.get("occurrence", 0),
				ast_path=raw.get("ast_path"),
			)
			found_call_ids.add(call_id)
			count += 1

		# Purge stale entries: any call from a fully-processed file whose
		# call_id was not produced by the current scan has a shifted line number
		# and is no longer valid.
		stale = [
			cid
			for cid, call in self.data["calls"].items()
			if call.file_path in purgeable_file_strs and cid not in found_call_ids
		]
		for cid in stale:
			del self.data["calls"][cid]

		return count

	def scan_file(self, file_path: Path, force: bool = False) -> int:
		"""Scan a single file and register its SQL calls."""
		file_path_str = str(file_path)
		raw_calls = _extract_sql_from_file(file_path_str, force=force)

		# File was skipped (compile error and not forced) — preserve existing entries.
		if raw_calls is None:
			return 0

		found_call_ids: set[str] = set()
		for raw in raw_calls:
			if not raw.get("sql_query"):
				# Regex-fallback entry — skip registration, preserve existing entries.
				continue
			call_id = self.register_sql_call(
				raw["file_path"],
				raw["line_num"],
				raw["sql_query"],
				raw["function_context"],
				raw["sql_params"],
				raw["sql_kwargs"],
				raw["variable_name"],
				occurrence=raw.get("occurrence", 0),
				ast_path=raw.get("ast_path"),
			)
			found_call_ids.add(call_id)

		# Purge stale entries only if the file compiled successfully.
		stale = [
			cid
			for cid, call in self.data["calls"].items()
			if call.file_path == file_path_str and cid not in found_call_ids
		]
		for cid in stale:
			del self.data["calls"][cid]

		return len(found_call_ids)

	def generate_report(self) -> str:
		metadata = self.data["metadata"]
		calls = self.data["calls"]

		total = len(calls)
		by_type = {}
		orm_count = 0
		todo_count = 0
		manual_count = 0
		qb_count = 0

		for call in calls.values():
			by_type[call.implementation_type] = by_type.get(call.implementation_type, 0) + 1
			if call.query_builder_equivalent:
				if (
					"# MANUAL:" in call.query_builder_equivalent
					or "# Error" in call.query_builder_equivalent
				):
					manual_count += 1
				elif "frappe.get_all(" in call.query_builder_equivalent:
					orm_count += 1
				elif "# TODO" in call.query_builder_equivalent:
					todo_count += 1
				else:
					qb_count += 1

		last_scan = metadata.get("last_scan")
		if isinstance(last_scan, datetime):
			last_scan = last_scan.isoformat()

		report = f"""# SQL Operations Registry Report

**Repository**: {metadata.get('repository', 'N/A')}
**Last Updated**: {last_scan or 'Never'}
**Commit**: {metadata.get('commit_hash', 'N/A')}
**Total SQL Operations**: {total}

## Conversion Status
| Status | Count | Percentage |
|--------|-------|------------|
| ✅ Query Builder | {qb_count} | {(qb_count / max(total, 1) * 100):.1f}% |
| 💡 ORM-eligible | {orm_count} | {(orm_count / max(total, 1) * 100):.1f}% |
| 🔧 Manual Review | {manual_count} | {(manual_count / max(total, 1) * 100):.1f}% |
| ⚠️ Has TODOs | {todo_count} | {(todo_count / max(total, 1) * 100):.1f}% |

**Legend:**
- ✅ Validated Query Builder conversion ready to apply
- 💡 Simple query that can use `frappe.get_all()` instead of Query Builder
- 🔧 Validation failed - complex query needs manual conversion
- ⚠️ Conversion has TODO comments requiring attention

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

		# fmt: off
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
				status = "✅"
				if call.query_builder_equivalent:
					if (
						"# MANUAL:" in call.query_builder_equivalent
						or "# Error" in call.query_builder_equivalent
					):
						status = "🔧"
					elif "frappe.get_all(" in call.query_builder_equivalent:
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
		# fmt: on

		report += f"""
## Summary
- **Files with SQL Operations**: {len(by_file)}
- **Total Operations Tracked**: {total}
- **Unique Query Patterns**: {len(set(c.semantic_signature for c in calls.values()))}
"""

		return report
