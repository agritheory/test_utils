"""Extract ``frappe.call`` / ``frappe.xcall`` sites from JS/TS/Vue with loop context (tree-sitter)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from tree_sitter import Language, Node, Parser
import tree_sitter_javascript as _tsjs
import tree_sitter_typescript as _tsts

from ..static_analysis.path_resolver import is_dynamic_path, is_potential_dotted_path

from .path_excludes import is_excluded_relative_path

# Array / collection iteration methods that imply per-item work when the body contains a call.
_LOOP_MEMBER_METHODS: frozenset[str] = frozenset(
	{
		"forEach",
		"map",
		"filter",
		"reduce",
		"reduceRight",
		"some",
		"every",
		"find",
		"findIndex",
		"flatMap",
	}
)
# jQuery-style
_JQUERY_LOOP_METHODS: frozenset[str] = frozenset({"each", "map"})

_STATEMENT_LOOP_TYPES: frozenset[str] = frozenset(
	{
		"for_statement",
		"for_in_statement",
		"while_statement",
		"do_statement",
	}
)

_VUE_SCRIPT_RE = re.compile(
	r"<script(?:\s[^>]*)?>(.*?)</script>",
	re.DOTALL | re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class JsCallSite:
	"""A single JS/TS ``frappe.call`` / ``frappe.xcall`` invocation."""

	file_path: str
	line: int
	target_path: str | None
	loop_context: bool
	loop_type: str | None


def _language_for_path(path: Path) -> Language:
	suffix = path.suffix.lower()
	if suffix in (".ts", ".tsx"):
		return Language(_tsts.language_typescript())
	if suffix in (".js", ".jsx", ".mjs", ".cjs"):
		return Language(_tsjs.language())
	# Default to JS grammar for unknown extensions after Vue extraction
	return Language(_tsjs.language())


def _make_parser(lang: Language) -> Parser:
	p = Parser()
	p.language = lang
	return p


def _node_text(node: Node, source: bytes) -> str:
	return source[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


def _walk(node: Node):
	yield node
	for c in node.children:
		yield from _walk(c)


def _is_frappe_call(node: Node, source: bytes) -> bool:
	if node.type != "call_expression":
		return False
	fn = node.child_by_field_name("function")
	if fn is None or fn.type != "member_expression":
		return False
	obj = fn.child_by_field_name("object")
	prop = fn.child_by_field_name("property")
	if obj is None or prop is None:
		return False
	if obj.type != "identifier" or _node_text(obj, source) != "frappe":
		return False
	if prop.type != "property_identifier":
		return False
	meth = _node_text(prop, source)
	return meth in ("call", "xcall")


def _first_string_fragment(arg_node: Node, source: bytes) -> str | None:
	# string or template_string (skip dynamic templates)
	if arg_node.type == "string":
		for c in arg_node.children:
			if c.type == "string_fragment":
				return _node_text(c, source)
		return None
	if arg_node.type == "template_string":
		return None
	return None


def _method_from_object_literal(obj: Node, source: bytes) -> str | None:
	# object / pair nodes (JS + TS grammars)
	for pair in obj.children:
		if pair.type != "pair":
			continue
		key = pair.child_by_field_name("key")
		val = pair.child_by_field_name("value")
		if key is None or val is None:
			continue
		key_name: str | None = None
		if key.type == "property_identifier":
			key_name = _node_text(key, source)
		elif key.type == "string":
			key_name = _first_string_fragment(key, source)
		if key_name != "method":
			continue
		if val.type == "string":
			return _first_string_fragment(val, source)
	return None


def _extract_target_path(call: Node, source: bytes) -> str | None:
	args = call.child_by_field_name("arguments")
	if args is None:
		return None
	children = [c for c in args.named_children]
	if not children:
		return None
	first = children[0]
	if first.type == "string":
		raw = _first_string_fragment(first, source)
		if raw and is_potential_dotted_path(raw) and not is_dynamic_path(raw):
			return raw
		return None
	if first.type == "object":
		return _method_from_object_literal(first, source)
	return None


def _outer_loop_method_name(call: Node, source: bytes) -> str | None:
	"""If *call* is ``arr.forEach(...)`` / ``$.each(...)``, return a short label."""
	fn = call.child_by_field_name("function")
	if fn is None or fn.type != "member_expression":
		return None
	obj = fn.child_by_field_name("object")
	prop = fn.child_by_field_name("property")
	if obj is None or prop is None or prop.type != "property_identifier":
		return None
	prop_name = _node_text(prop, source)
	obj_txt = _node_text(obj, source)
	if obj_txt == "$" and prop_name in _JQUERY_LOOP_METHODS:
		return f"$.{prop_name}"
	if prop_name in _LOOP_MEMBER_METHODS:
		return prop_name
	return None


def _loop_context_and_type_with_source(
	call: Node, source: bytes
) -> tuple[bool, str | None]:
	cur: Node | None = call.parent
	while cur is not None:
		t = cur.type
		if t in _STATEMENT_LOOP_TYPES:
			return True, t
		if t == "call_expression":
			label = _outer_loop_method_name(cur, source)
			if label:
				return True, label
		cur = cur.parent
	return False, None


def extract_callsites_from_source(
	relpath: str,
	source_text: str,
	*,
	language: Language | None = None,
) -> list[JsCallSite]:
	"""Parse *source_text* and return all Frappe call sites. *relpath* is stored on each row."""
	source = source_text.encode("utf-8")
	lang = language or _language_for_path(Path(relpath))
	parser = _make_parser(lang)
	tree = parser.parse(source)
	root = tree.root_node
	out: list[JsCallSite] = []
	for node in _walk(root):
		if not _is_frappe_call(node, source):
			continue
		target = _extract_target_path(node, source)
		loop_ctx, loop_ty = _loop_context_and_type_with_source(node, source)
		line = node.start_point[0] + 1
		out.append(
			JsCallSite(
				file_path=relpath,
				line=line,
				target_path=target,
				loop_context=loop_ctx,
				loop_type=loop_ty,
			)
		)
	return out


def extract_callsites_for_file(app_root: Path, file_path: Path) -> list[JsCallSite]:
	"""Read *file_path* under *app_root* and extract call sites (handles ``.vue``)."""
	try:
		raw = file_path.read_text(encoding="utf-8", errors="replace")
	except OSError:
		return []

	relpath = file_path.relative_to(app_root).as_posix()
	suffix = file_path.suffix.lower()

	if suffix == ".vue":
		results: list[JsCallSite] = []
		for m in _VUE_SCRIPT_RE.finditer(raw):
			body = m.group(1) or ""
			if not body.strip():
				continue
			opening = raw[m.start() : m.start(1)]
			lang_key = "ts" if 'lang="ts"' in opening or "lang='ts'" in opening else "js"
			lang_obj = (
				Language(_tsts.language_typescript())
				if lang_key == "ts"
				else Language(_tsjs.language())
			)
			line_base = raw.count("\n", 0, m.start(1))
			for site in extract_callsites_from_source(relpath, body, language=lang_obj):
				results.append(
					JsCallSite(
						file_path=site.file_path,
						line=line_base + site.line,
						target_path=site.target_path,
						loop_context=site.loop_context,
						loop_type=site.loop_type,
					)
				)
		return results

	return extract_callsites_from_source(relpath, raw)


def iter_js_callsites_in_app(
	app_root: Path, exclude_globs: list[str] | None = None
) -> list[JsCallSite]:
	"""Scan *app_root* for frontend extensions and yield all call sites."""
	from ..static_analysis.frontend_validator import FRONTEND_EXTENSIONS

	from .constants import SKIP_DIR_PARTS

	globs = exclude_globs or []
	found: list[JsCallSite] = []
	for ext in FRONTEND_EXTENSIONS:
		for fp in app_root.rglob(f"*{ext}"):
			if fp.parts and any(part in SKIP_DIR_PARTS for part in fp.parts):
				continue
			if not fp.is_file():
				continue
			relp = fp.relative_to(app_root).as_posix()
			if is_excluded_relative_path(relp, globs):
				continue
			found.extend(extract_callsites_for_file(app_root, fp))
	return found
