"""Data models for the SQL registry."""

from dataclasses import asdict, dataclass
from datetime import datetime


class UnresolvedParameterError(Exception):
	"""Raised when a SQL parameter cannot be resolved to a Python variable."""

	pass


class VarRef:
	"""A Python variable/expression reference — distinct from a SQL string literal.

	When a SQL param is a Python variable (e.g. ``{"as_of_date": as_of_date}``),
	the value should be emitted *unquoted* in generated code.  When it is a SQL
	string literal (e.g. ``'Active'``), it must be emitted *quoted*.

	``VarRef`` carries an expression string (the result of ``ast.unparse``) through
	the pipeline so that formatters can distinguish the two cases reliably, instead
	of relying on heuristics like ``str.isidentifier()``.

	JSON wire format: ``"__varref__:<expr>"`` — stored this way in the registry so
	that the distinction survives serialization/deserialization.
	"""

	_PREFIX = "__varref__:"

	def __init__(self, expr: str) -> None:
		self.expr = expr

	def __repr__(self) -> str:
		return f"VarRef({self.expr!r})"

	def __eq__(self, other: object) -> bool:
		return isinstance(other, VarRef) and self.expr == other.expr

	def __hash__(self) -> int:
		return hash(("VarRef", self.expr))

	# ── serialization helpers ──────────────────────────────────────────────────

	def to_json(self) -> str:
		return f"{self._PREFIX}{self.expr}"

	@classmethod
	def from_json(cls, s: str) -> "VarRef":
		return cls(s[len(cls._PREFIX) :])

	@staticmethod
	def is_json(value: object) -> bool:
		return isinstance(value, str) and value.startswith(VarRef._PREFIX)


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
	conversion_eligible: bool = True
	conversion_validated: bool = False
	ineligibility_reason: str | None = None
	# Occurrence index within (file, function_context, sql_query) — kept as a
	# fallback for registries that predate ast_path storage.
	occurrence_in_function: int = 0
	# Absolute XPath address of this Call node in the lxml XML representation of
	# the file's AST (e.g. "/Module/body/ClassDef/body/FunctionDef/body/Expr/value/Call").
	# Generated via lxml.etree.ElementTree.getpath() at scan time.  Structural,
	# not positional — survives line-number shifts and reformatting.  Used by the
	# rewriter to locate the exact node for inline substitution.
	ast_path: str | None = None

	def to_dict(self) -> dict:
		d = asdict(self)
		d["created_at"] = self.created_at.isoformat()
		d["updated_at"] = self.updated_at.isoformat()
		# Serialize VarRef objects so the JSON round-trip preserves them.
		# asdict() deep-copies non-dataclass values, leaving VarRef instances intact
		# in the result dict — which json.dumps cannot handle.
		if d.get("sql_params"):
			d["sql_params"] = {
				k: v.to_json() if isinstance(v, VarRef) else v for k, v in d["sql_params"].items()
			}
		return d

	@classmethod
	def from_dict(cls, d: dict) -> "SQLCall":
		d = d.copy()
		for key in ("created_at", "updated_at"):
			if key in d and isinstance(d[key], str):
				d[key] = datetime.fromisoformat(d[key])
		if d.get("sql_params"):
			d["sql_params"] = {
				k: VarRef.from_json(v) if VarRef.is_json(v) else v
				for k, v in d["sql_params"].items()
			}
		# Tolerate registries written before these fields were added.
		d.setdefault("occurrence_in_function", 0)
		d.setdefault("ast_path", None)
		return cls(**d)


@dataclass
class SQLStructure:
	"""Normalized structure extracted from SQL for comparison."""

	query_type: str
	tables: list[str]
	fields: list[str]
	conditions: list[str]
	joins: list[str]
	group_by: list[str]
	order_by: list[str]
	limit: int | None
	has_aggregation: bool
