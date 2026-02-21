"""Data models for the SQL registry."""

from dataclasses import asdict, dataclass
from datetime import datetime


class UnresolvedParameterError(Exception):
	"""Raised when a SQL parameter cannot be resolved to a Python variable."""

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
	conversion_eligible: bool = True
	conversion_validated: bool = False
	ineligibility_reason: str | None = None

	def to_dict(self) -> dict:
		d = asdict(self)
		d["created_at"] = self.created_at.isoformat()
		d["updated_at"] = self.updated_at.isoformat()
		return d

	@classmethod
	def from_dict(cls, d: dict) -> "SQLCall":
		d = d.copy()
		for key in ("created_at", "updated_at"):
			if key in d and isinstance(d[key], str):
				d[key] = datetime.fromisoformat(d[key])
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
