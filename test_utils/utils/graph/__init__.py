"""
Frappe code graph: DuckDB artifacts built from a single app tree.

Requires optional dependencies (``poetry install --with graph``):

- ``duckdb``, ``networkx``

Tree-sitter (JS/TS) is a core dependency of ``test_utils`` and is always
available for :mod:`js_callsites`.

Example::

    from pathlib import Path
    from test_utils.utils.graph import build_graph, complexity_leaderboard

    build_graph(Path(\"/bench/apps/myapp\"), Path(\"myapp_graph.duckdb\"))
    top = complexity_leaderboard(\"myapp_graph.duckdb\", limit=20)
"""

from __future__ import annotations

__all__ = [
	"build_graph",
	"complexity_leaderboard",
	"graph_meta",
	"js_callsites_in_loops",
	"JsCallSite",
	"extract_callsites_from_source",
	"iter_js_callsites_in_app",
]

from .builder import build_graph
from .js_callsites import (
	JsCallSite,
	extract_callsites_from_source,
	iter_js_callsites_in_app,
)
from .queries import complexity_leaderboard, graph_meta, js_callsites_in_loops
