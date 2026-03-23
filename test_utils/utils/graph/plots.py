"""Generate static plots from a code graph DuckDB file (requires matplotlib)."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def render_plots(db_path: Path, output_dir: Path, *, top_n: int = 25) -> list[Path]:
	"""Write PNG charts under *output_dir*; return paths created.

	Raises ``ImportError`` if matplotlib or duckdb is missing.
	"""
	try:
		import matplotlib

		matplotlib.use("Agg")
		import matplotlib.pyplot as plt
	except ImportError as e:  # pragma: no cover
		raise ImportError(
			"matplotlib is required for plotting. Install dev deps: poetry install --with dev"
		) from e

	try:
		import duckdb
	except ImportError as e:  # pragma: no cover
		raise ImportError("duckdb is required. Install: poetry install --with graph") from e

	db_path = Path(db_path).resolve()
	output_dir = Path(output_dir).resolve()
	output_dir.mkdir(parents=True, exist_ok=True)
	written: list[Path] = []

	con = duckdb.connect(str(db_path), read_only=True)
	try:
		# --- Top N composite complexity (horizontal bar) ---
		rows = con.execute(
			"""
			SELECT dotted_path, cyclomatic, max_ast_depth,
				(COALESCE(cyclomatic, 0) * 2 + COALESCE(max_ast_depth, 0)) AS score
			FROM functions
			ORDER BY score DESC NULLS LAST
			LIMIT ?
			""",
			[int(top_n)],
		).fetchall()
		if rows:
			fig, ax = plt.subplots(figsize=(10, max(6, top_n * 0.28)))
			short_labels: list[str] = []
			for r in rows:
				dp = r[0]
				short_labels.append(dp if len(dp) <= 52 else "…" + dp[-50:])
			scores = [r[3] for r in rows]
			y = range(len(rows))
			ax.barh(list(y), scores, color="steelblue")
			ax.set_yticks(list(y))
			ax.set_yticklabels(short_labels, fontsize=7)
			ax.invert_yaxis()
			ax.set_xlabel("Score = 2 × cyclomatic + max_ast_depth")
			ax.set_title(f"Top {len(rows)} functions by composite complexity")
			plt.tight_layout()
			p = output_dir / "top_complexity.png"
			fig.savefig(p, dpi=150, bbox_inches="tight")
			plt.close(fig)
			written.append(p)

		# --- Cyclomatic histogram ---
		cycl_rows = con.execute(
			"SELECT cyclomatic FROM functions WHERE cyclomatic IS NOT NULL"
		).fetchall()
		vals = [c[0] for c in cycl_rows]
		if vals:
			fig, ax = plt.subplots(figsize=(9, 5))
			bins = min(45, max(12, len(set(vals))))
			ax.hist(vals, bins=bins, color="coral", edgecolor="white", alpha=0.9)
			ax.set_xlabel("Cyclomatic complexity")
			ax.set_ylabel("Function count")
			ax.set_title("Distribution of cyclomatic complexity (all functions)")
			plt.tight_layout()
			p = output_dir / "cyclomatic_histogram.png"
			fig.savefig(p, dpi=150, bbox_inches="tight")
			plt.close(fig)
			written.append(p)

		# --- Nesting depth histogram ---
		depth_rows = con.execute(
			"SELECT max_ast_depth FROM functions WHERE max_ast_depth IS NOT NULL"
		).fetchall()
		dvals = [d[0] for d in depth_rows]
		if dvals:
			fig, ax = plt.subplots(figsize=(9, 5))
			ax.hist(
				dvals,
				bins=min(20, max(dvals) + 1),
				color="seagreen",
				edgecolor="white",
				alpha=0.9,
			)
			ax.set_xlabel("Max AST nesting depth (compound statements)")
			ax.set_ylabel("Function count")
			ax.set_title("Distribution of max nesting depth")
			plt.tight_layout()
			p = output_dir / "nesting_depth_histogram.png"
			fig.savefig(p, dpi=150, bbox_inches="tight")
			plt.close(fig)
			written.append(p)

		# --- Whitelist vs not ---
		wl_rows: list[tuple[Any, ...]] = con.execute(
			"SELECT is_whitelisted, COUNT(*) FROM functions GROUP BY is_whitelisted ORDER BY 1"
		).fetchall()
		if wl_rows:
			fig, ax = plt.subplots(figsize=(6, 4))
			labels = ["whitelisted" if r[0] else "not whitelisted" for r in wl_rows]
			counts = [r[1] for r in wl_rows]
			colors = ["#2ecc71" if r[0] else "#95a5a6" for r in wl_rows]
			ax.bar(labels, counts, color=colors)
			ax.set_ylabel("Function count")
			ax.set_title("Functions by @frappe.whitelist (or pyproject allowlist)")
			plt.tight_layout()
			p = output_dir / "whitelist_counts.png"
			fig.savefig(p, dpi=150, bbox_inches="tight")
			plt.close(fig)
			written.append(p)

		# --- JS frappe.call: in-loop vs not ---
		js_rows = con.execute(
			"""
			SELECT loop_context, COUNT(*) AS n
			FROM js_callsites
			GROUP BY loop_context
			ORDER BY 1
			"""
		).fetchall()
		if js_rows:
			fig, ax = plt.subplots(figsize=(6, 4))
			labels = ["in loop" if r[0] else "not in loop" for r in js_rows]
			counts = [r[1] for r in js_rows]
			colors = ["#e74c3c" if r[0] else "#3498db" for r in js_rows]
			ax.bar(labels, counts, color=colors)
			ax.set_ylabel("Call sites")
			ax.set_title("JS frappe.call / xcall: loop context (tree-sitter)")
			plt.tight_layout()
			p = output_dir / "js_loop_context.png"
			fig.savefig(p, dpi=150, bbox_inches="tight")
			plt.close(fig)
			written.append(p)

		# --- Hook registrations by hooks.py key (top 15) ---
		hook_rows = con.execute(
			"""
			SELECT hook_variable, COUNT(*) AS n
			FROM hook_registrations
			GROUP BY 1
			ORDER BY n DESC
			LIMIT 15
			"""
		).fetchall()
		if hook_rows:
			fig, ax = plt.subplots(figsize=(8, 5))
			labels = [r[0][:40] for r in hook_rows]
			counts = [r[1] for r in hook_rows]
			ax.barh(range(len(labels)), counts, color="mediumpurple")
			ax.set_yticks(range(len(labels)))
			ax.set_yticklabels(labels, fontsize=8)
			ax.invert_yaxis()
			ax.set_xlabel("Paths registered")
			ax.set_title("Top hooks.py variable groups (path count)")
			plt.tight_layout()
			p = output_dir / "hook_variables.png"
			fig.savefig(p, dpi=150, bbox_inches="tight")
			plt.close(fig)
			written.append(p)

	finally:
		con.close()

	return written
