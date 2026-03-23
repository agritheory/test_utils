"""CLI: ``code_graph`` — build/query/plot DuckDB code graphs for a Frappe app tree."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _cmd_build(args: argparse.Namespace) -> int:
	from .builder import build_graph

	app = Path(args.app).resolve()
	out = Path(args.output).resolve()
	deps = [Path(p).resolve() for p in args.dep] if args.dep else None
	build_graph(
		app,
		out,
		dependency_paths=deps,
		exclude_globs=args.exclude if args.exclude else None,
		exclude_tests=not args.include_tests,
	)
	print(f"Wrote graph: {out}", file=sys.stderr)
	return 0


def _cmd_leaderboard(args: argparse.Namespace) -> int:
	from .queries import complexity_leaderboard

	rows = complexity_leaderboard(
		args.db,
		limit=args.limit,
		min_cyclomatic=args.min_cyclomatic,
	)
	print(json.dumps(rows, indent=2))
	return 0


def _cmd_js_loops(args: argparse.Namespace) -> int:
	from .queries import js_callsites_in_loops

	rows = js_callsites_in_loops(args.db)
	print(json.dumps(rows, indent=2))
	return 0


def _cmd_plot(args: argparse.Namespace) -> int:
	from .plots import render_plots

	out = Path(args.output).resolve()
	paths = render_plots(Path(args.db), out, top_n=args.top)
	for p in paths:
		print(str(p))
	return 0


def _cmd_export_view(args: argparse.Namespace) -> int:
	from .interactive_export import write_graph_json, write_interactive_html

	db = Path(args.db).resolve()
	out = Path(args.output).resolve()

	if args.json_only:
		json_path = out if out.suffix.lower() == ".json" else (out / "graph.json")
		if json_path.suffix.lower() != ".json":
			json_path = out / "graph.json"
		json_path.parent.mkdir(parents=True, exist_ok=True)
		write_graph_json(db, json_path)
		print(str(json_path), file=sys.stderr)
		return 0

	if out.suffix.lower() == ".html":
		html_path = out
		html_path.parent.mkdir(parents=True, exist_ok=True)
	else:
		out.mkdir(parents=True, exist_ok=True)
		html_path = out / "code_graph.html"
	write_interactive_html(db, html_path)
	print(str(html_path), file=sys.stderr)

	if args.json:
		jp = html_path.with_name(html_path.stem + ".graph.json")
		write_graph_json(db, jp)
		print(str(jp), file=sys.stderr)
	return 0


def main(argv: list[str] | None = None) -> int:
	p = argparse.ArgumentParser(
		prog="code_graph",
		description="Code graph tooling for Frappe app source trees (test_utils)",
	)
	sub = p.add_subparsers(dest="cmd", required=True)

	b = sub.add_parser("build", help="Build DuckDB graph for one app")
	b.add_argument("app", help="Path to Frappe app root")
	b.add_argument(
		"-o",
		"--output",
		required=True,
		help="Output .duckdb path",
	)
	b.add_argument(
		"--dep",
		action="append",
		default=[],
		help="Additional dependency app path (repeatable)",
	)
	b.add_argument(
		"--exclude",
		action="append",
		default=[],
		help="Glob relative to app root to skip when scanning (repeatable), e.g. '**/node_modules/**'",
	)
	b.add_argument(
		"--include-tests",
		action="store_true",
		help="Include default test paths (by default **/tests/** and test_*.py are excluded)",
	)
	b.set_defaults(func=_cmd_build)

	lb = sub.add_parser("leaderboard", help="Print complexity leaderboard JSON")
	lb.add_argument("db", help="Path to .duckdb file")
	lb.add_argument("--limit", type=int, default=100)
	lb.add_argument("--min-cyclomatic", type=int, default=None)
	lb.set_defaults(func=_cmd_leaderboard)

	jl = sub.add_parser("js-loops", help="List JS frappe.call sites inside loops (JSON)")
	jl.add_argument("db", help="Path to .duckdb file")
	jl.set_defaults(func=_cmd_js_loops)

	pl = sub.add_parser(
		"plot",
		help="Write PNG charts from a graph DB (needs: poetry install --with dev --with graph)",
	)
	pl.add_argument("db", help="Path to .duckdb file")
	pl.add_argument(
		"-o",
		"--output",
		required=True,
		help="Output directory for PNG files",
	)
	pl.add_argument("--top", type=int, default=25, help="Top N functions on bar chart")
	pl.set_defaults(func=_cmd_plot)

	ev = sub.add_parser(
		"export-view",
		help="Obsidian-style interactive graph (HTML + vis-network) and/or graph.json",
	)
	ev.add_argument("db", help="Path to .duckdb file")
	ev.add_argument(
		"-o",
		"--output",
		required=True,
		help="Output: path ending in .html, or a directory (creates code_graph.html inside)",
	)
	ev.add_argument(
		"--json",
		action="store_true",
		help="Also write graph.json beside the HTML file",
	)
	ev.add_argument(
		"--json-only",
		action="store_true",
		help="Write only graph.json (-o may be a file path or directory)",
	)
	ev.set_defaults(func=_cmd_export_view)

	args = p.parse_args(argv)
	return args.func(args)


if __name__ == "__main__":
	raise SystemExit(main())
