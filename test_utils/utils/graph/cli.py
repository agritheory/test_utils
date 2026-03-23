"""CLI: ``frappe_graph build``."""

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
	build_graph(app, out, dependency_paths=deps)
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


def main(argv: list[str] | None = None) -> int:
	p = argparse.ArgumentParser(
		prog="frappe_graph", description="Frappe code graph tooling"
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

	args = p.parse_args(argv)
	return args.func(args)


if __name__ == "__main__":
	raise SystemExit(main())
