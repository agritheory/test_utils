#!/usr/bin/env python3
"""
Validate ``mermaid`` fenced blocks in Markdown using Mermaid CLI (``mmdc`` via ``npx``).

Requires Node.js (``npx`` on PATH). The first run may download Puppeteer/Chromium for
``@mermaid-js/mermaid-cli``; later runs reuse the npm cache.

The CLI package pin should stay aligned with ``.pre-commit-config.yaml`` /
``.pre-commit-hooks.yaml`` hook metadata where this tool is referenced.
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from collections.abc import Iterator, Sequence

# npx install spec; keep in sync with hook docs / comments in .pre-commit-config.yaml
MERMAID_CLI_PACKAGE = "@mermaid-js/mermaid-cli@11.4.2"


def argv_tail_for_cli() -> list[str]:
	"""Arguments after the interpreter / ``-m module`` prefix (supports ``python -m``)."""
	argv = sys.argv
	if len(argv) >= 3 and argv[1] == "-m":
		return list(argv[3:])
	return list(argv[1:])


def _fence_opener(line: str) -> bool:
	s = line.strip()
	return s.lower().startswith("```mermaid")


def iter_mermaid_blocks(content: str) -> Iterator[tuple[int, str | None, str | None]]:
	"""Yield ``(start_line, body, structural_error)`` for each mermaid fenced code block."""
	lines = content.splitlines()
	i = 0
	while i < len(lines):
		if not _fence_opener(lines[i]):
			i += 1
			continue
		start_line = i + 1
		i += 1
		body_lines: list[str] = []
		while i < len(lines) and lines[i].strip() != "```":
			body_lines.append(lines[i])
			i += 1
		if i >= len(lines):
			yield (start_line, None, "unclosed ```mermaid fence (missing closing ```)")
			break
		yield (start_line, "\n".join(body_lines), None)
		i += 1


def validate_file(path: Path, npx: str, work: Path) -> list[tuple[int, str]]:
	errors: list[tuple[int, str]] = []
	text = path.read_text(encoding="utf-8")
	path_key = hashlib.sha256(str(path.resolve()).encode("utf-8")).hexdigest()[:16]
	block_index = 0
	for start_line, body, structural in iter_mermaid_blocks(text):
		if structural:
			errors.append((start_line, structural))
			continue
		assert body is not None
		trimmed = body.strip()
		if not trimmed:
			continue
		inp = work / f"{path_key}_{block_index}.mmd"
		outp = work / f"{path_key}_{block_index}.svg"
		block_index += 1
		inp.write_text(trimmed + "\n", encoding="utf-8")
		proc = subprocess.run(
			[
				npx,
				"--yes",
				"-p",
				MERMAID_CLI_PACKAGE,
				"mmdc",
				"-i",
				str(inp),
				"-o",
				str(outp),
				"-q",
			],
			capture_output=True,
			text=True,
			cwd=work,
		)
		if proc.returncode != 0:
			detail = (proc.stderr or proc.stdout or "").strip() or f"exit {proc.returncode}"
			errors.append((start_line, detail))
	return errors


def run(argv: Sequence[str] | None = None) -> int:
	if argv is None:
		argv = argv_tail_for_cli()
	parser = argparse.ArgumentParser(
		description="Validate Mermaid fenced blocks in Markdown files.",
	)
	parser.add_argument(
		"paths",
		nargs="*",
		type=Path,
		help="Markdown files to check",
	)
	parser.add_argument(
		"--require-node",
		action="store_true",
		help="Exit with code 1 if npx is not available (default: skip with 0)",
	)
	args = parser.parse_args(list(argv))
	paths = [p for p in args.paths if str(p)]
	if not paths:
		return 0

	npx = shutil.which("npx")
	if not npx:
		msg = "validate_mermaid: npx not found. Install Node.js to validate Mermaid diagrams."
		if args.require_node:
			print(msg, file=sys.stderr)
			return 1
		print(msg + " Skipping.", file=sys.stderr)
		return 0

	all_errors: list[tuple[Path, int, str]] = []
	with tempfile.TemporaryDirectory() as work:
		work_path = Path(work)
		for path in paths:
			p = Path(path)
			if not p.is_file():
				print(f"validate_mermaid: not a file: {p}", file=sys.stderr)
				return 1
			for line_no, msg in validate_file(p, npx, work_path):
				all_errors.append((p, line_no, msg))

	if not all_errors:
		return 0
	for p, line_no, msg in all_errors:
		print(f"{p}:{line_no}: mermaid: {msg}", file=sys.stderr)
	return 1


def main() -> int:
	return run(None)


if __name__ == "__main__":
	raise SystemExit(main())
