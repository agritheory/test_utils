"""
Semgrep formatter — pre-commit / CLI entry point.

Runs ``semgrep --json`` and prints compact, grouped, colorized output:
one block per rule, one file:line entry per finding.  No repeated rule
descriptions, no banner headers.

Usage (from an app root):

    semgrep_fmt [--directory PATH] [semgrep args...]

    # run in-place (pre-commit, bench root, etc.)
    semgrep_fmt --config=.semgrep.yml --disable-version-check --skip-unknown-extensions

    # run against a different directory (e.g. when the venv lives elsewhere)
    semgrep_fmt --directory ~/quercus/apps/shipstation_integration \\
        --config=.semgrep.yml --disable-version-check --skip-unknown-extensions

Exit codes:
    0  No findings
    1  One or more findings (same semantics as ``semgrep --error``)
"""

import json
import os
import subprocess
import sys
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[36m"
YELLOW = "\033[93m"
RED = "\033[31m"

SEVERITY_COLOR = {
	"ERROR": RED,
	"WARNING": YELLOW,
	"INFO": DIM,
}


def colorize(text: str, *codes: str) -> str:
	if not sys.stdout.isatty():
		return text
	return "".join(codes) + text + RESET


def severity_color(severity: str) -> str:
	return SEVERITY_COLOR.get(severity.upper(), "")


def _run(directory: Path | None, semgrep_args: list[str]) -> int:
	cwd = directory.resolve() if directory else None

	result = subprocess.run(
		["semgrep", "--json", "--quiet"] + semgrep_args,
		capture_output=True,
		text=True,
		cwd=cwd,
	)

	if result.stderr:
		sys.stderr.write(result.stderr)

	try:
		data = json.loads(result.stdout)
	except json.JSONDecodeError:
		if result.stdout:
			sys.stdout.write(result.stdout)
		return result.returncode

	errors = data.get("errors", [])
	for error in errors:
		print(colorize(f"semgrep error: {error.get('message', error)}", RED), file=sys.stderr)

	findings = data.get("results", [])
	if not findings:
		if not errors:
			print(colorize("No findings.", BOLD))
		return 1 if errors else 0

	by_rule: dict[str, list] = defaultdict(list)
	for finding in findings:
		rule_id = finding["check_id"].split(".")[-1]
		by_rule[rule_id].append(finding)

	for rule_id, matches in sorted(by_rule.items()):
		severity = matches[0]["extra"]["severity"]
		color = severity_color(severity)
		count = len(matches)
		label = "finding" if count == 1 else "findings"
		print(colorize(f"{rule_id}", BOLD, color) + "  " + colorize(f"{count} {label}", DIM))

		for match in sorted(matches, key=lambda x: (x["path"], x["start"]["line"])):
			path = match["path"]
			line = match["start"]["line"]
			print("  " + colorize(path, CYAN) + colorize(f":{line}", DIM))
		print()

	return 1


def main(argv: Sequence[str] | None = None) -> None:
	args = list(argv) if argv is not None else sys.argv[1:]

	if "--directory" in args:
		idx = args.index("--directory")
		directory = Path(args[idx + 1]).expanduser()
		args = args[:idx] + args[idx + 2 :]
	else:
		# `poetry -C <path> run` changes the process cwd to the project root before
		# launching the command. Fall back to the shell's PWD, which always reflects
		# the directory the user was actually in when they ran the command.
		directory = Path(os.environ.get("PWD", os.getcwd()))

	sys.exit(_run(directory, args))


if __name__ == "__main__":
	main()
