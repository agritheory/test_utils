#!/usr/bin/env python3
"""
Check for code duplication using jscpd.

Runs jscpd to detect copy-paste in Python, JavaScript, and TypeScript.
Exits with code 1 if duplication exceeds thresholds.

Usage:
    check_code_duplication [--max-clones 60] [--max-percentage 5.0]
"""

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


# Pin below 4.2.5: that release pulls commander@15 (ESM-only), which breaks on Node < 20.19.
JSCPD_VERSION = "jscpd@4.2.4"

JSCPD_IGNORE = (
	"**/node_modules/**,**/.venv/**,**/venv/**,**/__pycache__/**,"
	"**/dist/**,**/build/**,**/*.bundle.js,**/tests/**,**/test_*.py,"
	"**/*_test.py,**/*.test.js,**/*.spec.js,**/fixtures/**,"
	"**/*fixtures.py,**/*.min.js,**/*.min.css,**/migrations/**,**/.git/**,**/.github/**"
)


def main() -> int:
	parser = argparse.ArgumentParser(description="Check for code duplication with jscpd")
	parser.add_argument(
		"--max-clones",
		type=int,
		default=60,
		help="Maximum clone count before failing (default: 60)",
	)
	parser.add_argument(
		"--max-percentage",
		type=float,
		default=5.0,
		help="Maximum duplication percentage before failing (default: 5.0)",
	)
	args = parser.parse_args()

	if not shutil.which("npx"):
		print(
			"check_code_duplication: npx not found. Install Node.js or skip this hook.",
			file=sys.stderr,
		)
		return 0  # Skip rather than fail if node not available

	with tempfile.TemporaryDirectory() as report_dir:
		cmd = [
			"npx",
			JSCPD_VERSION,
			".",
			"--format",
			"python,javascript,typescript",
			"--ignore",
			JSCPD_IGNORE,
			"--min-lines",
			"20",
			"--min-tokens",
			"150",
			"--reporters",
			"json,console",
			"--output",
			report_dir,
			"--threshold",
			"6",
			"--exitCode",
			"0",  # We check thresholds ourselves
		]
		result = subprocess.run(cmd, capture_output=True, text=True)
		print(result.stdout, end="")
		if result.stderr:
			print(result.stderr, end="", file=sys.stderr)

		report_path = Path(report_dir)
		json_report = report_path / "jscpd-report.json"
		if not json_report.is_file():
			found = list(report_path.rglob("jscpd-report.json"))
			json_report = found[0] if found else None

		if json_report is None or not json_report.is_file():
			if result.returncode != 0:
				print(
					f"check_code_duplication: jscpd failed (exit {result.returncode})",
					file=sys.stderr,
				)
				return 1
			return 0

		try:
			with open(json_report) as f:
				data = json.load(f)
			clones = data.get("statistics", {}).get("total", {}).get("clones", 0)
			percentage = float(
				data.get("statistics", {}).get("total", {}).get("percentage") or 0
			)
		except (OSError, json.JSONDecodeError, KeyError):
			print(
				"check_code_duplication: failed to read jscpd report",
				file=sys.stderr,
			)
			return 1

		failed = False
		if clones > args.max_clones:
			print(
				f"Clone count {clones} exceeds threshold of {args.max_clones}", file=sys.stderr
			)
			failed = True
		if percentage > args.max_percentage:
			print(
				f"Duplication {percentage:.1f}% exceeds threshold of {args.max_percentage}%",
				file=sys.stderr,
			)
			failed = True

		return 1 if failed else 0


if __name__ == "__main__":
	sys.exit(main())
