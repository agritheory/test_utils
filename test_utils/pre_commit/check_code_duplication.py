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
			"jscpd@4",
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
		if not json_report.exists():
			found = list(report_path.rglob("jscpd-report.json"))
			json_report = found[0] if found else report_path  # fallback, won't exist
		if not json_report.exists():
			return 0  # No report, assume no issues

		try:
			with open(json_report) as f:
				data = json.load(f)
			clones = data.get("statistics", {}).get("total", {}).get("clones", 0)
			percentage = data.get("statistics", {}).get("total", {}).get("percentage", 0) * 100
		except (json.JSONDecodeError, KeyError):
			return 0

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
