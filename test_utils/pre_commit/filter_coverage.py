#!/usr/bin/env python3
"""
Filter coverage report to exclude files with 0% coverage.
"""

import sys
import re
from pathlib import Path


def filter_coverage_report(input_file, output_file, min_coverage=1):
	"""
	Filter coverage report to exclude files below minimum coverage threshold.

	Args:
	    input_file: Path to input coverage report
	    output_file: Path to output filtered report
	    min_coverage: Minimum coverage percentage to include (default: 1%)
	"""
	with open(input_file) as f:
		lines = f.readlines()

	filtered_lines = []
	skip_next = False

	for i, line in enumerate(lines):
		# Check if this line contains coverage percentage
		match = re.search(r"\s+(\d+)%\s*$", line)

		if match:
			coverage = int(match.group(1))
			if coverage < min_coverage:
				# Skip this line (it's below threshold)
				continue

		filtered_lines.append(line)

	with open(output_file, "w") as f:
		f.writelines(filtered_lines)

	print(f"Filtered coverage report written to {output_file}")
	print(f"Excluded files with coverage < {min_coverage}%")


def main():
	if len(sys.argv) < 2:
		print("Usage: python filter_coverage.py <input_file> [output_file] [min_coverage]")
		sys.exit(1)

	input_file = sys.argv[1]
	output_file = sys.argv[2] if len(sys.argv) > 2 else "filtered-coverage.txt"
	min_coverage = int(sys.argv[3]) if len(sys.argv) > 3 else 1

	if not Path(input_file).exists():
		print(f"Error: Input file '{input_file}' not found")
		sys.exit(1)

	filter_coverage_report(input_file, output_file, min_coverage)


if __name__ == "__main__":
	main()
