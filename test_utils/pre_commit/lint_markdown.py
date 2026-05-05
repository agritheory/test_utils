#!/usr/bin/env python3
"""
Markdown checks for test_utils (extend here as new rules are added).

Currently runs :func:`test_utils.pre_commit.validate_mermaid.run` only.
"""

from __future__ import annotations

import sys

from test_utils.pre_commit.validate_mermaid import argv_tail_for_cli, run


def main() -> int:
	return run(argv_tail_for_cli())


if __name__ == "__main__":
	sys.exit(main())
