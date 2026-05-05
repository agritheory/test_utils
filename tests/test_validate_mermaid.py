"""Tests for Mermaid validation in Markdown."""

from __future__ import annotations

import shutil
import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


@unittest.skipUnless(
	shutil.which("npx"), "npx is required for Mermaid validation tests"
)
class TestValidateMermaid(unittest.TestCase):
	def test_docs_sql_registry_succeeds(self) -> None:
		target = REPO_ROOT / "docs" / "sql_registry.md"
		r = subprocess.run(
			[
				sys.executable,
				"-m",
				"test_utils.pre_commit.validate_mermaid",
				str(target),
			],
			cwd=REPO_ROOT,
			capture_output=True,
			text=True,
			check=False,
		)
		self.assertEqual(r.returncode, 0, msg=r.stderr + r.stdout)

	def test_invalid_fence_reports_error(self) -> None:
		bad = REPO_ROOT / "tests" / "fixtures" / "mermaid_invalid.md"
		r = subprocess.run(
			[
				sys.executable,
				"-m",
				"test_utils.pre_commit.validate_mermaid",
				str(bad),
			],
			cwd=REPO_ROOT,
			capture_output=True,
			text=True,
			check=False,
		)
		self.assertNotEqual(r.returncode, 0)
		self.assertIn("mermaid:", r.stderr)


class TestLintMarkdownCli(unittest.TestCase):
	@unittest.skipUnless(shutil.which("npx"), "npx is required")
	def test_lint_markdown_delegates(self) -> None:
		target = REPO_ROOT / "docs" / "sql_registry.md"
		r = subprocess.run(
			[
				sys.executable,
				"-m",
				"test_utils.pre_commit.lint_markdown",
				str(target),
			],
			cwd=REPO_ROOT,
			capture_output=True,
			text=True,
			check=False,
		)
		self.assertEqual(r.returncode, 0, msg=r.stderr + r.stdout)
