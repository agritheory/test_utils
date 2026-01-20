#!/usr/bin/env python3
"""
SQL Pipeline - Orchestrates incremental SQL-to-Query-Builder conversions.

This tool manages the conversion of frappe.db.sql calls to Query Builder
in chunks that can be tested against both MariaDB and PostgreSQL backends.
"""

import argparse
import json
import os
import pickle
import subprocess
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from test_utils.utils.sql_registry import SQLRegistry, SQLCall
from test_utils.pre_commit.sql_rewriter_functions import SQLRewriter


# ANSI color codes
class Colors:
	RED = "\033[91m"
	GREEN = "\033[92m"
	YELLOW = "\033[93m"
	BLUE = "\033[94m"
	MAGENTA = "\033[95m"
	CYAN = "\033[96m"
	WHITE = "\033[97m"
	RESET = "\033[0m"
	BOLD = "\033[1m"

	@classmethod
	def disable(cls):
		"""Disable colors for non-TTY output"""
		for attr in [
			"RED",
			"GREEN",
			"YELLOW",
			"BLUE",
			"MAGENTA",
			"CYAN",
			"WHITE",
			"RESET",
			"BOLD",
		]:
			setattr(cls, attr, "")


if not sys.stdout.isatty():
	Colors.disable()


@dataclass
class Chunk:
	"""Represents a unit of work for SQL conversion."""

	chunk_id: str
	chunk_type: str  # 'test', 'report', 'orm', 'qb'
	app: str
	module: str
	files: list[str]
	call_ids: list[str]
	estimated_loc: int
	status: str = "pending"  # pending, in_progress, completed, failed
	created_at: str = field(default_factory=lambda: datetime.now().isoformat())
	completed_at: str | None = None
	commit_hash: str | None = None
	notes: str | None = None


@dataclass
class PipelineState:
	"""Tracks pipeline progress for an app."""

	app: str
	app_path: str
	registry_path: str
	chunks: dict[str, Chunk] = field(default_factory=dict)
	last_scan: str | None = None
	last_chunk_generation: str | None = None

	def to_dict(self) -> dict:
		return {
			"app": self.app,
			"app_path": self.app_path,
			"registry_path": self.registry_path,
			"chunks": {k: asdict(v) for k, v in self.chunks.items()},
			"last_scan": self.last_scan,
			"last_chunk_generation": self.last_chunk_generation,
		}

	@classmethod
	def from_dict(cls, data: dict) -> "PipelineState":
		chunks = {k: Chunk(**v) for k, v in data.get("chunks", {}).items()}
		return cls(
			app=data["app"],
			app_path=data["app_path"],
			registry_path=data["registry_path"],
			chunks=chunks,
			last_scan=data.get("last_scan"),
			last_chunk_generation=data.get("last_chunk_generation"),
		)


class ChunkGenerator:
	"""Generates work chunks from SQL registry based on configured rules."""

	# Chunk type rules
	MAX_LOC_ORM = 250
	MAX_LOC_QB = 250
	MAX_FILES_ORM = 5
	MAX_FILES_QB = 5

	def __init__(self, registry: SQLRegistry, app: str, app_path: str):
		self.registry = registry
		self.app = app
		self.app_path = Path(app_path)
		self.calls = list(registry.data["calls"].values())

	def generate_chunks(self) -> list[Chunk]:
		"""Generate all chunks based on rules."""
		chunks = []

		# Categorize calls by file type and characteristics
		test_files = {}
		report_files = {}
		orm_calls_by_module = {}
		qb_calls_by_module = {}

		for call in self.calls:
			file_path = Path(call.file_path)
			file_name = file_path.name
			module = self._extract_module(file_path)

			# Categorize by file type
			if file_name.startswith("test_") or "/tests/" in str(file_path):
				if file_path not in test_files:
					test_files[file_path] = []
				test_files[file_path].append(call)

			elif "/report/" in str(file_path) or file_name.endswith("_report.py"):
				if file_path not in report_files:
					report_files[file_path] = []
				report_files[file_path].append(call)

			else:
				# Categorize by conversion type
				is_orm = self._is_orm_eligible(call)
				if is_orm:
					if module not in orm_calls_by_module:
						orm_calls_by_module[module] = []
					orm_calls_by_module[module].append(call)
				else:
					if module not in qb_calls_by_module:
						qb_calls_by_module[module] = []
					qb_calls_by_module[module].append(call)

		# Generate test file chunks (whole files)
		for file_path, calls in test_files.items():
			chunk = self._create_test_chunk(file_path, calls)
			chunks.append(chunk)

		# Generate report chunks (atomic)
		for file_path, calls in report_files.items():
			chunk = self._create_report_chunk(file_path, calls)
			chunks.append(chunk)

		# Generate ORM chunks (by module, with constraints)
		for module, calls in orm_calls_by_module.items():
			module_chunks = self._create_constrained_chunks(
				module, calls, "orm", self.MAX_LOC_ORM, self.MAX_FILES_ORM
			)
			chunks.extend(module_chunks)

		# Generate QB chunks (by module, with constraints)
		for module, calls in qb_calls_by_module.items():
			module_chunks = self._create_constrained_chunks(
				module, calls, "qb", self.MAX_LOC_QB, self.MAX_FILES_QB
			)
			chunks.extend(module_chunks)

		return chunks

	def _extract_module(self, file_path: Path) -> str:
		"""Extract module name from file path."""
		parts = file_path.parts
		# Look for common module patterns in Frappe apps
		for i, part in enumerate(parts):
			if part in ["doctype", "report", "api", "controllers", "utils", "patches"]:
				if i > 0:
					return parts[i - 1]
			if part == self.app:
				# Return next meaningful directory
				if i + 1 < len(parts):
					return parts[i + 1]
		return "misc"

	def _is_orm_eligible(self, call: SQLCall) -> bool:
		"""Check if a call is ORM-eligible (simple frappe.get_all)."""
		if call.query_builder_equivalent:
			return "frappe.get_all(" in call.query_builder_equivalent
		return False

	def _estimate_loc(self, calls: list[SQLCall]) -> int:
		"""Estimate lines of code that will change."""
		total = 0
		for call in calls:
			# Estimate: original SQL lines + QB equivalent lines
			original_lines = call.sql_query.count("\n") + 1
			qb_lines = (
				call.query_builder_equivalent.count("\n") + 1
				if call.query_builder_equivalent
				else 0
			)
			total += original_lines + qb_lines
		return total

	def _create_test_chunk(self, file_path: Path, calls: list[SQLCall]) -> Chunk:
		"""Create a chunk for an entire test file."""
		file_name = file_path.stem
		chunk_id = f"{self.app}-test-{file_name.replace('test_', '')}"

		return Chunk(
			chunk_id=chunk_id,
			chunk_type="test",
			app=self.app,
			module="tests",
			files=[str(file_path)],
			call_ids=[c.call_id for c in calls],
			estimated_loc=self._estimate_loc(calls),
		)

	def _create_report_chunk(self, file_path: Path, calls: list[SQLCall]) -> Chunk:
		"""Create a chunk for a report file."""
		file_name = file_path.stem
		module = self._extract_module(file_path)
		chunk_id = f"{self.app}-report-{file_name.replace('_report', '')}"

		return Chunk(
			chunk_id=chunk_id,
			chunk_type="report",
			app=self.app,
			module=module,
			files=[str(file_path)],
			call_ids=[c.call_id for c in calls],
			estimated_loc=self._estimate_loc(calls),
		)

	def _create_constrained_chunks(
		self,
		module: str,
		calls: list[SQLCall],
		chunk_type: str,
		max_loc: int,
		max_files: int,
	) -> list[Chunk]:
		"""Create chunks respecting LOC and file constraints."""
		chunks = []
		current_calls = []
		current_files = set()
		current_loc = 0
		chunk_num = 1

		# Sort calls by file to group them
		sorted_calls = sorted(calls, key=lambda c: c.file_path)

		for call in sorted_calls:
			call_loc = self._estimate_loc([call])
			call_file = call.file_path

			# Check if adding this call would exceed constraints
			would_exceed_loc = current_loc + call_loc > max_loc
			would_exceed_files = (
				call_file not in current_files and len(current_files) >= max_files
			)

			if current_calls and (would_exceed_loc or would_exceed_files):
				# Create chunk from current batch
				chunk = self._finalize_chunk(
					module, chunk_type, chunk_num, current_calls, current_files, current_loc
				)
				chunks.append(chunk)
				chunk_num += 1

				# Reset for next batch
				current_calls = []
				current_files = set()
				current_loc = 0

			# Add call to current batch
			current_calls.append(call)
			current_files.add(call_file)
			current_loc += call_loc

		# Don't forget the last batch
		if current_calls:
			chunk = self._finalize_chunk(
				module, chunk_type, chunk_num, current_calls, current_files, current_loc
			)
			chunks.append(chunk)

		return chunks

	def _finalize_chunk(
		self,
		module: str,
		chunk_type: str,
		chunk_num: int,
		calls: list[SQLCall],
		files: set[str],
		loc: int,
	) -> Chunk:
		"""Create a finalized chunk from accumulated calls."""
		suffix = f"-{chunk_num}" if chunk_num > 1 else ""
		chunk_id = f"{self.app}-{module}-{chunk_type}{suffix}"

		return Chunk(
			chunk_id=chunk_id,
			chunk_type=chunk_type,
			app=self.app,
			module=module,
			files=list(files),
			call_ids=[c.call_id for c in calls],
			estimated_loc=loc,
		)


class SQLPipeline:
	"""Main pipeline orchestrator."""

	def __init__(self, app: str, app_path: str):
		self.app = app
		self.app_path = Path(app_path).resolve()
		self.state_dir = self.app_path / ".sql_pipeline"
		self.state_file = self.state_dir / "progress.json"
		self.chunks_file = self.state_dir / "chunks.json"
		self.registry_file = self.state_dir / ".sql_registry.pkl"

		# Ensure state directory exists
		self.state_dir.mkdir(exist_ok=True)

		# Load or create state
		self.state = self._load_state()

	def _load_state(self) -> PipelineState:
		"""Load pipeline state from disk."""
		if self.state_file.exists():
			try:
				with open(self.state_file) as f:
					data = json.load(f)
				return PipelineState.from_dict(data)
			except Exception as e:
				print(
					f"{Colors.YELLOW}Warning: Could not load state ({e}), creating new{Colors.RESET}"
				)

		return PipelineState(
			app=self.app,
			app_path=str(self.app_path),
			registry_path=str(self.registry_file),
		)

	def _save_state(self):
		"""Save pipeline state to disk."""
		with open(self.state_file, "w") as f:
			json.dump(self.state.to_dict(), f, indent=2)

	def scan(self) -> int:
		"""Scan app for SQL operations and update registry."""
		print(f"{Colors.BOLD}Scanning {self.app} for SQL operations...{Colors.RESET}")
		print(f"App path: {self.app_path}")

		registry = SQLRegistry(str(self.registry_file))
		count = registry.scan_directory(self.app_path, pattern="**/*.py")
		registry.save_registry()

		self.state.last_scan = datetime.now().isoformat()
		self._save_state()

		print(f"{Colors.GREEN}Found {count} SQL operations{Colors.RESET}")
		return count

	def generate_chunks(self) -> list[Chunk]:
		"""Generate work chunks from registry."""
		if not self.registry_file.exists():
			print(f"{Colors.RED}No registry found. Run 'scan' first.{Colors.RESET}")
			return []

		print(f"{Colors.BOLD}Generating chunks for {self.app}...{Colors.RESET}")

		registry = SQLRegistry(str(self.registry_file))
		generator = ChunkGenerator(registry, self.app, str(self.app_path))
		chunks = generator.generate_chunks()

		# Update state with new chunks
		for chunk in chunks:
			if chunk.chunk_id not in self.state.chunks:
				self.state.chunks[chunk.chunk_id] = chunk
			else:
				# Preserve status of existing chunks
				existing = self.state.chunks[chunk.chunk_id]
				chunk.status = existing.status
				chunk.completed_at = existing.completed_at
				chunk.commit_hash = existing.commit_hash
				self.state.chunks[chunk.chunk_id] = chunk

		self.state.last_chunk_generation = datetime.now().isoformat()
		self._save_state()

		# Also save chunks to separate file for easy inspection
		with open(self.chunks_file, "w") as f:
			json.dump([asdict(c) for c in chunks], f, indent=2)

		print(f"{Colors.GREEN}Generated {len(chunks)} chunks{Colors.RESET}")
		return chunks

	def list_chunks(
		self, filter_status: str | None = None, filter_type: str | None = None
	):
		"""List all chunks with their status."""
		if not self.state.chunks:
			print(f"{Colors.YELLOW}No chunks found. Run 'chunks generate' first.{Colors.RESET}")
			return

		# Group by type
		by_type = {"test": [], "report": [], "orm": [], "qb": []}
		for chunk in self.state.chunks.values():
			if filter_status and chunk.status != filter_status:
				continue
			if filter_type and chunk.chunk_type != filter_type:
				continue
			by_type[chunk.chunk_type].append(chunk)

		# Print summary
		total = sum(len(v) for v in by_type.values())
		completed = sum(1 for c in self.state.chunks.values() if c.status == "completed")
		print(
			f"\n{Colors.BOLD}Chunks for {self.app}: {total} total, {completed} completed{Colors.RESET}"
		)
		print("=" * 70)

		type_colors = {
			"test": Colors.CYAN,
			"report": Colors.MAGENTA,
			"orm": Colors.GREEN,
			"qb": Colors.BLUE,
		}

		status_icons = {
			"pending": "○",
			"in_progress": "◐",
			"completed": "●",
			"failed": "✗",
		}

		for chunk_type in ["test", "report", "orm", "qb"]:
			chunks = by_type[chunk_type]
			if not chunks:
				continue

			color = type_colors[chunk_type]
			print(f"\n{color}{Colors.BOLD}{chunk_type.upper()} ({len(chunks)}){Colors.RESET}")

			for chunk in sorted(chunks, key=lambda c: c.chunk_id):
				icon = status_icons.get(chunk.status, "?")
				files_str = f"{len(chunk.files)} file{'s' if len(chunk.files) > 1 else ''}"
				ops_str = f"{len(chunk.call_ids)} ops"
				loc_str = f"~{chunk.estimated_loc} LOC"

				status_color = Colors.GREEN if chunk.status == "completed" else Colors.RESET
				print(
					f"  {icon} {color}{chunk.chunk_id}{Colors.RESET} "
					f"[{files_str}, {ops_str}, {loc_str}] "
					f"{status_color}{chunk.status}{Colors.RESET}"
				)

	def show_chunk(self, chunk_id: str):
		"""Show detailed information about a chunk."""
		# Find chunk by prefix match
		matches = [c for c in self.state.chunks.values() if c.chunk_id.startswith(chunk_id)]

		if not matches:
			print(f"{Colors.RED}No chunk found matching '{chunk_id}'{Colors.RESET}")
			return

		if len(matches) > 1:
			print(f"{Colors.YELLOW}Multiple chunks match '{chunk_id}':{Colors.RESET}")
			for c in matches:
				print(f"  - {c.chunk_id}")
			return

		chunk = matches[0]
		registry = SQLRegistry(str(self.registry_file))

		print(f"\n{Colors.BOLD}Chunk: {chunk.chunk_id}{Colors.RESET}")
		print("=" * 60)
		print(f"Type: {chunk.chunk_type}")
		print(f"Module: {chunk.module}")
		print(f"Status: {chunk.status}")
		print(f"Estimated LOC: {chunk.estimated_loc}")
		print(f"Created: {chunk.created_at}")
		if chunk.completed_at:
			print(f"Completed: {chunk.completed_at}")
		if chunk.commit_hash:
			print(f"Commit: {chunk.commit_hash}")

		print(f"\n{Colors.BOLD}Files ({len(chunk.files)}):{Colors.RESET}")
		for f in chunk.files:
			print(f"  - {f}")

		print(f"\n{Colors.BOLD}SQL Operations ({len(chunk.call_ids)}):{Colors.RESET}")
		for call_id in chunk.call_ids:
			if call_id in registry.data["calls"]:
				call = registry.data["calls"][call_id]
				preview = call.sql_query.replace("\n", " ")[:60]
				print(f"  {call_id[:8]} L{call.line_number}: {preview}...")

	def apply_chunk(
		self, chunk_id: str, dry_run: bool = True, commit: bool = False
	) -> bool:
		"""Apply conversions for a chunk using batch processing."""
		# Find chunk
		matches = [c for c in self.state.chunks.values() if c.chunk_id.startswith(chunk_id)]

		if not matches:
			print(f"{Colors.RED}No chunk found matching '{chunk_id}'{Colors.RESET}")
			return False

		if len(matches) > 1:
			print(f"{Colors.YELLOW}Multiple chunks match. Be more specific.{Colors.RESET}")
			return False

		chunk = matches[0]

		if chunk.status == "completed":
			print(f"{Colors.YELLOW}Chunk already completed{Colors.RESET}")
			return True

		print(f"\n{Colors.BOLD}Applying chunk: {chunk.chunk_id}{Colors.RESET}")
		print(f"Type: {chunk.chunk_type}, Operations: {len(chunk.call_ids)}")

		if dry_run:
			print(f"{Colors.CYAN}DRY RUN - no changes will be made{Colors.RESET}")

		# Update status
		chunk.status = "in_progress"
		self._save_state()

		# Use batch rewrite to handle multiple calls per file correctly
		rewriter = SQLRewriter(str(self.registry_file))
		success_count, failed_ids = rewriter.rewrite_batch(
			chunk.call_ids, dry_run=dry_run, backup=True
		)

		print(f"\n{Colors.BOLD}Results:{Colors.RESET}")
		print(f"  Successful: {success_count}/{len(chunk.call_ids)}")
		if failed_ids:
			print(f"  Failed: {len(failed_ids)}")
			for fid in failed_ids:
				print(f"    - {fid[:12]}")

		if not dry_run:
			if failed_ids:
				chunk.status = "failed"
				chunk.notes = f"Failed IDs: {', '.join(f[:12] for f in failed_ids)}"
			else:
				chunk.status = "completed"
				chunk.completed_at = datetime.now().isoformat()

			self._save_state()

			if commit and not failed_ids:
				self._create_commit(chunk)

		return len(failed_ids) == 0

	def _create_commit(self, chunk: Chunk):
		"""Create a git commit for the chunk."""
		# Determine commit message based on chunk type
		type_prefixes = {
			"test": "test",
			"report": "refactor",
			"orm": "refactor",
			"qb": "refactor",
		}
		prefix = type_prefixes.get(chunk.chunk_type, "refactor")

		scope = chunk.module if chunk.module != "tests" else chunk.chunk_type
		msg = f"{prefix}({scope}): convert SQL to Query Builder"
		if chunk.chunk_type == "test":
			msg = f"test({chunk.module}): convert SQL to Query Builder in tests"

		try:
			# Stage changed files
			for f in chunk.files:
				subprocess.run(["git", "add", f], cwd=self.app_path, check=True)

			# Create commit
			result = subprocess.run(
				["git", "commit", "-m", msg],
				cwd=self.app_path,
				capture_output=True,
				text=True,
			)

			if result.returncode == 0:
				# Get commit hash
				hash_result = subprocess.run(
					["git", "rev-parse", "HEAD"],
					cwd=self.app_path,
					capture_output=True,
					text=True,
				)
				chunk.commit_hash = hash_result.stdout.strip()[:7]
				self._save_state()
				print(f"{Colors.GREEN}Committed: {chunk.commit_hash} - {msg}{Colors.RESET}")
			else:
				print(f"{Colors.RED}Commit failed: {result.stderr}{Colors.RESET}")

		except Exception as e:
			print(f"{Colors.RED}Error creating commit: {e}{Colors.RESET}")

	def validate(self, site: str, test_filter: str | None = None) -> bool:
		"""Run tests against a specific site."""
		print(f"\n{Colors.BOLD}Validating against site: {site}{Colors.RESET}")

		cmd = ["bench", "--site", site, "run-parallel-tests", "--app", self.app]
		if test_filter:
			cmd.extend(["--test", test_filter])

		try:
			result = subprocess.run(cmd, cwd=self.app_path.parent.parent)
			return result.returncode == 0
		except Exception as e:
			print(f"{Colors.RED}Error running tests: {e}{Colors.RESET}")
			return False

	def status(self):
		"""Show overall pipeline status."""
		print(f"\n{Colors.BOLD}Pipeline Status: {self.app}{Colors.RESET}")
		print("=" * 50)
		print(f"App path: {self.app_path}")
		print(f"State dir: {self.state_dir}")
		print(f"Last scan: {self.state.last_scan or 'Never'}")
		print(f"Last chunk generation: {self.state.last_chunk_generation or 'Never'}")

		if self.registry_file.exists():
			registry = SQLRegistry(str(self.registry_file))
			print(f"\nRegistry: {len(registry.data['calls'])} SQL operations")

		if self.state.chunks:
			by_status = {}
			for chunk in self.state.chunks.values():
				by_status[chunk.status] = by_status.get(chunk.status, 0) + 1

			print(f"\nChunks: {len(self.state.chunks)} total")
			for status, count in sorted(by_status.items()):
				icon = "●" if status == "completed" else "○"
				print(f"  {icon} {status}: {count}")

			# Progress bar
			completed = by_status.get("completed", 0)
			total = len(self.state.chunks)
			pct = (completed / total * 100) if total > 0 else 0
			bar_width = 30
			filled = int(bar_width * completed / total) if total > 0 else 0
			bar = "█" * filled + "░" * (bar_width - filled)
			print(f"\nProgress: [{bar}] {pct:.1f}%")


def main():
	parser = argparse.ArgumentParser(
		description="SQL Pipeline - Orchestrate SQL to Query Builder conversions",
		formatter_class=argparse.RawDescriptionHelpFormatter,
		epilog="""
Examples:
  sql_pipeline scan --app hrms --app-path ~/apps/hrms
  sql_pipeline chunks generate --app hrms
  sql_pipeline chunks list --app hrms
  sql_pipeline chunks apply hrms-test-salary-slip --app hrms
  sql_pipeline validate --app hrms --site test_postgres
  sql_pipeline status --app hrms
""",
	)

	parser.add_argument("--app", required=True, help="App name (e.g., hrms, erpnext)")
	parser.add_argument(
		"--app-path", help="Path to app (defaults to ~/ironwood/apps/<app>)"
	)

	subparsers = parser.add_subparsers(dest="command", help="Commands")

	# scan command
	subparsers.add_parser("scan", help="Scan app for SQL operations")

	# chunks command
	chunks_parser = subparsers.add_parser("chunks", help="Manage work chunks")
	chunks_subparsers = chunks_parser.add_subparsers(dest="chunks_command")

	chunks_subparsers.add_parser("generate", help="Generate chunks from registry")

	list_parser = chunks_subparsers.add_parser("list", help="List chunks")
	list_parser.add_argument(
		"--status", choices=["pending", "in_progress", "completed", "failed"]
	)
	list_parser.add_argument("--type", choices=["test", "report", "orm", "qb"])

	show_parser = chunks_subparsers.add_parser("show", help="Show chunk details")
	show_parser.add_argument("chunk_id", help="Chunk ID (can be prefix)")

	apply_parser = chunks_subparsers.add_parser("apply", help="Apply chunk conversions")
	apply_parser.add_argument("chunk_id", help="Chunk ID to apply")
	apply_parser.add_argument(
		"--no-dry-run", action="store_true", help="Actually apply changes"
	)
	apply_parser.add_argument(
		"--commit", action="store_true", help="Create git commit after apply"
	)

	# validate command
	validate_parser = subparsers.add_parser("validate", help="Run tests against a site")
	validate_parser.add_argument(
		"--site", required=True, help="Site name (e.g., test_postgres)"
	)
	validate_parser.add_argument("--test", help="Specific test filter")

	# status command
	subparsers.add_parser("status", help="Show pipeline status")

	args = parser.parse_args()

	if not args.command:
		parser.print_help()
		sys.exit(1)

	# Determine app path
	app_path = args.app_path
	if not app_path:
		# Default to ~/ironwood/apps/<app>
		app_path = Path.home() / "ironwood" / "apps" / args.app
		if not app_path.exists():
			# Try current directory structure
			app_path = Path.cwd() / "apps" / args.app

	if not Path(app_path).exists():
		print(f"{Colors.RED}App path not found: {app_path}{Colors.RESET}")
		print("Use --app-path to specify the correct location")
		sys.exit(1)

	pipeline = SQLPipeline(args.app, str(app_path))

	if args.command == "scan":
		pipeline.scan()

	elif args.command == "chunks":
		if args.chunks_command == "generate":
			pipeline.generate_chunks()
		elif args.chunks_command == "list":
			pipeline.list_chunks(filter_status=args.status, filter_type=args.type)
		elif args.chunks_command == "show":
			pipeline.show_chunk(args.chunk_id)
		elif args.chunks_command == "apply":
			dry_run = not args.no_dry_run
			pipeline.apply_chunk(args.chunk_id, dry_run=dry_run, commit=args.commit)
		else:
			chunks_parser.print_help()

	elif args.command == "validate":
		success = pipeline.validate(args.site, test_filter=args.test)
		sys.exit(0 if success else 1)

	elif args.command == "status":
		pipeline.status()


if __name__ == "__main__":
	main()
