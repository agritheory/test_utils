import argparse
import sys
from pathlib import Path
from collections.abc import Sequence

try:
	from test_utils.utils.sql_registry import SQLRegistry
except ImportError:
	sys.path.append(str(Path(__file__).parent.parent / "utils"))
	from sql_registry import SQLRegistry


def main(argv: Sequence[str] = None):
	"""Pre-commit hook entry point for SQL registry tracking"""
	script_name = Path(sys.argv[0]).name if sys.argv else ""
	is_rewriter_mode = (
		script_name == "sql_rewriter_cli"
		or (argv and "--rewriter-mode" in argv)
		or any("rewriter" in str(arg) for arg in (argv or []))
	)

	parser = argparse.ArgumentParser(
		description="Track database operations for long-term stability"
	)
	parser.add_argument(
		"filenames", nargs="*", help="Files to check (provided by pre-commit)"
	)
	parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	parser.add_argument("--directory", help="Directory to scan (overrides filenames)")
	parser.add_argument(
		"--scan-all",
		action="store_true",
		help="Scan entire repository instead of just provided filenames",
	)

	# Rewriter mode arguments
	if is_rewriter_mode:
		parser.add_argument(
			"--rewriter-mode", action="store_true", help="Enable rewriter CLI functionality"
		)
		parser.add_argument("--list", action="store_true", help="List SQL calls")
		parser.add_argument("--show", help="Show details for call ID")
		parser.add_argument("--rewrite", help="Rewrite SQL call by ID")
		parser.add_argument("--apply", action="store_true", help="Apply rewrite changes")
		parser.add_argument("--no-backup", action="store_true", help="Skip backup creation")
		parser.add_argument("--file-filter", help="Filter by file path")

	args = parser.parse_args(argv)

	# Check if we're in rewriter mode
	if is_rewriter_mode or getattr(args, "rewriter_mode", False):
		try:
			from sql_rewriter_functions import SQLRewriter
		except ImportError:
			# Try alternative import path - don't re-import sys
			sys.path.append(str(Path(__file__).parent))
			from sql_rewriter_functions import SQLRewriter

		rewriter = SQLRewriter(args.registry)

		if args.list:
			rewriter.list_sql_calls(args.file_filter, args.complex_only)
			return 0
		elif args.show:
			rewriter.show_sql_details(args.show)
			return 0
		elif args.rewrite:
			dry_run = not args.apply
			backup = not args.no_backup
			success = rewriter.rewrite_sql(args.rewrite, dry_run, backup)
			return 0 if success else 1
		else:
			print("Rewriter mode requires --list, --show, or --rewrite")
			print("\nExamples:")
			print("  pre-commit run sql_rewriter -- --list")
			print("  pre-commit run sql_rewriter -- --show abc123")
			print("  pre-commit run sql_rewriter -- --rewrite abc123 --apply")
			return 1

	registry = SQLRegistry(args.registry)

	# Determine what to scan
	files_to_scan = []

	if args.scan_all:
		# Scan entire repository
		current_dir = Path.cwd()
		files_to_scan = list(current_dir.glob("**/*.py"))
		print(f"Scanning all Python files in repository ({len(files_to_scan)} files)")
	elif args.directory:
		# Scan entire directory
		directory_path = Path(args.directory)
		files_to_scan = list(directory_path.glob("**/*.py"))
	else:
		# Only scan provided filenames that are Python files
		files_to_scan = [
			Path(f) for f in args.filenames if f.endswith(".py") and Path(f).exists()
		]

	if not files_to_scan:
		print("No Python files to scan")
		return 0

	# Register database operations in files
	new_operations = 0
	high_complexity_operations = []

	for file_path in files_to_scan:
		try:
			# Count operations before scanning
			old_operations = [
				c for c in registry.data["calls"].values() if c.file_path == str(file_path)
			]
			old_count = len(old_operations)

			file_new_ops = registry._scan_file(file_path)

			file_operations = [
				c for c in registry.data["calls"].values() if c.file_path == str(file_path)
			]
			new_count = len(file_operations)

			operations_added = new_count - old_count
			new_operations += operations_added

			# Check for high complexity operations
			for op in file_operations:
				if op.complexity_score > 0.7:
					high_complexity_operations.append(
						(str(file_path), op.line_number, op.complexity_score)
					)

		except Exception as e:
			print(f"Error scanning {file_path}: {e}")
			continue

	# Save registry if we found new operations
	if new_operations > 0:
		registry.save_registry()
		print(f"📊 Registered {new_operations} new database operations")

	# Handle complexity warnings/failures
	if high_complexity_operations:
		if args.warn_complex or args.fail_on_complex:
			print(f"⚠️  Found {len(high_complexity_operations)} high-complexity operations:")
			for file_path, line, complexity in high_complexity_operations:
				rel_path = (
					Path(file_path).relative_to(Path.cwd())
					if Path(file_path).is_absolute()
					else file_path
				)
				print(f"   {rel_path}:{line} (complexity: {complexity:.2f})")

			if args.fail_on_complex:
				print("❌ Failing due to high-complexity database operations")
				print("   Consider reviewing these for potential optimization or simplification")
				return 1
			else:
				print("   Consider reviewing these for potential optimization or simplification")

	if new_operations > 0:
		print("✅ Database operations registered for stability tracking")

	return 0


if __name__ == "__main__":
	sys.exit(main())
