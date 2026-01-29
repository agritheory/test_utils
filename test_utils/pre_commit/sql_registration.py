import argparse
import sys
from pathlib import Path
from collections.abc import Sequence
from test_utils.pre_commit.sql_rewriter_functions import SQLRewriter
from test_utils.utils.sql_registry import SQLRegistry


def main(argv: Sequence[str] = None):
	"""Pre-commit hook entry point for SQL registry operations"""

	parser = argparse.ArgumentParser(description="SQL Operations Registry")

	subparsers = parser.add_subparsers(dest="command", help="Available commands")

	# Scan command
	scan_parser = subparsers.add_parser("scan", help="Scan for SQL operations")
	scan_parser.add_argument(
		"filenames", nargs="*", help="Files to check (provided by pre-commit)"
	)
	scan_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	scan_parser.add_argument("--directory", help="Directory to scan (overrides filenames)")
	scan_parser.add_argument(
		"--scan-all", action="store_true", help="Scan entire repository"
	)

	# List command
	list_parser = subparsers.add_parser("list", help="List SQL calls")
	list_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	list_parser.add_argument("--file-filter", help="Filter by file path")

	# Show command
	show_parser = subparsers.add_parser("show", help="Show details for specific call")
	show_parser.add_argument("call_id", help="Call ID to show details for")
	show_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)

	# Rewrite command
	rewrite_parser = subparsers.add_parser(
		"rewrite", help="Rewrite SQL call to Query Builder"
	)
	rewrite_parser.add_argument("call_id", help="Call ID to rewrite")
	rewrite_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	rewrite_parser.add_argument(
		"--apply", action="store_true", help="Apply changes to file"
	)

	# Report command
	report_parser = subparsers.add_parser("report", help="Generate usage report")
	report_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)
	report_parser.add_argument("--output", help="Output file for report")

	# Todos command - list calls with TODO in their conversion
	todos_parser = subparsers.add_parser(
		"todos", help="List calls with TODO in conversion"
	)
	todos_parser.add_argument(
		"--registry", default=".sql_registry.pkl", help="Registry file path"
	)

	args = parser.parse_args(argv)

	if not args.command:
		print("Available commands: scan, list, show, rewrite, report, todos")
		print("\nExamples:")
		print("  sql_registry scan --directory /path/to/module")
		print("  sql_registry list --file-filter myfile.py")
		print("  sql_registry show abc123")
		print("  sql_registry todos")
		print("  sql_registry rewrite abc123 --apply")
		return 0

	registry = SQLRegistry(args.registry)

	if args.command == "scan":
		# Determine what to scan
		files_to_scan = []

		if hasattr(args, "scan_all") and args.scan_all:
			# Scan entire repository
			current_dir = Path.cwd()
			files_to_scan = list(current_dir.glob("**/*.py"))
			print(f"Scanning all Python files in repository ({len(files_to_scan)} files)")
		elif hasattr(args, "directory") and args.directory:
			# Scan entire directory
			directory_path = Path(args.directory)
			files_to_scan = list(directory_path.glob("**/*.py"))
			print(f"Scanning directory {args.directory} ({len(files_to_scan)} files)")
		else:
			# Only scan provided filenames that are Python files
			files_to_scan = [
				Path(f)
				for f in getattr(args, "filenames", [])
				if f.endswith(".py") and Path(f).exists()
			]

		if not files_to_scan:
			print("No Python files to scan")
			return 0

		# Register database operations in files
		new_operations = 0
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

			except Exception as e:
				print(f"Error scanning {file_path}: {e}")
				continue

		# Save registry if we found new operations
		if new_operations > 0:
			registry.save_registry()
			print(f"📊 Registered {new_operations} new SQL operations")

		print("✅ SQL operations scan completed")
		return 0

	elif args.command == "list":
		calls = registry.data["calls"]
		if not calls:
			print("No SQL calls found in registry. Run scan first.")
			return 0

		filtered_calls = []
		for call in calls.values():
			if (
				hasattr(args, "file_filter")
				and args.file_filter
				and not call.file_path.endswith(args.file_filter)
			):
				continue
			filtered_calls.append(call)

		if not filtered_calls:
			print("No SQL calls match the specified criteria.")
			return 0

		print(f"\n📋 Found {len(filtered_calls)} SQL calls:")
		print("=" * 80)

		for call in sorted(filtered_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			print(f"\n{call.call_id[:8]}  {file_name}:{call.line_number}")
			print(f"   Function: {call.function_context}")
			print(f"   SQL: {call.sql_query[:100]}{'...' if len(call.sql_query) > 100 else ''}")

		return 0

	elif args.command == "show":
		# Find call by ID (support partial IDs)
		matching_calls = [
			call
			for call in registry.data["calls"].values()
			if call.call_id.startswith(args.call_id)
		]

		if not matching_calls:
			print(f"No SQL call found with ID starting with '{args.call_id}'")
			return 1

		if len(matching_calls) > 1:
			print(f"Multiple calls match '{args.call_id}'. Please be more specific:")
			for call in matching_calls:
				print(f"  {call.call_id[:12]} - {Path(call.file_path).name}:{call.line_number}")
			return 1

		call = matching_calls[0]
		print(f"\n📝 SQL Call Details: {call.call_id}")
		print("=" * 60)
		print(f"File: {call.file_path}")
		print(f"Line: {call.line_number}")
		print(f"Function: {call.function_context}")
		print(f"Implementation: {call.implementation_type}")
		print(f"Created: {call.created_at}")
		print(f"Updated: {call.updated_at}")

		print("\n🔍 Original SQL:")
		print("-" * 40)
		print(call.sql_query)

		print("\n🏗️ Query Builder Equivalent:")
		print("-" * 40)
		print(call.query_builder_equivalent)

		if call.notes:
			print("\n📋 Notes:")
			print("-" * 40)
			print(call.notes)

		return 0

	elif args.command == "rewrite":
		rewriter = SQLRewriter(args.registry)
		dry_run = not getattr(args, "apply", False)

		success = rewriter.rewrite_sql(args.call_id, dry_run)
		return 0 if success else 1

	elif args.command == "report":
		report = registry.generate_report()
		if hasattr(args, "output") and args.output:
			Path(args.output).write_text(report)
			print(f"Report saved to {args.output}")
		else:
			print(report)
		return 0

	elif args.command == "todos":
		calls = registry.data["calls"]
		if not calls:
			print("No SQL calls found in registry. Run scan first.")
			return 0

		todo_calls = []
		for call in calls.values():
			if call.query_builder_equivalent and "# TODO" in call.query_builder_equivalent:
				todo_calls.append(call)

		if not todo_calls:
			print("✅ No TODOs found - all conversions complete!")
			return 0

		print(f"\n⚠️  Found {len(todo_calls)} calls with TODOs:")
		print("=" * 80)

		for call in sorted(todo_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			print(f"\n{call.call_id[:8]}  {file_name}:{call.line_number}")
			print(f"   Function: {call.function_context}")
			# Extract the TODO line
			for line in call.query_builder_equivalent.split("\n"):
				if "# TODO" in line:
					print(f"   TODO: {line.strip()}")

		return 0

	else:
		print(f"Unknown command: {args.command}")
		return 1


if __name__ == "__main__":
	sys.exit(main())
