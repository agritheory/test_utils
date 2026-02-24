"""CLI for sql_registry."""

import argparse
import sys
from pathlib import Path

from test_utils.utils.sql_registry.registry import SQLRegistry


def main():
	parser = argparse.ArgumentParser(description="SQL Operations Registry")

	subparsers = parser.add_subparsers(dest="command", help="Available commands")

	scan_parser = subparsers.add_parser("scan", help="Scan directory for SQL operations")
	scan_parser.add_argument("--directory", default=".", help="Directory to scan")
	scan_parser.add_argument(
		"--registry", default=".sql_registry.json", help="Registry file path"
	)
	scan_parser.add_argument(
		"--include-patches",
		action="store_true",
		help="Include SQL in patch files (excluded by default - patches are one-time migrations)",
	)
	scan_parser.add_argument(
		"--force",
		action="store_true",
		help=(
			"Attempt to scan files that do not compile using a regex fallback. "
			"SQL content from broken files will be incomplete (line numbers only)."
		),
	)

	report_parser = subparsers.add_parser("report", help="Generate usage report")
	report_parser.add_argument(
		"--registry", default=".sql_registry.json", help="Registry file path"
	)
	report_parser.add_argument("--output", help="Output file for report")

	list_parser = subparsers.add_parser("list", help="List SQL calls")
	list_parser.add_argument(
		"--registry", default=".sql_registry.json", help="Registry file path"
	)
	list_parser.add_argument("--file-filter", help="Filter by file path")

	show_parser = subparsers.add_parser("show", help="Show details for specific call")
	show_parser.add_argument("call_id", help="Call ID to show details")
	show_parser.add_argument(
		"--registry", default=".sql_registry.json", help="Registry file path"
	)

	rewrite_parser = subparsers.add_parser(
		"rewrite", help="Rewrite SQL call to Query Builder"
	)
	rewrite_parser.add_argument("call_id", help="Call ID to rewrite")
	rewrite_parser.add_argument(
		"--registry", default=".sql_registry.json", help="Registry file path"
	)
	rewrite_parser.add_argument(
		"--apply", action="store_true", help="Apply changes to file"
	)
	rewrite_parser.add_argument(
		"--force",
		action="store_true",
		help=(
			"Override two safety gates: (1) attempt MANUAL-flagged calls using the "
			"unvalidated generated code stored in the registry, and (2) write the "
			"output even if it does not compile. A .bak backup is always created. "
			"The result will almost certainly need manual cleanup."
		),
	)

	todos_parser = subparsers.add_parser(
		"todos", help="List calls with TODO in conversion"
	)
	todos_parser.add_argument(
		"--registry", default=".sql_registry.json", help="Registry file path"
	)

	orm_parser = subparsers.add_parser(
		"orm", help="List calls converted to simple ORM (frappe.get_all)"
	)
	orm_parser.add_argument(
		"--registry", default=".sql_registry.json", help="Registry file path"
	)

	args = parser.parse_args()

	if not args.command:
		parser.print_help()
		return

	registry = SQLRegistry(args.registry)

	if args.command == "scan":
		print("Scanning for SQL operations...")
		count = registry.scan_directory(
			Path(args.directory),
			include_patches=getattr(args, "include_patches", False),
			force=getattr(args, "force", False),
		)
		registry.save_registry()
		print(f"Found and registered {count} SQL operations")

	elif args.command == "report":
		report = registry.generate_report()
		if args.output:
			Path(args.output).write_text(report)
			print(f"Report saved to {args.output}")
		else:
			print(report)

	elif args.command == "list":
		calls = registry.data["calls"]
		if not calls:
			print("No SQL calls found in registry.")
			return

		filtered_calls = []
		for call in calls.values():
			if args.file_filter and not call.file_path.endswith(args.file_filter):
				continue
			filtered_calls.append(call)

		print(f"\nFound {len(filtered_calls)} SQL calls:")
		print("=" * 80)

		for call in sorted(filtered_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			print(f"\n{call.call_id[:8]}  {file_name}:{call.line_number}")
			print(f"   Function: {call.function_context}")
			print(f"   SQL: {call.sql_query[:100]}{'...' if len(call.sql_query) > 100 else ''}")

	elif args.command == "show":
		matching_calls = [
			call
			for call in registry.data["calls"].values()
			if call.call_id.startswith(args.call_id)
		]

		if not matching_calls:
			print(f"No SQL call found with ID starting with '{args.call_id}'")
			return

		if len(matching_calls) > 1:
			print(f"Multiple calls match '{args.call_id}'. Please be more specific:")
			for call in matching_calls:
				print(f"  {call.call_id[:12]} - {Path(call.file_path).name}:{call.line_number}")
			return

		call = matching_calls[0]
		print(f"\nSQL Call Details: {call.call_id}")
		print("=" * 60)
		print(f"File: {call.file_path}")
		print(f"Line: {call.line_number}")
		print(f"Function: {call.function_context}")
		print(f"Variable: {call.variable_name or 'None'}")
		print(f"Implementation: {call.implementation_type}")
		print(f"Created: {call.created_at}")
		print(f"Updated: {call.updated_at}")

		if call.sql_params:
			print("\nSQL Parameters:")
			print("-" * 40)
			for key, value in call.sql_params.items():
				print(f"  {key}: {value}")

		if call.sql_kwargs:
			print("\nSQL Kwargs:")
			print("-" * 40)
			for key, value in call.sql_kwargs.items():
				print(f"  {key}: {value}")

		print("\nOriginal SQL:")
		print("-" * 40)
		print(call.sql_query)

		is_manual = "# MANUAL:" in call.query_builder_equivalent
		is_orm = "frappe.get_all(" in call.query_builder_equivalent
		has_todo = "# TODO" in call.query_builder_equivalent

		print("\nQuery Builder Equivalent:")
		print("-" * 40)
		if is_manual:
			print("🔧 [MANUAL REVIEW REQUIRED] Validation failed - needs manual conversion")
		elif is_orm:
			print("💡 [ORM-ELIGIBLE] Can use frappe.get_all instead of frappe.db.sql")
		elif has_todo:
			print("⚠️  [HAS TODO] Needs manual review")
		print(call.query_builder_equivalent)

		if call.notes:
			print("\nNotes:")
			print("-" * 40)
			print(call.notes)

	elif args.command == "rewrite":
		from test_utils.pre_commit.sql_rewriter_functions import SQLRewriter

		rewriter = SQLRewriter(args.registry)
		dry_run = not args.apply
		force = getattr(args, "force", False)

		success = rewriter.rewrite_sql(args.call_id, dry_run, force=force)
		if not success:
			sys.exit(1)

	elif args.command == "todos":
		calls = registry.data["calls"]
		if not calls:
			print("No SQL calls found in registry.")
			return

		todo_calls = []
		for call in calls.values():
			if call.query_builder_equivalent and "# TODO" in call.query_builder_equivalent:
				todo_calls.append(call)

		if not todo_calls:
			print("✅ No TODOs found - all conversions complete!")
			return

		print(f"\n⚠️  Found {len(todo_calls)} calls with TODOs:")
		print("=" * 80)

		for call in sorted(todo_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			print(f"\n{call.call_id[:8]}  {file_name}:{call.line_number}")
			print(f"   Function: {call.function_context}")
			for line in call.query_builder_equivalent.split("\n"):
				if "# TODO" in line:
					print(f"   {line.strip()}")

	elif args.command == "orm":
		calls = registry.data["calls"]
		if not calls:
			print("No SQL calls found in registry.")
			return

		orm_calls = []
		for call in calls.values():
			if (
				call.query_builder_equivalent and "frappe.get_all(" in call.query_builder_equivalent
			):
				orm_calls.append(call)

		if not orm_calls:
			print("No ORM-eligible calls found.")
			return

		print(f"\n🔄 Found {len(orm_calls)} calls that can use simple ORM (frappe.get_all):")
		print("=" * 80)
		print(
			"These are simple queries that could be refactored to use frappe.get_all/get_value"
		)
		print("instead of frappe.db.sql - often quick wins!\n")

		for call in sorted(orm_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			print(f"{call.call_id[:8]}  {file_name}:{call.line_number}")
			print(f"   Function: {call.function_context}")
			print(f"   SQL: {call.sql_query[:80]}{'...' if len(call.sql_query) > 80 else ''}")


if __name__ == "__main__":
	main()
