import argparse


def find_unused():
	"""Find unused custom fields in the database."""
	print("Finding unused custom fields...")
	# TODO: Implement logic to find unused custom fields
	print("Feature not implemented yet.")


def drop_unused_custom_fields(
	doctype: str, fields: list, field_types: list, columns: list, dry_run: bool = False
):
	"""Drop unused custom fields from the database."""
	import frappe

	if len(fields) != len(field_types):
		raise ValueError("Lengths of inputs fields and field_types are not equal")

	table_name = f"tab{doctype}"

	print("====== Altering doctype ======")
	for field, field_type in zip(fields, field_types):
		if dry_run:
			print(f"[DRY RUN] ALTER TABLE `{table_name}` MODIFY `{field}` {field_type};")
		else:
			print(f"ALTER TABLE `{table_name}` MODIFY `{field}` {field_type};")
			frappe.db.sql_ddl(f"ALTER TABLE `{table_name}` MODIFY `{field}` {field_type};")

	print("====== Dropping Columns ======")
	for column in columns:
		if dry_run:
			print('[DRY RUN] frappe.delete_doc_if_exists("Custom Field", "{doctype}-{column}")')
			print(f"[DRY RUN] ALTER TABLE `{table_name}` DROP COLUMN IF EXISTS `{column}`;")
		else:
			print(f'frappe.delete_doc_if_exists("Custom Field", "{doctype}-{column}")')
			frappe.delete_doc_if_exists("Custom Field", f"{doctype}-{column}")
			print(f"ALTER TABLE `{table_name}` DROP COLUMN IF EXISTS `{column}`;")
			frappe.db.sql_ddl(f"ALTER TABLE `{table_name}` DROP COLUMN IF EXISTS `{column}`;")


def main():
	"""Main CLI entry point."""
	parser = argparse.ArgumentParser(description="Custom fields management utility")
	subparsers = parser.add_subparsers(dest="command", help="Available commands")

	# find-unused command
	subparsers.add_parser("find-unused", help="Find unused custom fields in the database")

	# drop-unused command
	drop_parser = subparsers.add_parser(
		"drop-unused",
		help="Drop unused custom fields",
		epilog="""
Example:
  custom_fields drop-unused --doctype "Sales Order" --fields parent parentfield parenttype --field-types TEXT TEXT TEXT --columns company_gstin custom_field_unused --dry-run
		""".strip(),
		formatter_class=argparse.RawDescriptionHelpFormatter,
	)
	drop_parser.add_argument(
		"--dry-run",
		action="store_true",
		help="Show what would be dropped without actually dropping",
	)
	drop_parser.add_argument("--doctype", required=True, help="DocType to clean up")
	drop_parser.add_argument(
		"--fields", nargs="+", required=True, help="Fields to modify (space-separated)"
	)
	drop_parser.add_argument(
		"--field-types",
		nargs="+",
		required=True,
		help="Field types corresponding to fields (space-separated)",
	)
	drop_parser.add_argument(
		"--columns", nargs="+", required=True, help="Columns to drop (space-separated)"
	)

	args = parser.parse_args()

	if args.command == "find-unused":
		find_unused()
	elif args.command == "drop-unused":
		drop_unused_custom_fields(
			doctype=args.doctype,
			fields=args.fields,
			field_types=args.field_types,
			columns=args.columns,
			dry_run=args.dry_run,
		)
	else:
		parser.print_help()


if __name__ == "__main__":
	main()
