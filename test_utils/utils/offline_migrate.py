import sys
import os
import json
import subprocess
import click

import frappe
from frappe.utils import get_datetime, cint
from frappe.model.sync import get_doc_files
from frappe.modules.import_file import read_doc_from_file, calculate_hash
from frappe.commands import pass_context


def get_customizations():
	"""
	frappe.modules.utils
	"""
	doctype_custom_fields_update = []

	for app_name in frappe.get_installed_apps():
		for module_name in frappe.local.app_modules.get(app_name) or []:
			folder = frappe.get_app_path(app_name, module_name, "custom")
			if not os.path.exists(folder):
				continue

			for fname in os.listdir(folder):
				if not fname.endswith(".json"):
					continue

				with open(os.path.join(folder, fname)) as f:
					data = json.loads(f.read())

				if not data.get("sync_on_migrate"):
					continue

				doctype = data["doctype"]

				if not frappe.db.exists("DocType", doctype):
					continue

				if data["custom_fields"]:

					key = "custom_fields"
					doctype_fieldname = "dt"
					doctypes = list(set(map(lambda row: row.get(doctype_fieldname), data[key])))

					for doc_type in doctypes:
						for d in data[key]:
							field = frappe.db.get_value(
								"Custom Field", {"dt": doc_type, "fieldname": d["fieldname"]}
							)
							if not field:
								# new field
								doctype_custom_fields_update.append(doc_type)
							else:
								custom_field = frappe.get_doc("Custom Field", field)
								if custom_field.modified < get_datetime(d.get("modified")):
									# updated field
									doctype_custom_fields_update.append(doc_type)

	return doctype_custom_fields_update


def get_doctypes_to_be_migrated():

	doctypes_to_be_migrated = []

	for app_name in frappe.get_installed_apps():
		files = []
		for module_name in frappe.local.app_modules.get(app_name) or []:
			folder = os.path.dirname(frappe.get_module(app_name + "." + module_name).__file__)
			files = get_doc_files(files=files, start_path=folder)

		for doc_path in files:
			try:
				docs = read_doc_from_file(doc_path)
			except OSError:
				print(f"{doc_path} missing")
				continue

			if not docs:
				continue

			calculated_hash = calculate_hash(doc_path)

			if not isinstance(docs, list):
				docs = [docs]

			for doc in docs:
				# modified timestamp in db, none if doctype's first import
				db_modified_timestamp = frappe.db.get_value(doc["doctype"], doc["name"], "modified")
				is_db_timestamp_latest = db_modified_timestamp and (
					get_datetime(doc.get("modified")) <= get_datetime(db_modified_timestamp)
				)

				if db_modified_timestamp:
					stored_hash = None
					if doc["doctype"] == "DocType":
						try:
							stored_hash = frappe.db.get_value(doc["doctype"], doc["name"], "migration_hash")
						except Exception:
							pass

					# if hash exists and is equal no need to update
					if stored_hash and stored_hash == calculated_hash:
						continue

					# if hash doesn't exist, check if db timestamp is same as json timestamp, add hash if from doctype
					if is_db_timestamp_latest and doc["doctype"] != "DocType":
						continue

				doctypes_to_be_migrated.append(doc["name"])

	return doctypes_to_be_migrated


def get_table_row_count(doctype):
	return frappe.db.sql(f"SELECT COUNT(*) FROM `tab{doctype}`")[0][0]


def get_create_statements(db_table):
	from frappe.model import log_types

	additional_definitions = ""
	engine = db_table.meta.get("engine") or "InnoDB"
	varchar_len = frappe.db.VARCHAR_LEN
	name_column = f"name varchar({varchar_len}) primary key"

	# columns
	column_defs = db_table.get_column_definitions()
	if column_defs:
		additional_definitions += ",\n".join(column_defs) + ",\n"

	# index
	index_defs = db_table.get_index_definitions()
	if index_defs:
		additional_definitions += ",\n".join(index_defs) + ",\n"

	# child table columns
	if db_table.meta.get("istable") or 0:
		additional_definitions += (
			",\n".join(
				(
					f"parent varchar({varchar_len})",
					f"parentfield varchar({varchar_len})",
					f"parenttype varchar({varchar_len})",
					"index parent(parent)",
				)
			)
			+ ",\n"
		)

	"""
	# creating sequence(s)
	if (not db_table.meta.issingle and db_table.meta.autoname == "autoincrement") or db_table.doctype in log_types:
		frappe.db.create_sequence(db_table.doctype, check_not_exists=True, cache=frappe.db.SEQUENCE_CACHE)

		# NOTE: not used nextval func as default as the ability to restore
		# database with sequences has bugs in mariadb and gives a scary error.
		# issue link: https://jira.mariadb.org/browse/MDEV-20070
		name_column = "name bigint primary key"
	"""

	# create table
	query = f"""create table `{db_table.table_name}` (
		{name_column},
		creation datetime(6),
		modified datetime(6),
		modified_by varchar({varchar_len}),
		owner varchar({varchar_len}),
		docstatus int(1) not null default '0',
		idx int(8) not null default '0',
		{additional_definitions}
		index modified(modified))
		ENGINE={engine}
		ROW_FORMAT=DYNAMIC
		CHARACTER SET=utf8mb4
		COLLATE=utf8mb4_unicode_ci"""

	return query


def get_actual_table_columns(doctype):
	"""
	Get actual columns that exist in the database table
	Returns a set of lowercase column names
	"""
	try:
		columns = frappe.db.sql(
			"""
			SELECT COLUMN_NAME
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = %s
			AND TABLE_NAME = %s
		""",
			(frappe.conf.db_name, f"tab{doctype}"),
			as_dict=1,
		)

		return {col["COLUMN_NAME"].lower() for col in columns}
	except Exception as e:
		print(f"Error getting columns for {doctype}: {e}")
		return set()


def get_alter_statements(db_table):
	"""
	Generate ALTER statements for schema changes
	Only include columns that don't already exist
	"""
	for col in db_table.columns.values():
		col.build_for_alter_table(db_table.current_columns.get(col.fieldname.lower()))

	actual_columns = get_actual_table_columns(db_table.doctype)

	statements = []

	for col in db_table.add_column:
		fieldname_lower = col.fieldname.lower()

		if fieldname_lower in db_table.current_columns:
			continue
		if fieldname_lower in actual_columns:
			continue

		statements.append(f"ADD COLUMN `{col.fieldname}` {col.get_definition()}")

	columns_to_modify = set(db_table.change_type + db_table.set_default)
	for col in columns_to_modify:
		fieldname_lower = col.fieldname.lower()

		if fieldname_lower in db_table.current_columns or fieldname_lower in actual_columns:
			statements.append(
				f"MODIFY `{col.fieldname}` {col.get_definition(for_modification=True)}"
			)

	for col in db_table.add_unique:
		statements.append(
			f"ADD UNIQUE INDEX IF NOT EXISTS {col.fieldname} (`{col.fieldname}`)"
		)

	for col in db_table.add_index:
		if not frappe.db.get_column_index(db_table.table_name, col.fieldname, unique=False):
			statements.append(f"ADD INDEX `{col.fieldname}_index`(`{col.fieldname}`)")

	if db_table.meta.sort_field == "creation" and not frappe.db.get_column_index(
		db_table.table_name, "creation", unique=False
	):
		statements.append("ADD INDEX `creation`(`creation`)")

	for col in {*db_table.drop_index, *db_table.drop_unique}:
		if col.fieldname == "name":
			continue

		current_column = db_table.current_columns.get(col.fieldname.lower())
		if not current_column:
			continue

		unique_constraint_changed = current_column.unique != col.unique
		if unique_constraint_changed and not col.unique:
			if unique_index := frappe.db.get_column_index(
				db_table.table_name, col.fieldname, unique=True
			):
				statements.append(f"DROP INDEX `{unique_index.Key_name}`")

		index_constraint_changed = current_column.index != col.set_index
		if index_constraint_changed and not col.set_index:
			if index_record := frappe.db.get_column_index(
				db_table.table_name, col.fieldname, unique=False
			):
				statements.append(f"DROP INDEX `{index_record.Key_name}`")

	return statements


def get_statements(doctype):
	from frappe.database.mariadb.schema import MariaDBTable
	from frappe.model.meta import Meta

	meta = Meta(doctype)
	db_table = MariaDBTable(doctype, meta)

	if db_table.is_new():
		statements = [get_create_statements(db_table)]
	else:
		statements = get_alter_statements(db_table)

	return db_table, statements


def get_large_tables_with_changes():
	threshold = frappe.get_hooks("row_threshold_for_offline_migrate")

	if not threshold:
		return []

	threshold = cint(threshold[0])
	large_tables = []

	print("\nChecking for schema changes...")

	doctypes_with_changes = list(set(get_doctypes_to_be_migrated() + get_customizations()))

	print(f"Found {len(doctypes_with_changes)} doctype(s) with potential changes")

	for doctype in doctypes_with_changes:
		try:
			count = get_table_row_count(doctype)

			if count >= threshold:
				actual_columns = get_actual_table_columns(doctype)
				print(f"\n  {doctype}: {count:,} rows")
				print(f"Existing columns: {len(actual_columns)}")

				db_table, statements = get_statements(doctype)

				if statements:
					if isinstance(statements, list):
						statements = [s for s in statements if s]

					if statements:
						large_tables.append((doctype, count, statements))
						print(f"Changes detected: {len(statements)} statement(s)")
						for stmt in statements[:3]:
							print(f"- {stmt[:60]}...")
					else:
						print("No changes needed (filtered out)")
				else:
					print("No changes needed")
		except Exception as e:
			print(f"Error checking {doctype}: {str(e)}")
			import traceback

			traceback.print_exc()
			continue

	return large_tables


def before_migrate():
	large_tables = get_large_tables_with_changes()

	if not large_tables:
		return

	print("\n" + "=" * 70)
	print("MIGRATION BLOCKED - Large Tables Detected")
	print("=" * 70)
	print(
		"\nThe following tables exceed the row threshold and require offline migration:\n"
	)

	for doctype, count, statements in large_tables:
		print(f"{doctype}")
		print(f"   Rows: {count:,}")
		print(f"   Changes: {len(statements)} statement(s)")
		for stmt in statements[:3]:
			print(f"   - {stmt[:80]}...")
		if len(statements) > 3:
			print(f"   ... and {len(statements) - 3} more")
		print()

	print("To migrate these tables offline, run:")
	print(f"\n   bench --site {frappe.local.site} offline-migrate\n")
	print("=" * 70 + "\n")
	sys.exit(1)


def run_pt_online_schema_change(
	doctype,
	statements,
	db_name=None,
	db_user=None,
	db_password=None,
	db_host=None,
	db_port=None,
	dry_run=False,
):
	""" """
	if not statements or (isinstance(statements, list) and not any(statements)):
		print(f"No changes needed for {doctype}")
		return True

	table = f"tab{doctype}"
	db_name = db_name or frappe.conf.db_name
	db_user = db_user or frappe.conf.db_name  # frappe.conf.db_user
	db_password = db_password or frappe.conf.db_password
	db_host = db_host or frappe.conf.get("db_host", "127.0.0.1")
	db_port = db_port or frappe.conf.get("db_port", 3306)

	if isinstance(statements, list):
		flat_statements = []
		for stmt in statements:
			if isinstance(stmt, list):
				flat_statements.extend([s for s in stmt if s])
			elif stmt:
				flat_statements.append(stmt)

		if not flat_statements:
			print(f"No changes needed for {doctype}")
			return True

		flat_statements = list(dict.fromkeys(flat_statements))
		alter_statement = ", ".join(flat_statements)
	else:
		alter_statement = statements

	if not alter_statement:
		print(f"No changes needed for {doctype}")
		return True

	dsn = f"D={db_name},t={table},h={db_host},P={db_port},u={db_user},p={db_password}"

	cmd = [
		"pt-online-schema-change",
		f"--alter={alter_statement}",
		dsn,
		"--no-check-replication-filters",
		"--alter-foreign-keys-method=auto",
		"--progress=percentage,1",
		"--chunk-size=1000",
		"--max-load=Threads_running=50",
		"--critical-load=Threads_running=100",
		"--no-check-alter",
	]

	if dry_run:
		cmd.append("--dry-run")
	else:
		cmd.append("--execute")

	print(f"\n{'='*70}")
	print(f"Migrating: {doctype}")
	print(f"{'='*70}")
	print(f"Alter statement: {alter_statement[:100]}...")
	print()

	try:
		result = subprocess.run(cmd, check=True, capture_output=True, text=True)
		print(result.stdout)
		print(f"Successfully migrated {doctype}\n")
		return True
	except subprocess.CalledProcessError as e:
		print(f"\nError migrating {doctype}")
		print(f"Exit code: {e.returncode}")

		# Try to run again with captured output for debugging
		print("\nRetrying with captured output for debugging...")
		result = subprocess.run(cmd, capture_output=True, text=True)

		if result.stdout:
			print("\nSTDOUT:")
			print(result.stdout)
		if result.stderr:
			print("\nSTDERR:")
			print(result.stderr)

		return False
	except FileNotFoundError:
		print("   pt-online-schema-change not found. Please install percona-toolkit:")
		print("   Ubuntu/Debian: sudo apt-get install percona-toolkit")
		print("   CentOS/RHEL: sudo yum install percona-toolkit")
		print("   macOS: brew install percona-toolkit")
		return False
	except Exception as e:
		print(f"Unexpected error: {type(e).__name__}: {str(e)}")
		return False
