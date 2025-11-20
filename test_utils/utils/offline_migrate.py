import sys
import os
import json
import subprocess
from contextlib import contextmanager

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


def get_custom_fields_from_disk(doctype):
	custom_fields = []
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

				if data.get("custom_fields"):
					for cf in data["custom_fields"]:
						if cf.get("dt") == doctype:
							custom_fields.append(cf)
	return custom_fields


@contextmanager
def capture_sql_ddl():
	statements = []
	original_sql_ddl = frappe.db.sql_ddl

	def capture(query):
		statements.append(query)

	frappe.db.sql_ddl = capture
	try:
		yield statements
	finally:
		frappe.db.sql_ddl = original_sql_ddl


def get_statements(doctype):
	from frappe.database.mariadb.schema import MariaDBTable
	from frappe.model.meta import Meta
	from frappe.modules.utils import get_doc_path

	# Force reload meta to ensure we have the latest JSON changes
	frappe.clear_cache(doctype=doctype)

	meta = None
	module = frappe.db.get_value("DocType", doctype, "module")
	if module:
		try:
			doc_path = get_doc_path(module, "DocType", doctype)
			json_path = os.path.join(doc_path, f"{frappe.scrub(doctype)}.json")
			if os.path.exists(json_path):
				with open(json_path) as f:
					doc_dict = json.load(f)
				# Create in-memory DocType from JSON
				doc = frappe.get_doc(doc_dict)
				meta = Meta(doc)
		except Exception as e:
			print(f"Failed to load meta from file for {doctype}: {e}")

	if not meta:
		meta = Meta(doctype)

	# Inject custom fields from disk
	custom_fields = get_custom_fields_from_disk(doctype)
	if custom_fields:
		existing_fields = {f.fieldname: f for f in meta.fields}
		for cf in custom_fields:
			if cf["fieldname"] in existing_fields:
				existing_fields[cf["fieldname"]].update(cf)
			else:
				meta.fields.append(frappe._dict(cf))

	db_table = MariaDBTable(doctype, meta)

	# Compare JSON meta with Database schema
	# This populates db_table.columns, db_table.current_columns, and change lists
	db_table.setup_table_columns()

	statements = []

	if db_table.is_new():
		pass
	else:
		with capture_sql_ddl() as captured:
			db_table.alter()

		for query in captured:
			# query is like "ALTER TABLE `tabSales Order` ADD COLUMN ..."
			# We need "ADD COLUMN ..."
			prefix = f"ALTER TABLE `{db_table.table_name}` "
			if query.startswith(prefix):
				statements.append(query[len(prefix) :])
			else:
				print(f"Warning: Unexpected query format: {query}")

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
				print(f"\n  {doctype}: {count:,} rows")

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
