import sys
import os
import json

import frappe
from frappe.utils import get_datetime
from frappe.model.sync import get_doc_files
from frappe.modules.import_file import read_doc_from_file, calculate_hash


def get_customizations():
	"""
	frappe.models.utils
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
				update_schema = False

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

		l = len(files)
		if l:
			for i, doc_path in enumerate(files):
				try:
					docs = read_doc_from_file(doc_path)
				except OSError:
					print(f"{doc_path} missing")
					return

				calculated_hash = calculate_hash(doc_path)

				if docs:
					if not isinstance(docs, list):
						docs = [docs]

					for doc in docs:
						# modified timestamp in db, none if doctype's first import
						db_modified_timestamp = frappe.db.get_value(
							doc["doctype"], doc["name"], "modified"
						)
						is_db_timestamp_latest = db_modified_timestamp and (
							get_datetime(doc.get("modified")) <= get_datetime(db_modified_timestamp)
						)

						if db_modified_timestamp:
							stored_hash = None
							if doc["doctype"] == "DocType":
								try:
									stored_hash = frappe.db.get_value(
										doc["doctype"], doc["name"], "migration_hash"
									)
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


def before_migrate():
	threshold = frappe.get_hooks("row_threshold_for_offline_migrate")

	if not threshold:
		return

	threshold = threshold[0]
	large_tables = []

	doctypes_with_changes = get_doctypes_to_be_migrated() + get_customizations()

	for doctype in doctypes_with_changes:
		count = get_table_row_count(doctype)
		if count >= threshold:
			large_tables.append((doctype, count))

	if large_tables:
		print("\nThe following tables exceed the row threshold for online migration:")
		for doctype, count in large_tables:
			print(f"  - {doctype}: {count} rows")
		print("\nPlease abort and use the offline migration command..")
		sys.exit(1)
