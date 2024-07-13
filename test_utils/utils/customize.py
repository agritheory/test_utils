import ast
import json
import sys
import tempfile
import types
from pathlib import Path

try:
	import frappe
	from frappe.cache_manager import clear_global_cache
	from frappe.custom.doctype.custom_field.custom_field import create_custom_fields
	from frappe.database.schema import add_column
	from frappe.modules import get_doctype_module
	from frappe.modules.import_file import calculate_hash
except Exception as e:
	raise (e)


def load_customizations():
	apps = frappe.get_installed_apps()
	frappe_apps = ["frappe", "erpnext", "hrms", "payments", "lms", "insights"]
	for app in apps:
		if app in frappe_apps:
			continue

		customizations_directory = Path().cwd().parent / "apps" / app / app / app / "custom"

		add_column(doctype="DocType", column_name="customization_hash", fieldtype="Text")
		clear_global_cache()

		files = list(customizations_directory.glob("**/*.json"))

		files_to_be_synced = get_files_to_be_synced(files)
		if len(files_to_be_synced) > 0:
			print(f"\nLoading {app} customizations")

		for i, file in enumerate(files_to_be_synced, start=1):
			customizations = json.loads(Path(file).read_text())
			doctype = customizations.get("doctype")

			modules = frappe.get_module_list(app)

			print(f"Updating {doctype} of ({i} of {len(files_to_be_synced)})")

			for module in modules:
				for field in customizations.get("custom_fields"):
					if field.get("module") != module:
						continue
					existing_field = frappe.get_value("Custom Field", field.get("name"))
					custom_field = (
						frappe.get_doc("Custom Field", field.get("name"))
						if existing_field
						else frappe.new_doc("Custom Field")
					)
					field.pop("modified")
					{custom_field.set(key, value) for key, value in field.items()}
					custom_field.flags.ignore_permissions = True
					custom_field.flags.ignore_version = True
					custom_field.save()
				for prop in customizations.get("property_setters"):
					if field.get("module") != module:
						continue
					property_setter = frappe.get_doc(
						{
							"name": prop.get("name"),
							"doctype": "Property Setter",
							"doctype_or_field": prop.get("doctype_or_field"),
							"doc_type": prop.get("doc_type"),
							"field_name": prop.get("field_name"),
							"property": prop.get("property"),
							"value": prop.get("value"),
							"property_type": prop.get("property_type"),
						}
					)
					property_setter.flags.ignore_permissions = True
					property_setter.insert()

	if "hrms" in apps:
		sync_hrms_customizations()


def get_files_to_be_synced(all_files):
	files = []
	for file in all_files:
		customizations = json.loads(Path(file).read_text())
		doctype = customizations.get("doctype")

		hash_updated = add_customization_hash(doctype, file)
		if not hash_updated:
			continue
		files.append(file)

	return files


def sync_hrms_customizations():
	app_dir = Path().cwd().parent / "apps"
	p = ast.parse((app_dir / "hrms" / "hrms" / "setup.py").read_text())
	for node in p.body[:]:
		if not isinstance(node, ast.FunctionDef) or node.name != "get_custom_fields":
			p.body.remove(node)
	module = types.ModuleType("hrms")
	code = compile(p, "setup.py", "exec")
	sys.modules["hrms"] = module
	exec(code, module.__dict__)
	import hrms

	hrms_custom_fields = hrms.get_custom_fields()
	create_custom_fields(hrms_custom_fields, ignore_validate=True)


def add_customization_hash(doctype, file):
	module = get_doctype_module(doctype)
	standard_json_file = frappe.get_module_path(
		module, "doctype", doctype, f"{frappe.scrub(doctype)}.json"
	)

	with open(standard_json_file, "rb") as file1, open(file, "rb") as file2:
		standard_file_data = file1.read()
		custom_file_data = file2.read()

	concatenated_data = standard_file_data + custom_file_data

	# Create a temporary file to store the concatenated data
	with tempfile.NamedTemporaryFile(delete=False) as temp_file:
		temp_file.write(concatenated_data)

	# calculate the hash of the concatenated data for standard and custom json
	temp_file_hash = calculate_hash(temp_file.name)

	stored_hash = frappe.db.get_value("DocType", doctype, "customization_hash")
	if not stored_hash:
		frappe.db.set_value(
			"DocType",
			doctype,
			"customization_hash",
			temp_file_hash,
			update_modified=False,
		)
		return True

	if stored_hash == temp_file_hash:
		return False

	frappe.db.set_value(
		"DocType",
		doctype,
		"customization_hash",
		temp_file_hash,
		update_modified=False,
	)

	return True
