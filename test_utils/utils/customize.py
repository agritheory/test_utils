import json
from pathlib import Path
import tempfile

try:
	import frappe
	from frappe.database.schema import add_column
	from frappe.cache_manager import clear_global_cache
	from frappe.modules.import_file import calculate_hash
	from frappe.modules import get_doctype_module
except Exception as e:
	raise (e)


def load_customizations():
	apps = frappe.get_installed_apps()
	frappe_apps = ["frappe", "erpnext", "hrms", "payments", "lms", "insights"]
	for app in apps:
		if app in frappe_apps:
			continue

		print(f"Loading {app} customizations")
		customizations_directory = (
			Path().cwd().parent / "apps" / app / app / app / "custom"
		)

		add_column(
			doctype="DocType", column_name="customization_hash", fieldtype="Text"
		)
		clear_global_cache()

		files = list(customizations_directory.glob("**/*.json"))
		for file in files:
			customizations = json.loads(Path(file).read_text())
			doctype = customizations.get("doctype")

			hash_updated = add_customization_hash(doctype, file)
			if not hash_updated:
				continue

			modules = frappe.get_module_list(app)
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

	# calucate the hash of the concatenated data for standard and custom json
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
