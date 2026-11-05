import argparse
import json
import pathlib
import sys
import tempfile
import shutil
from collections.abc import Sequence

from test_utils.pre_commit.validate_customizations import _resolve_app_dir

CUSTOM_FIELD_DEFAULTS = {
	"allow_in_quick_entry": 0,
	"allow_on_submit": 0,
	"bold": 0,
	"collapsible": 0,
	"columns": 0,
	"fetch_if_empty": 0,
	"hidden": 0,
	"hide_border": 0,
	"hide_days": 0,
	"hide_seconds": 0,
	"ignore_user_permissions": 0,
	"ignore_xss_filter": 0,
	"in_global_search": 0,
	"in_list_view": 0,
	"in_preview": 0,
	"in_standard_filter": 0,
	"is_system_generated": 0,
	"is_virtual": 0,
	"length": 0,
	"no_copy": 0,
	"non_negative": 0,
	"permlevel": 0,
	"print_hide": 0,
	"print_hide_if_no_value": 0,
	"read_only": 0,
	"report_hide": 0,
	"reqd": 0,
	"search_index": 0,
	"show_dashboard": 0,
	"sort_options": 0,
	"translatable": 0,
	"unique": 0,
}

PROPERTY_SETTER_DEFAULTS = {
	"idx": 0,
	"is_system_generated": 0,
}


def is_frappe_bench_environment():
	current_dir = pathlib.Path.cwd()

	for path in [current_dir] + list(current_dir.parents):
		required_dirs = ["sites", "env", "apps"]
		if all((path / dirname).is_dir() for dirname in required_dirs):
			bench_indicators = [
				"common_site_config.json",
				"Procfile",
				"apps.txt",
			]

			sites_dir = path / "sites"
			has_bench_files = any(
				(path / indicator).exists() for indicator in bench_indicators
			) or any((sites_dir / indicator).exists() for indicator in bench_indicators)

			if has_bench_files:
				return True

			return True

	return False


def validate_and_clean_customized_doctypes(
	customized_doctypes: dict[str, list[pathlib.Path]]
):
	modified_files = []
	for doctype, customize_files in customized_doctypes.items():
		for customize_file in customize_files:
			with open(customize_file) as f:
				file_contents = json.load(f)

			original_content = json.dumps(file_contents, sort_keys=True, indent=2)

			doc_export = file_contents

			cleaned = strip_default_fields(file_contents, doc_export)

			new_content = json.dumps(cleaned, sort_keys=True, indent=2)

			if new_content != original_content:
				with tempfile.NamedTemporaryFile("w", delete=False) as temp_file:
					temp_file.write(new_content)
					temp_path = temp_file.name

				shutil.move(temp_path, customize_file)
				modified_files.append(str(customize_file))

		if modified_files:
			print(f"Cleaned fields in {customize_file}")


def strip_default_fields(doc: dict, doc_export: bool = False):
	"""
	Standalone reimplementation of frappe.utils.export_file.strip_default_fields.
	Cleans out system fields from exported DocType JSON and its children.
	"""

	DEFAULT_FIELDS = {
		"owner",
		"creation",
		"modified",
		"modified_by",
		"docstatus",
		"parent",
		"parentfield",
		"parenttype",
	}

	CHILD_TABLE_FIELDS = {
		"owner",
		"creation",
		"modified",
		"modified_by",
		"parent",
		"parentfield",
		"parenttype",
		"docstatus",
	}

	EXTRA_DEFAULT_KEYS = {"_assign", "_comments", "_liked_by", "_user_tags", "_seen"}

	PRESERVE_KEYS = {"property_setters", "custom_fields", "value"}
	DEFAULT_KEYS = DEFAULT_FIELDS | EXTRA_DEFAULT_KEYS

	# Rows in these top-level lists are matched back to their owning doctype by
	# "parent" during sync_customizations_for_doctype, so it must not be stripped.
	PRESERVE_PARENT_FOR_KEYS = {"links", "custom_perms"}

	def strip_customization_value(key, val, record_type):
		if key == "default" and val is None:
			return True
		if record_type == "custom_field" and key in CUSTOM_FIELD_DEFAULTS:
			return val == CUSTOM_FIELD_DEFAULTS[key]
		if record_type == "property_setter" and key in PROPERTY_SETTER_DEFAULTS:
			return val == PROPERTY_SETTER_DEFAULTS[key]
		return False

	def clean(obj, is_child=False, preserve_parent=False, record_type=None):
		if isinstance(obj, dict):
			new_obj = {}

			for key, val in obj.items():
				if key in DEFAULT_KEYS and not (key == "parent" and preserve_parent):
					continue
				if record_type and strip_customization_value(key, val, record_type):
					continue
				if key not in PRESERVE_KEYS and val in (None, "", [], {}):
					continue

				if key == "custom_fields" and isinstance(val, list):
					new_obj[key] = [
						clean(v, record_type="custom_field") for v in val if v not in (None, "", [], {})
					]
				elif key == "property_setters" and isinstance(val, list):
					new_obj[key] = [
						clean(v, record_type="property_setter")
						for v in val
						if v not in (None, "", [], {})
					]
				elif isinstance(val, (dict, list)):
					new_obj[key] = clean(
						val,
						is_child=is_child,
						preserve_parent=key in PRESERVE_PARENT_FOR_KEYS,
						record_type=record_type,
					)
				else:
					new_obj[key] = val

			if new_obj.get("doctype") == "DocType":
				new_obj.pop("migration_hash", None)

				for field in new_obj.get("fields", []):
					if field.get("fieldtype") == "Table" and "options" in field:
						child_table_name = field["options"]
						for child in new_obj.get(child_table_name, []):
							clean(child, is_child=True)

			if is_child and not preserve_parent:
				for field in CHILD_TABLE_FIELDS:
					new_obj.pop(field, None)

			return new_obj

		elif isinstance(obj, list):
			return [
				clean(
					v,
					is_child=is_child,
					preserve_parent=preserve_parent,
					record_type=record_type,
				)
				for v in obj
				if v not in (None, "", [], {})
			]

		else:
			return obj

	return clean(doc)


def get_customized_doctypes(app_dir: pathlib.Path | None = None):
	customized_doctypes = {}
	this_app = (app_dir or pathlib.Path().resolve()).resolve()
	modules_file = this_app / this_app.stem / "modules.txt"
	if not modules_file.exists():
		return customized_doctypes
	modules = modules_file.read_text().split("\n")
	for module in modules:
		if not (this_app / this_app.stem / scrub(module) / "custom").exists():
			continue
		for custom_file in list(
			(this_app / this_app.stem / scrub(module) / "custom").glob("**/*.json")
		):
			if custom_file.stem in customized_doctypes:
				customized_doctypes[custom_file.stem].append(custom_file.resolve())
			else:
				customized_doctypes[custom_file.stem] = [custom_file.resolve()]

	return dict(sorted(customized_doctypes.items()))


def scrub(txt: str) -> str:
	"""Returns sluggified string. e.g. `Sales Order` becomes `sales_order`."""
	return txt.replace(" ", "_").replace("-", "_").lower()


def unscrub(txt: str) -> str:
	"""Returns titlified string. e.g. `sales_order` becomes `Sales Order`."""
	return txt.replace("_", " ").replace("-", " ").title()


def clean_customizations(app_dir: pathlib.Path):
	customized_doctypes = get_customized_doctypes(app_dir)
	exceptions = validate_and_clean_customized_doctypes(customized_doctypes)
	return exceptions


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument("filenames", nargs="*")
	parser.add_argument("--app", action="append", help="Target app to clean")
	args = parser.parse_args(argv)

	if not is_frappe_bench_environment():
		sys.exit(0)

	app_name = args.app[0] if args.app else None
	app_dir = _resolve_app_dir(app_name)
	exceptions = clean_customizations(app_dir)
	if exceptions:
		for exception in list(set(exceptions)):
			print(exception)

	sys.exit(1) if exceptions else sys.exit(0)


if __name__ == "__main__":
	main()
