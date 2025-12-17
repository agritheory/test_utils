import argparse
import json
import pathlib
import sys
import tempfile
import shutil
from collections.abc import Sequence


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

	PRESERVE_KEYS = {"property_setters", "custom_fields", "value", "default"}
	DEFAULT_KEYS = DEFAULT_FIELDS | EXTRA_DEFAULT_KEYS

	def clean(obj, is_child=False):
		if isinstance(obj, dict):
			new_obj = {}

			for key, val in obj.items():
				if key in DEFAULT_KEYS:
					continue
				if key not in PRESERVE_KEYS and val in (None, "", [], {}):
					continue

				if isinstance(val, (dict, list)):
					new_obj[key] = clean(val, is_child=is_child)
				else:
					new_obj[key] = val

			if new_obj.get("doctype") == "DocType":
				new_obj.pop("migration_hash", None)

				for field in new_obj.get("fields", []):
					if field.get("fieldtype") == "Table" and "options" in field:
						child_table_name = field["options"]
						for child in new_obj.get(child_table_name, []):
							clean(child, is_child=True)

			if is_child:
				for field in CHILD_TABLE_FIELDS:
					new_obj.pop(field, None)

			return new_obj

		elif isinstance(obj, list):
			return [clean(v, is_child=is_child) for v in obj if v not in (None, "", [], {})]

		else:
			return obj

	return clean(doc)


def get_customized_doctypes():
	customized_doctypes = {}
	this_app = pathlib.Path().resolve()
	modules = (this_app / this_app.stem / "modules.txt").read_text().split("\n")
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


def clean_customizations():
	customized_doctypes = get_customized_doctypes()
	exceptions = validate_and_clean_customized_doctypes(customized_doctypes)
	return exceptions


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument("filenames", nargs="*")
	parser.add_argument("--app", action="append", help="Target app to clean")
	args = parser.parse_args(argv)

	if is_frappe_bench_environment():
		exceptions = clean_customizations()
		if exceptions:
			for exception in list(set(exceptions)):
				print(exception)

		sys.exit(1) if exceptions else sys.exit(0)


if __name__ == "__main__":
	main()
