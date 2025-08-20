import argparse
import json
import os
import pathlib
import sys
import tempfile
from collections.abc import Sequence

import frappe
from frappe.utils.export_file import strip_default_fields


def get_customized_doctypes_to_clean(app: str):
	customized_doctypes = {}

	app_dir = pathlib.Path(frappe.get_app_path(app)).resolve()
	if not app_dir.is_dir():
		return customized_doctypes

	modules_txt = app_dir / "modules.txt"
	if not modules_txt.exists():
		return customized_doctypes

	modules = modules_txt.read_text().splitlines()
	for module in modules:
		custom_dir = app_dir / frappe.scrub(module) / "custom"
		if not custom_dir.exists():
			continue

		for custom_file in custom_dir.glob("**/*.json"):
			customized_doctypes.setdefault(custom_file.stem, []).append(custom_file.resolve())

	return customized_doctypes


def validate_and_clean_customized_doctypes(
	customized_doctypes: dict[str, list[pathlib.Path]]
):
	modified_files = []
	for doctype, customize_files in customized_doctypes.items():
		for customize_file in customize_files:
			with open(customize_file) as f:
				file_contents = json.load(f)

			original_content = json.dumps(file_contents, sort_keys=True, indent=2)

			cleaned = strip_default_fields(file_contents)

			new_content = json.dumps(cleaned, sort_keys=True, indent=2)

			if new_content != original_content:
				with tempfile.NamedTemporaryFile("w", delete=False) as temp_file:
					temp_file.write(new_content)
					temp_path = temp_file.name

				os.replace(temp_path, customize_file)
				modified_files.append(str(customize_file))

	return modified_files


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument("filenames", nargs="*")
	parser.add_argument("--app", action="append", help="Target app to clean")
	args = parser.parse_args(argv)

	if not args.app:
		print("No app specified. Use --app <app_name>")
		sys.exit(1)

	app = args.app[0]
	customized_doctypes = get_customized_doctypes_to_clean(app)
	modified_files = validate_and_clean_customized_doctypes(customized_doctypes)

	for modified_file in modified_files:
		print(f"File cleaned: {modified_file}")

	sys.exit(0)


if __name__ == "__main__":
	main()
