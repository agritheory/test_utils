import argparse
import json
import os
import pathlib
import sys
import tempfile
from collections.abc import Sequence


def get_customized_doctypes_to_clean(app: str):
	customized_doctypes = {}

	import frappe

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

			from frappe.utils.export_file import strip_default_fields

			cleaned = strip_default_fields(file_contents)

			new_content = json.dumps(cleaned, sort_keys=True, indent=2)

			if new_content != original_content:
				with tempfile.NamedTemporaryFile("w", delete=False) as temp_file:
					temp_file.write(new_content)
					temp_path = temp_file.name

				os.replace(temp_path, customize_file)
				modified_files.append(str(customize_file))

	return modified_files


def is_frappe_bench_environment():
	"""
	Check if we're running in a valid Frappe bench environment

	Returns:
	        bool: True if valid Frappe bench, False otherwise
	"""
	# Get current working directory
	current_dir = pathlib.Path.cwd()

	# Look for bench structure - check current dir and parent dirs
	for path in [current_dir] + list(current_dir.parents):
		required_dirs = ["sites", "env", "apps"]
		if all((path / dirname).is_dir() for dirname in required_dirs):
			# Found the directory structure, now check for additional bench indicators
			bench_indicators = [
				"common_site_config.json",  # Common bench file
				"Procfile",  # Process file
				"apps.txt",  # Apps list (in sites folder)
			]

			# Check for bench files in the bench root or sites directory
			sites_dir = path / "sites"
			has_bench_files = any(
				(path / indicator).exists() for indicator in bench_indicators
			) or any((sites_dir / indicator).exists() for indicator in bench_indicators)

			if has_bench_files:
				return True

			# If we find the directory structure but no bench files,
			# we'll still consider it a bench (less strict check)
			return True

	return False


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument("filenames", nargs="*")
	parser.add_argument("--app", action="append", help="Target app to clean")
	args = parser.parse_args(argv)

	if not args.app:
		print("No app specified. Use --app <app_name>")
		sys.exit(1)

	app = args.app[0]
	if is_frappe_bench_environment():
		customized_doctypes = get_customized_doctypes_to_clean(app)
		modified_files = validate_and_clean_customized_doctypes(customized_doctypes)

	for modified_file in modified_files:
		print(f"File cleaned: {modified_file}")

	sys.exit(0)


if __name__ == "__main__":
	main()
