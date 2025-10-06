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


def find_bench_root(start: pathlib.Path | None = None) -> pathlib.Path | None:
	"""
	Walk upwards from start (or cwd) looking for a Frappe bench root.

	Returns:
	    Path to bench root if found, else None
	"""
	if start is None:
		start = pathlib.Path.cwd()

	indicators = [
		"sites/common_site_config.json",
		"sites/apps.txt",
		"sites/apps.json",
		"apps/frappe/pyproject.toml",
	]

	for path in [start] + list(start.parents):
		if all((path / d).is_dir() for d in ["sites", "apps", "env"]):
			if any((path / ind).exists() for ind in indicators):
				return path
	return None


def ensure_bench_python():
	"""
	Ensure we are running inside the bench virtualenv's python.
	If not, re-exec ourselves with that interpreter.
	"""
	bench_root = find_bench_root()
	if not bench_root:
		return False

	bench_python = bench_root / "env" / "bin" / "python"
	if not bench_python.exists():
		return False

	# Already running under bench python?
	if pathlib.Path(sys.executable).resolve() == bench_python.resolve():
		return True

	# Re-execute with bench python
	os.execv(str(bench_python), [str(bench_python)] + sys.argv)
	return True


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument("filenames", nargs="*")
	parser.add_argument("--app", action="append", help="Target app to clean")
	args = parser.parse_args(argv)

	if not args.app:
		print("No app specified. Use --app <app_name>")
		sys.exit(1)

	if not ensure_bench_python():
		print("Not inside a valid Frappe bench environment.")
		sys.exit(1)

	app = args.app[0]
	customized_doctypes = get_customized_doctypes_to_clean(app)
	modified_files = validate_and_clean_customized_doctypes(customized_doctypes)

	for modified_file in modified_files:
		print(f"File cleaned: {modified_file}")

	sys.exit(0)


if __name__ == "__main__":
	main()
