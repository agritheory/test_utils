import argparse
import datetime
import sys
import tempfile
import shutil
from collections.abc import Sequence


def validate_copyright(app, files):
	year = datetime.datetime.now().year
	app_publisher = ""

	# Handle both string and dictionary inputs
	if isinstance(app, str):
		# String input: infer Frappe app and read from hooks.py
		hooks_file = f"{app}/hooks.py"
		with open(hooks_file) as file:
			for line in file:
				if "app_publisher" in line:
					app_publisher = line.split("=")[1].strip().replace('"', "")
	elif isinstance(app, dict):
		# Dictionary input: extract app_publisher directly
		app_publisher = app.get("app_publisher", "")
	else:
		raise ValueError(
			"app must be either a string (app path) or dictionary with 'app_publisher' key"
		)

	initial_js_string = "// Copyright (c) "
	initial_py_string = "# Copyright (c) "
	initial_md_string = "<!-- Copyright (c) "
	initial_sql_string = "-- Copyright (c) "

	copyright_js_string = f"// Copyright (c) {year}, {app_publisher} and contributors\n// For license information, please see license.txt\n\n"
	copyright_py_string = f"# Copyright (c) {year}, {app_publisher} and contributors\n# For license information, please see license.txt\n\n"
	copyright_md_string = f"<!-- Copyright (c) {year}, {app_publisher} and contributors\nFor license information, please see license.txt-->\n\n"
	copyright_sql_string = f"-- Copyright (c) {year}, {app_publisher} and contributors\n-- For license information, please see license.txt\n\n"

	for file in files:
		if file.endswith(".js") or file.endswith(".ts"):
			validate_and_write_file(file, initial_js_string, copyright_js_string)

		elif file.endswith(".py"):
			validate_and_write_file(file, initial_py_string, copyright_py_string)

		elif file.endswith(".md"):
			validate_and_write_file(file, initial_md_string, copyright_md_string)

		elif file.endswith(".sql"):
			validate_and_write_file(file, initial_sql_string, copyright_sql_string)


def validate_and_write_file(file, initial_string, copyright_string):
	# using tempfile to avoid issues while reading large files
	temp_file_path = tempfile.mktemp()
	with open(file) as original_file, open(temp_file_path, "w") as temp_file:
		first_line = original_file.readline()
		if not first_line.startswith(initial_string):
			temp_file.write(copyright_string)
			temp_file.write(first_line)
			temp_file.writelines(original_file)

			# Replace the original file with the temp file
			shutil.move(temp_file_path, file)


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument("filenames", nargs="*")
	parser.add_argument("--app", action="append", help="An argument for the hook")
	args = parser.parse_args(argv)

	app = args.app[0]
	files = args.filenames
	if files:
		validate_copyright(app, files)

	sys.exit(0)


if __name__ == "__main__":
	main()
