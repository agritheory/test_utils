import argparse
import datetime
import os
import sys
import tempfile
from collections.abc import Sequence


def validate_copyright(app, files):
	year = datetime.datetime.now().year
	app_publisher = ""

	hooks_file = f"{app}/hooks.py"
	with open(hooks_file) as file:
		for line in file:
			if "app_publisher" in line:
				app_publisher = line.split("=")[1].strip().replace('"', "")

	initial_js_string = "// Copyright (c) "
	initial_py_string = "# Copyright (c) "
	initial_md_string = "<!-- Copyright (c) "

	copyright_js_string = f"// Copyright (c) {year}, {app_publisher} and contributors\n// For license information, please see license.txt\n\n"
	copyright_py_string = f"# Copyright (c) {year}, {app_publisher} and contributors\n# For license information, please see license.txt\n\n"
	copyright_md_string = f"<!-- Copyright (c) {year}, {app_publisher} and contributors\nFor license information, please see license.txt-->\n\n"

	for file in files:
		if file.endswith(".js") or file.endswith(".ts"):
			validate_and_write_file(file, initial_js_string, copyright_js_string)

		elif file.endswith(".py"):
			validate_and_write_file(file, initial_py_string, copyright_py_string)

		elif file.endswith(".md"):
			validate_and_write_file(file, initial_md_string, copyright_md_string)


def validate_and_write_file(file, initial_string, copyright_string):
	# using tempfile to avoid issues while reading large files
	temp_file_path = tempfile.mktemp()
	with open(file) as original_file, open(temp_file_path, "w") as temp_file:
		first_line = original_file.readline()
		if not first_line.startswith(initial_string):
			temp_file.write(copyright_string)
			temp_file.write(first_line)
			temp_file.writelines(original_file)
			temp_file.writelines(original_file)

			# Replace the original file with the temp file
			os.replace(temp_file_path, file)


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
