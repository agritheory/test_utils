import os
import pathlib
import datetime
import tempfile
import sys

def get_files(app):
	files = []

	app_dir = pathlib.Path(__file__).resolve().parent.parent.parent / app
	if not app_dir.is_dir():
		return files

	for root, _, filenames in os.walk(app_dir):
		# exclude directories node_modules, dist
		if "node_modules" in root or "dist" in root:
			continue

		for filename in filenames:
			if filename.endswith((".js", ".ts", ".py", ".md")):
				if os.path.getsize(os.path.join(root, filename)) > 0:
					files.append(os.path.join(root, filename))
	return files

def validate_copyright(app, files):
	year = datetime.datetime.now().year
	app_dir = pathlib.Path(__file__).resolve().parent.parent.parent / app / app

	app_publisher = ""
	hooks_file = app_dir / "hooks.py"
	if hooks_file.is_file():
		with open(hooks_file, "r") as file:
			for line in file:
				if "app_publisher" in line:
					app_publisher = line.split("=")[1].strip().replace('"', "")
					break

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
	with open(file, "r") as original_file, open(temp_file_path, "w") as temp_file:
		first_line = original_file.readline()
		if not first_line.startswith(initial_string):
			temp_file.write(copyright_string)
			temp_file.write(first_line)
			temp_file.writelines(original_file)
			temp_file.writelines(original_file)

			# Replace the original file with the temp file
			os.replace(temp_file_path, file)

if __name__ == "__main__":
	if sys.argv[1]:
		files = get_files(sys.argv[1])
		if files:
			validate_copyright(sys.argv[1], files)
