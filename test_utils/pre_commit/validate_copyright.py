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
		raise ValueError("app must be either a string (app path) or dictionary with 'app_publisher' key")

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
