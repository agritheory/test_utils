import datetime
import json
import os
import pathlib
import sys
import tempfile

from validate_customizations import scrub

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

def get_customized_doctypes(app):
	customized_doctypes = {}

	app_dir = pathlib.Path(__file__).resolve().parent.parent.parent / app
	if not app_dir.is_dir():
		return customized_doctypes

	modules = (app_dir / app / "modules.txt").read_text().split("\n")
	for module in modules:
		if not (app_dir / app / scrub(module) / "custom").exists():
			continue
	
		for custom_file in list((app_dir / app / scrub(module) / "custom").glob("**/*.json")):
			if custom_file.stem in customized_doctypes:
				customized_doctypes[custom_file.stem].append(custom_file.resolve())
			else:
				customized_doctypes[custom_file.stem] = [custom_file.resolve()]

	return customized_doctypes

def validate_and_clean_customized_doctypes(customized_doctypes):
	for doctype, customize_files in customized_doctypes.items():
		for customize_file in customize_files:
			temp_file_path = tempfile.mktemp()
			with open(customize_file, "r") as f, open(temp_file_path, "w") as temp_file:
				file_contents = json.load(f)
				for key, value in list(file_contents.items()):
					if isinstance(value, list):
						for item in value:
							for item_key, item_value in list(item.items()):
								if item_value is None and item_key not in ["default", "value"]:
									del item[item_key]
								if item_key == "modified":
									item["modified"] = datetime.datetime.now().strftime(DATETIME_FORMAT)

					elif value is None and key not in ["default", "value"]:
						del file_contents[key]

				temp_file.write(json.dumps(file_contents, indent="\t", sort_keys=True))
				os.replace(temp_file_path, customize_file)

if __name__ == "__main__":
	if sys.argv[1]:
		customized_doctypes = get_customized_doctypes(sys.argv[1])
		validate_and_clean_customized_doctypes(customized_doctypes)
