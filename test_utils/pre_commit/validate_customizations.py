import argparse
import json
import pathlib
import sys
import shutil
from collections.abc import Sequence

try:
	import tomllib
except ImportError:
	import tomli as tomllib  # type: ignore[no-redef]


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


def scrub(txt: str) -> str:
	"""Returns sluggified string. e.g. `Sales Order` becomes `sales_order`."""
	return txt.replace(" ", "_").replace("-", "_").lower()


def unscrub(txt: str) -> str:
	"""Returns titlified string. e.g. `sales_order` becomes `Sales Order`."""
	return txt.replace("_", " ").replace("-", " ").title()


def _resolve_app_dir(app_name: str | None) -> pathlib.Path:
	"""Resolve app directory from --app or cwd. Returns path to app (e.g. apps/cad)."""
	cwd = pathlib.Path().resolve()
	if not app_name:
		return cwd
	# Bench root: has apps/ subdir; app lives at apps/<app_name>
	if (cwd / "apps" / app_name).is_dir():
		return (cwd / "apps" / app_name).resolve()
	if cwd.name == app_name:
		return cwd
	# Maybe cwd is inside the app (e.g. apps/cad/cad)
	for parent in [cwd.parent, cwd.parent.parent]:
		if (parent / "apps" / app_name).is_dir():
			return (parent / "apps" / app_name).resolve()
		if parent.name == app_name:
			return parent
	return cwd


def get_customized_doctypes(app_dir: pathlib.Path):
	apps_dir = app_dir.parent
	apps_order = app_dir.parent.parent / "sites" / "apps.txt"
	apps_order = apps_order.read_text().split("\n")
	customized_doctypes = {}
	for _app_dir in apps_order:
		app_dir = (apps_dir / _app_dir).resolve()
		if not app_dir.is_dir():
			continue
		modules = (app_dir / _app_dir / "modules.txt").read_text().split("\n")
		for module in modules:
			if not (app_dir / _app_dir / scrub(module) / "custom").exists():
				continue
			for custom_file in list(
				(app_dir / _app_dir / scrub(module) / "custom").glob("**/*.json")
			):
				if custom_file.stem in customized_doctypes:
					customized_doctypes[custom_file.stem].append(custom_file.resolve())
				else:
					customized_doctypes[custom_file.stem] = [custom_file.resolve()]
		if app_dir.stem == "hrms":
			pass

			# def _bypass(*args, **kwargs):
			# 	return args[0]

			# _ = _bypass

			# p = ast.parse((app_dir / "hrms" / "setup.py").read_text())
			# for node in p.body[:]:
			# 	if not isinstance(node, ast.FunctionDef) or node.name != "get_custom_fields":
			# 		p.body.remove(node)
			# module = types.ModuleType("hrms")
			# code = compile(p, "setup.py", "exec")
			# sys.modules["hrms"] = module
			# exec(code, module.__dict__)
			# import hrms

			# hrms_custom_fields = hrms.get_custom_fields()
			# for doctype, fields in hrms_custom_fields.items():
			# 	if doctype in customized_doctypes:
			# 		customized_doctypes[scrub(doctype)].append({"custom_fields": fields})
			# 	else:
			# 		customized_doctypes[scrub(doctype)] = [{"custom_fields": fields}]

	return dict(sorted(customized_doctypes.items()))


def validate_module(customized_doctypes, app_dir: pathlib.Path):
	exceptions = []
	this_app = app_dir.name
	if not (app_dir / this_app / "modules.txt").exists():
		modules = []
	else:
		modules = (app_dir / this_app / "modules.txt").read_text().split("\n")
	for doctype, customize_files in customized_doctypes.items():
		for customize_file in customize_files:
			if not this_app in str(customize_file):
				continue
			file_contents = json.loads(customize_file.read_text())
			if file_contents.get("custom_fields"):
				for custom_field in file_contents.get("custom_fields"):
					if not custom_field.get("module"):
						exceptions.append(
							f"Custom Field for {custom_field.get('dt')} in {this_app} '{custom_field.get('fieldname')}' does not have a module key"
						)
						continue
					elif custom_field.get("module") not in modules:
						exceptions.append(
							f"Custom Field for {custom_field.get('dt')} in {this_app} '{custom_field.get('fieldname')}' has module key ({custom_field.get('module')}) associated with another app"
						)
						continue
			if file_contents.get("property_setters"):
				for ps in file_contents.get("property_setters"):
					if not ps.get("module"):
						exceptions.append(
							f"Property Setter for {ps.get('doc_type')} in {this_app} '{ps.get('property')}' on {ps.get('field_name')} does not have a module key"
						)
						continue
					elif ps.get("module") not in modules:
						exceptions.append(
							f"Property Setter for {ps.get('doc_type')} in {this_app} '{ps.get('property')}' on {ps.get('field_name')} has module key ({ps.get('module')}) associated with another app"
						)
						continue

	return exceptions


def validate_no_custom_perms(customized_doctypes, app_dir: pathlib.Path):
	exceptions = []
	this_app = app_dir.name
	for doctype, customize_files in customized_doctypes.items():
		for customize_file in customize_files:
			if not this_app in str(customize_file):
				continue
			file_contents = json.loads(customize_file.read_text())
			if file_contents.get("custom_perms"):
				exceptions.append(
					f"Customization for {doctype} in {this_app} contains custom permissions"
				)
	return exceptions


def load_customization_allowlist(app_dir: pathlib.Path) -> dict:
	"""Load allowlist from pyproject.toml [tool.test_utils.validate_customizations]."""
	allowlist: dict = {
		"allow_duplicate_property_setters": [],
		"allow_duplicate_custom_fields": [],
	}
	for search_dir in [app_dir, app_dir.parent, *app_dir.parents]:
		pyproject = search_dir / "pyproject.toml"
		if not pyproject.exists():
			continue
		try:
			with open(pyproject, "rb") as f:
				data = tomllib.load(f)
			config = (
				data.get("tool", {}).get("test_utils", {}).get("validate_customizations", {})
			)
			if config:
				allowlist["allow_duplicate_property_setters"] = config.get(
					"allow_duplicate_property_setters", []
				)
				allowlist["allow_duplicate_custom_fields"] = config.get(
					"allow_duplicate_custom_fields", []
				)
				break
		except (ValueError, OSError):
			pass
	return allowlist


def validate_duplicate_customizations(customized_doctypes, app_dir: pathlib.Path):
	exceptions = []
	common_fields = {}
	common_property_setters = {}
	this_app = app_dir.name
	allowlist = load_customization_allowlist(app_dir)
	for doctype, customize_files in customized_doctypes.items():
		if len(customize_files) == 1:
			continue
		common_fields[doctype] = {}
		common_property_setters[doctype] = {}
		for customize_file in customize_files:
			if isinstance(customize_file, dict):
				module = "hrms"
				app = "hrms"
				file_contents = customize_file
			else:
				module = customize_file.parent.parent.stem
				app = customize_file.parent.parent.parent.parent.stem
				file_contents = json.loads(customize_file.read_text())
			if file_contents.get("custom_fields"):
				fields = [cf.get("fieldname") for cf in file_contents.get("custom_fields")]
				common_fields[doctype][module] = fields
			if file_contents.get("property_setters"):
				ps = [ps.get("name") for ps in file_contents.get("property_setters")]
				common_property_setters[doctype][module] = ps

	allowed_fields = set(allowlist["allow_duplicate_custom_fields"])
	allowed_property_setters = set(allowlist["allow_duplicate_property_setters"])

	for doctype, module_and_fields in common_fields.items():
		if this_app not in module_and_fields.keys():
			continue
		this_modules_fields = module_and_fields.pop(this_app)
		for module, fields in module_and_fields.items():
			for field in fields:
				if field in this_modules_fields and field not in allowed_fields:
					exceptions.append(
						f"Custom Field for {unscrub(doctype)} in {this_app} '{field}' also appears in customizations for {module}"
					)

	for doctype, module_and_ps in common_property_setters.items():
		if this_app not in module_and_ps.keys():
			continue
		this_modules_ps = module_and_ps.pop(this_app)
		for module, ps in module_and_ps.items():
			for p in ps:
				if p in this_modules_ps and p not in allowed_property_setters:
					exceptions.append(
						f"Property Setter for {unscrub(doctype)} in {this_app} on '{p}' also appears in customizations for {module}"
					)

	return exceptions


def validate_system_generated(customized_doctypes, app_dir: pathlib.Path):
	exceptions = []
	this_app = app_dir.name
	for doctype, customize_files in customized_doctypes.items():
		for customize_file in customize_files:
			# checking if customize_file is a dict, as for hrms it returns a dict of custom_fields
			if isinstance(customize_file, dict):
				continue
			if not this_app in str(customize_file):
				continue
			file_contents = json.loads(customize_file.read_text())
			if file_contents.get("custom_fields"):
				for cf in file_contents.get("custom_fields"):
					if cf.get("is_system_generated"):
						exceptions.append(
							f"{cf.get('dt')} Custom Field {cf.get('fieldname')} is system generated"
						)

			if file_contents.get("property_setters"):
				for ps in file_contents.get("property_setters"):
					if ps.get("is_system_generated"):
						exceptions.append(f"Property Setter {ps.get('name')} is system generated")

	return exceptions


def validate_customizations_on_own_doctypes(customized_doctypes, app_dir: pathlib.Path):
	exceptions = []
	this_app = app_dir.name
	if not (app_dir / this_app / "modules.txt").exists():
		modules = []
	else:
		modules = (app_dir / this_app / "modules.txt").read_text().split("\n")
	own_doctypes = {}
	app_modules_dir = app_dir / this_app
	for module in modules:
		if not (app_modules_dir / scrub(module)).is_dir():
			continue
		if not (app_modules_dir / scrub(module) / "doctype").is_dir():
			continue
		for doctype in (app_modules_dir / scrub(module) / "doctype").iterdir():
			doctype_definition = doctype / f"{doctype.stem}.json"
			if doctype_definition.exists():
				file_contents = json.loads(doctype_definition.read_text())
				name = file_contents.get("name")
				if name:
					own_doctypes[name] = (module, doctype_definition)

	for doctype, customize_files in customized_doctypes.items():
		for customize_file in customize_files:
			# checking if customize_file is a dict, as for hrms it returns a dict of custom_fields
			if isinstance(customize_file, dict):
				continue
			if not this_app in str(customize_file):
				continue
			file_contents = json.loads(customize_file.read_text())
			if file_contents.get("doctype") in own_doctypes.keys():
				exceptions.append(
					f"Customizations for doctype defined in {own_doctypes[ file_contents.get('doctype')][0]} for {file_contents.get('doctype')} exist"
				)
	return exceptions


def validate_email_literals(customized_doctypes, app_dir: pathlib.Path):
	this_app = app_dir.name
	for doctype, customize_files in customized_doctypes.items():
		for customize_file in customize_files:
			file_contents = json.loads(customize_file.read_text())
			modified = False

			if file_contents.get("custom_fields"):
				for cf in file_contents.get("custom_fields"):
					if cf.get("owner") and "@" in cf.get("owner"):
						cf["owner"] = "Administrator"
						modified = True

					if cf.get("modified_by") and "@" in cf.get("modified_by"):
						cf["modified_by"] = "Administrator"
						modified = True

			if file_contents.get("property_setters"):
				for ps in file_contents.get("property_setters"):
					if ps.get("owner") and "@" in ps.get("owner"):
						ps["owner"] = "Administrator"
						modified = True

					if ps.get("modified_by") and "@" in ps.get("modified_by"):
						ps["modified_by"] = "Administrator"
						modified = True

			if modified:
				customize_file.write_text(json.dumps(file_contents, indent="\t"))
				print(f"Updated owner/modified_by fields in {customize_file} to 'Administrator'")


def validate_customizations(app_dir: pathlib.Path):
	customized_doctypes = get_customized_doctypes(app_dir)
	exceptions = validate_no_custom_perms(customized_doctypes, app_dir)
	exceptions += validate_module(customized_doctypes, app_dir)
	exceptions += validate_system_generated(customized_doctypes, app_dir)
	exceptions += validate_customizations_on_own_doctypes(customized_doctypes, app_dir)
	exceptions += validate_duplicate_customizations(customized_doctypes, app_dir)
	validate_email_literals(customized_doctypes, app_dir)
	return exceptions


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument("filenames", nargs="*")
	parser.add_argument(
		"--app", help="App name (e.g. cad); used to locate app dir and pyproject.toml"
	)
	args = parser.parse_args(argv)

	if is_frappe_bench_environment():
		app_dir = _resolve_app_dir(args.app)
		exceptions = validate_customizations(app_dir)
		if exceptions:
			for exception in list(set(exceptions)):
				print(exception)

		sys.exit(1) if exceptions else sys.exit(0)


if __name__ == "__main__":
	main()
