import ast
import json
import pathlib
import sys
import types


def scrub(txt: str) -> str:
	"""Returns sluggified string. e.g. `Sales Order` becomes `sales_order`."""
	return txt.replace(" ", "_").replace("-", "_").lower()


def unscrub(txt: str) -> str:
	"""Returns titlified string. e.g. `sales_order` becomes `Sales Order`."""
	return txt.replace("_", " ").replace("-", " ").title()


def get_customized_doctypes():
	apps_dir = pathlib.Path(__file__).resolve().parent.parent.parent
	apps_order = pathlib.Path(__file__).resolve().parent.parent.parent.parent / "sites" / "apps.txt"
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
			for custom_file in list((app_dir / _app_dir / scrub(module) / "custom").glob("**/*.json")):
				if custom_file.stem in customized_doctypes:
					customized_doctypes[custom_file.stem].append(custom_file.resolve())
				else:
					customized_doctypes[custom_file.stem] = [custom_file.resolve()]
		if app_dir.stem == "hrms":
			p = ast.parse((app_dir / "hrms" / "setup.py").read_text())
			for node in p.body[:]:
				if not isinstance(node, ast.FunctionDef) or node.name != "get_custom_fields":
					p.body.remove(node)
			module = types.ModuleType("hrms")
			code = compile(p, "setup.py", "exec")
			sys.modules["hrms"] = module
			exec(code, module.__dict__)
			import hrms

			hrms_custom_fields = hrms.get_custom_fields()
			for doctype, fields in hrms_custom_fields.items():
				if doctype in customized_doctypes:
					customized_doctypes[scrub(doctype)].append({"custom_fields": fields})
				else:
					customized_doctypes[scrub(doctype)] = [{"custom_fields": fields}]

	return dict(sorted(customized_doctypes.items()))


def validate_module(customized_doctypes, set_module=False):
	exceptions = []
	app_dir = pathlib.Path(__file__).resolve().parent.parent
	this_app = app_dir.stem
	if not pathlib.Path.exists(app_dir / this_app / "modules.txt"):
		modules = []
	else:
		modules = (app_dir / this_app / "modules.txt").read_text().split("\n")
	for doctype, customize_files in customized_doctypes.items():
		for customize_file in customize_files:
			if not this_app in str(customize_file):
				continue
			module = customize_file.parent.parent.stem
			file_contents = json.loads(customize_file.read_text())
			if file_contents.get("custom_fields"):
				for custom_field in file_contents.get("custom_fields"):
					if set_module:
						custom_field["module"] = unscrub(module)
						continue
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
					if set_module:
						ps["module"] = unscrub(module)
						continue
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
			if set_module:
				with customize_file.open("w", encoding="UTF-8") as target:
					json.dump(file_contents, target, sort_keys=True, indent=2)

	return exceptions


def validate_no_custom_perms(customized_doctypes):
	exceptions = []
	this_app = pathlib.Path(__file__).resolve().parent.parent.stem
	for doctype, customize_files in customized_doctypes.items():
		for customize_file in customize_files:
			if not this_app in str(customize_file):
				continue
			file_contents = json.loads(customize_file.read_text())
			if file_contents.get("custom_perms"):
				exceptions.append(f"Customization for {doctype} in {this_app} contains custom permissions")
	return exceptions


def validate_duplicate_customizations(customized_doctypes):
	exceptions = []
	common_fields = {}
	common_property_setters = {}
	app_dir = pathlib.Path(__file__).resolve().parent.parent
	this_app = app_dir.stem
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

	for doctype, module_and_fields in common_fields.items():
		if this_app not in module_and_fields.keys():
			continue
		this_modules_fields = module_and_fields.pop(this_app)
		for module, fields in module_and_fields.items():
			for field in fields:
				if field in this_modules_fields:
					exceptions.append(
						f"Custom Field for {unscrub(doctype)} in {this_app} '{field}' also appears in customizations for {module}"
					)

	for doctype, module_and_ps in common_property_setters.items():
		if this_app not in module_and_ps.keys():
			continue
		this_modules_ps = module_and_ps.pop(this_app)
		for module, ps in module_and_ps.items():
			for p in ps:
				if p in this_modules_ps:
					exceptions.append(
						f"Property Setter for {unscrub(doctype)} in {this_app} on '{p}' also appears in customizations for {module}"
					)

	return exceptions


def validate_system_generated(customized_doctypes):
	exceptions = []
	this_app = pathlib.Path(__file__).resolve().parent.parent.stem
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
						exceptions.append(f"{cf.get('dt')} Custom Field {cf.get('fieldname')} is system generated")

			if file_contents.get("property_setters"):
				for ps in file_contents.get("property_setters"):
					if ps.get("is_system_generated"):
						exceptions.append(f"Property Setter {ps.get('name')} is system generated")

	return exceptions


def validate_customizations_on_own_doctypes(customized_doctypes):
	exceptions = []
	app_dir = pathlib.Path(__file__).resolve().parent.parent
	this_app = pathlib.Path(__file__).resolve().parent.parent.stem
	if not pathlib.Path.exists(app_dir / this_app / "modules.txt"):
		modules = []
	else:
		modules = (app_dir / app_dir.stem / "modules.txt").read_text().split("\n")
	own_doctypes = {}
	for module in modules:
		app_dir = (app_dir / app_dir.stem).resolve()
		if not (app_dir / scrub(module)).is_dir():
			continue
		if not (app_dir / scrub(module) / "doctype").is_dir():
			continue
		for doctype in (app_dir / scrub(module) / "doctype").iterdir():
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


def validate_customizations(set_module):
	customized_doctypes = get_customized_doctypes()
	exceptions = validate_no_custom_perms(customized_doctypes)
	exceptions += validate_module(customized_doctypes, set_module)
	exceptions += validate_system_generated(customized_doctypes)
	exceptions += validate_customizations_on_own_doctypes(customized_doctypes)
	exceptions += validate_duplicate_customizations(customized_doctypes)

	return exceptions


if __name__ == "__main__":
	exceptions = []
	set_module = False
	for arg in sys.argv:
		if arg == "--set-module":
			set_module = True
		exceptions.append(validate_customizations(set_module))

	if exceptions:
		for exception in exceptions:
			[print(e) for e in exception]  # TODO: colorize

	sys.exit(1) if all(exceptions) else sys.exit(0)
