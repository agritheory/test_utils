import pathlib
import json

try:
	import frappe
except Exception as e:
	raise (e)

def get_fixtures_data_from_file(filename):
	app_dir = pathlib.Path(__file__).resolve().parent.parent / "fixtures"
	if pathlib.Path.exists(app_dir / filename):
		with open(app_dir / filename) as f:
			return json.load(f)

def create_items():
	items = get_fixtures_data_from_file(filename="items.json")
	for item in items:
		if frappe.db.exists("Item", item.get("item_code")):
			continue
		i = frappe.new_doc("Item")
		i.update(item)
		i.save()

def create_item_groups():
	item_groups = get_fixtures_data_from_file(filename="item_groups.json")

	for item_group in item_groups:
		if frappe.db.exists("Item Group", item_group.get("item_group_name")):
			continue
		ig = frappe.new_doc("Item Group")
		ig.update(item_group)
		ig.save()
