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

def create_items(settings):
	items = get_fixtures_data_from_file(filename="items.json")
	for item in items:
		if frappe.db.exists("Item", item.get("item_code")):
			continue
		i = frappe.new_doc("Item")
		i.item_code = i.item_name = item.get("item_code")
		i.item_group = item.get("item_group")
		i.stock_uom = item.get("uom")
		i.description = item.get("description")
		i.is_stock_item = 0 if item.get("is_stock_item") == 0 else 1
		i.include_item_in_manufacturing = 1
		i.valuation_rate = item.get("valuation_rate") or 0
		i.is_sub_contracted_item = item.get("is_sub_contracted_item") or 0
		i.default_warehouse = item.get("default_warehouse")
		i.default_material_request_type = (
			"Purchase"
			if item.get("item_group") in ("Bakery Supplies", "Ingredients")
			or item.get("is_sub_contracted_item")
			else "Manufacture"
		)
		i.valuation_method = "Moving Average"
		if item.get("uom_conversion_detail"):
			for uom, cf in item.get("uom_conversion_detail").items():
				i.append("uoms", {"uom": uom, "conversion_factor": cf})
		i.is_purchase_item = (
			1
			if item.get("item_group") in ("Bakery Supplies", "Ingredients")
			or item.get("is_sub_contracted_item")
			else 0
		)
		i.is_sales_item = 1 if item.get("item_group") == "Baked Goods" else 0
		i.append(
			"item_defaults",
			{
				"company": settings.company,
				"default_warehouse": item.get("default_warehouse"),
				"default_supplier": item.get("default_supplier"),
				"requires_rfq": True if item.get("item_code") == "Cloudberry" else False,
			},
		)
		if i.is_purchase_item and item.get("supplier"):
			if isinstance(item.get("supplier"), list):
				[i.append("supplier_items", {"supplier": s}) for s in item.get("supplier")]
			else:
				i.append("supplier_items", {"supplier": item.get("supplier")})
		i.save()

def create_item_groups():
	item_groups = get_fixtures_data_from_file(filename="item_groups.json")

	for item_group in item_groups:
		if frappe.db.exists("Item Group", item_group.get("item_group_name")):
			continue
		item_group = frappe.new_doc("Item Group")
		item_group.item_group_name = item_group.get("item_group_name")
		item_group.parent_item_group = item_group.get("parent_item_group")
		item_group.save()
