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


def create_suppliers(settings, only_create=None):
	suppliers = get_fixtures_data_from_file("suppliers.json")
	addresses = get_fixtures_data_from_file("addresses.json")

	for supplier in suppliers:
		if only_create and supplier.get("supplier_name") not in only_create:
			continue

		if frappe.db.exists("Supplier", supplier.get("supplier_name")):
			continue

		biz = frappe.new_doc("Supplier")
		biz.update(supplier)
		biz.save()

	for address in addresses:
		existing_address = frappe.get_value(
			"Address", {"address_line1": address.get("address_line1")}
		)
		if not existing_address:
			addr = frappe.new_doc("Address")
			addr.update(address)
			addr.save()


def create_payment_terms_template(settings, only_create=None):
	payment_terms_templates = get_fixtures_data_from_file(
		"payment_terms_templates.json"
	)

	for payment_terms_template in payment_terms_templates:
		if only_create and payment_terms_template.get("template_name") not in only_create:
			continue

		if frappe.db.exists(
			"Payment Terms Template", payment_terms_template.get("template_name")
		):
			continue

		for payment_term in payment_terms_template.get("terms"):
			pt = frappe.new_doc("Payment Term")
			pt.payment_term_name = payment_term.get("payment_term")
			pt.update(payment_term)
			pt.save()

		ptt = frappe.new_doc("Payment Terms Temmplate")
		ptt.update(payment_terms_template)
		ptt.save()


def create_supplier_groups(settings, only_create=None):
	supplier_groups = get_fixtures_data_from_file("supplier_groups.json")
	for supplier_group in supplier_groups:

		if only_create and supplier_group.get("supplier_group_name") not in only_create:
			continue

		if frappe.db.exists(
			"Supplier Group", supplier_group.get("supplier_group_name")
		):
			continue

		bsg = frappe.new_doc("Supplier Group")
		bsg.update(supplier_group)
		bsg.save()


def create_items(settings, only_create=None):
	items = get_fixtures_data_from_file(filename="items.json")
	for item in items:

		if only_create and item.get("item_code") not in only_create:
			continue

		if frappe.db.exists("Item", item.get("item_code")):
			continue

		i = frappe.new_doc("Item")
		i.update(item)
		i.save()


def create_item_groups(settings, only_create=None):
	item_groups = get_fixtures_data_from_file(filename="item_groups.json")

	for item_group in item_groups:

		if only_create and item_group.get("item_group_name") not in only_create:
			continue

		if frappe.db.exists("Item Group", item_group.get("item_group_name")):
			continue

		ig = frappe.new_doc("Item Group")
		ig.update(item_group)
		ig.save()
