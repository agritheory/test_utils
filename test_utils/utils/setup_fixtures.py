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

def create_suppliers(settings):
	suppliers = get_fixtures_data_from_file("suppliers.json")

	for supplier in suppliers:
		biz = frappe.new_doc("Supplier")
		biz.supplier_name = supplier.get("supplier_name")
		biz.supplier_group = "Bakery"
		biz.country = "United States"
		biz.supplier_default_mode_of_payment = supplier.get("supplier_default_mode_of_payment")
		if biz.supplier_default_mode_of_payment == "ACH/EFT":
			biz.bank = "Local Bank"
			biz.bank_account = "123456789"
		biz.currency = "USD"
		if biz.supplier_name == "Credible Contract Baking":
			biz.append(
				"subcontracting_defaults",
				{
					"company": settings.company,
					"wip_warehouse": "Credible Contract Baking - APC",
					"return_warehouse": "Baked Goods - APC",
				},
			)
		elif supplier.get("supplier_name") == "Tireless Equipment Rental, Inc":
			biz.number_of_invoices_per_check_voucher = 1
		biz.default_price_list = "Standard Buying"
		biz.save()

		existing_address = frappe.get_value("Address", {"address_line1": supplier.get("address")["address_line1"]})
		if not existing_address:
			addr = frappe.new_doc("Address")
			addr.address_title = f"{supplier.get('supplier_name')} - {supplier.get('address')['city']}"
			addr.address_type = "Billing"
			addr.address_line1 = supplier.get("address")["address_line1"]
			addr.city = supplier.get("address")["city"]
			addr.state = supplier.get("address")["state"]
			addr.country = supplier.get("address")["country"]
			addr.pincode = supplier.get("address")["pincode"]
		else:
			addr = frappe.get_doc("Address", existing_address)
		addr.append("links", {"link_doctype": "Supplier", "link_name": supplier.get("supplier_name")})
		addr.save()

	addr = frappe.new_doc("Address")
	addr.address_type = "Billing"
	addr.address_title = "HIJ Telecom - Burlingame"
	addr.address_line1 = "167 Auto Terrace"
	addr.city = "Burlingame"
	addr.state = "ME"
	addr.country = "United States"
	addr.pincode = "79749"
	addr.append("links", {"link_doctype": "Supplier", "link_name": "HIJ Telecom, Inc"})
	addr.save()

def create_supplier_groups():
	supplier_groups = get_fixtures_data_from_file("supplier_groups.json")
	for supplier_group in supplier_groups:
		if not frappe.db.exists("Supplier Group", supplier_group.get("supplier_group_name")):
			bsg = frappe.new_doc("Supplier Group")
			bsg.supplier_group_name = supplier_group.get("supplier_group_name")
			bsg.parent_supplier_group = supplier_group.get("parent_supplier_group")
			bsg.save()