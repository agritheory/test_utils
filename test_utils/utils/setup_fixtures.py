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

def create_customers():
	customers = get_fixtures_data_from_file("customers.json")

	for customer in customers:
		if frappe.db.exists("Customer", customer.get("customer_name")):
			continue

		cust = frappe.new_doc("Customer")
		cust.customer_name = customer.get("customer_name")
		cust.customer_type = customer.get("customer_type")
		cust.customer_group = customer.get("customer_group")
		cust.territory = customer.get("territory")
		cust.tax_id = customer.get("tax_id")
		cust.save()

		if customer.get("address"):
			addr = frappe.new_doc("Address")
			addr.address_title = f"{customer.get('customer_name')} - {customer.get('address')['city']}"
			addr.address_type = "Shipping"
			addr.address_line1 = customer.get("address")["address_line1"]
			addr.city = customer.get("address")["city"]
			addr.state = customer.get("address")["state"]
			addr.country = customer.get("address")["country"]
			addr.pincode = customer.get("address")["pincode"]
			addr.append("links", {"link_doctype": "Customer", "link_name": cust.name})
			addr.save()

		if customer.get("user"):
			user = frappe.new_doc("User")
			user.first_name = customer.get("user").split(" ")[0]
			user.last_name = customer.get("user").split(" ")[1]
			user.username = customer.get("email")
			user.time_zone = "America/New_York"
			user.email = customer.get("email")
			user.user_type = "System User"
			user.send_welcome_email = 0
			user.append("roles", {"role": "Customer"})
			user.save()

			contact = frappe.new_doc("Contact")
			contact.first_name = user.first_name
			contact.last_name = user.last_name
			contact.user = user.name
			if addr:
				contact.address = addr.name
			contact.append("email_ids", {"email_id": user.name, "is_primary": 1})
			contact.append("links", {"link_doctype": "Customer", "link_name": cust.name})
			contact.save()
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
