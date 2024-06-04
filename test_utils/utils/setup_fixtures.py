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


def create_customers(settings, only_create=None):
	customers = get_fixtures_data_from_file("customers.json")
	addresses = get_fixtures_data_from_file("addresses.json")
	users = get_fixtures_data_from_file("users.json")
	contacts = get_fixtures_data_from_file("contacts.json")

	for customer in customers:
		if only_create and customer.get("customer_name") not in only_create:
			continue

		if frappe.db.exists("Customer", customer.get("customer_name")):
			continue

		cust = frappe.new_doc("Customer")
		cust.update(customer)
		cust.save()

		for address in addresses:
			existing_address = frappe.get_value(
				"Address", {"address_line1": address.get("address_line1")}
			)
			if existing_address:
				continue

			for address_link in address.get("links"):
				if (
					address_link.get("link_doctype") == "Customer"
					and address_link.get("link_name") == cust.customer_name
				):
					addr = frappe.new_doc("Address")
					addr.update(address)
					addr.save()

		for user in users:
			if frappe.db.exists("User", user.get("username")):
				continue
			for role in user.get("roles"):
				if role.get("role") == "Customer":
					user_doc = frappe.new_doc("User")
					user_doc.update(user)
					user_doc.save()

		for contact in contacts:
			existing_contact = frappe.db.get_value(
				"Contact", {"email_id": customer.get("email")}
			)
			if existing_contact:
				continue
			for contact_link in contact.get("links"):
				if (
					contact_link.get("link_doctype") == "Customer"
					and contact_link.get("link_name") == cust.customer_name
				):
					contact_doc = frappe.new_doc("Contact")
					contact_doc.update(contact)
					contact_doc.save()


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
