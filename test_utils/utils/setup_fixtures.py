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
			if existing_address:
				continue

			for link in address.get("links"):
				if (
					link.get("link_doctype") == "Supplier"
					and link.get("link_name") == biz.supplier_name
				):
					addr = frappe.new_doc("Address")
					addr.update(address)
					addr.save()


def create_payment_terms_template(settings, only_create=None):
	payment_terms_templates = get_fixtures_data_from_file(
		"payment_terms_templates.json"
	)

	for payment_terms_template in payment_terms_templates:
		if (
			only_create
			and payment_terms_template.get("template_name") not in only_create
		):
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


def create_workstations(only_create=None):
	workstations = get_fixtures_data_from_file(filename="workstations.json")

	for workstation in workstations:
		if only_create and workstation.get("workstation_name") not in only_create:
			continue

		if frappe.db.exists("Workstation", workstation.get("workstation_name")):
			continue

		workstation_doc = frappe.new_doc("Workstation")
		workstation_doc.update(workstation)
		workstation_doc.save()


def create_operations(only_create=None):
	operations = get_fixtures_data_from_file(filename="operations.json")

	for operation in operations:
		if only_create and operation.get("name") not in only_create:
			continue

		if frappe.db.exists("Operation", operation.get("name")):
			continue

		operation_doc = frappe.new_doc("Operation")
		operation_doc.update(operation)
		operation_doc.save()


def create_boms(settings, only_create=None):
	boms = get_fixtures_data_from_file(filename="boms.json")

	for bom in boms:

		if only_create and bom.get("item") not in only_create:
			continue

		bom_doc = frappe.new_doc("BOM")
		bom_doc.update(bom)
		bom.save()


def create_employees(settings, only_create=None):
	employees = get_fixtures_data_from_file(filename="employees.json")
	addresses = get_fixtures_data_from_file(filename="addresses.json")

	for employee in employees:
		if only_create and employee.get("employee_name") not in only_create:
			continue

		if frappe.db.exists(
			"Employee", {"employee_name": employee.get("employee_name")}
		):
			continue

		empl = frappe.new_doc("Employee")
		empl.update(employee)
		empl.save()

		for address in addresses:
			existing_address = frappe.get_value(
				"Address", {"address_line1": address.get("address_line1")}
			)
			if existing_address:
				continue

			for link in address.get("links"):
				if (
					link.get("link_doctype") == "Employee"
					and link.get("link_title") == empl.employee_name
				):
					for addr_link in address.get("links"):
						if addr_link.get("link_title") == empl.employee_name:
							addr_link.update({"link_name": empl.name})

					addr = frappe.new_doc("Address")
					addr.update(address)
					addr.save()
					empl.employee_primary_address = addr.name
					empl.save()
			break
