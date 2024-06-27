import pathlib
import json

try:
	import frappe
	from frappe.desk.page.setup_wizard.setup_wizard import setup_complete
except Exception as e:
	raise (e)


def get_fixtures_data_from_file(filename):
	app_dir = pathlib.Path(__file__).resolve().parent.parent / "fixtures"
	if pathlib.Path.exists(app_dir / filename):
		with open(app_dir / filename) as f:
			return json.load(f)


def before_test(company):
	frappe.clear_cache()
	today = frappe.utils.getdate()
	setup_data = get_fixtures_data_from_file("company.json")
	companies = [d.get("company_name") for d in setup_data]

	if company not in companies:
		frappe.throw(f"Company: {company} does not exist in setup data.")

	for setup in setup_data:
		if company == setup.get("company_name"):
			setup.update(
				{
					"fy_start_date": today.replace(month=1, day=1).isoformat(),
					"fy_end_date": today.replace(month=12, day=31).isoformat(),
				}
			)
			setup_complete(setup)
			frappe.db.commit()

	# add create_test_data() and create address for company
	for module in frappe.get_all("Module Onboarding"):
		frappe.db.set_value("Module Onboarding", module, "is_complete", 1)
	frappe.set_value("Website Settings", "Website Settings", "home_page", "login")
	frappe.db.commit()


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
		if settings.company:
			for sc_default in biz.get("subcontracting_defaults"):
				sc_default.update({"company": settings.company})
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


def create_warehouses(settings):
	items = get_fixtures_data_from_file(filename="items.json")
	warehouses = []
	for item in items:
		for item_default in item.get("item_defaults"):
			if item_default.get("default_warehouse") and item_default.get("default_warehouse") not in warehouses:
				warehouses.append(item_default.get("default_warehouse"))

	company_abbr = frappe.db.get_value("Company", settings.company, "abbr")
	root_wh = frappe.get_value("Warehouse", {"company": settings.company, "is_group": 1})
	if frappe.db.exists("Warehouse", f"Stores - {company_abbr}"):
		frappe.rename_doc("Warehouse", f"Stores - {company_abbr}", f"Storeroom - {company_abbr}", force=True)
	if frappe.db.exists("Warehouse", f"Finished Goods - {company_abbr}"):
		frappe.rename_doc("Warehouse", f"Finished Goods - {company_abbr}", f"Baked Goods - {company_abbr}", force=True)
		frappe.set_value("Warehouse", f"Baked Goods - {company_abbr}", "is_group", 1)

	for wh in frappe.get_all("Warehouse", {"company": settings.company}, ["name", "is_group"]):
		if wh.name not in warehouses and not wh.is_group:
			frappe.delete_doc("Warehouse", wh.name)

	for warehouse in warehouses:
		if frappe.db.exists("Warehouse", warehouse):
			continue
		wh_doc = frappe.new_doc("Warehouse")
		wh_doc.warehouse_name = warehouse.split(" - ")[0]
		wh_doc.parent_warehouse = root_wh
		wh_doc.company = settings.company
		wh_doc.save()

	if not frappe.db.exists("Warehouse", f"Bakery Display - {company_abbr}"):
		wh = frappe.new_doc("Warehouse")
		wh.warehouse_name = "Bakery Display"
		wh.parent_warehouse = f"Baked Goods - {company_abbr}"
		wh.company = settings.company
		wh.save()

	if frappe.db.exists("Warehouse", f"Refrigerated Display - {company_abbr}"):
		wh = frappe.get_doc("Warehouse", "Refrigerated Display - APC")
		wh.parent_warehouse = f"Baked Goods - {company_abbr}"
		wh.save()



def create_items(settings, only_create=None):
	items = get_fixtures_data_from_file(filename="items.json")
	for item in items:

		if only_create and item.get("item_code") not in only_create:
			continue

		if frappe.db.exists("Item", item.get("item_code")):
			continue

		i = frappe.new_doc("Item")
		i.update(item)
		if settings.company:
			for item_default in i.get("item_defaults"):
				item_default.update({"company": settings.company})
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
		if settings.company:
			bom.company = settings.company
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
		if settings.company:
			empl.company = settings.company
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
