import json
import os
import pathlib

try:
	import frappe
except Exception as e:
	raise (e)

from frappe import _
from frappe.desk.form.linked_with import get_linked_fields
from erpnext.accounts.doctype.account.account import update_account_number
from erpnext.accounts.doctype.account.chart_of_accounts.chart_of_accounts import create_charts
from erpnext.accounts.doctype.chart_of_accounts_importer.chart_of_accounts_importer import unset_existing_data, set_default_accounts
from erpnext.setup.setup_wizard.operations.install_fixtures import create_bank_account


def setup_chart_of_accounts(company=None, chart_template="Standard with Numbers"):
	"""
	:param company: str | None; the company to apply the Chart of Accounts changes to. If None,
	uses the default company
	:param chart_template: str; options for which type of Chart of Accounts to create:
	    - Standard with Numbers (default; ERPNext option that includes numbered accounts)
	    - Standard (ERPNext option that excludes numbered accounts)
	    - IFRS: imports basic IFRS Chart of Accounts
	    - Farm: imports a standard COA with Numbers plus additional income/expense items to match
		  Schedule F
	:return: None

	Meant to be called after the setup_complete function (which has set up one of ERPNext's
	default `chart_of_accounts` options for a US-based company.
	"""
	company = company or frappe.defaults.get_defaults().company
	orig_ct, chart_template = chart_template, chart_template.lower()
	supported_types = ["Standard with Numbers", "Standard", "IFRS", "Farm"]

	if chart_template not in [ct.lower() for ct in supported_types]:
		frappe.throw(
			msg=_("Unsupported Chart of Accounts Template"),
			title=_(f"The selected Chart of Accounts template {orig_ct} is not recognized. Please try one of {', '.join(supported_types)}")
		)

	if "standard" in chart_template:
		rename_standard_accounts(company=company, with_numbers="numbers" in chart_template)
		if "electronic_payments" in frappe.get_installed_apps():
			create_electronic_payments_accounts(company=company)
	else:
		unset_existing_data(company)
		custom_chart = load_custom_chart(chart_template)
		create_charts(company=company, custom_chart=custom_chart)
		
		args = frappe._dict({"company_name": company, "bank_account": "Primary Checking", "set_default": 1})
		create_bank_account(args)
		set_default_accounts(company)  # TODO: sets receivable, payable, and provisional account, sets country fixtures - need any other defaults set (see Company.set_default_accounts)?
		
		invalid_acct_links = find_invalid_account_links()
		if invalid_acct_links:
			link_list = "</li><li>".join([f"DocType: {d['dt']}, Document: {d['dn']}, Field Name: {d['fieldname']}" for d in invalid_acct_links])
			message = _(f"The following Document(s) contain a link to an invalid Account in the noted field:<br><br><ul><li>{link_list}</li></ul>")
			frappe.log_error(
				title=_("Chart of Accounts Account Link Error"),
				message=message,
				reference_doctype="Company",
				reference_name=company,
			)


def rename_standard_accounts(company=None, with_numbers=True):
	"""
	:param company: str | None; uses default Company if not provided
	"""
	company = company or frappe.defaults.get_defaults().company
	company_abbr = frappe.get_value("Company", company, "abbr") or ""

	acct_number_prefix = "1000 - " if with_numbers else ""
	frappe.rename_doc(
		"Account",
		f"{acct_number_prefix}Application of Funds (Assets) - {company_abbr}",
		f"{acct_number_prefix}Assets - {company_abbr}",
		force=True,
	)

	acct_number_prefix = "2000 - " if with_numbers else ""
	frappe.rename_doc(
		"Account",
		f"{acct_number_prefix}Source of Funds (Liabilities) - {company_abbr}",
		f"{acct_number_prefix}Liabilities - {company_abbr}",
		force=True,
	)

	acct_number_prefix = "1310 - " if with_numbers else ""
	frappe.rename_doc(
		"Account", f"{acct_number_prefix}Debtors - {company_abbr}", f"{acct_number_prefix}Accounts Receivable - {company_abbr}", force=True
	)

	acct_number_prefix = "2110 - " if with_numbers else ""
	frappe.rename_doc(
		"Account", f"{acct_number_prefix}Creditors - {company_abbr}", f"{acct_number_prefix}Accounts Payable - {company_abbr}", force=True
	)

	acct_number_prefix = "1110 - " if with_numbers else ""
	update_account_number(f"{acct_number_prefix}Cash - {company_abbr}", "Petty Cash", account_number=acct_number_prefix[:4] if with_numbers else "")
	update_account_number(f"Primary Checking - {company_abbr}", "Primary Checking", account_number="1201" if with_numbers else "")


def create_electronic_payments_accounts(company=None):
	"""
	Creates Electronic Payment app-specific accounts - supports Standard or Standard with Numbers
	Chart of Account options (if based off Existing Company, will base off parent COA)

	:param company: str | None; uses default Company if not provided
	"""
	company = company or frappe.defaults.get_defaults().company
	coa_company = company
	while True:
		based_on, coa, existing_co = frappe.get_value("Company", coa_company, ["create_chart_of_accounts_based_on", "chart_of_accounts", "existing_company"])
		if based_on == "Standard Template":
			break
		coa_company = existing_co
	with_numbers="Numbers" in coa
	company_abbr = frappe.get_value("Company", company, "abbr") or ""

	rca = frappe.new_doc("Account")  # receivable clearing account
	rca.account_name = "Electronic Payments Receivable"
	rca.account_number = "1320" if with_numbers else ""
	rca.account_type = "Receivable"
	rca.parent_account = frappe.get_value("Account", {"name": ["like", "%Accounts Receivable%"], "is_group": 1})
	rca.currency = "USD"
	rca.company = company
	rca.save()

	pca = frappe.new_doc("Account")  # payable clearing account
	pca.account_name = "Electronic Payments Payable"
	pca.account_number = "2130" if with_numbers else ""
	pca.account_type = "Payable"
	pca.parent_account = frappe.get_value("Account", {"name": ["like", "%Accounts Payable%"], "is_group": 1})
	pca.currency = "USD"
	pca.company = company
	pca.save()

	fee = frappe.new_doc("Account")  # provider fee expense account
	fee.account_name = "Electronic Payments Provider Fees"
	fee.account_number = "5223" if with_numbers else ""
	# fee.account_type = ""
	fee.parent_account = frappe.get_value("Account", {"name": ["like", "%Indirect Expenses%"], "is_group": 1})
	fee.currency = "USD"
	fee.company = company
	fee.save()


def load_custom_chart(chart_template):
	"""
	Returns custom Chart of Accounts data from fixtures folder.

	:param chart_template: str; should be one of non-standard supported types
	:return: json Chart of Accounts tree
	"""
	filename = f"{chart_template}_coa.json"
	chart_file_dir = pathlib.Path(__file__).resolve().parent.parent / "fixtures"

	if pathlib.Path.exists(chart_file_dir / filename):
		with open(chart_file_dir / filename) as f:
			chart = f.read()
			if chart:
				return json.loads(chart).get("tree")


def find_invalid_account_links():
	"""
	Collects invalid values (if any) in document fields that link to Account

	:return: list[dict[str, str]] for any invalid account links in following format, empty list if none:
	{
	    "dt": DocType,
		"dn": Document name,
		"fieldname": field name,
	}
	"""
	invalid_accounts = []
	account_links = get_linked_fields("Account")  # format: {"DocType1": {"fieldname": [...]}, "DocType2": {"child_doctype": "DocType", "fieldname": [...]}}
	for doctype, data_dict in account_links.items():
		fieldnames = data_dict.get("fieldname")
		if "child_doctype" in data_dict or not fieldnames:
			continue
		for doc in frappe.get_all(doctype, ["name"] + fieldnames):
			for fieldname in fieldnames:
				if not frappe.db.exists("Account", doc[fieldname]):
					invalid_accounts.append(frappe._dict({"dt": doctype, "dn": doc["name"], "fieldname": fieldname}))

	return invalid_accounts


def create_bank_and_bank_account(settings):
	if not settings.company_account:
		frappe.log_error(
			title=_("Error Setting Up MOP, Bank, and Bank Account - No Default Company Account"),
			message=_(f"No Company Default bank account found for {settings.company}")
		)

	if not frappe.db.exists("Bank", "Local Bank"):
		bank = frappe.new_doc("Bank")
		bank.bank_name = "Local Bank"
		bank.aba_number = "07200091"
		bank.save()

	if not frappe.db.exists("Bank Account", "Primary Checking - Local Bank"):
		bank_account = frappe.new_doc("Bank Account")
		bank_account.account_name = "Primary Checking"
		bank_account.bank = bank.name
		bank_account.is_default = 1
		bank_account.is_company_account = 1
		bank_account.company = settings.company
		bank_account.account = settings.company_account
		bank_account.check_number = 2500
		bank_account.company_ach_id = "1381655417"
		bank_account.bank_account_no = "072000915"
		bank_account.branch_code = "07200091"
		bank_account.save()

	doc = frappe.new_doc("Journal Entry")
	doc.posting_date = settings.day
	doc.voucher_type = "Opening Entry"
	doc.company = settings.company
	opening_balance = 50000.00
	doc.append(
		"accounts",
		{"account": settings.company_account, "debit_in_account_currency": opening_balance},
	)
	retained_earnings = frappe.get_value(
		"Account", {"account_name": "Retained Earnings", "company": settings.company, "is_group": 0}
	)
	doc.append(
		"accounts",
		{"account": retained_earnings, "credit_in_account_currency": opening_balance},
	)
	doc.save()
	doc.submit()
