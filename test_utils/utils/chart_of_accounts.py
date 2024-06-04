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


def setup_chart_of_accounts(chart_template="Standard with Numbers", company=None):
	"""
	:param chart_template: str; options for which type of Chart of Accounts to create:
	    - Standard with Numbers (default; ERPNext option that includes numbered accounts)
	    - Standard (ERPNext option that excludes numbered accounts)
	    - IFRS: imports sections 1-5 of standard IFRS Chart of Accounts
	    - Farm: imports a farm Chart of Accounts to match Schedule F
	:param company: str | None; the company to apply the Chart of Accounts changes to. If None,
	uses the default company
	:return: None

	Meant to be called after the setup_complete function (which has set up one of ERPNext's
	default `chart_of_accounts` options for a US-based company.
	"""
	company = company or frappe.defaults.get_defaults().company
	chart_template = chart_template.lower()
	supported_types = ["standard with numbers", "standard", "ifrs", "farm"]

	if chart_template not in supported_types:
		frappe.throw(
			msg=_("Unsupported Chart of Accounts Template"),
			title=_(f"The selected Chart of Accounts template {chart_template} is not recognized. Please try one of {', '.join([s for s in supported_types])}")
		)

	if "standard" in chart_template:
		rename_standard_accounts(company=company, with_numbers="numbers" in chart_template)
		if "electronic_payments" in frappe.get_installed_apps():
			create_electronic_payments_accounts(company=company)
	else:
		unset_existing_data(company)
		custom_chart = load_custom_chart(chart_template)
		create_charts(company=company, custom_chart=custom_chart)
		set_default_accounts(company)  # TODO: sets receivable, payable, and provisional account, sets country fixtures - need any other defaults set?
		invalid_acct_links = find_invalid_account_links()
		if invalid_acct_links:
			link_list = "</li><li>".join([f"DocType: {d['dt']}, Document: {d['dn']}, Field Name: {d['fieldname']}" for d in invalid_acct_links])
			message = _(f"The following Document(s) contain a link to an invalid Account in the noted field:<br><br><ul><li>{link_list}</li></ul>")
			frappe.log_error(
				title=_("Chart of Accounts Setup Error"),
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

	if with_numbers:
		update_account_number("1110 - Cash - CFC", "Petty Cash", account_number="1110")
		update_account_number("Primary Checking - CFC", "Primary Checking", account_number="1201")


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
