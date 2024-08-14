try:
	import frappe
except Exception as e:
	raise (e)


def create_partition():
	# eg. partition_doctypes = ["Customer", "Item", "Sales Order"]
	partition_doctypes = frappe.get_hooks("partition_doctypes")

	fiscal_years = frappe.get_all(
		"Fiscal Year", fields=["name", "year_start_date", "year_end_date"]
	)

	for doctype in partition_doctypes:
		partition_based_on = "modified"

		partition_sql = (
			f"ALTER TABLE `tab{doctype}` PARTITION BY RANGE (YEAR({partition_based_on})) ("
		)

		for year in fiscal_years:
			year_start = year.get("year_start_date").year
			year_end = year.get("year_end_date").year + 1
			partition_sql += (
				f"PARTITION fiscal_year_{year_start} VALUES LESS THAN ({year_end}), "
			)

		partition_sql = partition_sql.rstrip(", ") + ");"

		try:
			frappe.db.sql(partition_sql)
			frappe.db.commit()
			print(f"Partitioning for {doctype} completed successfully.")
		except Exception as e:
			print(f"Error while partitioning {doctype}: {e}")
