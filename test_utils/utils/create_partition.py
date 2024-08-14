try:
	import frappe
	from frappe.utils import get_table_name
except Exception as e:
	raise (e)


def primary_key_exists(table_name):
	try:
		result = frappe.db.sql(f"""
		SELECT COUNT(*) 
		FROM information_schema.TABLE_CONSTRAINTS
		WHERE TABLE_NAME = '{table_name}' 
		AND CONSTRAINT_TYPE = 'PRIMARY KEY';
		""")
		return result[0][0] > 0
	except Exception as e:
		print(f"Error checking primary key existence: {e}")
		return False


def modify_primary_key(table_name, date_field):
	try:
		if primary_key_exists(table_name):
			drop_pk_sql = f"""
			ALTER TABLE `{table_name}`
			DROP PRIMARY KEY;
			"""
			frappe.db.sql(drop_pk_sql)
			
			pk_columns =  f"name, {date_field}"
			add_pk_sql = f"""
			ALTER TABLE `{table_name}`
			ADD PRIMARY KEY ({pk_columns});
			"""
			frappe.db.sql(add_pk_sql)
			frappe.db.commit()
			print(f"Primary key modified in table {table_name} to include columns: {pk_columns}")
	except Exception as e:
		print(f"Error modifying primary key: {e}")


def create_partition():
	"""
	partition_doctypes = {
		"Sales Order": {
			"date_field": "transaction_date",
		},
		"Sales Invoice": {
			"date_field": "posting_date",
		},
	"""
	partition_doctypes = frappe.get_hooks("partition_doctypes")

	fiscal_years = frappe.get_all(
		"Fiscal Year", fields=["name", "year_start_date", "year_end_date"], order_by="year_start_date ASC"
	)

	for doctype, settings in partition_doctypes.items():
		table_name = get_table_name(doctype)
		date_field = settings["date_field"][0]
		modify_primary_key(table_name, date_field)

		partition_sql = (
			f"ALTER TABLE `{table_name}` PARTITION BY RANGE (YEAR({date_field})) ("
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
