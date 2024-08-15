try:
	import frappe
	from frappe.utils import get_table_name
except Exception as e:
	raise (e)


def primary_key_exists(table_name):
	try:
		result = frappe.db.sql(
			f"""
		SELECT COUNT(*)
		FROM information_schema.TABLE_CONSTRAINTS
		WHERE TABLE_NAME = '{table_name}'
		AND CONSTRAINT_TYPE = 'PRIMARY KEY';
		"""
		)
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

			pk_columns = f"name, {date_field}"
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
	                                "partition_by": "fiscal_year",  # Options: "fiscal_year", "quarter", "month"
	                },
	                "Sales Invoice": {
	                                "date_field": "posting_date",
	                                "partition_by": "fiscal_year",  # Options: "fiscal_year", "quarter", "month"
	                },
	"""
	partition_doctypes = frappe.get_hooks("partition_doctypes")
	fiscal_years = frappe.get_all(
		"Fiscal Year",
		fields=["name", "year_start_date", "year_end_date"],
		order_by="year_start_date ASC",
	)

	for doctype, settings in partition_doctypes.items():
		table_name = get_table_name(doctype)
		date_field = settings.get("date_field", "posting_date")[0]
		partition_by = settings.get("partition_by", "fiscal_year")[0]

		modify_primary_key(table_name, date_field)

		partitions = []

		for fiscal_year in fiscal_years:
			year_start = fiscal_year.get("year_start_date").year
			year_end = fiscal_year.get("year_end_date").year + 1

			if partition_by == "fiscal_year":
				partition_sql = (
					f"ALTER TABLE `{table_name}` PARTITION BY RANGE (YEAR(`{date_field}`)) (\n"
				)
				for fiscal_year in fiscal_years:
					partitions.append(
						f"PARTITION fiscal_year_{year_start} VALUES LESS THAN ({year_end}), "
					)

			elif partition_by == "quarter":
				partition_sql = f"ALTER TABLE `{table_name}` PARTITION BY RANGE (YEAR(`{date_field}`) * 10 + QUARTER(`{date_field}`)) (\n"
				for quarter in range(1, 5):
					quarter_code = year_start * 10 + quarter
					partitions.append(
						f"PARTITION {year_start}_quarter_{quarter} VALUES LESS THAN ({quarter_code + 1})"
					)

			elif partition_by == "month":
				partition_sql = f"ALTER TABLE `{table_name}` PARTITION BY RANGE (YEAR(`{date_field}`) * 100 + MONTH(`{date_field}`)) (\n"
				for month in range(1, 13):
					month_code = year_start * 100 + month
					partitions.append(
						f"PARTITION {year_start}_month_{month:02d} VALUES LESS THAN ({month_code + 1})"
					)

		partition_sql += ",\n".join(partitions)
		partition_sql += ");"

		try:
			frappe.db.sql(partition_sql)
			frappe.db.commit()
			print(f"Partitioning for {doctype} completed successfully.")
		except Exception as e:
			print(f"Error while partitioning {doctype}: {e}")
