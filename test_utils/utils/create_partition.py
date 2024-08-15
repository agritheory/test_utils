try:
	import frappe
	from frappe.utils import get_table_name
except Exception as e:
	raise (e)


def add_custom_field(parent_doctype, partition_field):
	parent_doctype_meta = frappe.get_meta(parent_doctype)
	partition_docfield = parent_doctype_meta._fields.get(partition_field)

	for child_doctype in [df.options for df in parent_doctype_meta.get_table_fields()]:

		if frappe.get_all(
			"Custom Field", filters={"dt": child_doctype, "fieldname": partition_field}
		):
			return

		custom_field = frappe.get_doc(
			{
				"doctype": "Custom Field",
				"dt": child_doctype,
				"fieldname": partition_docfield.fieldname,
				"fieldtype": partition_docfield.fieldtype,
				"label": partition_docfield.label,
				"options": partition_docfield.options,
				"default": partition_docfield.default,
				"read_only": 1,
				"hidden": 1,
			}
		)
		try:
			custom_field.insert()
		except Exception:
			continue


def populate_partition_fields(doc, event):
	"""
	doc_events = {
	        "*": {
	                "before_insert": "yourapp.utils.populate_partition_fields",
	                "before_save": "yourapp.utils.populate_partition_fields",
	        }
	}
	"""
	partition_doctypes = frappe.get_hooks("partition_doctypes")

	if doc.doctype not in partition_doctypes.keys():
		return

	partition_field = partition_doctypes[doc.doctype]["field"][0]

	for child_doctype_fieldname in [
		(df.options, df.fieldname) for df in frappe.get_meta(doc.doctype).get_table_fields()
	]:
		child_doctype = child_doctype_fieldname[0]
		child_fieldname = child_doctype_fieldname[1]

		if not frappe.get_meta(child_doctype)._fields.get(partition_field):
			add_custom_field(doc.doctype, partition_field)

		for row in getattr(doc, child_fieldname):
			setattr(row, partition_field, doc.get(partition_field))


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


def modify_primary_key(table_name, partition_field):
	try:
		if primary_key_exists(table_name):
			drop_pk_sql = f"""
			ALTER TABLE `{table_name}`
			DROP PRIMARY KEY;
			"""
			frappe.db.sql(drop_pk_sql)

			unique_indexes = frappe.db.sql(
				f"SHOW INDEXES FROM `{table_name}` WHERE Non_unique = 0", as_dict=True
			)

			for index in unique_indexes:
				index_name = index["Key_name"]
				columns = index["Column_name"]
				frappe.db.sql(f"ALTER TABLE `{table_name}` DROP INDEX `{index_name}`;")
				if partition_field not in columns:
					columns = f"{columns}, `{partition_field}`"
				frappe.db.sql(
					f"ALTER TABLE `{table_name}` ADD UNIQUE INDEX `{index_name}` ({columns});"
				)

			pk_columns = f"name, {partition_field}"
			add_pk_sql = f"""
			ALTER TABLE `{table_name}`
			ADD PRIMARY KEY ({pk_columns});
			"""
			frappe.db.sql(add_pk_sql)
			frappe.db.commit()
			print(f"Primary key modified in table {table_name} to include columns: {pk_columns}")
	except Exception as e:
		print(f"Error modifying primary key in table {table_name}: {e}")


def get_partition_doctypes_extended():
	partition_doctypes = frappe.get_hooks("partition_doctypes")
	partition_doctypes_extended = {}

	for doctype, settings in partition_doctypes.items():
		partition_doctypes_extended[doctype] = settings
		for child_doctype in [
			df.options for df in frappe.get_meta(doctype).get_table_fields()
		]:
			partition_doctypes_extended[child_doctype] = settings

	return partition_doctypes_extended


def create_partition():
	"""
	partition_doctypes = {
	                "Sales Order": {
	                                "field": "transaction_date",
	                                "partition_by": "fiscal_year",  # Options: "fiscal_year", "quarter", "month", "field"
	                },
	                "Sales Invoice": {
	                                "field": "posting_date",
	                                "partition_by": "fiscal_year",  # Options: "fiscal_year", "quarter", "month", "field"
	                },
	                "Item": {
	                                "field": "disabled",
	                                "partition_by": "field",  # Options: "fiscal_year", "quarter", "month", "field"
	                },
	"""
	partition_doctypes = frappe.get_hooks("partition_doctypes")

	fiscal_years = frappe.get_all(
		"Fiscal Year",
		fields=["name", "year_start_date", "year_end_date"],
		order_by="year_start_date ASC",
	)

	# Creates the field for child doctypes if it doesn't exists yet
	for doctype, settings in partition_doctypes.items():
		add_custom_field(doctype, settings.get("field", "posting_date")[0])

	for doctype, settings in get_partition_doctypes_extended().items():
		table_name = get_table_name(doctype)
		partition_field = settings.get("field", "posting_date")[0]
		partition_by = settings.get("partition_by", "fiscal_year")[0]

		modify_primary_key(table_name, partition_field)

		if partition_by == "field":
			partitions = []
			partition_values = frappe.db.sql(
				f"SELECT DISTINCT `{partition_field}` FROM `{table_name}`", as_dict=True
			)
			for value in partition_values:
				partition_value = value[partition_field]
				partition_name = f"{partition_field}_{partition_value}"
				if partition_name not in [p.split()[1] for p in partitions]:
					partitions.append(f"PARTITION {partition_name} VALUES IN ({partition_value})")

			if not partitions:
				continue

			partition_sql = (
				f"ALTER TABLE `{table_name}` PARTITION BY LIST (`{partition_field}`) (\n"
			)
			partition_sql += ",\n".join(partitions)
			partition_sql += ");"

			try:
				frappe.db.sql(partition_sql)
				frappe.db.commit()
				print(f"Partitioning for {doctype} completed successfully.")
			except Exception as e:
				print(f"Error while partitioning {doctype}: {e}")

		else:
			partitions = []
			for fiscal_year in fiscal_years:
				year_start = fiscal_year.get("year_start_date").year
				year_end = fiscal_year.get("year_end_date").year + 1

				if partition_by == "fiscal_year":
					partition_sql = (
						f"ALTER TABLE `{table_name}` PARTITION BY RANGE (YEAR(`{partition_field}`)) (\n"
					)
					for fiscal_year in fiscal_years:
						partitions.append(
							f"PARTITION fiscal_year_{year_start} VALUES LESS THAN ({year_end}), "
						)

				elif partition_by == "quarter":
					partition_sql = f"ALTER TABLE `{table_name}` PARTITION BY RANGE (YEAR(`{partition_field}`) * 10 + QUARTER(`{partition_field}`)) (\n"
					for quarter in range(1, 5):
						quarter_code = year_start * 10 + quarter
						partitions.append(
							f"PARTITION {year_start}_quarter_{quarter} VALUES LESS THAN ({quarter_code + 1})"
						)

				elif partition_by == "month":
					partition_sql = f"ALTER TABLE `{table_name}` PARTITION BY RANGE (YEAR(`{partition_field}`) * 100 + MONTH(`{partition_field}`)) (\n"
					for month in range(1, 13):
						month_code = year_start * 100 + month
						partitions.append(
							f"PARTITION {year_start}_month_{month:02d} VALUES LESS THAN ({month_code + 1})"
						)

				elif partition_by == "field":
					field_partitions = []
					partition_values = frappe.db.sql(
						f"SELECT DISTINCT `{partition_field}` FROM `{table_name}`", as_dict=True
					)
					for value in partition_values:
						partition_value = value[partition_field]
						partition_name = f"{partition_field}_{partition_value}"
						if partition_name not in [p.split()[1] for p in partitions]:
							field_partitions.append(
								f"PARTITION {partition_name} VALUES IN ({partition_value})"
							)

					if not field_partitions:
						continue

					partition_sql = (
						f"ALTER TABLE `{table_name}` PARTITION BY LIST (`{partition_field}`) (\n"
					)
					partitions += field_partitions

			partition_sql += ",\n".join(partitions)
			partition_sql += ");"
			try:
				frappe.db.sql(partition_sql)
				frappe.db.commit()
				print(f"Partitioning for {doctype} completed successfully.")
			except Exception as e:
				print(f"Error while partitioning {doctype}: {e}")
