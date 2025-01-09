try:
	import frappe
	from frappe.utils import get_table_name
except Exception as e:
	raise (e)


def add_custom_field(parent_doctype, partition_field):
	# Skip creation of standard fields
	if partition_field in frappe.model.default_fields:
		return
	parent_doctype_meta = frappe.get_meta(parent_doctype)
	partition_docfield = parent_doctype_meta._fields.get(partition_field)
	if not partition_docfield:
		# V13 compatibility
		partition_docfields = list(
			filter(lambda x: x.get("fieldname") == partition_field, parent_doctype_meta.fields)
		)
		partition_docfield = partition_docfields[0] if partition_docfields else None
	if not partition_docfield:
		raise ValueError(
			f"Partition field {partition_field} does not exist for {parent_doctype}"
		)

	for child_doctype in [df.options for df in parent_doctype_meta.get_table_fields()]:
		if frappe.get_all(
			"Custom Field", filters={"dt": child_doctype, "fieldname": partition_field}
		):
			continue
		print(f"Adding {partition_field} to {child_doctype} for {parent_doctype}")
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
		except Exception as e:
			print(f"Error adding {partition_field} to {child_doctype} for {parent_doctype}: {e}")
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


def populate_partition_fields_for_existing_data():
	partition_doctypes = frappe.get_hooks("partition_doctypes")

	for doctype, settings in partition_doctypes.items():
		partition_field = settings["field"][0]
		for child_doctype in [
			df.options for df in frappe.get_meta(doctype).get_table_fields()
		]:
			print(f"Child Doctype: {child_doctype}")
			if not frappe.get_meta(child_doctype)._fields.get(partition_field):
				continue

			try:
				frappe.db.sql(
					f"""
					UPDATE `tab{child_doctype}` child
					JOIN `tab{doctype}` parent ON child.parent = parent.name
					SET child.{partition_field} = parent.{partition_field}
				"""
				)
				frappe.db.commit()
				print(
					f"Partition field {partition_field} in child table {child_doctype} populated."
				)
			except Exception as e:
				print(
					f"Error populationg partition field {partition_field} in child table {child_doctype}: {e}"
				)


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

			pk_info = frappe.db.sql(
				f"SHOW KEYS FROM `{table_name}` WHERE Key_name = 'PRIMARY'", as_dict=True
			)
			current_pk_columns = [row["Column_name"] for row in pk_info]

			if partition_field in current_pk_columns:
				print(
					f"Primary key in table {table_name} already includes the partition field `{partition_field}`. No changes needed."
				)
				return

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


def partition_exists(table_name, partition_name):
	try:
		result = frappe.db.sql(
			f"SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_NAME='{table_name}' AND PARTITION_NAME='{partition_name}'"
		)
		return bool(result)
	except Exception as e:
		print(f"Error checking partition existence: {e}")
		return False


def get_date_range(doctype, field):
	current_year = frappe.utils.now_datetime().year
	result = frappe.db.sql(
		f"SELECT MIN(`{field}`) AS min_date FROM `tab{doctype}`", as_dict=True
	)
	if result and result[0].get("min_date"):
		return result[0]["min_date"].year, current_year
	return None, None


def create_partition():
	"""
	partition_doctypes = {
	                                                                "Sales Order": {
	                                                                                                                                "field": "transaction_date",
	                                                                                                                                "partition_by": "month",  # Options: "fiscal_year", "quarter", "month", "field"
	                                                                },
	                                                                "Sales Invoice": {
	                                                                                                                                "field": "posting_date",
	                                                                                                                                "partition_by": "quarter",  # Options: "fiscal_year", "quarter", "month", "field"
	                                                                },
	                                                                "Item": {
	                                                                                                                                "field": "disabled",
	                                                                                                                                "partition_by": "field",  # Options: "fiscal_year", "quarter", "month", "field"
	                                                                },
	}
	"""
	partition_doctypes = frappe.get_hooks("partition_doctypes")

	# Creates the field for child doctypes if it doesn't exists yet
	for doctype, settings in partition_doctypes.items():
		add_custom_field(doctype, settings.get("field", "posting_date")[0])

	for doctype, settings in get_partition_doctypes_extended().items():
		table_name = get_table_name(doctype)
		partition_field = settings.get("field", "posting_date")[0]
		partition_by = settings.get("partition_by", "fiscal_year")[0]

		modify_primary_key(table_name, partition_field)

		partitions = []
		partition_sql = ""

		start_year, end_year = get_date_range(doctype, partition_field)
		if start_year is None or end_year is None:
			print(f"No data found for {doctype}, skipping partitioning.")
			continue

		if partition_by == "field":
			partition_values = frappe.db.sql(
				f"SELECT DISTINCT `{partition_field}` FROM `{table_name}`", as_dict=True
			)
			for value in partition_values:
				partition_value = value[partition_field]
				partition_name = f"{frappe.scrub(doctype)}_{partition_field}_{partition_value}"
				if partition_name not in [p.split()[1] for p in partitions]:
					partitions.append(f"PARTITION {partition_name} VALUES IN ({partition_value})")
					print(f"Creating partition {partition_name} for {doctype}")

			if not partitions:
				continue

			partition_sql = (
				f"ALTER TABLE `{table_name}` PARTITION BY LIST (`{partition_field}`) (\n"
			)
			partition_sql += ",\n".join(partitions)
			partition_sql += ");"
		elif partition_by == "fiscal_year":
			fiscal_years = frappe.get_all(
				"Fiscal Year",
				fields=["name", "year_start_date", "year_end_date"],
				order_by="year_start_date ASC",
			)
			for fiscal_year in fiscal_years:
				year_start = fiscal_year.get("year_start_date").year
				year_end = fiscal_year.get("year_end_date").year + 1

				if partition_by == "fiscal_year":
					partition_sql = (
						f"ALTER TABLE `{table_name}` PARTITION BY RANGE (YEAR(`{partition_field}`)) (\n"
					)
					partition_name = f"{frappe.scrub(doctype)}_fiscal_year_{year_start}"
					if partition_exists(table_name, partition_name):
						continue
					partitions.append(f"PARTITION {partition_name} VALUES LESS THAN ({year_end}), ")
					print(f"Creating partition {partition_name} for {doctype}")

			if not partitions:
				continue

			partition_sql = (
				f"ALTER TABLE `{table_name}` PARTITION BY LIST (`{partition_field}`) (\n"
			)
			partition_sql += ",\n".join(partitions)
			partition_sql += ");"
		else:
			for year_start in range(start_year, end_year + 1):
				if partition_by == "quarter":
					partition_sql = f"ALTER TABLE `{table_name}` PARTITION BY RANGE (YEAR(`{partition_field}`) * 10 + QUARTER(`{partition_field}`)) (\n"
					for quarter in range(1, 5):
						partition_name = f"{frappe.scrub(doctype)}_{year_start}_quarter_{quarter}"
						if partition_exists(table_name, partition_name):
							continue
						quarter_code = year_start * 10 + quarter
						partitions.append(
							f"PARTITION {partition_name} VALUES LESS THAN ({quarter_code + 1})"
						)
						print(f"Creating partition {partition_name} for {doctype}")

				elif partition_by == "month":
					partition_sql = f"ALTER TABLE `{table_name}` PARTITION BY RANGE (YEAR(`{partition_field}`) * 100 + MONTH(`{partition_field}`)) (\n"
					for month in range(1, 13):
						partition_name = f"{frappe.scrub(doctype)}_{year_start}_month_{month:02d}"
						if partition_exists(table_name, partition_name):
							continue
						month_code = year_start * 100 + month
						partitions.append(
							f"PARTITION {partition_name} VALUES LESS THAN ({month_code + 1})"
						)
						print(f"Creating partition {partition_name} for {doctype}")

			if not partitions:
				print(f"No need to create partitions for {doctype}, skipping partitioning.")
				continue

			partition_sql += ",\n".join(partitions)
			partition_sql += ");"

		try:
			frappe.db.sql(partition_sql)
			frappe.db.commit()
			print(f"Partitioning for {doctype} completed successfully.")
		except Exception as e:
			print(f"Error while partitioning {doctype}: {e}")
