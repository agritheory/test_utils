import subprocess
import datetime
import shlex
import frappe
import pymysql
import os
import importlib.resources


def get_child_doctypes(doctype):
	doctype_meta = frappe.get_meta(doctype)
	return [df.options for df in doctype_meta.get_table_fields()]


def get_partitioned_tables():
	partitioned_tables = []
	partition_doctypes = frappe.get_hooks("partition_doctypes")
	for doctype in partition_doctypes.keys():
		partitioned_tables.append(f"tab{doctype}")
		for child_doctype in get_child_doctypes(doctype):
			partitioned_tables.append(f"tab{child_doctype}")
	return list(set(partitioned_tables))


def dump_schema_only(site, schema_dump_file):
	exclude_tables = get_partitioned_tables()
	try:
		command = (
			f"mysqldump -u {site['user']} -p{site['password']} -h {site['host']} {site['db']} "
			f"--no-data " + " ".join([shlex.quote(table) for table in exclude_tables])
		)
		with open(schema_dump_file, "w") as f:
			subprocess.run(
				command, shell=True, stdout=f, stderr=subprocess.PIPE, text=True, check=True
			)
		print(f"Schema dump completed successfully. File saved as {schema_dump_file}.")
		return schema_dump_file
	except subprocess.CalledProcessError as e:
		print(f"Error during schema dump: {e.stderr}")
	except Exception as e:
		print(f"Unexpected error: {e}")


def backup_full_database(site, full_backup_file):
	exclude_tables = get_partitioned_tables()  # Fetch list of tables to exclude

	try:
		# Locate the shell script within the module
		with importlib.resources.path(
			"test_utils.utils", "mysqldump_wrapper.sh"
		) as script_path:
			# Copy the script to a temporary location
			temp_script_path = "/tmp/mysqldump_wrapper.sh"
			with open(script_path) as src_file:
				with open(temp_script_path, "w") as temp_file:
					temp_file.write(src_file.read())

			# Make the temporary script executable
			os.chmod(temp_script_path, 0o755)

			# Prepare the command and arguments for the wrapper script
			command = [
				temp_script_path,
				site["user"],
				site["password"],
				site["host"],
				site["db"],
				full_backup_file,
			] + exclude_tables

			# Execute the wrapper script
			print("Executing command:", " ".join(command))  # Debug output

			result = subprocess.run(command, capture_output=True, text=True, check=False)

			# Check for errors
			if result.returncode != 0:
				print(f"Error occurred: {result.stderr}")
				raise Exception(f"Backup failed with return code {result.returncode}")

			return full_backup_file
	except Exception as e:
		print(f"Unexpected error: {e}")


def merge_sql_files(
	schema_dump_path,
	full_backup_path,
	output_path="/tmp/schema_and_non_partitioned_data.sql",
):
	try:
		# Construct the cat command
		command = [
			"bash",
			"-c",
			f'cat "{schema_dump_path}" "{full_backup_path}" > "{output_path}"',
		]

		# Execute the command
		result = subprocess.run(command, capture_output=True, text=True, check=False)

		# Check for errors
		if result.returncode != 0:
			print(f"Error occurred: {result.stderr}")
			raise Exception(f"Merge failed with return code {result.returncode}")

		print(f"Files merged successfully into {output_path}")
		return output_path

	except Exception as e:
		print(f"An error occurred: {e}")


def restore_database(site, backup_file):
	try:
		command = [
			"bench",
			"--site",
			site["name"],
			"restore",
			backup_file,
			"--db-root-password",
			site["password"],
		]
		result = subprocess.run(command, capture_output=True, text=True, check=False)

		if result.returncode != 0:
			print(f"Error occurred: {result.stderr}")
			raise Exception(f"Restore failed with return code {result.returncode}")

		print("Restore completed successfully.")
	except Exception as e:
		print(f"Unexpected error: {e}")


def get_partitions_to_backup(tables_partitions):
	partitions_to_backup = {}
	for doctype, partitions in tables_partitions.items():
		partitions_to_backup[f"tab{doctype}"] = partitions
		for child_doctype in get_child_doctypes(doctype):
			partitions_to_backup[f"tab{child_doctype}"] = partitions
	return partitions_to_backup


def backup_partition(site, table, current_partition):
	timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
	output_file = (
		f'/tmp/{table.lower().replace(" ", "")}_{current_partition}_{timestamp}.sql'
	)

	sql_query = f"""
    SELECT * FROM `{table}` PARTITION ({current_partition})
    INTO OUTFILE '{output_file}'
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n';
    """

	try:
		connection = pymysql.connect(
			user=site["user"], password=site["password"], host=site["host"], database=site["db"]
		)
		cursor = connection.cursor()
		cursor.execute(sql_query)
		connection.commit()
		cursor.close()
		connection.close()
		print(
			f"Backup of partition {current_partition} completed successfully. File saved as {output_file}."
		)
		return output_file
	except pymysql.MySQLError as e:
		print(f"Error during backup: {e}")
		return None
	except Exception as e:
		print(f"Unexpected error: {e}")
		return None


def restore_partition(site, table, partition_bkp_file):
	try:
		connection = pymysql.connect(
			user=site["user"], password=site["password"], host=site["host"], database=site["db"]
		)
		cursor = connection.cursor()
		sql_query = f"""
        LOAD DATA INFILE '{partition_bkp_file}'
        INTO TABLE `{table}`
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES;
        """
		cursor.execute(sql_query)
		connection.commit()
		cursor.close()
		connection.close()
		print(f"Data imported successfully from {partition_bkp_file}.")
	except pymysql.MySQLError as e:
		print(f"Error during import: {e}")
	except Exception as e:
		print(f"Unexpected error: {e}")


"""
from_site = {
    "user": "root",
    "password": "123",
    "host": "172.18.0.2",
    "db": "_9f2612621965ef9a",
    "name": "partition.localhost",
}

to_site = {
    "user": "root",
    "password": "123",
    "host": "172.18.0.2",
    "db": "partitionrestore.localhost",
    "name": "partitionrestore.localhost",
}

tables_partitions = {
    'Sales Order': ['2018_month_01', '2018_month_02'],
    'Sales Invoice': ['2018_month_01', '2018_month_02'],
}

"""


def restore(from_site, to_site, tables_partitions):
	schema_dump_file = dump_schema_only(from_site, "/tmp/schema_dump.sql")
	full_bkp_file = backup_full_database(from_site, "/tmp/full_backup_file.sql")
	schema_and_non_partitioned_data = merge_sql_files(
		schema_dump_file,
		full_bkp_file,
		output_path="/tmp/schema_and_non_partitioned_data.sql",
	)
	restore_database(to_site, schema_and_non_partitioned_data)

	partitions_to_backup = get_partitions_to_backup(tables_partitions)

	bkps_files = []
	for table, partitions in partitions_to_backup.items():
		for partition in partitions:
			partition_bkp_file = backup_partition(from_site, table, partition)
			bkps_files.append({"table": table, "partition_bkp_file": partition_bkp_file})

	for file in bkps_files:
		restore_partition(to_site, file["table"], file["partition_bkp_file"])
