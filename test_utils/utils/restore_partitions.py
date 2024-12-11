import subprocess
import datetime
import shlex
import os
import importlib.resources
import pymysql
import csv
import gzip
import shutil
import json
import sys
import frappe


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


def dump_schema_only(site, backup_dir, compress):
	exclude_tables = list(
		set(frappe.get_hooks("exclude_tables") + get_partitioned_tables())
	)
	schema_dump_file = f"{backup_dir}/schema_dump.sql"
	try:
		command = (
			f"mysqldump -u {site['user']} -p{site['password']} -h {site['host']} {site['db']} "
			f"--no-data " + " ".join([shlex.quote(table) for table in exclude_tables])
		)

		if compress:
			compressed_file_path = f"{schema_dump_file}.gz"
			with gzip.open(compressed_file_path, "wb") as f:
				process = subprocess.run(command, shell=True, capture_output=True, check=True)
				f.write(process.stdout)
			print(
				f"Schema dump completed and compressed successfully. File saved as {compressed_file_path}."
			)
			return compressed_file_path
		else:
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


def backup_full_database(site, backup_dir, compress):
	partitioned_tables = get_partitioned_tables()
	exclude = list(set(frappe.get_hooks("exclude_tables") + partitioned_tables))

	full_backup_file = f"{backup_dir}/full_backup_file.sql"
	try:
		with importlib.resources.path(
			"test_utils.utils", "mysqldump_wrapper.sh"
		) as script_path:
			temp_script_path = "/tmp/mysqldump_wrapper.sh"
			with open(script_path) as src_file:
				with open(temp_script_path, "w") as temp_file:
					temp_file.write(src_file.read())

			os.chmod(temp_script_path, 0o755)

			command = [
				temp_script_path,
				site["user"],
				site["password"],
				site["host"],
				site["db"],
				full_backup_file,
			] + exclude

			result = subprocess.run(command, capture_output=True, text=True, check=False)

			if result.returncode != 0:
				print(f"Error occurred: {result.stderr}")
				raise Exception(f"Backup failed with return code {result.returncode}")

			if compress:
				compressed_file_path = f"{full_backup_file}.gz"
				with open(full_backup_file, "rb") as f_in:
					with gzip.open(compressed_file_path, "wb") as f_out:
						f_out.writelines(f_in)
				os.remove(full_backup_file)
				print(
					f"Backup completed and compressed successfully. File saved as {compressed_file_path}."
				)
				return compressed_file_path
			else:
				print(f"Backup completed successfully. File saved as {full_backup_file}.")
				return full_backup_file
	except Exception as e:
		print(f"Unexpected error: {e}")


def merge_sql_files(schema_dump_path, full_backup_path, backup_dir, compress):
	output_path = f"{backup_dir}/schema_and_non_partitioned_data.sql"
	try:
		command = [
			"bash",
			"-c",
			f'cat "{schema_dump_path}" "{full_backup_path}" > "{output_path}"',
		]
		result = subprocess.run(command, capture_output=True, text=True, check=False)
		if result.returncode != 0:
			print(f"Error occurred: {result.stderr}")
			raise Exception(f"Merge failed with return code {result.returncode}")

		if compress:
			compressed_file_path = f"{output_path}.gz"
			with open(output_path, "rb") as output_file:
				with gzip.open(compressed_file_path, "wb") as gz_file:
					shutil.copyfileobj(output_file, gz_file)
			os.remove(output_path)
			print(f"Files merged successfully and compressed into {compressed_file_path}.")
			return compressed_file_path
		else:
			print(f"Files merged successfully into {output_path}")
			return output_path
	except Exception as e:
		print(f"An error occurred: {e}")


def restore_database(site, backup_file):
	backup_file = uncompress_if_needed(backup_file)
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


def get_last_n_partitions_for_tables(table_names, n):
	table_names_list = "', '".join(table_names)

	query = f"""
		SELECT TABLE_NAME, PARTITION_NAME
		FROM information_schema.PARTITIONS
		WHERE TABLE_NAME IN ('{table_names_list}')
		AND TABLE_ROWS > 0
		ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION DESC
	"""

	partitions = frappe.db.sql(query, as_dict=True)

	result = {}
	for partition in partitions:
		table_name = partition["TABLE_NAME"]
		partition_name = partition["PARTITION_NAME"]
		if table_name not in result:
			result[table_name] = []
		result[table_name].append(partition_name)

	final_result = {}
	for table_name, partitions in result.items():
		final_result[table_name] = partitions[:n]

	return final_result


def get_partitions_to_backup(partitioned_doctypes_to_restore=None, last_n_partitions=1):
	if partitioned_doctypes_to_restore:
		tables_partitions = partitioned_doctypes_to_restore
	else:
		tables_partitions = list(frappe.get_hooks("partition_doctypes").keys())

	table_names = []
	for doctype in tables_partitions:
		table_names.append(f"tab{doctype}")
		for child_doctype in get_child_doctypes(doctype):
			table_names.append(f"tab{child_doctype}")

	return get_last_n_partitions_for_tables(table_names, last_n_partitions)


def backup_partition(site, table, current_partition, compress):
	timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
	output_file = os.path.join(
		"/tmp/", f'{table.lower().replace(" ", "")}_{current_partition}_{timestamp}.csv'
	)

	sql_query = f"""
	SELECT * FROM `{table}` PARTITION ({current_partition});
	"""
	try:
		connection = pymysql.connect(
			user=site["user"], password=site["password"], host=site["host"], database=site["db"]
		)
		cursor = connection.cursor()
		cursor.execute(sql_query)
		rows = cursor.fetchall()
		columns = [desc[0] for desc in cursor.description]

		with open(output_file, "w", newline="", encoding="utf-8") as f:
			writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
			writer.writerow(columns)
			for row in rows:
				writer.writerow([item if item is not None else "" for item in row])

		connection.commit()
		cursor.close()
		connection.close()
		if compress:
			compressed_file_path = f"{output_file}.gz"
			with open(output_file, "rb") as f_in:
				with gzip.open(compressed_file_path, "wb") as f_out:
					shutil.copyfileobj(f_in, f_out)
			os.remove(output_file)
			print(
				f"Backup of partition {current_partition} completed and compressed. File saved as {compressed_file_path}."
			)
			return compressed_file_path
		else:
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

	csv.field_size_limit(sys.maxsize)
	try:
		partition_bkp_file = uncompress_if_needed(partition_bkp_file)
		connection = pymysql.connect(
			user=site["user"], password=site["password"], host=site["host"], database=site["db"]
		)
		cursor = connection.cursor()

		with open(partition_bkp_file, encoding="utf-8") as f:
			reader = csv.reader(f)
			header = next(reader)
			sanitized_header = [f"`{col}`" for col in header]

			for row in reader:
				row = [None if field == "" else field for field in row]
				sql_query = f"""
				INSERT INTO `{table}` ({', '.join(sanitized_header)})
				VALUES ({', '.join(['%s'] * len(header))});
				"""
				try:
					cursor.execute(sql_query, row)
				except pymysql.MySQLError as e:
					print(f"Error inserting row {row}: {e}")
					continue
		connection.commit()
		cursor.close()
		connection.close()
		print(f"Data imported successfully from {partition_bkp_file}.")
	except pymysql.MySQLError as e:
		print(f"Error during import {partition_bkp_file}: {e}")
	except Exception as e:
		print(f"Unexpected error {partition_bkp_file}: {e}")


def restore_partitions(
	from_site, to_site, compress, partitioned_doctypes_to_restore=None, last_n_partitions=1
):
	partitions_to_backup = get_partitions_to_backup(
		partitioned_doctypes_to_restore, last_n_partitions
	)

	bkps_files = []
	for table, partitions in partitions_to_backup.items():
		for partition in partitions:
			partition_bkp_file = backup_partition(from_site, table, partition, compress)
			bkps_files.append({"table": table, "partition_bkp_file": partition_bkp_file})

	for file in bkps_files:
		restore_partition(to_site, file["table"], file["partition_bkp_file"])


def uncompress_if_needed(file_path):
	if file_path.endswith(".gz"):
		uncompressed_path = file_path[:-3]
		with gzip.open(file_path, "rb") as gz_file:
			with open(uncompressed_path, "wb") as out_file:
				shutil.copyfileobj(gz_file, out_file)
		return uncompressed_path
	return file_path


def get_site_config_data(site_name):
	try:
		site_config_path = f"./{site_name}/site_config.json"

		if not os.path.exists(site_config_path):
			raise FileNotFoundError(f"site_config.json not found for site: {site_name}")

		with open(site_config_path) as f:
			site_config = json.load(f)

		return site_config

	except Exception as e:
		print(f"Error reading site config for site {site_name}: {str(e)}")
		return None


def delete_backup_files(backup_dir):
	try:
		for filename in os.listdir(backup_dir):
			file_path = os.path.join(backup_dir, filename)
			if any(filename.endswith(ext) for ext in [".sql", ".gz", ".csv"]):
				os.remove(file_path)
				print(f"Deleted file: {file_path}")
	except Exception as e:
		print(f"Error while deleting backup files: {str(e)}")


def restore(
	from_site,
	to_site,
	mariadb_user,
	mariadb_password,
	mariadb_host="localhost",
	backup_dir="/tmp",
	partitioned_doctypes_to_restore=None,
	last_n_partitions=1,
	compress=False,
	delete_files=False,
):
	from_site_config = get_site_config_data(from_site)
	to_site_config = get_site_config_data(to_site)
	from_site_config.update(
		{
			"user": mariadb_user,
			"host": mariadb_host,
			"name": from_site,
			"password": mariadb_password,
			"db": from_site_config["db_name"],
		}
	)
	to_site_config.update(
		{
			"user": mariadb_user,
			"host": mariadb_host,
			"name": to_site,
			"password": mariadb_password,
			"db": to_site_config["db_name"],
		}
	)
	schema_dump_file = dump_schema_only(from_site_config, backup_dir, compress)
	full_bkp_file = backup_full_database(from_site_config, backup_dir, compress)
	schema_and_non_partitioned_data = merge_sql_files(
		schema_dump_file, full_bkp_file, backup_dir, compress
	)
	restore_database(to_site_config, schema_and_non_partitioned_data)
	restore_partitions(
		from_site_config,
		to_site_config,
		compress,
		partitioned_doctypes_to_restore,
		last_n_partitions,
	)
	if delete_files:
		delete_backup_files(backup_dir)
