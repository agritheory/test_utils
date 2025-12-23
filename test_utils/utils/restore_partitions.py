import csv
import datetime
import gzip
import importlib.resources
import ipaddress
import json
import os
import shlex
import shutil
import subprocess
import sys
from typing import Optional

import frappe
import pymysql


class SiteConnection:
	def __init__(self, site_name: str):
		self.site_name = site_name
		self.config = frappe.get_site_config()
		self.host = self._get_mariadb_host()
		self.port = int(self.config.get("db_port", 3306))
		self.database = self.config.get("db_name")
		self.user = self.config.get("db_name")
		self.password = self.config.get("db_password")

	def _get_mariadb_host(self) -> str:
		try:
			db_host = self.config.get("db_host", "localhost")

			try:
				result = subprocess.run(
					["getent", "hosts", db_host], capture_output=True, text=True
				)
				if result.stdout:
					return result.stdout.split()[0]
			except Exception:
				pass

			try:
				ipaddress.ip_address(db_host)
				return db_host
			except ValueError:
				pass

			return db_host
		except Exception as e:
			print(f"ERROR: Error getting MariaDB Host: {e}")
			return "localhost"

	def get_connection(self):
		return pymysql.connect(
			host=self.host,
			port=self.port,
			user=self.user,
			password=self.password,
			database=self.database,
		)

	def to_dict(self) -> dict:
		return {
			"name": self.site_name,
			"host": self.host,
			"port": self.port,
			"db": self.database,
			"db_name": self.database,
			"user": self.user,
			"password": self.password,
		}


class PartitionAnalyzer:
	@staticmethod
	def get_child_doctypes(doctype: str) -> list[str]:
		return [df.options for df in frappe.get_meta(doctype).get_table_fields()]

	@staticmethod
	def get_partitioned_tables() -> list[str]:
		partitioned_tables = []
		for doctype in frappe.get_hooks("partition_doctypes").keys():
			partitioned_tables.append(f"tab{doctype}")
			for child_doctype in PartitionAnalyzer.get_child_doctypes(doctype):
				partitioned_tables.append(f"tab{child_doctype}")
		return list(set(partitioned_tables))

	@staticmethod
	def get_last_n_partitions(table_names: list[str], n: int) -> dict[str, list[str]]:
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

	@staticmethod
	def get_partitions_to_backup(
		partitioned_doctypes_to_restore: list | None = None, last_n_partitions: int = 1
	) -> dict[str, list[str]]:
		if partitioned_doctypes_to_restore:
			tables_partitions = partitioned_doctypes_to_restore
		else:
			tables_partitions = list(frappe.get_hooks("partition_doctypes").keys())
		table_names = []
		for doctype in tables_partitions:
			table_names.append(f"tab{doctype}")
			for child_doctype in PartitionAnalyzer.get_child_doctypes(doctype):
				table_names.append(f"tab{child_doctype}")
		return PartitionAnalyzer.get_last_n_partitions(table_names, last_n_partitions)


class BackupEngine:
	def __init__(self, site: SiteConnection, backup_dir: str, compress: bool = False):
		self.site = site
		self.backup_dir = backup_dir
		self.compress = compress

	def dump_schema_only(self) -> str:
		site_dict = self.site.to_dict()
		exclude_tables = list(
			set(frappe.get_hooks("exclude_tables") + PartitionAnalyzer.get_partitioned_tables())
		)
		schema_dump_file = f"{self.backup_dir}/schema_dump.sql"

		try:
			command = (
				f"mysqldump -u {site_dict['user']} -p{site_dict['password']} "
				f"-h {site_dict['host']} {site_dict['db']} --no-data " + shlex.join(exclude_tables)
			)

			if self.compress:
				compressed_file_path = f"{schema_dump_file}.gz"
				with gzip.open(compressed_file_path, "wb") as f:
					process = subprocess.run(command, shell=True, capture_output=True, check=True)
					f.write(process.stdout)
				print(f"Schema dump completed: {compressed_file_path}")
				return compressed_file_path
			else:
				with open(schema_dump_file, "w") as f:
					subprocess.run(
						command, shell=True, stdout=f, stderr=subprocess.PIPE, text=True, check=True
					)
				print(f"Schema dump completed: {schema_dump_file}")
				return schema_dump_file
		except subprocess.CalledProcessError as e:
			print(f"ERROR: Error during schema dump: {e.stderr}")
			raise
		except Exception as e:
			print(f"ERROR: Unexpected error: {e}")
			raise

	def backup_full_database(self) -> str:
		site_dict = self.site.to_dict()
		partitioned_tables = PartitionAnalyzer.get_partitioned_tables()
		exclude = list(set(frappe.get_hooks("exclude_tables") + partitioned_tables))

		full_backup_file = f"{self.backup_dir}/full_backup_file.sql"
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
					site_dict["user"],
					site_dict["password"],
					site_dict["host"],
					site_dict["db"],
					full_backup_file,
				] + exclude

				result = subprocess.run(command, capture_output=True, text=True, check=False)

				if result.returncode != 0:
					print(f"ERROR: Error occurred: {result.stderr}")
					raise Exception(f"Backup failed with return code {result.returncode}")

				if self.compress:
					compressed_file_path = f"{full_backup_file}.gz"
					with open(full_backup_file, "rb") as f_in:
						with gzip.open(compressed_file_path, "wb") as f_out:
							f_out.writelines(f_in)
					os.remove(full_backup_file)
					print(f"Backup completed: {compressed_file_path}")
					return compressed_file_path
				else:
					print(f"Backup completed: {full_backup_file}")
					return full_backup_file
		except Exception as e:
			print(f"ERROR: Unexpected error: {e}")
			raise

	def backup_partition(self, table: str, partition: str) -> str:
		timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
		output_file = os.path.join(
			self.backup_dir, f'{table.lower().replace(" ", "")}_{partition}_{timestamp}.csv'
		)
		sql_query = f"SELECT * FROM `{table}` PARTITION ({partition});"
		try:
			connection = self.site.get_connection()
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

			if self.compress:
				compressed_file_path = f"{output_file}.gz"
				with open(output_file, "rb") as f_in:
					with gzip.open(compressed_file_path, "wb") as f_out:
						shutil.copyfileobj(f_in, f_out)
				os.remove(output_file)
				print(
					f"Backup of partition {partition} completed and compressed: {compressed_file_path}"
				)
				return compressed_file_path
			else:
				print(f"Backup of partition {partition} completed: {output_file}")
				return output_file
		except pymysql.MySQLError as e:
			print(f"ERROR: Error during backup of table {table}, partition {partition}: {e}")
			return None
		except Exception as e:
			print(f"ERROR: Unexpected error: {e}")
			return None

	@staticmethod
	def merge_sql_files(
		schema_dump_path: str, full_backup_path: str, backup_dir: str, compress: bool
	) -> str:
		output_path = f"{backup_dir}/schema_and_non_partitioned_data.sql"
		try:
			command = [
				"bash",
				"-c",
				f'cat "{schema_dump_path}" "{full_backup_path}" > "{output_path}"',
			]
			result = subprocess.run(command, capture_output=True, text=True, check=False)
			if result.returncode != 0:
				print(f"ERROR: Error occurred: {result.stderr}")
				raise Exception(f"Merge failed with return code {result.returncode}")

			if compress:
				compressed_file_path = f"{output_path}.gz"
				with open(output_path, "rb") as output_file:
					with gzip.open(compressed_file_path, "wb") as gz_file:
						shutil.copyfileobj(output_file, gz_file)
				os.remove(output_path)
				print(f"Files merged successfully and compressed into {compressed_file_path}")
				return compressed_file_path
			else:
				print(f"Files merged successfully into {output_path}")
				return output_path
		except Exception as e:
			print(f"ERROR: Unexpected error: {e}")
			raise

	@staticmethod
	def delete_backup_files(backup_dir: str):
		try:
			for filename in os.listdir(backup_dir):
				file_path = os.path.join(backup_dir, filename)
				if any(filename.endswith(ext) for ext in [".sql", ".gz", ".csv"]):
					os.remove(file_path)
					print(f"Deleted file: {file_path}")
		except Exception as e:
			print(f"ERROR: Error while deleting backup files: {str(e)}")


class RestoreEngine:
	def __init__(self, site: SiteConnection):
		self.site = site

	@staticmethod
	def _uncompress_if_needed(file_path: str) -> str:
		if file_path is None:
			raise ValueError("Backup file path is None")
		if file_path.endswith(".gz"):
			uncompressed_path = file_path[:-3]
			with gzip.open(file_path, "rb") as gz_file:
				with open(uncompressed_path, "wb") as out_file:
					shutil.copyfileobj(gz_file, out_file)
			return uncompressed_path
		return file_path

	def restore_database(self, backup_file: str, bubble_backup: bool = False):
		backup_file = self._uncompress_if_needed(backup_file)
		site_dict = self.site.to_dict()

		if bubble_backup:
			try:
				command = [
					"mysql",
					"-u",
					f"{site_dict['user']}",
					f"-p{site_dict['password']}",
					"-h",
					f"{site_dict['host']}",
					f"{site_dict['db_name']}",
					"-e",
					f"source {backup_file}",
				]
				result = subprocess.run(command, capture_output=True, text=True, check=False)

				if result.returncode != 0:
					print(f"ERROR: Error occurred: {result.stderr}")
					raise Exception(f"Restore failed with return code {result.returncode}")

				print("Restore completed successfully")
			except Exception as e:
				print(f"ERROR: Unexpected error: {e}")
				raise
		else:
			try:
				command = [
					"bench",
					"--site",
					site_dict["name"],
					"restore",
					backup_file,
					"--db-root-password",
					site_dict["password"],
				]
				result = subprocess.run(command, capture_output=True, text=True, check=False)

				if result.returncode != 0:
					print(f"ERROR: Error occurred: {result.stderr}")
					raise Exception(f"Restore failed with return code {result.returncode}")

				print("Restore completed successfully")
			except Exception as e:
				print(f"ERROR: Unexpected error: {e}")
				raise

	def restore_partition(self, table: str, partition_bkp_file: str):
		csv.field_size_limit(sys.maxsize)
		try:
			partition_bkp_file = self._uncompress_if_needed(partition_bkp_file)
			connection = self.site.get_connection()
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
			print(f"Data imported successfully from {partition_bkp_file}")
		except pymysql.MySQLError as e:
			print(f"ERROR: Error during import {partition_bkp_file}: {e}")
			raise
		except Exception as e:
			print(f"ERROR: Unexpected error {partition_bkp_file}: {e}")
			raise


def restore_partitions(
	from_site, to_site, compress, partitioned_doctypes_to_restore=None, last_n_partitions=1
):
	partitions_to_backup = PartitionAnalyzer.get_partitions_to_backup(
		partitioned_doctypes_to_restore, last_n_partitions
	)

	backup_engine = BackupEngine(from_site, "/tmp", compress)
	restore_engine = RestoreEngine(to_site)

	bkps_files = []
	for table, partitions in partitions_to_backup.items():
		for partition in partitions:
			partition_bkp_file = backup_engine.backup_partition(table, partition)
			if partition_bkp_file is not None:
				bkps_files.append({"table": table, "partition_bkp_file": partition_bkp_file})
			else:
				print(
					f"WARNING: Skipping restore for {table} partition {partition} (backup failed)"
				)

	for file in bkps_files:
		restore_engine.restore_partition(file["table"], file["partition_bkp_file"])


def uncompress_if_needed(file_path):
	"""Legacy wrapper for backward compatibility"""
	return RestoreEngine._uncompress_if_needed(file_path)


def get_site_config_data(site_name):
	try:
		site_config_path = f"./{site_name}/site_config.json"

		if not os.path.exists(site_config_path):
			raise FileNotFoundError(f"site_config.json not found for site: {site_name}")

		with open(site_config_path) as f:
			site_config = json.load(f)

		return site_config

	except Exception as e:
		print(f"ERROR: Error reading site config for site {site_name}: {str(e)}")
		return None


def restore(
	mariadb_user,
	mariadb_password,
	to_site=None,
	to_database=None,
	backup_dir="/tmp",
	partitioned_doctypes_to_restore=None,
	last_n_partitions=1,
	compress=False,
	delete_files=True,
):
	from_site_name = os.path.basename(frappe.get_site_path())
	from_site_connection = SiteConnection(from_site_name)

	if not to_site and not to_database:
		print("ERROR: Should specify to_site or to_database")
		return

	if to_site:
		bubble_backup = False
		to_site_connection = SiteConnection(to_site)
	else:
		bubble_backup = True
		# Create a connection for the temp database
		to_site_connection = SiteConnection.__new__(SiteConnection)
		to_site_connection.site_name = to_database
		to_site_connection.database = to_database
		to_site_connection.user = mariadb_user
		to_site_connection.password = mariadb_password
		to_site_connection.host = from_site_connection.host
		to_site_connection.port = from_site_connection.port

	backup_engine = BackupEngine(from_site_connection, backup_dir, compress)
	restore_engine = RestoreEngine(to_site_connection)

	schema_dump_file = backup_engine.dump_schema_only()
	full_bkp_file = backup_engine.backup_full_database()
	schema_and_non_partitioned_data = BackupEngine.merge_sql_files(
		schema_dump_file, full_bkp_file, backup_dir, compress
	)
	restore_engine.restore_database(schema_and_non_partitioned_data, bubble_backup)
	restore_partitions(
		from_site_connection,
		to_site_connection,
		compress,
		partitioned_doctypes_to_restore,
		last_n_partitions,
	)

	if delete_files:
		BackupEngine.delete_backup_files(backup_dir)


def bubble_backup(
	mariadb_user,
	mariadb_password,
	backup_dir="/tmp",
	partitioned_doctypes_to_restore=None,
	last_n_partitions=1,
	delete_files=True,
	keep_temp_db=False,
):
	from_site_name = os.path.basename(frappe.get_site_path())
	site_connection = SiteConnection(from_site_name)
	temp_db_name = f"temp_restore_{datetime.datetime.now().strftime('%Y%m%d%H%M')}"
	connection = pymysql.connect(
		host=site_connection.host,
		port=site_connection.port,
		user=mariadb_user,
		password=mariadb_password,
	)

	try:
		try:
			with connection.cursor() as cursor:
				cursor.execute(f"DROP DATABASE IF EXISTS {temp_db_name}")
				cursor.execute(f"CREATE DATABASE {temp_db_name}")
			connection.commit()
		finally:
			connection.close()
			print(f"Temporary database {temp_db_name} created")

		restore(
			mariadb_user,
			mariadb_password,
			to_site=None,
			to_database=temp_db_name,
			backup_dir=backup_dir,
			partitioned_doctypes_to_restore=partitioned_doctypes_to_restore,
			last_n_partitions=last_n_partitions,
			compress=False,
			delete_files=delete_files,
		)
		bubble_bkp_name = f"./{site_connection.site_name}/private/backups/{datetime.datetime.now().strftime('%Y%m%d%H%M')}_bubble.sql"
		dump_command = f"mysqldump -u {mariadb_user} -h {site_connection.host} -p{mariadb_password} {temp_db_name} | gzip > {bubble_bkp_name}.gz"
		subprocess.run(dump_command, shell=True, check=True)
		print(f"Backup SQL dump saved to {bubble_bkp_name}.gz")

	finally:
		if not keep_temp_db:
			connection = pymysql.connect(
				host=site_connection.host, user=mariadb_user, password=mariadb_password
			)
			cursor = connection.cursor()
			cursor.execute(f"DROP DATABASE {temp_db_name};")
			cursor.close()
			connection.close()
			print(f"Temporary database {temp_db_name} deleted")
