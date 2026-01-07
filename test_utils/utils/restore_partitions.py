import datetime
import gzip
import ipaddress
import json
import os
import shutil
import subprocess

import pymysql

import frappe


def _escape_tsv_value(value):
	r"""
	Escape a value for TSV format compatible with MySQL LOAD DATA INFILE.
	Returns \N for NULL, properly escapes special characters.
	"""
	if value is None:
		return "\\N"  # MySQL NULL marker (literal backslash-N)

	s = str(value)
	s = s.replace("\\", "\\\\")
	s = s.replace("\t", "\\t")
	s = s.replace("\n", "\\n")
	s = s.replace("\r", "\\r")
	s = s.replace("\0", "\\0")
	return s


def _unescape_tsv_value(value):
	r"""
	Unescape a TSV value, returning None for NULL marker.
	Reverses the escaping done by _escape_tsv_value.
	"""
	if value == "\\N":
		return None

	result = []
	i = 0
	while i < len(value):
		if value[i] == "\\" and i + 1 < len(value):
			next_char = value[i + 1]
			if next_char == "\\":
				result.append("\\")
			elif next_char == "t":
				result.append("\t")
			elif next_char == "n":
				result.append("\n")
			elif next_char == "r":
				result.append("\r")
			elif next_char == "0":
				result.append("\0")
			elif next_char == "N":
				return None
			else:
				result.append(value[i])
				result.append(next_char)
			i += 2
		else:
			result.append(value[i])
			i += 1

	return "".join(result)


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
		for doctype in frappe.get_hooks("partition_doctypes"):
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
			tables_partitions = frappe.get_hooks("partition_doctypes")
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

	def _build_ignore_table_args(self, tables: list[str]) -> list[str]:
		"""Build --ignore-table arguments as a list for subprocess"""
		site_dict = self.site.to_dict()
		args = []
		for table in tables:
			args.extend(["--ignore-table", f"{site_dict['db']}.{table}"])
		return args

	def dump_schema_only(self) -> str:
		site_dict = self.site.to_dict()
		# Don't exclude ANY tables from schema - we need all table structures
		# Data exclusions happen in backup_full_database()
		schema_dump_file = f"{self.backup_dir}/schema_dump.sql"

		try:
			command = [
				"mysqldump",
				"-u",
				site_dict["user"],
				f"-p{site_dict['password']}",
				"-h",
				site_dict["host"],
				"--no-data",
				"--skip-triggers",  # Exclude pt_osc triggers that reference source DB name
				site_dict["db"],
			]

			if self.compress:
				compressed_file_path = f"{schema_dump_file}.gz"
				process = subprocess.run(command, capture_output=True, check=True)
				with gzip.open(compressed_file_path, "wb") as f:
					f.write(process.stdout)
				print(f"Schema dump completed: {compressed_file_path}")
				return compressed_file_path
			else:
				with open(schema_dump_file, "w") as f:
					result = subprocess.run(
						command, stdout=f, stderr=subprocess.PIPE, text=True, check=True
					)
				print(f"Schema dump completed: {schema_dump_file}")
				return schema_dump_file
		except subprocess.CalledProcessError as e:
			print(f"ERROR: Error during schema dump: {e.stderr}")
			raise
		except Exception as e:
			print(f"ERROR: Unexpected error: {e}")
			raise

	def backup_full_database(self, append_to_file: str = None) -> str:
		site_dict = self.site.to_dict()
		partitioned_tables = PartitionAnalyzer.get_partitioned_tables()
		exclude = list(set(frappe.get_hooks("exclude_tables") + partitioned_tables))

		print(f"\n  Excluding {len(exclude)} tables:")
		ignored_tables = frappe.get_hooks("exclude_tables")
		print(f"    - {len(ignored_tables)} ignored tables")
		print(f"    - {len(partitioned_tables)} partitioned tables")

		try:
			conn = self.site.get_connection()
			cursor = conn.cursor()

			cursor.execute(
				"""
				SELECT TABLE_NAME,
					   ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) as size_mb,
					   TABLE_ROWS
				FROM information_schema.TABLES
				WHERE TABLE_SCHEMA = %s
				AND TABLE_TYPE = 'BASE TABLE'
				ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC
			""",
				(site_dict["db"],),
			)

			all_tables = cursor.fetchall()

			included_tables = []
			excluded_tables = []
			total_included_size = 0
			total_excluded_size = 0

			for table_name, size_mb, rows in all_tables:
				size_mb = size_mb or 0
				rows = rows or 0
				if table_name in exclude:
					excluded_tables.append((table_name, size_mb, rows))
					total_excluded_size += size_mb
				else:
					included_tables.append((table_name, size_mb, rows))
					total_included_size += size_mb

			print("\n  SUMMARY:")
			print(
				f"    Tables to INCLUDE: {len(included_tables)} ({total_included_size:.1f} MB)"
			)
			print(
				f"    Tables to EXCLUDE: {len(excluded_tables)} ({total_excluded_size:.1f} MB)"
			)

			print("\n  Top 15 INCLUDED tables:")
			for table_name, size_mb, rows in included_tables[:15]:
				print(f"    ✓ {table_name}: {size_mb:.1f} MB, ~{rows:,} rows")
			if len(included_tables) > 15:
				print(f"    ... and {len(included_tables) - 15} more tables")

			print("\n  Top 10 EXCLUDED tables:")
			for table_name, size_mb, rows in excluded_tables[:10]:
				print(f"    ✗ {table_name}: {size_mb:.1f} MB, ~{rows:,} rows")
			if len(excluded_tables) > 10:
				print(f"    ... and {len(excluded_tables) - 10} more tables")

			cursor.close()
			conn.close()
		except Exception as e:
			print(f"  Could not get table sizes: {e}")

		full_backup_file = append_to_file or f"{self.backup_dir}/full_backup_file.sql"

		try:
			command = [
				"mysqldump",
				"-u",
				site_dict["user"],
				f"-p{site_dict['password']}",
				"-h",
				site_dict["host"],
				"--single-transaction",
				"--quick",
				"--verbose",
			]
			command.extend(self._build_ignore_table_args(exclude))
			command.append(site_dict["db"])

			print(f"\n  Starting mysqldump with {len(exclude)} --ignore-table args...")
			start_time = datetime.datetime.now()

			if self.compress:
				compressed_file_path = f"{full_backup_file}.gz"
				dump_process = subprocess.Popen(
					command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
				)
				mode = "ab" if append_to_file and os.path.exists(append_to_file) else "wb"

				bytes_written = 0
				last_report = start_time

				with gzip.open(compressed_file_path, mode) as f:
					while True:
						chunk = dump_process.stdout.read(65536)
						if not chunk:
							break
						f.write(chunk)
						bytes_written += len(chunk)

						now = datetime.datetime.now()
						if (now - last_report).total_seconds() >= 5:
							elapsed = (now - start_time).total_seconds()
							rate = bytes_written / elapsed / 1024 / 1024 if elapsed > 0 else 0
							print(
								f"\r  Progress: {bytes_written / 1024 / 1024:.1f} MB written ({rate:.1f} MB/s)",
								end="",
								flush=True,
							)
							last_report = now

				dump_process.wait()
				elapsed = (datetime.datetime.now() - start_time).total_seconds()
				print(f"\n  Backup completed in {elapsed:.1f}s: {compressed_file_path}")

				if dump_process.returncode != 0:
					stderr = dump_process.stderr.read().decode()
					print(f"ERROR: Error occurred: {stderr}")
					raise Exception(f"Backup failed with return code {dump_process.returncode}")
				return compressed_file_path
			else:
				mode = "ab" if append_to_file and os.path.exists(append_to_file) else "wb"

				dump_process = subprocess.Popen(
					command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
				)

				bytes_written = 0
				last_report = start_time
				tables_dumped = 0
				current_table = ""

				import threading

				def read_stderr():
					nonlocal tables_dumped, current_table
					while True:
						line = dump_process.stderr.readline()
						if not line:
							break
						line_str = line.decode("utf-8", errors="ignore").strip()
						# mysqldump --verbose outputs "-- Retrieving table structure for table `X`..."
						if "Retrieving table structure for table" in line_str:
							tables_dumped += 1
							if "`" in line_str:
								current_table = line_str.split("`")[1]
						# Also match "-- Sending SELECT query..." as a fallback indicator
						elif "Sending SELECT query" in line_str and current_table:
							pass  # Just confirming we're dumping data for current table

				stderr_thread = threading.Thread(target=read_stderr)
				stderr_thread.daemon = True
				stderr_thread.start()

				with open(full_backup_file, mode) as f:
					while True:
						chunk = dump_process.stdout.read(65536)
						if not chunk:
							break
						f.write(chunk)
						bytes_written += len(chunk)

						now = datetime.datetime.now()
						if (now - last_report).total_seconds() >= 3:
							elapsed = (now - start_time).total_seconds()
							rate = bytes_written / elapsed / 1024 / 1024 if elapsed > 0 else 0
							print(
								f"\r  [{tables_dumped} tables] {current_table[:30]:<30} | {bytes_written / 1024 / 1024:.1f} MB ({rate:.1f} MB/s)",
								end="",
								flush=True,
							)
							last_report = now

				dump_process.wait()
				stderr_thread.join(timeout=1)

				elapsed = (datetime.datetime.now() - start_time).total_seconds()
				print(
					f"\n  Backup completed: {tables_dumped} tables, {bytes_written / 1024 / 1024:.1f} MB in {elapsed:.1f}s"
				)

				if dump_process.returncode != 0:
					print(f"ERROR: Backup had issues (return code {dump_process.returncode})")
				return full_backup_file

		except Exception as e:
			print(f"ERROR: Unexpected error: {e}")
			raise

	def backup_partition(self, table: str, partition: str) -> str:
		"""
		Backup a partition using SSCursor streaming directly to gzip.
		Uses TSV format for faster LOAD DATA INFILE restore.
		"""
		timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
		# Use .tsv.gz for LOAD DATA INFILE compatibility
		output_file = os.path.join(
			self.backup_dir,
			f'{table.lower().replace(" ", "").replace("`", "")}_{partition}_{timestamp}.tsv.gz',
		)
		sql_query = f"SELECT * FROM `{table}` PARTITION ({partition});"

		try:
			# Use SSCursor to stream rows instead of loading all into memory
			connection = pymysql.connect(
				host=self.site.host,
				port=self.site.port,
				user=self.site.user,
				password=self.site.password,
				database=self.site.database,
				cursorclass=pymysql.cursors.SSCursor,
			)
			cursor = connection.cursor()
			cursor.execute(sql_query)
			columns = [desc[0] for desc in cursor.description]

			if not columns:
				print(f"  No columns found for {table}")
				cursor.close()
				connection.close()
				return None

			# Write directly to gzip using raw TSV format (no csv.writer to avoid double-escaping)
			rows_written = 0
			start_time = datetime.datetime.now()
			last_report = start_time

			with gzip.open(output_file, "wt", encoding="utf-8", compresslevel=6) as f:
				# Write header as first line (will skip during LOAD DATA)
				f.write("\t".join(columns) + "\n")

				batch_size = 10000
				while True:
					rows = cursor.fetchmany(batch_size)
					if not rows:
						break
					for row in rows:
						# Escape each field properly for MySQL LOAD DATA
						escaped_row = []
						for r in row:
							if isinstance(r, (datetime.datetime, datetime.date)):
								escaped_row.append(_escape_tsv_value(str(r)))
							elif isinstance(r, bytes):
								escaped_row.append(_escape_tsv_value(r.decode("utf-8", errors="replace")))
							else:
								escaped_row.append(_escape_tsv_value(r))
						f.write("\t".join(escaped_row) + "\n")
					rows_written += len(rows)

					now = datetime.datetime.now()
					if (now - last_report).total_seconds() >= 2:
						elapsed = (now - start_time).total_seconds()
						rate = rows_written / elapsed if elapsed > 0 else 0
						print(
							f"\r  Exporting: {rows_written:,} rows ({rate:.0f} rows/s)", end="", flush=True
						)
						last_report = now

			cursor.close()
			connection.close()

			elapsed = (datetime.datetime.now() - start_time).total_seconds()
			file_size = os.path.getsize(output_file) / (1024 * 1024)
			print(
				f"\r  Exported: {rows_written:,} rows in {elapsed:.1f}s -> {file_size:.1f} MB compressed"
			)
			return output_file

		except pymysql.MySQLError as e:
			print(f"\nERROR: Error during backup of table {table}, partition {partition}: {e}")
			return None
		except Exception as e:
			print(f"\nERROR: Unexpected error: {e}")
			import traceback

			traceback.print_exc()
			return None

	@staticmethod
	def merge_sql_files(
		schema_dump_path: str, full_backup_path: str, backup_dir: str, compress: bool
	) -> str:
		output_path = f"{backup_dir}/schema_and_non_partitioned_data.sql"
		try:
			if compress:
				compressed_file_path = f"{output_path}.gz"
				with gzip.open(compressed_file_path, "wb") as gz_out:
					with open(schema_dump_path, "rb") as f:
						shutil.copyfileobj(f, gz_out)
					os.remove(schema_dump_path)
					print(f"  Merged and deleted: {schema_dump_path}")
					with open(full_backup_path, "rb") as f:
						shutil.copyfileobj(f, gz_out)
					os.remove(full_backup_path)
					print(f"  Merged and deleted: {full_backup_path}")
				print(f"Files merged and compressed into {compressed_file_path}")
				return compressed_file_path
			else:
				with open(schema_dump_path, "ab") as out_file:
					with open(full_backup_path, "rb") as in_file:
						shutil.copyfileobj(in_file, out_file)
				os.remove(full_backup_path)
				print(f"  Appended and deleted: {full_backup_path}")
				os.rename(schema_dump_path, output_path)
				print(f"Files merged successfully into {output_path}")
				return output_path
		except Exception as e:
			print(f"ERROR: Unexpected error during merge: {e}")
			raise

	@staticmethod
	def delete_backup_files(backup_dir: str):
		try:
			for filename in os.listdir(backup_dir):
				file_path = os.path.join(backup_dir, filename)
				if any(filename.endswith(ext) for ext in [".sql", ".gz", ".csv", ".tsv"]):
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
				file_size = os.path.getsize(backup_file)
				print(
					f"\nRestoring {file_size / 1024 / 1024:.1f} MB SQL file to {site_dict['db_name']}..."
				)

				command = [
					"mysql",
					"-u",
					site_dict["user"],
					f"-p{site_dict['password']}",
					"-h",
					site_dict["host"],
					site_dict["db_name"],
				]

				start_time = datetime.datetime.now()

				with open(backup_file, "rb") as f:
					process = subprocess.Popen(
						command,
						stdin=subprocess.PIPE,
						stdout=subprocess.PIPE,
						stderr=subprocess.PIPE,
					)

					bytes_sent = 0
					last_report = start_time
					chunk_size = 65536

					try:
						while True:
							chunk = f.read(chunk_size)
							if not chunk:
								break
							try:
								process.stdin.write(chunk)
							except BrokenPipeError:
								# mysql crashed - get the error message
								break
							bytes_sent += len(chunk)

							# Progress every 3 seconds
							now = datetime.datetime.now()
							if (now - last_report).total_seconds() >= 3:
								elapsed = (now - start_time).total_seconds()
								rate = bytes_sent / elapsed / 1024 / 1024 if elapsed > 0 else 0
								pct = (bytes_sent / file_size * 100) if file_size > 0 else 0
								print(
									f"\r  Restoring: {bytes_sent / 1024 / 1024:.1f}/{file_size / 1024 / 1024:.1f} MB ({pct:.1f}%) - {rate:.1f} MB/s",
									end="",
									flush=True,
								)
								last_report = now
					finally:
						try:
							process.stdin.close()
						except Exception:
							pass

					stdout = process.stdout.read() if process.stdout else b""
					stderr = process.stderr.read() if process.stderr else b""
					process.wait()

					elapsed = (datetime.datetime.now() - start_time).total_seconds()

					if process.returncode != 0:
						print(f"\n  ERROR at byte {bytes_sent:,} ({bytes_sent / 1024 / 1024:.1f} MB)")
						print(f"  MySQL error: {stderr.decode()}")
						raise Exception(f"Restore failed with return code {process.returncode}")

					print(f"\n  Restore completed in {elapsed:.1f}s")

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

	def restore_partition_fast(self, table: str, partition_bkp_file: str) -> bool:
		"""
		Restore a partition using LOAD DATA LOCAL INFILE - much faster than INSERT.
		Expects a TSV.gz file created by backup_partition().
		"""
		try:
			# Decompress to temp file (LOAD DATA needs uncompressed)
			if partition_bkp_file.endswith(".gz"):
				temp_file = partition_bkp_file[:-3]  # Remove .gz
				print("  Decompressing for LOAD DATA...")
				with gzip.open(partition_bkp_file, "rb") as f_in:
					with open(temp_file, "wb") as f_out:
						shutil.copyfileobj(f_in, f_out)
				needs_cleanup = True
			else:
				temp_file = partition_bkp_file
				needs_cleanup = False

			# Get column names from first line
			with open(temp_file, encoding="utf-8") as f:
				header_line = f.readline().strip()
				columns = header_line.split("\t")

			sanitized_columns = ", ".join([f"`{col}`" for col in columns])

			# Use mysql client for LOAD DATA LOCAL INFILE
			# This is faster than pymysql for large files
			site_dict = self.site.to_dict()

			load_sql = f"""
				SET foreign_key_checks=0;
				SET unique_checks=0;
				ALTER TABLE `{table}` DISABLE KEYS;
				LOAD DATA LOCAL INFILE '{temp_file}'
				INTO TABLE `{table}`
				FIELDS TERMINATED BY '\\t'
				OPTIONALLY ENCLOSED BY '"'
				ESCAPED BY '\\\\'
				LINES TERMINATED BY '\\n'
				IGNORE 1 LINES
				({sanitized_columns});
				ALTER TABLE `{table}` ENABLE KEYS;
				SET unique_checks=1;
				SET foreign_key_checks=1;
			"""

			start_time = datetime.datetime.now()

			# Use mysql client with --local-infile enabled
			cmd = [
				"mysql",
				"-u",
				site_dict["user"],
				f"-p{site_dict['password']}",
				"-h",
				site_dict["host"],
				"--local-infile=1",
				site_dict["db_name"],
			]

			result = subprocess.run(cmd, input=load_sql, capture_output=True, text=True)

			elapsed = (datetime.datetime.now() - start_time).total_seconds()

			if result.returncode != 0:
				if (
					"local infile" in result.stderr.lower()
					or "loading local data" in result.stderr.lower()
				):
					print("  LOAD DATA LOCAL not enabled, falling back to INSERT method...")
					if needs_cleanup:
						os.remove(temp_file)
					return self._restore_partition_insert(table, partition_bkp_file)
				else:
					print(f"  ERROR: LOAD DATA failed: {result.stderr}")
					if needs_cleanup:
						os.remove(temp_file)
					return False

			conn = self.site.get_connection()
			cursor = conn.cursor()
			cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
			row_count = cursor.fetchone()[0]
			cursor.close()
			conn.close()

			print(f"  ✓ LOAD DATA completed: {row_count:,} rows in {elapsed:.1f}s")

			if needs_cleanup:
				os.remove(temp_file)

			return True

		except Exception as e:
			print(f"  ERROR: Fast restore failed: {e}")
			import traceback

			traceback.print_exc()
			# Fall back to INSERT method
			return self._restore_partition_insert(table, partition_bkp_file)

	def _restore_partition_insert(self, table: str, partition_bkp_file: str) -> bool:
		"""
		Fallback restore using INSERT statements (slower but always works).
		Reads raw TSV format created by backup_partition.
		"""
		try:
			# Handle both .gz and uncompressed files
			def open_file(filepath):
				if filepath.endswith(".gz"):
					return gzip.open(filepath, "rt", encoding="utf-8")
				return open(filepath, encoding="utf-8")

			connection = self.site.get_connection()
			cursor = connection.cursor()

			# Disable checks for speed
			cursor.execute("SET foreign_key_checks=0")
			cursor.execute("SET unique_checks=0")
			cursor.execute(f"ALTER TABLE `{table}` DISABLE KEYS")

			total_inserted = 0
			start_time = datetime.datetime.now()
			last_report = start_time

			with open_file(partition_bkp_file) as f:
				header_line = f.readline().rstrip("\n")
				header = header_line.split("\t")
				sanitized_header = [f"`{col}`" for col in header]
				placeholders = ", ".join(["%s"] * len(header))
				insert_sql = (
					f"INSERT INTO `{table}` ({', '.join(sanitized_header)}) VALUES ({placeholders})"
				)

				batch = []
				batch_size = 5000

				for line in f:
					# Parse TSV line manually (don't use csv.reader to avoid escaping issues)
					line = line.rstrip("\n")
					fields = line.split("\t")

					# Unescape each field and convert \N to None
					row = [_unescape_tsv_value(field) for field in fields]
					batch.append(row)

					if len(batch) >= batch_size:
						try:
							cursor.executemany(insert_sql, batch)
							total_inserted += len(batch)
						except pymysql.MySQLError as e:
							# Fall back to individual inserts
							for single_row in batch:
								try:
									cursor.execute(insert_sql, single_row)
									total_inserted += 1
								except pymysql.MySQLError:
									pass
						batch = []

						now = datetime.datetime.now()
						if (now - last_report).total_seconds() >= 2:
							elapsed = (now - start_time).total_seconds()
							rate = total_inserted / elapsed if elapsed > 0 else 0
							print(
								f"\r  Inserting: {total_inserted:,} rows ({rate:.0f} rows/s)",
								end="",
								flush=True,
							)
							last_report = now

				if batch:
					try:
						cursor.executemany(insert_sql, batch)
						total_inserted += len(batch)
					except pymysql.MySQLError:
						for single_row in batch:
							try:
								cursor.execute(insert_sql, single_row)
								total_inserted += 1
							except pymysql.MySQLError:
								pass

			# Re-enable and commit
			cursor.execute(f"ALTER TABLE `{table}` ENABLE KEYS")
			cursor.execute("SET unique_checks=1")
			cursor.execute("SET foreign_key_checks=1")
			connection.commit()
			cursor.close()
			connection.close()

			elapsed = (datetime.datetime.now() - start_time).total_seconds()
			rate = total_inserted / elapsed if elapsed > 0 else 0
			print(
				f"\r  ✓ INSERT completed: {total_inserted:,} rows in {elapsed:.1f}s ({rate:.0f} rows/s)"
			)
			return True

		except Exception as e:
			print(f"\n  ERROR: Insert restore failed: {e}")
			import traceback

			traceback.print_exc()
			return False

	def restore_partition(self, table: str, partition_bkp_file: str):
		"""
		Restore a partition - uses fast LOAD DATA if available, falls back to INSERT.
		"""
		if partition_bkp_file.endswith(".tsv.gz") or partition_bkp_file.endswith(".tsv"):
			success = self.restore_partition_fast(table, partition_bkp_file)
			if success:
				return

		self._restore_partition_insert(table, partition_bkp_file)


def _safe_delete(file_path: str):
	"""Safely delete a file if it exists"""
	try:
		if file_path and os.path.exists(file_path):
			os.remove(file_path)
			gz_path = f"{file_path}.gz"
			if os.path.exists(gz_path):
				os.remove(gz_path)
		if file_path and file_path.endswith(".gz"):
			uncompressed_path = file_path[:-3]
			if os.path.exists(uncompressed_path):
				os.remove(uncompressed_path)
	except Exception as e:
		print(f"WARNING: Could not delete {file_path}: {e}")


def restore_partitions(
	from_site, to_site, compress, partitioned_doctypes_to_restore=None, last_n_partitions=1
):
	"""
	Restore partitions one at a time using intermediate files.
	"""
	partitions_to_backup = PartitionAnalyzer.get_partitions_to_backup(
		partitioned_doctypes_to_restore, last_n_partitions
	)

	backup_engine = BackupEngine(from_site, "/tmp", compress)
	restore_engine = RestoreEngine(to_site)

	total_partitions = sum(len(partitions) for partitions in partitions_to_backup.values())
	current = 0

	for table, partitions in partitions_to_backup.items():
		for partition in partitions:
			current += 1
			print(f"\n[{current}/{total_partitions}] Processing {table} partition {partition}")

			partition_bkp_file = backup_engine.backup_partition(table, partition)

			if partition_bkp_file is None:
				print(
					f"WARNING: Skipping restore for {table} partition {partition} (backup failed)"
				)
				continue

			try:
				restore_engine.restore_partition(table, partition_bkp_file)
			finally:
				_safe_delete(partition_bkp_file)
				print("Deleted partition backup file")


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
	"""
	Restore database using intermediate files.
	"""
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
		to_site_connection = SiteConnection.__new__(SiteConnection)
		to_site_connection.site_name = to_database
		to_site_connection.database = to_database
		to_site_connection.user = mariadb_user
		to_site_connection.password = mariadb_password
		to_site_connection.host = from_site_connection.host
		to_site_connection.port = from_site_connection.port

	backup_engine = BackupEngine(from_site_connection, backup_dir, compress)
	restore_engine = RestoreEngine(to_site_connection)

	print("\n" + "=" * 60)
	print("Backup and Restore using intermediate files")
	print("=" * 60 + "\n")

	# Step 1: Dump schema
	schema_dump_file = backup_engine.dump_schema_only()

	# Step 2: Backup full database directly appended to schema (saves disk space)
	print("Appending full backup data to schema file...")
	schema_and_non_partitioned_data = backup_engine.backup_full_database(
		append_to_file=schema_dump_file
	)

	# Step 3: Restore merged database
	restore_engine.restore_database(schema_and_non_partitioned_data, bubble_backup)

	# Delete merged file after restore
	_safe_delete(schema_and_non_partitioned_data)
	print("Deleted merged backup file")

	# Step 4: Restore partitions one at a time
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
		print("\nCreating final compressed backup...")
		dump_command = f"mysqldump -u {mariadb_user} -h {site_connection.host} -p{mariadb_password} {temp_db_name} --single-transaction --quick | gzip > {bubble_bkp_name}.gz"
		result = subprocess.run(dump_command, shell=True, capture_output=True, text=True)
		if result.returncode != 0:
			print(f"ERROR: Final dump failed: {result.stderr}")
			raise Exception(f"Final dump failed with return code {result.returncode}")
		print(f"Backup SQL dump saved to {bubble_bkp_name}.gz")

	finally:
		if not keep_temp_db:
			try:
				connection = pymysql.connect(
					host=site_connection.host, user=mariadb_user, password=mariadb_password
				)
				cursor = connection.cursor()
				cursor.execute(f"DROP DATABASE IF EXISTS {temp_db_name};")
				cursor.close()
				connection.close()
				print(f"Temporary database {temp_db_name} deleted")
			except Exception as e:
				print(f"WARNING: Could not delete temp database: {e}")
