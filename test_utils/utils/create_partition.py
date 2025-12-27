import subprocess
import sys
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import frappe


class PerconaConfig:
	DEFAULT_OPTIONS = {
		"chunk_size": 10000,
		#'chunk_time': 0.05,
		"max_load": "Threads_running=100",
		"critical_load": "Threads_running=200",
		#'check_interval': 0.5,
		"recursion_method": "none",
		#'charset': 'utf8mb4',
		"progress": "time,30",
		"print": True,
		"no_check_unique_key_change": True,
		"no_check_alter": True,
		#'set_vars': 'lock_wait_timeout=1,innodb_lock_wait_timeout=1,wait_timeout=28800',
		#'max_lag': '2s',
		#'tries': 'create_triggers:300:0.2,drop_triggers:300:0.2,swap_tables:300:0.2',
		#'no_drop_new_table': True,
	}

	@classmethod
	def build_dsn(cls, host: str, database: str, table: str) -> str:
		return f"h={host},D={database},t={table}"

	@classmethod
	def build_command(
		cls, dsn: str, user: str, password: str, alter: str, **kwargs
	) -> list[str]:
		options = {**cls.DEFAULT_OPTIONS, **kwargs}

		cmd = [
			"pt-online-schema-change",
			f"--alter={alter}",
			dsn,
			f"--user={user}",
			f"--password={password}",
			"--execute",
		]

		for key, value in options.items():
			if isinstance(value, bool):
				if value:
					cmd.append(f'--{key.replace("_", "-")}')
			else:
				cmd.append(f'--{key.replace("_", "-")}={value}')

		return cmd


class DatabaseConnection:
	def __init__(self, root_user: str | None = None, root_password: str | None = None):
		self.config = frappe.get_site_config()
		self.host = self.config.get("db_host", "localhost")
		self.port = self.config.get("db_port", 3306)
		self.database = self.config.get("db_name")
		self.user = root_user or self.config.get("root_login") or self.config.get("db_user")
		self.password = (
			root_password
			or self.config.get("root_password")
			or self.config.get("db_password", "")
		)

	def execute(self, query: str, as_dict: bool = True):
		return frappe.db.sql(query, as_dict=as_dict)

	def get_dsn(self, table: str) -> str:
		return PerconaConfig.build_dsn(self.host, self.database, table)

	def clear_all_connections(self):
		try:
			print("Unlocking tables on current connection...")
			try:
				frappe.db.sql("UNLOCK TABLES")
				frappe.db.commit()
				print("Tables unlocked and transaction committed")
			except Exception as e:
				print(f"Error unlocking tables: {e}")

			connections = self.execute(
				f"""
				SELECT ID, USER, TIME, STATE, COMMAND, INFO
				FROM information_schema.PROCESSLIST
				WHERE DB = '{self.database}'
				AND ID != CONNECTION_ID()
			"""
			)

			if connections:
				print(f"Found {len(connections)} active connection(s) - killing them...")
				killed_count = 0

				for conn in connections:
					try:
						frappe.db.sql(f"KILL {conn['ID']}")
						print(
							f"Killed connection ID {conn['ID']} (Command: {conn['COMMAND']}, Time: {conn['TIME']}s)"
						)
						killed_count += 1
					except Exception as e:
						print(f"Could not kill connection {conn['ID']}: {e}")

				if killed_count > 0:
					import time

					print("Waiting 3 seconds for locks to clear...")
					time.sleep(3)

				return killed_count > 0
			else:
				print("No other connections found")
				return False
		except Exception as e:
			print(f"Error clearing connections: {e}")
			return False


class TableAnalyzer:
	def __init__(self, db: DatabaseConnection):
		self.db = db

	def is_partitioned(self, table: str) -> bool:
		result = self.db.execute(
			f"""
			SELECT COUNT(*) as cnt
			FROM information_schema.PARTITIONS
			WHERE TABLE_SCHEMA = '{self.db.database}'
			AND TABLE_NAME = '{table}'
			AND PARTITION_NAME IS NOT NULL
		"""
		)
		return result[0]["cnt"] > 0

	def get_primary_key(self, table: str) -> list[str]:
		result = self.db.execute(
			f"""
			SELECT COLUMN_NAME
			FROM information_schema.KEY_COLUMN_USAGE
			WHERE TABLE_SCHEMA = '{self.db.database}'
			AND TABLE_NAME = '{table}'
			AND CONSTRAINT_NAME = 'PRIMARY'
			ORDER BY ORDINAL_POSITION
		"""
		)
		return [row["COLUMN_NAME"] for row in result]

	def check_uniqueness(self, table: str, columns: list[str]) -> tuple[bool, int]:
		col_list = ", ".join([f"`{c}`" for c in columns])
		result = self.db.execute(
			f"""
			SELECT
				COUNT(*) as total,
				COUNT(DISTINCT {col_list}) as unique_count
			FROM `{table}`
		"""
		)

		total = result[0]["total"]
		unique = result[0]["unique_count"]
		return total == unique, total - unique

	def get_date_range(self, table: str, field: str) -> tuple[int | None, int | None]:
		result = self.db.execute(
			f"""
			SELECT
				YEAR(MIN(`{field}`)) as min_year,
				YEAR(MAX(`{field}`)) as max_year
			FROM `{table}`
			WHERE `{field}` IS NOT NULL
		"""
		)

		if result and result[0]["min_year"]:
			return result[0]["min_year"], result[0]["max_year"]
		return None, None

	def get_table_size(self, table: str) -> dict:
		result = self.db.execute(
			f"""
			SELECT
				TABLE_ROWS,
				ROUND(DATA_LENGTH / 1024 / 1024, 2) as DATA_MB,
				ROUND(INDEX_LENGTH / 1024 / 1024, 2) as INDEX_MB,
				ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) as TOTAL_MB
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = '{self.db.database}'
			AND TABLE_NAME = '{table}'
		"""
		)
		return result[0] if result else {}

	def get_existing_partitions(self, table: str) -> list[str]:
		result = self.db.execute(
			f"""
			SELECT PARTITION_NAME
			FROM information_schema.PARTITIONS
			WHERE TABLE_SCHEMA = '{self.db.database}'
			AND TABLE_NAME = '{table}'
			AND PARTITION_NAME IS NOT NULL
		"""
		)
		return [row["PARTITION_NAME"] for row in result]


class FieldManager:
	@staticmethod
	def add_custom_field(parent_doctype: str, partition_field: str):
		if partition_field in frappe.model.default_fields:
			if partition_field != "creation":
				return

			partition_docfield = frappe._dict(
				{
					"fieldname": "creation",
					"fieldtype": "Datetime",
					"label": "Creation",
					"options": "",
					"default": "",
				}
			)
		else:
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

		parent_doctype_meta = frappe.get_meta(parent_doctype)
		for child_doctype in [df.options for df in parent_doctype_meta.get_table_fields()]:
			if frappe.get_all(
				"Custom Field", filters={"dt": child_doctype, "fieldname": partition_field}
			) or frappe.get_meta(child_doctype)._fields.get(partition_field):
				continue

			print(f"INFO: Adding {partition_field} to {child_doctype} for {parent_doctype}")

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
				print(f"ERROR: error adding {partition_field} to {child_doctype}: {e}")

	@staticmethod
	def populate_partition_field_for_child(
		parent_doctype: str, child_doctype: str, partition_field: str, chunk_size: int = 50000
	):
		"""Populate partition field in a single child table from parent table"""
		import time

		print(f"\nINFO: Populating '{partition_field}' in '{child_doctype}'...")
		sys.stdout.flush()

		if partition_field not in frappe.model.default_fields and not frappe.get_meta(
			child_doctype
		)._fields.get(partition_field):
			print(
				f"WARNING: Field '{partition_field}' does not exist in '{child_doctype}'. Skipping."
			)
			return

		print(f"DEBUG: Counting unpopulated rows in '{child_doctype}'...")
		sys.stdout.flush()
		count_start = time.time()
		count_timeout_seconds = 60

		try:
			result = frappe.db.sql(
				f"""
				SELECT /*+ MAX_EXECUTION_TIME({count_timeout_seconds * 1000}) */ COUNT(*) as cnt
				FROM `tab{child_doctype}`
				WHERE `{partition_field}` IS NULL
				AND parenttype = %s
			""",
				(parent_doctype,),
				as_dict=True,
			)
			unpopulated_count = result[0]["cnt"] if result else 0

			count_time = time.time() - count_start
			if count_time > count_timeout_seconds:
				print(f"WARNING: Count took {count_time:.1f}s - assuming large table")
				unpopulated_count = -1
			else:
				print(f"DEBUG: Count completed in {count_time:.1f}s")
		except Exception as e:
			count_time = time.time() - count_start
			if "MAX_EXECUTION_TIME" in str(e) or count_time >= count_timeout_seconds:
				print(
					f"WARNING: Count query timed out after {count_timeout_seconds}s - assuming large table"
				)
			else:
				print(f"WARNING: Error counting rows: {e}")
			print("INFO: Proceeding with optimized update (no progress tracking)...")
			sys.stdout.flush()
			unpopulated_count = -1

		sys.stdout.flush()

		if unpopulated_count == 0:
			print(
				f"INFO: Partition field '{partition_field}' in '{child_doctype}' already populated. Skipping."
			)
			return

		if unpopulated_count > 0:
			print(f"INFO: Found {unpopulated_count:,} rows to update")
		else:
			print("INFO: Proceeding with update (count unknown)")
		sys.stdout.flush()

		try:
			print(f"DEBUG: Starting UPDATE for '{child_doctype}'...")
			sys.stdout.flush()
			update_start = time.time()

			if unpopulated_count > 1000000 or unpopulated_count == -1:
				print("INFO: Large table detected - using single optimized UPDATE")
				print("INFO: This may take 5-15 minutes, please wait...")
				sys.stdout.flush()

				frappe.db.sql(
					f"""
					UPDATE `tab{child_doctype}` AS child
					INNER JOIN `tab{parent_doctype}` AS parent ON child.parent = parent.name
					SET child.`{partition_field}` = parent.`{partition_field}`
					WHERE child.`{partition_field}` IS NULL
					AND child.parenttype = %s
				""",
					(parent_doctype,),
				)

				frappe.db.commit()

				elapsed_time = time.time() - update_start
				print(
					f"SUCCESS: Populated '{partition_field}' in '{child_doctype}' (completed in {elapsed_time/60:.1f} minutes)"
				)
				sys.stdout.flush()

			elif unpopulated_count > chunk_size:
				print(f"INFO: Using chunked updates (chunk_size={chunk_size:,})")
				sys.stdout.flush()
				total_updated = 0
				batch_num = 0
				start_time = time.time()

				while True:
					batch_num += 1
					batch_start = time.time()

					frappe.db.sql(
						f"""
						UPDATE `tab{child_doctype}` AS child
						INNER JOIN `tab{parent_doctype}` AS parent ON child.parent = parent.name
						SET child.`{partition_field}` = parent.`{partition_field}`
						WHERE child.`{partition_field}` IS NULL
						AND child.parenttype = %s
						LIMIT {chunk_size}
					""",
						(parent_doctype,),
					)

					rows_affected = frappe.db.sql("SELECT ROW_COUNT()")[0][0]
					if rows_affected == 0:
						break

					total_updated += rows_affected
					frappe.db.commit()

					batch_time = time.time() - batch_start
					elapsed_time = time.time() - start_time
					progress = min(100, (total_updated / unpopulated_count) * 100)

					if total_updated > 0:
						estimated_total = elapsed_time / (total_updated / unpopulated_count)
						remaining = estimated_total - elapsed_time
						eta_mins = remaining / 60

						print(
							f"  Batch {batch_num}: {rows_affected:,} rows in {batch_time:.1f}s "
							f"(Total: {total_updated:,}/{unpopulated_count:,} - {progress:.1f}% - ETA: {eta_mins:.1f}m)"
						)
					else:
						print(
							f"  Batch {batch_num}: Updated {rows_affected:,} rows "
							f"(Total: {total_updated:,}/{unpopulated_count:,} - {progress:.1f}%)"
						)
					sys.stdout.flush()

				total_time = time.time() - start_time
				print(
					f"SUCCESS: Populated '{partition_field}' in '{child_doctype}' "
					f"({total_updated:,} rows in {total_time/60:.1f} minutes)"
				)
				sys.stdout.flush()
			else:
				frappe.db.sql(
					f"""
					UPDATE `tab{child_doctype}` AS child
					INNER JOIN `tab{parent_doctype}` AS parent ON child.parent = parent.name
					SET child.`{partition_field}` = parent.`{partition_field}`
					WHERE child.`{partition_field}` IS NULL
					AND child.parenttype = %s
				""",
					(parent_doctype,),
				)
				frappe.db.commit()
				elapsed_time = time.time() - update_start
				print(
					f"SUCCESS: Populated '{partition_field}' in '{child_doctype}' ({elapsed_time:.1f}s)"
				)
				sys.stdout.flush()
		except Exception as e:
			print(f"ERROR: Error populating '{partition_field}' in '{child_doctype}': {e}")
			sys.stdout.flush()
			frappe.db.rollback()

	@staticmethod
	def populate_partition_fields(
		doctype: str, partition_field: str, chunk_size: int = 50000
	):
		"""Populate partition field in child tables from parent table"""
		import time

		parent_meta = frappe.get_meta(doctype)

		for df in parent_meta.get_table_fields():
			child_doctype = df.options

			print(f"\nINFO: Populating '{partition_field}' in '{child_doctype}'...")
			sys.stdout.flush()

			if partition_field not in frappe.model.default_fields and not frappe.get_meta(
				child_doctype
			)._fields.get(partition_field):
				print(
					f"WARNING: Field '{partition_field}' does not exist in '{child_doctype}'. Skipping."
				)
				continue

			print(f"DEBUG: Counting unpopulated rows in '{child_doctype}'...")
			sys.stdout.flush()
			count_start = time.time()
			count_timeout_seconds = 60

			try:
				result = frappe.db.sql(
					f"""
					SELECT /*+ MAX_EXECUTION_TIME({count_timeout_seconds * 1000}) */ COUNT(*) as cnt
					FROM `tab{child_doctype}`
					WHERE `{partition_field}` IS NULL
					AND parenttype = %s
				""",
					(doctype,),
					as_dict=True,
				)
				unpopulated_count = result[0]["cnt"] if result else 0

				count_time = time.time() - count_start
				if count_time > count_timeout_seconds:
					print(f"WARNING: Count took {count_time:.1f}s - assuming large table")
					unpopulated_count = -1
				else:
					print(f"DEBUG: Count completed in {count_time:.1f}s")
			except Exception as e:
				count_time = time.time() - count_start
				if "MAX_EXECUTION_TIME" in str(e) or count_time >= count_timeout_seconds:
					print(
						f"WARNING: Count query timed out after {count_timeout_seconds}s - assuming large table"
					)
				else:
					print(f"WARNING: Error counting rows: {e}")
				print("INFO: Proceeding with optimized update (no progress tracking)...")
				sys.stdout.flush()
				unpopulated_count = -1

			sys.stdout.flush()

			if unpopulated_count == 0:
				print(
					f"INFO: Partition field '{partition_field}' in '{child_doctype}' already populated. Skipping."
				)
				continue

			if unpopulated_count > 0:
				print(f"INFO: Found {unpopulated_count:,} rows to update")
			else:
				print("INFO: Proceeding with update (count unknown)")
			sys.stdout.flush()

			try:
				print(f"DEBUG: Starting UPDATE for '{child_doctype}'...")
				sys.stdout.flush()
				update_start = time.time()

				if unpopulated_count > 1000000 or unpopulated_count == -1:
					print("INFO: Large table detected - using single optimized UPDATE")
					print("INFO: This may take 5-15 minutes, please wait...")
					sys.stdout.flush()

					frappe.db.sql(
						f"""
						UPDATE `tab{child_doctype}` AS child
						INNER JOIN `tab{doctype}` AS parent ON child.parent = parent.name
						SET child.`{partition_field}` = parent.`{partition_field}`
						WHERE child.`{partition_field}` IS NULL
						AND child.parenttype = %s
					""",
						(doctype,),
					)

					frappe.db.commit()

					elapsed_time = time.time() - update_start
					print(
						f"SUCCESS: Populated '{partition_field}' in '{child_doctype}' (completed in {elapsed_time/60:.1f} minutes)"
					)
					sys.stdout.flush()

				elif unpopulated_count > chunk_size:
					print(f"INFO: Using chunked updates (chunk_size={chunk_size:,})")
					sys.stdout.flush()
					total_updated = 0
					batch_num = 0
					start_time = time.time()

					while True:
						batch_num += 1
						batch_start = time.time()

						frappe.db.sql(
							f"""
							UPDATE `tab{child_doctype}` AS child
							INNER JOIN `tab{doctype}` AS parent ON child.parent = parent.name
							SET child.`{partition_field}` = parent.`{partition_field}`
							WHERE child.`{partition_field}` IS NULL
							AND child.parenttype = %s
							LIMIT {chunk_size}
						""",
							(doctype,),
						)

						rows_affected = frappe.db.sql("SELECT ROW_COUNT()")[0][0]
						if rows_affected == 0:
							break

						total_updated += rows_affected
						frappe.db.commit()

						batch_time = time.time() - batch_start
						elapsed_time = time.time() - start_time
						progress = min(100, (total_updated / unpopulated_count) * 100)

						if total_updated > 0:
							estimated_total = elapsed_time / (total_updated / unpopulated_count)
							remaining = estimated_total - elapsed_time
							eta_mins = remaining / 60

							print(
								f"  Batch {batch_num}: {rows_affected:,} rows in {batch_time:.1f}s "
								f"(Total: {total_updated:,}/{unpopulated_count:,} - {progress:.1f}% - ETA: {eta_mins:.1f}m)"
							)
						else:
							print(
								f"  Batch {batch_num}: Updated {rows_affected:,} rows "
								f"(Total: {total_updated:,}/{unpopulated_count:,} - {progress:.1f}%)"
							)
						sys.stdout.flush()

					total_time = time.time() - start_time
					print(
						f"SUCCESS: Populated '{partition_field}' in '{child_doctype}' "
						f"({total_updated:,} rows in {total_time/60:.1f} minutes)"
					)
					sys.stdout.flush()
				else:
					frappe.db.sql(
						f"""
						UPDATE `tab{child_doctype}` AS child
						INNER JOIN `tab{doctype}` AS parent ON child.parent = parent.name
						SET child.`{partition_field}` = parent.`{partition_field}`
						WHERE child.`{partition_field}` IS NULL
						AND child.parenttype = %s
					""",
						(doctype,),
					)
					frappe.db.commit()
					elapsed_time = time.time() - update_start
					print(
						f"SUCCESS: Populated '{partition_field}' in '{child_doctype}' ({elapsed_time:.1f}s)"
					)
					sys.stdout.flush()
			except Exception as e:
				print(f"ERROR: Error populating '{partition_field}' in '{child_doctype}': {e}")
				sys.stdout.flush()
				frappe.db.rollback()


class PartitionStrategy:
	def __init__(self, field: str, strategy: str = "month"):
		self.field = field
		self.strategy = strategy
		self.strategies = {
			"month": self._generate_monthly,
			"quarter": self._generate_quarterly,
			"year": self._generate_yearly,
			"fiscal_year": self._generate_fiscal_year,
		}

		if strategy not in self.strategies:
			raise ValueError(
				f"Invalid strategy: {strategy}. Use: {list(self.strategies.keys())}"
			)

	def get_expression(self) -> str:
		expressions = {
			"month": f"YEAR(`{self.field}`) * 100 + MONTH(`{self.field}`)",
			"quarter": f"YEAR(`{self.field}`) * 10 + QUARTER(`{self.field}`)",
			"year": f"YEAR(`{self.field}`)",
			"fiscal_year": f"YEAR(`{self.field}`)",
		}
		return expressions[self.strategy]

	def generate_partitions(
		self, start_year: int, end_year: int, table_name: str
	) -> list[dict]:
		return self.strategies[self.strategy](start_year, end_year, table_name)

	def _generate_monthly(
		self, start_year: int, end_year: int, table_name: str
	) -> list[dict]:
		partitions = []
		for year in range(start_year, end_year + 1):
			for month in range(1, 13):
				less_than_value = (year + 1) * 100 + 1 if month == 12 else year * 100 + month + 1

				partitions.append(
					{
						"name": f"{frappe.scrub(table_name)}_{year}_month_{month:02d}",
						"value": less_than_value,
						"description": f"{year}-{month:02d}",
					}
				)
		return partitions

	def _generate_quarterly(
		self, start_year: int, end_year: int, table_name: str
	) -> list[dict]:
		partitions = []
		for year in range(start_year, end_year + 1):
			for quarter in range(1, 5):
				less_than_value = (year + 1) * 10 + 1 if quarter == 4 else year * 10 + quarter + 1

				partitions.append(
					{
						"name": f"{frappe.scrub(table_name)}_{year}_quarter_{quarter}",
						"value": less_than_value,
						"description": f"{year}-Q{quarter}",
					}
				)
		return partitions

	def _generate_yearly(
		self, start_year: int, end_year: int, table_name: str
	) -> list[dict]:
		partitions = []
		for year in range(start_year, end_year + 1):
			partitions.append(
				{
					"name": f"{frappe.scrub(table_name)}_{year}",
					"value": year + 1,
					"description": str(year),
				}
			)
		return partitions

	def _generate_fiscal_year(
		self, start_year: int, end_year: int, table_name: str
	) -> list[dict]:
		partitions = []
		fiscal_years = frappe.get_all(
			"Fiscal Year",
			fields=["name", "year_start_date", "year_end_date"],
			order_by="year_start_date ASC",
		)

		for fiscal_year in fiscal_years:
			year_start = fiscal_year.year_start_date.year
			year_end = fiscal_year.year_end_date.year + 1

			partitions.append(
				{
					"name": f"{frappe.scrub(table_name)}_fiscal_year_{year_start}",
					"value": year_end,
					"description": f"FY {year_start}",
				}
			)

		return partitions


class PartitionEngine:
	def __init__(self, db: DatabaseConnection, use_percona: bool = False):
		self.db = db
		self.analyzer = TableAnalyzer(db)
		self.use_percona = use_percona

	def partition_table(
		self,
		table: str,
		partition_field: str,
		strategy: str = "month",
		years_back: int = 2,
		years_ahead: int = 10,
		dry_run: bool = False,
	) -> bool:
		print(f"\n{'='*80}")
		print(f"Partitioning Table: {table}")
		print(f"Strategy: {strategy} | Field: {partition_field}")
		print(f"{'='*80}\n")

		is_partitioned = self.analyzer.is_partitioned(table)

		if is_partitioned:
			print(f"Table {table} is already partitioned")
			return self._add_new_partitions(
				table, partition_field, strategy, years_back, years_ahead, dry_run
			)

		# Step 1: Analyze table
		if not self._analyze_table(table, partition_field):
			return False

		# Step 2: Modify primary key
		if not self._ensure_primary_key(table, partition_field, dry_run):
			return False

		# Step 3: Apply partitioning
		if not self._apply_partitioning(
			table, partition_field, strategy, years_back, years_ahead, dry_run
		):
			return False

		print(f"\nTable {table} partitioned successfully!\n")
		return True

	def _analyze_table(self, table: str, field: str) -> bool:
		"""Analyze table for partitioning readiness"""
		print("Analyzing table...")

		size_info = self.analyzer.get_table_size(table)
		print(f"   Rows: {size_info.get('TABLE_ROWS', 0):,}")
		print(f"   Size: {size_info.get('TOTAL_MB', 0):.2f} MB")

		min_year, max_year = self.analyzer.get_date_range(table, field)
		if not min_year:
			print("   No data in table - will use current year for partitions")
		else:
			print(f"   Date range: {min_year} to {max_year}")
		print()
		return True

	def _ensure_primary_key(self, table: str, partition_field: str, dry_run: bool) -> bool:
		print("Checking primary key...")

		current_pk = self.analyzer.get_primary_key(table)

		if partition_field in current_pk:
			print(f"  Primary key already includes '{partition_field}'")
			print()
			return True

		new_pk = current_pk + [partition_field]

		# If 'name' is in PK, uniqueness is guaranteed since 'name' is always unique in Frappe
		# Skip the check because COUNT(DISTINCT col1, col2) excludes NULL values,
		# causing false "duplicates" when partition_field has NULLs
		if "name" in current_pk:
			print("  Primary key contains 'name' - uniqueness guaranteed")
		else:
			# Only check uniqueness if 'name' is not in PK (rare edge case)
			is_unique, duplicates = self.analyzer.check_uniqueness(table, new_pk)

			if not is_unique:
				print(f"   Cannot add '{partition_field}' to PK: {duplicates} duplicates found")
				return False

			print("  Uniqueness verified - safe to proceed")

		pk_columns = ", ".join([f"`{col}`" for col in new_pk])

		if dry_run:
			print(f" [DRY RUN] Would modify PK to: ({pk_columns})")
			print()
			return True

		unique_indexes = frappe.db.sql(
			f"SHOW INDEXES FROM `{table}` WHERE Non_unique = 0", as_dict=True
		)

		index_columns = {}
		for idx in unique_indexes:
			idx_name = idx["Key_name"]
			if idx_name not in index_columns:
				index_columns[idx_name] = []
			index_columns[idx_name].append(idx["Column_name"])

		if self.use_percona:
			print(f"   Modifying PK with Percona to include '{partition_field}'...")

			alter_parts = ["DROP PRIMARY KEY"]

			for idx_name, columns in index_columns.items():
				if idx_name == "PRIMARY":
					continue
				alter_parts.append(f"DROP INDEX `{idx_name}`")
				if partition_field not in columns:
					columns.append(partition_field)
				cols_str = ", ".join([f"`{col}`" for col in columns])
				alter_parts.append(f"ADD UNIQUE INDEX `{idx_name}` ({cols_str})")

			alter_parts.append(f"ADD PRIMARY KEY ({pk_columns})")
			alter_statement = ", ".join(alter_parts)
			success = self._run_percona(table, alter_statement)

			if success:
				print("Primary key modified successfully")
			else:
				print("Failed to modify primary key")
			print()
			return success
		else:
			print(f"   Modifying PK to include '{partition_field}'...")
			try:
				frappe.db.sql(f"ALTER TABLE `{table}` DROP PRIMARY KEY")

				for idx_name, columns in index_columns.items():
					if idx_name == "PRIMARY":
						continue
					frappe.db.sql(f"ALTER TABLE `{table}` DROP INDEX `{idx_name}`")
					if partition_field not in columns:
						columns.append(partition_field)
					cols_str = ", ".join([f"`{col}`" for col in columns])
					frappe.db.sql(f"ALTER TABLE `{table}` ADD UNIQUE INDEX `{idx_name}` ({cols_str})")

				frappe.db.sql(f"ALTER TABLE `{table}` ADD PRIMARY KEY ({pk_columns})")
				frappe.db.commit()

				print("Primary key modified successfully")
				print()
				return True
			except Exception as e:
				print(f"Failed to modify primary key: {e}")
				frappe.db.rollback()
				print()
				return False

	def _apply_partitioning(
		self,
		table: str,
		field: str,
		strategy: str,
		years_back: int,
		years_ahead: int,
		dry_run: bool,
	) -> bool:

		print("Creating partitions...")

		min_year, max_year = self.analyzer.get_date_range(table, field)

		if not min_year:
			current_year = datetime.now().year
			start_year = current_year - years_back
			end_year = current_year + years_ahead
			print(f"No data found - using years {start_year} to {end_year}")
		else:
			start_year = min(min_year, datetime.now().year - years_back)
			end_year = max(max_year, datetime.now().year + years_ahead)

		partition_strategy = PartitionStrategy(field, strategy)
		partitions = partition_strategy.generate_partitions(start_year, end_year, table)

		print(f"Generating {len(partitions)} partitions from {start_year} to {end_year}")

		partition_expr = partition_strategy.get_expression()
		partition_defs = [
			f"PARTITION {p['name']} VALUES LESS THAN ({p['value']})" for p in partitions
		]

		alter_stmt = f"PARTITION BY RANGE ({partition_expr}) ({', '.join(partition_defs)})"

		if dry_run:
			print("\n[DRY RUN] Would create partitions:")
			for p in partitions[:5]:
				print(f"      - {p['name']}: {p['description']}")
			if len(partitions) > 5:
				print(f"      ... and {len(partitions) - 5} more")
			print()
			return True

		if self.use_percona:
			print("Applying partitioning with Percona (this may take a while)...")
			success = self._run_percona(table, alter_stmt)
		else:
			print("Applying partitioning with direct ALTER...")
			try:
				frappe.db.sql(f"ALTER TABLE `{table}` {alter_stmt}")
				frappe.db.commit()
				success = True
			except Exception as e:
				print(f"Failed: {e}")
				frappe.db.rollback()
				success = False

		if success:
			print("Partitions created successfully")
		else:
			print("Failed to create partitions")

		print()
		return success

	def _add_new_partitions(
		self,
		table: str,
		field: str,
		strategy: str,
		years_back: int,
		years_ahead: int,
		dry_run: bool,
	) -> bool:
		print("Adding new partitions...")

		existing_partitions = self.analyzer.get_existing_partitions(table)

		min_year, max_year = self.analyzer.get_date_range(table, field)

		if not min_year:
			current_year = datetime.now().year
			start_year = current_year - years_back
			end_year = current_year + years_ahead
		else:
			start_year = min(min_year, datetime.now().year - years_back)
			end_year = max(max_year, datetime.now().year + years_ahead)

		partition_strategy = PartitionStrategy(field, strategy)
		all_partitions = partition_strategy.generate_partitions(start_year, end_year, table)

		new_partitions = [p for p in all_partitions if p["name"] not in existing_partitions]

		if not new_partitions:
			print(" All partitions already exist")
			return True

		print(f"Found {len(new_partitions)} new partitions to add")

		if dry_run:
			print("\n[DRY RUN] Would add partitions:")
			for p in new_partitions[:5]:
				print(f"      - {p['name']}: {p['description']}")
			if len(new_partitions) > 5:
				print(f"      ... and {len(new_partitions) - 5} more")
			return True

		# Add each partition
		for partition in new_partitions:
			partition_def = (
				f"PARTITION {partition['name']} VALUES LESS THAN ({partition['value']})"
			)
			alter_stmt = f"ADD PARTITION ({partition_def})"

			try:
				if self.use_percona:
					self._run_percona(table, alter_stmt)
				else:
					frappe.db.sql(f"ALTER TABLE `{table}` {alter_stmt}")
					frappe.db.commit()
				print(f"Added partition: {partition['name']}")
			except Exception as e:
				print(f"Failed to add {partition['name']}: {e}")
				if not self.use_percona:
					frappe.db.rollback()

		return True

	def _run_percona(self, table: str, alter_stmt: str, **options) -> bool:
		print("\nEnsuring no database locks...")
		self.db.clear_all_connections()

		dsn = self.db.get_dsn(table)
		cmd = PerconaConfig.build_command(
			dsn, self.db.user, self.db.password, alter_stmt, **options
		)

		safe_cmd = [arg if "--password=" not in arg else "--password=***" for arg in cmd]
		print(f"\n   Command: {' '.join(safe_cmd)}\n")
		print(f"   {'='*76}")
		print("   Percona Toolkit Output:")
		print(f"   {'='*76}\n")

		try:
			process = subprocess.Popen(
				cmd,
				stdout=subprocess.PIPE,
				stderr=subprocess.STDOUT,
				text=True,
				bufsize=1,
				universal_newlines=True,
			)

			for line in process.stdout:
				print(f"   {line.rstrip()}")

			return_code = process.wait()
			print(f"\n   {'='*76}")

			if return_code == 0:
				print("Operation completed successfully")
				return True
			else:
				print(f"Operation failed with exit code {return_code}")
				return False

		except FileNotFoundError:
			print("\n pt-online-schema-change not found")
			print("   Install: apt-get install percona-toolkit")
			return False
		except KeyboardInterrupt:
			print("\n\nOperation interrupted by user")
			return False
		except Exception as e:
			print(f"\nError: {e}")
			return False


def create_partition(
	doc=None, years_ahead=10, use_percona=False, root_user=None, root_password=None
):
	"""
	Create partitions for doctypes configured in hooks.py

	Args:
	        doc: Optional document to partition (if provided, only partition that doctype)
	        years_ahead: Number of years ahead to create partitions
	        use_percona: Use pt-online-schema-change for partitioning
	        root_user: Database root user (optional)
	        root_password: Database root password (optional)

	Example:
	        # Partition all doctypes from hooks
	        create_partition(use_percona=True)

	        # Partition specific doctype
	        doc = frappe.get_doc('Sales Order', 'SO-001')
	        create_partition(doc=doc)
	"""
	from frappe.utils import get_table_name

	partition_doctypes = frappe.get_hooks("partition_doctypes")

	if not partition_doctypes:
		print("\nNo partition_doctypes found in hooks")
		print("Add to hooks.py:")
		print(
			"""
			partition_doctypes = {
				"Sales Order": {
					"field": ["transaction_date"],
					"partition_by": ["month"]
				}
			}
		"""
		)
		return False

	if doc:
		if doc.doctype in partition_doctypes:
			partition_doctypes = {doc.doctype: partition_doctypes[doc.doctype]}
		else:
			print(f"ERROR: {doc.doctype} not in partition_doctypes hook")
			return False

	db = DatabaseConnection(root_user, root_password)
	engine = PartitionEngine(db, use_percona)
	field_mgr = FieldManager()

	success_count = 0
	total_count = 0

	for doctype, settings in partition_doctypes.items():
		partition_field = settings.get("field", ["posting_date"])[0]
		partition_by = settings.get("partition_by", ["month"])[0]

		print(f"\n{'='*80}")
		print(f"Processing: {doctype}")
		print(f"Field: {partition_field} | Strategy: {partition_by}")
		print(f"{'='*80}")

		field_mgr.add_custom_field(doctype, partition_field)

		main_table = get_table_name(doctype)
		child_tables = []

		meta = frappe.get_meta(doctype)
		for df in meta.get_table_fields():
			child_tables.append((df.options, get_table_name(df.options)))

		total_count += 1
		if engine.partition_table(
			main_table, partition_field, partition_by, 2, years_ahead, dry_run=False
		):
			success_count += 1

			print(f"\nDEBUG: About to process {len(child_tables)} child tables...")

			for idx, (child_doctype, child_table) in enumerate(child_tables, 1):
				print(f"\nDEBUG: Processing child {idx}/{len(child_tables)}: {child_doctype}")
				print(f"\n{'='*80}")
				print(f"Processing child table: {child_doctype}")
				print(f"{'='*80}")

				field_mgr.populate_partition_field_for_child(
					doctype, child_doctype, partition_field
				)
				frappe.db.commit()

				total_count += 1
				if engine.partition_table(
					child_table, partition_field, partition_by, 2, years_ahead, dry_run=False
				):
					success_count += 1

				print(f"DEBUG: Completed child {idx}/{len(child_tables)}: {child_doctype}")

	print(f"\n{'='*80}")
	print(f"Summary: {success_count}/{total_count} tables partitioned successfully")
	print(f"{'='*80}\n")

	return success_count == total_count


def analyze_table(doctype: str):
	from frappe.utils import get_table_name

	db = DatabaseConnection()
	analyzer = TableAnalyzer(db)
	table = get_table_name(doctype)

	print(f"\n{'='*80}")
	print(f"Table Analysis: {table}")
	print(f"{'='*80}\n")

	size = analyzer.get_table_size(table)
	print(f"Rows: {size.get('TABLE_ROWS', 0):,}")
	print(f"Data: {size.get('DATA_MB', 0):.2f} MB")
	print(f"Index: {size.get('INDEX_MB', 0):.2f} MB")
	print(f"Total: {size.get('TOTAL_MB', 0):.2f} MB")

	pk = analyzer.get_primary_key(table)
	print(f"\nPrimary Key: {', '.join(pk)}")

	is_part = analyzer.is_partitioned(table)
	print(f"Partitioned: {'Yes' if is_part else 'No'}")

	if is_part:
		partitions = analyzer.get_existing_partitions(table)
		print(f"Partition Count: {len(partitions)}")

	print()


def inspect_partitions(doctype: str):
	from frappe.utils import get_table_name

	db = DatabaseConnection()
	table = get_table_name(doctype)

	print(f"\n{'='*80}")
	print(f"Partition Inspection: {doctype} ({table})")
	print(f"{'='*80}\n")

	result = db.execute(
		f"""
        SELECT
            PARTITION_NAME,
            PARTITION_METHOD,
            PARTITION_EXPRESSION,
            PARTITION_DESCRIPTION,
            TABLE_ROWS,
            ROUND(DATA_LENGTH / 1024 / 1024, 2) as DATA_MB,
            ROUND(INDEX_LENGTH / 1024 / 1024, 2) as INDEX_MB
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = '{db.database}'
        AND TABLE_NAME = '{table}'
        AND PARTITION_NAME IS NOT NULL
        ORDER BY PARTITION_ORDINAL_POSITION
    """
	)

	if not result:
		print("Table is NOT partitioned\n")
		return

	print("Table is partitioned")
	print(f"Method: {result[0]['PARTITION_METHOD']}")
	print(f"Expression: {result[0]['PARTITION_EXPRESSION']}")
	print(f"\nTotal partitions: {len(result)}\n")

	print(f"{'Partition':<40} {'Rows':>10} {'Data MB':>10} {'Index MB':>10}")
	print(f"{'-'*40} {'-'*10} {'-'*10} {'-'*10}")

	total_rows = 0
	total_data = 0
	total_index = 0

	for p in result:
		print(
			f"{p['PARTITION_NAME']:<40} {p['TABLE_ROWS']:>10,} {p['DATA_MB']:>10.2f} {p['INDEX_MB']:>10.2f}"
		)
		total_rows += p["TABLE_ROWS"] or 0
		total_data += p["DATA_MB"] or 0
		total_index += p["INDEX_MB"] or 0

	print(f"{'-'*40} {'-'*10} {'-'*10} {'-'*10}")
	print(f"{'TOTAL':<40} {total_rows:>10,} {total_data:>10.2f} {total_index:>10.2f}\n")
