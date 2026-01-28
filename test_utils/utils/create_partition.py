import os
import subprocess
import sys
import time
from datetime import datetime
import frappe

# Tables with fewer rows use direct ALTER
PERCONA_THRESHOLD_ROWS = 100000

# We'll create virtual columns to normalize to 'posting_date' for doctypes that use transaction_date
DOCTYPE_DATE_FIELD_MAP = {
	"Sales Order": "transaction_date",
	"Sales Invoice": "posting_date",
	"Delivery Note": "posting_date",
	"Purchase Order": "transaction_date",
	"Purchase Invoice": "posting_date",
	"Purchase Receipt": "posting_date",
	"Quotation": "transaction_date",
	"Supplier Quotation": "transaction_date",
	"Material Request": "schedule_date",
	"Stock Entry": "posting_date",
	"POS Invoice": "posting_date",
	"Stock Reconciliation": "posting_date",
	"Stock Ledger Entry": "posting_date",
	"Payment Entry": "posting_date",
	"Landed Cost Voucher": "posting_date",
}

# Doctypes that need a virtual posting_date column (they use transaction_date)
DOCTYPES_NEEDING_VIRTUAL_POSTING_DATE = [
	doctype
	for doctype, field in DOCTYPE_DATE_FIELD_MAP.items()
	if field == "transaction_date"
]


def _get_setting_value(settings: dict, key: str, default: str) -> str:
	"""
	Extract a setting value that could be either a string or list.
	Frappe hooks convert dict values to lists, but direct params are strings.
	"""
	value = settings.get(key, default)
	if isinstance(value, list):
		return value[0] if value else default
	return value if value else default


class PerconaConfig:
	DEFAULT_OPTIONS = {
		"chunk_size": 10000,
		"max_load": "Threads_running=100",
		"critical_load": "Threads_running=200",
		"recursion_method": "none",
		"progress": "time,30",
		"print": True,
		"no_check_unique_key_change": True,
		"no_check_alter": True,
		"preserve_triggers": True,
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

	def kill_blocking_queries(self, table: str, max_wait: int = 60) -> bool:
		"""Kill queries that might be blocking operations on a table"""
		print(f"INFO: Checking for blocking queries on {table}...")

		try:
			# Find queries accessing this table or waiting for locks
			blocking = self.execute(
				f"""
				SELECT ID, USER, TIME, STATE, INFO
				FROM information_schema.PROCESSLIST
				WHERE DB = '{self.database}'
				AND ID != CONNECTION_ID()
				AND (
					INFO LIKE '%{table}%'
					OR STATE LIKE '%lock%'
					OR STATE LIKE '%Waiting%'
				)
				AND TIME > 5
				ORDER BY TIME DESC
			"""
			)

			if blocking:
				print(f"WARNING: Found {len(blocking)} potentially blocking queries:")
				for q in blocking[:5]:
					print(f"  - ID {q['ID']}: {q['STATE']} ({q['TIME']}s) - {(q['INFO'] or '')[:80]}")

				for q in blocking:
					try:
						frappe.db.sql(f"KILL {q['ID']}")
						print(f"  Killed query ID {q['ID']}")
					except Exception as e:
						print(f"  Could not kill {q['ID']}: {e}")

				time.sleep(2)
				return True
			else:
				print("INFO: No blocking queries found")
				return False

		except Exception as e:
			print(f"WARNING: Could not check blocking queries: {e}")
			return False

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
					print("Waiting 3 seconds for locks to clear...")
					time.sleep(3)

				return killed_count > 0
			else:
				print("No other connections found")
				return False
		except Exception as e:
			print(f"Error clearing connections: {e}")
			return False

	def column_exists(self, table: str, column: str) -> bool:
		result = self.execute(
			f"""
			SELECT COUNT(*) as cnt
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = '{self.database}'
			AND TABLE_NAME = '{table}'
			AND COLUMN_NAME = '{column}'
		"""
		)
		return result[0]["cnt"] > 0


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

	def get_row_count(self, table: str) -> int:
		result = self.db.execute(
			f"""
			SELECT TABLE_ROWS
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = '{self.db.database}'
			AND TABLE_NAME = '{table}'
			"""
		)
		return result[0]["TABLE_ROWS"] if result else 0

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

	def get_date_range(
		self, table: str, field: str, timeout_seconds: int = 30
	) -> tuple[int | None, int | None]:
		try:
			result = self.db.execute(
				f"""
				SELECT /*+ MAX_EXECUTION_TIME({timeout_seconds * 1000}) */
					YEAR(MIN(`{field}`)) as min_year,
					YEAR(MAX(`{field}`)) as max_year
				FROM `{table}`
				WHERE `{field}` IS NOT NULL
			"""
			)

			if result and result[0]["min_year"]:
				return result[0]["min_year"], result[0]["max_year"]
		except Exception as e:
			print(f"   WARNING: Date range query timed out or failed: {e}")
			print("   Using default year range instead")
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
	def ensure_virtual_posting_date(doctype: str, db: DatabaseConnection) -> bool:
		"""
		Add a virtual posting_date column to doctypes that use transaction_date.
		This normalizes the date field so all shared child tables can use 'posting_date'.
		Also creates the Custom Field metadata so Frappe recognizes the field.
		"""
		if doctype not in DOCTYPES_NEEDING_VIRTUAL_POSTING_DATE:
			return True  # Doctype already uses posting_date

		table = f"tab{doctype}"

		if db.column_exists(table, "posting_date"):
			print(f"INFO: Virtual 'posting_date' column already exists in {doctype}")
		else:
			print(
				f"INFO: Adding virtual 'posting_date' column to {doctype} (derived from transaction_date)"
			)

			try:
				frappe.db.sql(
					f"""
					ALTER TABLE `{table}`
					ADD COLUMN `posting_date` DATE
					GENERATED ALWAYS AS (`transaction_date`) VIRTUAL
				"""
				)
				frappe.db.commit()
				print(f"SUCCESS: Added virtual 'posting_date' column to {doctype}")
			except Exception as e:
				if "Duplicate column name" in str(e):
					print(f"INFO: Column 'posting_date' already exists in {doctype}")
				else:
					print(f"ERROR: Failed to add virtual column to {doctype}: {e}")
					return False

		if not frappe.get_all(
			"Custom Field", filters={"dt": doctype, "fieldname": "posting_date"}
		) and not frappe.get_meta(doctype)._fields.get("posting_date"):
			print(f"INFO: Creating Custom Field metadata for 'posting_date' in {doctype}")
			try:
				custom_field = frappe.get_doc(
					{
						"doctype": "Custom Field",
						"dt": doctype,
						"fieldname": "posting_date",
						"fieldtype": "Date",
						"label": "Posting Date",
						"read_only": 1,
						"hidden": 1,
						"is_virtual": 1,
						"description": "Virtual field derived from transaction_date for partitioning",
					}
				)
				custom_field.flags.ignore_validate = True
				custom_field.insert(ignore_permissions=True)
				frappe.db.commit()
				print(f"SUCCESS: Created Custom Field metadata for 'posting_date' in {doctype}")
			except Exception as e:
				print(f"WARNING: Could not create Custom Field metadata for {doctype}: {e}")
		else:
			print(f"INFO: Custom Field metadata for 'posting_date' already exists in {doctype}")

		return True

	@staticmethod
	def add_posting_date_to_child(child_doctype: str, db: DatabaseConnection) -> bool:
		"""
		Add posting_date column to a child table if it doesn't exist.
		Uses direct ALTER TABLE for speed on large tables.
		"""
		table = f"tab{child_doctype}"

		if db.column_exists(table, "posting_date"):
			print(f"INFO: Column 'posting_date' already exists in {child_doctype}")
			return True

		print(f"INFO: Adding 'posting_date' column to {child_doctype}")

		try:
			frappe.db.sql(
				f"""
				ALTER TABLE `{table}`
				ADD COLUMN `posting_date` DATE NULL,
				ALGORITHM=INPLACE, LOCK=NONE
			"""
			)
			frappe.db.commit()
			print(f"SUCCESS: Added 'posting_date' column to {child_doctype} (online DDL)")
		except Exception as e:
			if "Duplicate column name" in str(e):
				print("INFO: Column 'posting_date' already exists in database")
			elif "ALGORITHM=INPLACE" in str(e):
				# Fallback to regular ALTER if online DDL not supported
				try:
					frappe.db.sql(
						f"""
						ALTER TABLE `{table}`
						ADD COLUMN `posting_date` DATE NULL
					"""
					)
					frappe.db.commit()
					print(f"SUCCESS: Added 'posting_date' column to {child_doctype}")
				except Exception as e2:
					if "Duplicate column name" not in str(e2):
						print(f"ERROR: Failed to add column: {e2}")
						return False
			else:
				print(f"ERROR: Failed to add column: {e}")
				return False

		if not frappe.get_all(
			"Custom Field", filters={"dt": child_doctype, "fieldname": "posting_date"}
		) and not frappe.get_meta(child_doctype)._fields.get("posting_date"):
			try:
				custom_field = frappe.get_doc(
					{
						"doctype": "Custom Field",
						"dt": child_doctype,
						"fieldname": "posting_date",
						"fieldtype": "Date",
						"label": "Posting Date",
						"read_only": 1,
						"hidden": 1,
						"default": "Today",
					}
				)
				custom_field.insert(ignore_permissions=True)
				frappe.db.commit()
				print(
					f"SUCCESS: Created Custom Field metadata for 'posting_date' in {child_doctype}"
				)
			except Exception as e:
				print(f"WARNING: Could not create Custom Field metadata (column exists): {e}")

		return True

	@staticmethod
	def ensure_partition_indexes(
		table: str, date_field: str, db: DatabaseConnection
	) -> bool:
		"""
		Ensure table has optimal indexes for partitioning operations.
		- Index on name (usually exists as PK, but verify)
		- Composite index for child table JOINs
		"""
		print(f"\nINFO: Checking indexes on {table}...")
		sys.stdout.flush()

		indexes_created = 0

		try:
			existing_indexes = frappe.db.sql(
				"""
				SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
				FROM information_schema.STATISTICS
				WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
				ORDER BY INDEX_NAME, SEQ_IN_INDEX
				""",
				(frappe.conf.db_name, table),
				as_dict=True,
			)

			index_columns = {}
			for idx in existing_indexes:
				idx_name = idx["INDEX_NAME"]
				if idx_name not in index_columns:
					index_columns[idx_name] = []
				index_columns[idx_name].append(idx["COLUMN_NAME"])

			name_indexed = any("name" in cols for cols in index_columns.values())
			if not name_indexed:
				print(f"  WARNING: 'name' column not indexed on {table} - this is unusual")
			else:
				print("  ✓ 'name' column is indexed")

			# For child tables, ensure (parent, parenttype) index exists
			if db.column_exists(table, "parent") and db.column_exists(table, "parenttype"):
				parent_indexed = any(
					"parent" in cols and cols.index("parent") == 0 for cols in index_columns.values()
				)
				if not parent_indexed:
					print("  Creating index on (parent, parenttype)...")
					sys.stdout.flush()
					try:
						start = time.time()
						frappe.db.sql(
							f"""
							ALTER TABLE `{table}`
							ADD INDEX `idx_parent_type` (`parent`, `parenttype`)
							"""
						)
						frappe.db.commit()
						elapsed = time.time() - start
						print(f"  ✓ Index on (parent, parenttype) created in {elapsed:.1f}s")
						indexes_created += 1
					except Exception as e:
						if "Duplicate" in str(e):
							print("  ✓ Index on (parent, parenttype) already exists")
						else:
							print(f"  WARNING: Could not create index: {e}")
				else:
					print("  ✓ (parent, parenttype) is indexed")

			if indexes_created > 0:
				print(f"  Created {indexes_created} new index(es)")

			return True

		except Exception as e:
			print(f"  ERROR checking/creating indexes: {e}")
			return False

	@staticmethod
	def populate_posting_date_for_child(
		child_doctype: str,
		db: DatabaseConnection,
		chunk_size: int = 50000,
	):
		"""
		Populate posting_date in a child table from ALL parent types.
		Uses chunked UPDATEs with indexed JOINs for optimal performance.
		Handles parent tables that may not have posting_date by checking
		DOCTYPE_DATE_FIELD_MAP and creating virtual columns if needed.
		"""
		table = f"tab{child_doctype}"

		print(
			f"\nINFO: Populating 'posting_date' in '{child_doctype}' from all parent types..."
		)
		sys.stdout.flush()

		if not db.column_exists(table, "posting_date"):
			print(f"ERROR: Column 'posting_date' does not exist in '{child_doctype}'")
			return

		# Get all parent types for this child
		parent_types = frappe.db.sql(
			f"SELECT DISTINCT parenttype FROM `{table}` WHERE parenttype IS NOT NULL AND parenttype != ''",
			as_dict=True,
		)
		parent_types = [r["parenttype"] for r in parent_types]

		print(f"INFO: Found {len(parent_types)} parent types: {parent_types}")
		sys.stdout.flush()

		# CRITICAL: Ensure indexes exist for fast JOINs (without this, UPDATEs take hours)
		print(f"\nINFO: Ensuring indexes exist on '{child_doctype}' for fast JOINs...")
		FieldManager.ensure_partition_indexes(table, "posting_date", db)

		# Ensure all parent types that need virtual posting_date have it
		for parent_type in parent_types:
			if parent_type in DOCTYPES_NEEDING_VIRTUAL_POSTING_DATE:
				print(
					f"INFO: Ensuring virtual 'posting_date' exists for parent type: {parent_type}"
				)
				FieldManager.ensure_virtual_posting_date(parent_type, db)

		for parent_type in parent_types:
			print(f"\nINFO: Processing parent type: {parent_type}")
			print("INFO: Using chunked updates (skipping slow COUNT query)")
			sys.stdout.flush()

			# Determine source field and whether to use DATE() function
			parent_table = f"tab{parent_type}"
			if not db.column_exists(parent_table, "posting_date"):
				date_field = DOCTYPE_DATE_FIELD_MAP.get(parent_type)
				if date_field and date_field != "posting_date":
					source_field = date_field
					use_date_function = False
				else:
					source_field = "creation"
					use_date_function = True
			else:
				source_field = "posting_date"
				use_date_function = False

			print(
				f"  Using field: {source_field}"
				+ (" (with DATE() conversion)" if use_date_function else "")
			)

			# Process in chunks for progress tracking and to avoid long-running queries
			total_updated = 0
			batch_num = 0
			start_time = time.time()

			while True:
				batch_num += 1
				batch_start = time.time()

				try:
					# Build date expression
					if use_date_function:
						date_expr = f"DATE(parent.`{source_field}`)"
					else:
						date_expr = f"parent.`{source_field}`"

					# Chunked UPDATE with LIMIT for progress and to avoid table locks
					frappe.db.sql(
						f"""
						UPDATE `{table}` AS child
						INNER JOIN `{parent_table}` AS parent ON child.parent = parent.name
						SET child.`posting_date` = {date_expr}
						WHERE child.`posting_date` IS NULL
						AND child.parenttype = %s
						LIMIT %s
						""",
						(parent_type, chunk_size),
					)

					rows = frappe.db.sql("SELECT ROW_COUNT()")[0][0]
					if rows == 0:
						if batch_num == 1:
							print(f"  No NULL rows found for {parent_type}")
						break

					total_updated += rows
					frappe.db.commit()

					batch_time = time.time() - batch_start
					total_time = time.time() - start_time
					rate = total_updated / total_time if total_time > 0 else 0

					print(
						f"  Batch {batch_num}: {rows:,} rows in {batch_time:.1f}s "
						f"(Total: {total_updated:,}, Rate: {rate:,.0f} rows/s)"
					)
					sys.stdout.flush()

					if rows < chunk_size:
						break  # Last batch

				except Exception as e:
					print(f"  ERROR in batch {batch_num}: {e}")
					frappe.db.rollback()
					break

			if total_updated > 0:
				total_time = time.time() - start_time
				avg_rate = total_updated / total_time if total_time > 0 else 0
				print(
					f"SUCCESS: {total_updated:,} rows updated for {parent_type} "
					f"in {total_time:.1f}s ({avg_rate:,.0f} rows/s)"
				)
				sys.stdout.flush()

		# Handle remaining NULLs with creation date fallback (chunked)
		print("\nINFO: Setting fallback dates for any remaining NULLs...")
		sys.stdout.flush()

		try:
			fallback_total = 0
			while True:
				frappe.db.sql(
					f"""
					UPDATE `{table}`
					SET `posting_date` = DATE(`creation`)
					WHERE `posting_date` IS NULL
					LIMIT 100000
				"""
				)
				rows = frappe.db.sql("SELECT ROW_COUNT()")[0][0]
				if rows == 0:
					break
				fallback_total += rows
				frappe.db.commit()
				print(f"  Fallback: {fallback_total:,} rows updated...")

			if fallback_total > 0:
				print(f"SUCCESS: Set fallback date for {fallback_total:,} rows")
			else:
				print(f"INFO: No remaining NULL values in '{child_doctype}'")
		except Exception as e:
			print(f"ERROR: Error setting fallback dates: {e}")
			frappe.db.rollback()

	@staticmethod
	def populate_partition_field_for_child(
		parent_doctype: str, child_doctype: str, partition_field: str, chunk_size: int = 50000
	):
		"""Populate partition field in a single child table from parent table"""
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
				f"INFO: Partition field '{partition_field}' in '{child_doctype}' already populated for {parent_doctype}. Skipping."
			)
		elif unpopulated_count != 0:
			if unpopulated_count > 0:
				print(f"INFO: Found {unpopulated_count:,} rows to update for {parent_doctype}")
			else:
				print(f"INFO: Proceeding with update for {parent_doctype} (count unknown)")
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
		parent_meta = frappe.get_meta(doctype)

		for df in parent_meta.get_table_fields():
			child_doctype = df.options
			FieldManager.populate_partition_field_for_child(
				doctype, child_doctype, partition_field, chunk_size
			)


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

	def _should_use_percona(self, table: str) -> bool:
		if not self.use_percona:
			return False

		row_count = self.analyzer.get_row_count(table)

		if row_count < PERCONA_THRESHOLD_ROWS:
			print(
				f"INFO: Table has {row_count:,} rows (< {PERCONA_THRESHOLD_ROWS:,}) - using direct ALTER"
			)
			return False
		else:
			print(
				f"INFO: Table has {row_count:,} rows (>= {PERCONA_THRESHOLD_ROWS:,}) - using Percona"
			)
			return True

	def _direct_alter_pk(
		self, table: str, pk_field: str, pk_columns: str, index_columns: dict
	) -> bool:
		"""Perform direct ALTER TABLE to modify primary key

		Args:
		                                                                                                                                                                                                                                                                table: Table name
		                                                                                                                                                                                                                                                                pk_field: The field to add to primary key (real field, not virtual)
		                                                                                                                                                                                                                                                                pk_columns: Comma-separated list of PK columns
		                                                                                                                                                                                                                                                                index_columns: Dict of index name -> column list
		"""
		try:
			self.db.kill_blocking_queries(table)
			frappe.db.commit()

			current_pk = self.analyzer.get_primary_key(table)
			if current_pk:
				print(f"  Dropping existing PRIMARY KEY: {current_pk}")
				frappe.db.sql(f"ALTER TABLE `{table}` DROP PRIMARY KEY")
			else:
				print("  No existing PRIMARY KEY to drop")

			for idx_name, columns in index_columns.items():
				if idx_name == "PRIMARY":
					continue
				try:
					frappe.db.sql(f"ALTER TABLE `{table}` DROP INDEX `{idx_name}`")
					if pk_field not in columns:
						columns.append(pk_field)
					cols_str = ", ".join([f"`{col}`" for col in columns])
					frappe.db.sql(f"ALTER TABLE `{table}` ADD UNIQUE INDEX `{idx_name}` ({cols_str})")
				except Exception as idx_e:
					print(f"  WARNING: Could not modify index {idx_name}: {idx_e}")

			print(f"  Adding PRIMARY KEY ({pk_columns})")
			frappe.db.sql(f"ALTER TABLE `{table}` ADD PRIMARY KEY ({pk_columns})")
			frappe.db.commit()

			print("Primary key modified successfully")
			return True
		except Exception as e:
			print(f"Failed to modify primary key: {e}")
			frappe.db.rollback()
			return False

	def partition_table(
		self,
		table: str,
		partition_field: str,
		strategy: str = "month",
		years_back: int = 2,
		years_ahead: int = 10,
		dry_run: bool = False,
		pk_field: str = None,
	) -> bool:
		"""
		Partition a table by the specified field.

		Args:
		                                                                                                                                                                                                                                                                table: Table name
		                                                                                                                                                                                                                                                                partition_field: Field to use for partition expression
		                                                                                                                                                                                                                                                                strategy: Partition strategy (month, quarter, year, fiscal_year)
		                                                                                                                                                                                                                                                                years_back: Years of history to partition
		                                                                                                                                                                                                                                                                years_ahead: Future years to create partitions for
		                                                                                                                                                                                                                                                                dry_run: If True, only show what would be done
		                                                                                                                                                                                                                                                                pk_field: Field to add to primary key. Defaults to partition_field.
		                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  Use this when partition_field is virtual (can't be in PK).
		                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  For parent tables with transaction_date, pk_field should be
		                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  the real field (transaction_date), not virtual posting_date.
		"""
		if pk_field is None:
			pk_field = partition_field

		print(f"\n{'='*80}")
		print(f"Partitioning Table: {table}")
		print(
			f"Strategy: {strategy} | Partition Field: {partition_field} | PK Field: {pk_field}"
		)
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

		# Step 2: Modify primary key (uses pk_field, which must be a real column)
		if not self._ensure_primary_key(table, pk_field, dry_run):
			return False

		# Step 3: Apply partitioning (uses partition_field for the expression)
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

	def _ensure_primary_key(self, table: str, pk_field: str, dry_run: bool) -> bool:
		"""Ensure primary key includes the pk_field.

		Args:
		                                                                                                                                                                                                                                                                table: Table name
		                                                                                                                                                                                                                                                                pk_field: The REAL field to add to PK (not virtual column)
		                                                                                                                                                                                                                                                                dry_run: If True, only show what would be done
		"""
		print("Checking primary key...")

		current_pk = self.analyzer.get_primary_key(table)

		if pk_field in current_pk:
			print(f"  Primary key already includes '{pk_field}'")
			print()
			return True

		new_pk = current_pk + [pk_field]

		if "name" not in new_pk:
			new_pk = ["name"] + [pk_field]
			print(f"  WARNING: 'name' was not in PK, adding it: {new_pk}")

		if "name" in new_pk:
			print("  Primary key will include 'name' - uniqueness guaranteed")
		else:
			is_unique, duplicates = self.analyzer.check_uniqueness(table, new_pk)

			if not is_unique:
				print(f"   Cannot add '{pk_field}' to PK: {duplicates} duplicates found")
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

		use_percona_for_table = self._should_use_percona(table)

		if use_percona_for_table:
			print(f"   Modifying PK with Percona to include '{pk_field}'...")

			self.db.kill_blocking_queries(table)

			alter_parts = ["DROP PRIMARY KEY"]

			for idx_name, columns in index_columns.items():
				if idx_name == "PRIMARY":
					continue
				alter_parts.append(f"DROP INDEX `{idx_name}`")
				if pk_field not in columns:
					columns.append(pk_field)
				cols_str = ", ".join([f"`{col}`" for col in columns])
				alter_parts.append(f"ADD UNIQUE INDEX `{idx_name}` ({cols_str})")

			alter_parts.append(f"ADD PRIMARY KEY ({pk_columns})")
			alter_statement = ", ".join(alter_parts)
			success = self._run_percona(table, alter_statement)

			if success:
				print("Primary key modified successfully")
			else:
				print("Percona failed, falling back to direct ALTER...")
				# Fallback to direct ALTER
				success = self._direct_alter_pk(table, pk_field, pk_columns, index_columns)
			print()
			return success
		else:
			print(f"   Modifying PK with direct ALTER to include '{pk_field}'...")
			success = self._direct_alter_pk(table, pk_field, pk_columns, index_columns)
			print()
			return success

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

		# NOTE: Percona pt-online-schema-change does NOT support partitioning operations
		# We must use native ALTER TABLE for partitioning
		# This is generally fast as it's primarily a metadata operation for RANGE partitioning
		print("Applying partitioning with native ALTER TABLE...")
		print(
			"NOTE: Partitioning is primarily a metadata operation and should be relatively fast"
		)

		try:
			self.db.kill_blocking_queries(table)
			frappe.db.commit()

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
			print("   All partitions already exist")
			return True

		print(f"Found {len(new_partitions)} new partitions to add")

		if dry_run:
			print("\n[DRY RUN] Would add partitions:")
			for p in new_partitions[:5]:
				print(f"      - {p['name']}: {p['description']}")
			if len(new_partitions) > 5:
				print(f"      ... and {len(new_partitions) - 5} more")
			return True

		# NOTE: Always use native ALTER TABLE for adding partitions
		# Adding partitions is a fast metadata-only operation - Percona is unnecessary
		# and causes lock/trigger issues on busy tables (it tries to copy the entire table)
		print("Adding partitions with native ALTER TABLE (metadata operation)...")

		success_count = 0
		for partition in new_partitions:
			partition_def = (
				f"PARTITION {partition['name']} VALUES LESS THAN ({partition['value']})"
			)
			alter_stmt = f"ALTER TABLE `{table}` ADD PARTITION ({partition_def})"

			try:
				self.db.kill_blocking_queries(table)
				frappe.db.sql(alter_stmt)
				frappe.db.commit()
				print(f"   Added partition: {partition['name']}")
				success_count += 1
			except Exception as e:
				error_msg = str(e)
				if "Duplicate partition" in error_msg or "already exists" in error_msg.lower():
					print(f"   Partition {partition['name']} already exists, skipping...")
					success_count += 1
				else:
					print(f"   Failed to add {partition['name']}: {e}")
					frappe.db.rollback()

		print(f"\nAdded {success_count}/{len(new_partitions)} partitions successfully")
		return success_count == len(new_partitions)

	def _run_percona(self, table: str, alter_stmt: str, **options) -> bool:
		print("\nEnsuring no database locks...")

		self.db.kill_blocking_queries(table)

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
	doc=None,
	years_ahead=10,
	use_percona=False,
	root_user=None,
	root_password=None,
	partition_doctypes=None,
):
	"""
	Create partitions for doctypes configured in hooks.py or passed directly.

	All doctypes are normalized to use 'posting_date' as the partition field.
	For doctypes that use 'transaction_date', a virtual column is created.
	"""
	from frappe.utils import get_table_name

	if partition_doctypes is None:
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
	processed_child_tables = set()

	for doctype, settings in partition_doctypes.items():
		original_partition_field = _get_setting_value(settings, "field", "posting_date")
		partition_by = _get_setting_value(settings, "partition_by", "month")

		print(f"\n{'='*80}")
		print(f"Processing: {doctype}")
		print(f"Original Field: {original_partition_field} | Strategy: {partition_by}")
		print(f"{'='*80}")

		# Get the actual date field for this doctype (transaction_date or posting_date)
		actual_date_field = DOCTYPE_DATE_FIELD_MAP.get(doctype, original_partition_field)
		print(f"INFO: Doctype uses '{actual_date_field}' as its date field")

		# Step 1: Ensure virtual posting_date exists for doctypes using transaction_date
		# This is ONLY for normalizing child table population, NOT for PK
		if actual_date_field == "transaction_date":
			field_mgr.ensure_virtual_posting_date(doctype, db)
			print("INFO: Virtual 'posting_date' created for child table normalization")

		main_table = get_table_name(doctype)
		child_tables = []

		meta = frappe.get_meta(doctype)
		for df in meta.get_table_fields():
			child_tables.append((df.options, get_table_name(df.options)))

		# Partition main table using REAL date field for both PK and partition
		# Parent tables use their actual date field (transaction_date or posting_date)
		total_count += 1
		print(
			f"INFO: Partitioning parent table with field='{actual_date_field}', pk_field='{actual_date_field}'"
		)
		if engine.partition_table(
			main_table,
			partition_field=actual_date_field,  # Use real field for partition expression
			strategy=partition_by,
			years_back=2,
			years_ahead=years_ahead,
			dry_run=False,
			pk_field=actual_date_field,  # Use real field for PK (not virtual)
		):
			success_count += 1

			print(f"\nDEBUG: About to process {len(child_tables)} child tables...")

			for idx, (child_doctype, child_table) in enumerate(child_tables, 1):
				print(f"\nDEBUG: Processing child {idx}/{len(child_tables)}: {child_doctype}")
				print(f"\n{'='*80}")
				print(f"Processing child table: {child_doctype}")
				print(f"{'='*80}")

				# Skip if already processed (shared child table)
				if child_table in processed_child_tables:
					print(f"INFO: {child_doctype} already processed, skipping...")
					continue

				processed_child_tables.add(child_table)

				# Add posting_date column to child table
				field_mgr.add_posting_date_to_child(child_doctype, db)

				# Populate posting_date from all parent types
				field_mgr.populate_posting_date_for_child(child_doctype, db)
				frappe.db.commit()

				# Partition child table using posting_date (REAL column, not virtual)
				# Child tables always use posting_date for both PK and partition
				total_count += 1
				print(
					"INFO: Partitioning child table with field='posting_date', pk_field='posting_date'"
				)
				if engine.partition_table(
					child_table,
					partition_field="posting_date",  # Real column for partition
					strategy=partition_by,
					years_back=2,
					years_ahead=years_ahead,
					dry_run=False,
					pk_field="posting_date",  # Real column for PK
				):
					success_count += 1

				print(f"DEBUG: Completed child {idx}/{len(child_tables)}: {child_doctype}")

	print(f"\n{'='*80}")
	print(f"Summary: {success_count}/{total_count} tables partitioned successfully")
	print(f"{'='*80}\n")
	return success_count == total_count


def create_partition_phase1(
	doc=None,
	root_user=None,
	root_password=None,
	partition_doctypes=None,
):
	"""
	PHASE 1: Create posting_date columns only (no data population)
	- Creates virtual posting_date columns for parent tables using transaction_date
	- Adds posting_date columns to all child tables
	- Creates Custom Field metadata
	"""
	from frappe.utils import get_table_name

	phase_start_time = time.time()

	if partition_doctypes is None:
		partition_doctypes = frappe.get_hooks("partition_doctypes")

	if not partition_doctypes:
		print("\nNo partition_doctypes found in hooks")
		return False

	if doc:
		if doc.doctype in partition_doctypes:
			partition_doctypes = {doc.doctype: partition_doctypes[doc.doctype]}
		else:
			print(f"ERROR: {doc.doctype} not in partition_doctypes hook")
			return False

	db = DatabaseConnection(root_user, root_password)
	field_mgr = FieldManager()
	processed_child_tables = set()

	print(f"\n{'='*80}")
	print("PHASE 1: Creating posting_date columns")
	print(f"{'='*80}\n")

	success_count = 0
	total_count = 0
	timing_details = []

	for doctype, settings in partition_doctypes.items():
		doctype_start = time.time()
		print(f"\n{'='*80}")
		print(f"Processing: {doctype}")
		print(f"{'='*80}")

		actual_date_field = DOCTYPE_DATE_FIELD_MAP.get(
			doctype, _get_setting_value(settings, "field", "posting_date")
		)

		main_table = get_table_name(doctype)
		FieldManager.ensure_partition_indexes(main_table, actual_date_field, db)

		# Step 1: Create virtual posting_date for parent if needed
		if actual_date_field == "transaction_date":
			total_count += 1
			op_start = time.time()
			if field_mgr.ensure_virtual_posting_date(doctype, db):
				success_count += 1
				print(f"✓ Virtual posting_date created for {doctype}")
			else:
				print(f"✗ Failed to create virtual posting_date for {doctype}")
			timing_details.append(
				{
					"doctype": doctype,
					"operation": "virtual_column",
					"elapsed": time.time() - op_start,
				}
			)

		# Step 2: Add posting_date columns to all child tables
		meta = frappe.get_meta(doctype)
		for df in meta.get_table_fields():
			child_doctype = df.options
			child_table = get_table_name(df.options)

			if child_table in processed_child_tables:
				print(f"INFO: {child_doctype} already processed, skipping...")
				continue

			processed_child_tables.add(child_table)

			total_count += 1
			op_start = time.time()
			if field_mgr.add_posting_date_to_child(child_doctype, db):
				success_count += 1
				print(f"✓ Column added to {child_doctype}")
				FieldManager.ensure_partition_indexes(child_table, "posting_date", db)
			else:
				print(f"✗ Failed to add column to {child_doctype}")
			timing_details.append(
				{
					"doctype": child_doctype,
					"operation": "add_column",
					"elapsed": time.time() - op_start,
				}
			)

		doctype_elapsed = time.time() - doctype_start
		print(f"\n {doctype} completed in {doctype_elapsed:.1f}s")

	phase_elapsed = time.time() - phase_start_time

	print(f"\n{'='*80}")
	print("PHASE 1 Summary")
	print("=" * 80)
	print(f"Operations: {success_count}/{total_count} successful")
	print(f"Total time: {phase_elapsed:.1f}s ({phase_elapsed/60:.1f} minutes)")
	print(f"{'='*80}\n")

	# Print timing breakdown
	if timing_details:
		print("Timing Breakdown:")
		print(f"{'Operation':<50} {'Time':>10}")
		print(f"{'-'*50} {'-'*10}")
		for t in timing_details:
			print(f"{t['doctype'][:45]:<50} {t['elapsed']:>10.1f}s")
		print()

	return success_count == total_count


def create_partition_phase2(
	doc=None,
	chunk_size=50000,
	root_user=None,
	root_password=None,
	partition_doctypes=None,
):
	"""
	PHASE 2: Populate posting_date columns with data
	- Populates posting_date in all child tables from their parent tables
	- Uses chunked updates for progress tracking
	- Can be resumed if interrupted
	"""
	from frappe.utils import get_table_name

	phase_start_time = time.time()

	if partition_doctypes is None:
		partition_doctypes = frappe.get_hooks("partition_doctypes")

	if not partition_doctypes:
		print("\nNo partition_doctypes found in hooks")
		return False

	if doc:
		if doc.doctype in partition_doctypes:
			partition_doctypes = {doc.doctype: partition_doctypes[doc.doctype]}
		else:
			print(f"ERROR: {doc.doctype} not in partition_doctypes hook")
			return False

	db = DatabaseConnection(root_user, root_password)
	field_mgr = FieldManager()
	processed_child_tables = set()

	print(f"\n{'='*80}")
	print("PHASE 2: Populating posting_date columns")
	print(f"{'='*80}\n")

	timing_details = []

	for doctype, settings in partition_doctypes.items():
		doctype_start = time.time()
		print(f"\n{'='*80}")
		print(f"Processing: {doctype}")
		print(f"{'='*80}")

		actual_date_field = DOCTYPE_DATE_FIELD_MAP.get(
			doctype, _get_setting_value(settings, "field", "posting_date")
		)

		# Ensure virtual column exists if needed
		if actual_date_field == "transaction_date":
			field_mgr.ensure_virtual_posting_date(doctype, db)

		# Populate all child tables
		meta = frappe.get_meta(doctype)
		for df in meta.get_table_fields():
			child_doctype = df.options
			child_table = get_table_name(df.options)

			if child_table in processed_child_tables:
				print(f"\nINFO: {child_doctype} already processed, skipping...")
				continue

			processed_child_tables.add(child_table)

			print(f"\n{'='*80}")
			print(f"Processing child table: {child_doctype}")
			print(f"{'='*80}")

			child_start = time.time()

			# Populate posting_date from all parent types
			field_mgr.populate_posting_date_for_child(child_doctype, db, chunk_size)
			frappe.db.commit()

			child_elapsed = time.time() - child_start
			timing_details.append(
				{
					"doctype": child_doctype,
					"table": child_table,
					"elapsed": child_elapsed,
				}
			)
			print(
				f"\n  {child_doctype} completed in {child_elapsed:.1f}s ({child_elapsed/60:.1f} min)"
			)

		doctype_elapsed = time.time() - doctype_start
		print(
			f"\n {doctype} (all children) completed in {doctype_elapsed:.1f}s ({doctype_elapsed/60:.1f} min)"
		)

	phase_elapsed = time.time() - phase_start_time

	print(f"\n{'='*80}")
	print("PHASE 2 Summary")
	print("=" * 80)
	print(f"Child tables processed: {len(timing_details)}")
	print(f"Total time: {phase_elapsed:.1f}s ({phase_elapsed/60:.1f} minutes)")
	print(f"{'='*80}\n")

	# Print timing breakdown sorted by longest first
	if timing_details:
		print("Timing Breakdown:")
		print(f"{'Child Table':<50} {'Time':>12} {'Min':>8}")
		print(f"{'-'*50} {'-'*12} {'-'*8}")
		for t in sorted(timing_details, key=lambda x: -x["elapsed"]):
			print(f"{t['doctype'][:45]:<50} {t['elapsed']:>12.1f}s {t['elapsed']/60:>8.1f}")
		print()

	return True


def create_partition_phase3(
	doc=None,
	output_file: str = None,
	root_user: str = None,
	root_password: str = None,
	partition_doctypes=None,
):
	"""
	PHASE 3: Generate Percona commands for PK modification (does not execute them)

	This phase generates pt-online-schema-change commands to modify primary keys
	to include the date field. Commands are printed to console and written to a
	shell script for manual execution outside bench console.

	Args:
	doc: Optional document to process only that doctype
	output_file: Path to write shell script (default: ./percona_pk_commands.sh)
	root_user: Database root user for Percona commands
	root_password: Database root password for Percona commands
	partition_doctypes: Optional dict to bypass hooks

	Returns:
	bool: True if commands were generated successfully

	Usage:
	1. Run this function to generate the shell script
	2. Execute the shell script manually in a terminal
	3. Run create_partition_phase4() to apply partitioning
	"""
	import shlex
	from frappe.utils import get_table_name

	if partition_doctypes is None:
		partition_doctypes = frappe.get_hooks("partition_doctypes")

	if not partition_doctypes:
		print("\nNo partition_doctypes found in hooks")
		return False

	if doc:
		if doc.doctype in partition_doctypes:
			partition_doctypes = {doc.doctype: partition_doctypes[doc.doctype]}
		else:
			print(f"ERROR: {doc.doctype} not in partition_doctypes hook")
			return False

	db = DatabaseConnection(root_user, root_password)
	analyzer = TableAnalyzer(db)
	processed_child_tables = set()

	if not db.user or not db.password:
		print("\nWARNING: Database credentials not provided.")
		print("You can either:")
		print("  1. Pass root_user and root_password parameters")
		print("  2. Set root_login and root_password in site_config.json")
		print("  3. Edit the generated script to add credentials manually")
		print()

	commands = []
	tables_info = []

	print(f"\n{'='*80}")
	print("PHASE 3: Generating Percona PK modification commands")
	print(f"{'='*80}\n")

	for doctype, settings in partition_doctypes.items():
		original_partition_field = _get_setting_value(settings, "field", "posting_date")
		actual_date_field = DOCTYPE_DATE_FIELD_MAP.get(doctype, original_partition_field)

		print(f"\nProcessing: {doctype}")

		main_table = get_table_name(doctype)
		cmd = _generate_pk_modification_command(
			table=main_table,
			pk_field=actual_date_field,
			db=db,
			analyzer=analyzer,
		)
		if cmd:
			commands.append(cmd)
			tables_info.append({"table": main_table, "doctype": doctype, "type": "parent"})
			print(f"  ✓ {main_table} - command generated")
		else:
			print(f"  ⊘ {main_table} - PK already includes date field or skipped")

		meta = frappe.get_meta(doctype)
		for df in meta.get_table_fields():
			child_doctype = df.options
			child_table = get_table_name(df.options)

			if child_table in processed_child_tables:
				continue

			processed_child_tables.add(child_table)

			cmd = _generate_pk_modification_command(
				table=child_table,
				pk_field="posting_date",
				db=db,
				analyzer=analyzer,
			)
			if cmd:
				commands.append(cmd)
				tables_info.append(
					{"table": child_table, "doctype": child_doctype, "type": "child"}
				)
				print(f"  ✓ {child_table} - command generated")
			else:
				print(f"  ⊘ {child_table} - PK already includes date field or skipped")

	if not commands:
		print("\nNo PK modifications needed - all tables already have date field in PK")
		print("You can proceed directly to create_partition_phase4()")
		return True

	if output_file is None:
		output_file = "./percona_pk_commands.sh"

	script_content = _generate_percona_script(commands, tables_info, db)

	with open(output_file, "w") as f:
		f.write(script_content)

	os.chmod(output_file, 0o755)

	print(f"\n{'='*80}")
	print("PHASE 3 Summary")
	print(f"{'='*80}")
	print(f"Commands generated: {len(commands)}")
	print(f"Shell script written to: {output_file}")
	print("\nNext steps:")
	print(f"  1. Review and execute the script: bash {output_file}")
	print("  2. After all commands complete, run: create_partition_phase4()")
	print(f"{'='*80}\n")

	# Also print commands to console
	print("\n" + "=" * 80)
	print("GENERATED COMMANDS (also saved to script file):")
	print("=" * 80 + "\n")
	for i, cmd in enumerate(commands, 1):
		print(f"# {i}. {tables_info[i-1]['doctype']} ({tables_info[i-1]['type']})")
		print(cmd)
		print()

	return True


def _generate_pk_modification_command(
	table: str,
	pk_field: str,
	db: DatabaseConnection,
	analyzer: TableAnalyzer,
) -> str | None:
	"""
	Generate the Percona pt-online-schema-change command for PK modification.

	Returns None if PK already includes the date field or on error.
	"""
	import shlex

	try:
		if not db.column_exists(table, pk_field):
			print(f"    WARNING: Column '{pk_field}' does not exist in {table}")
			return None

		current_pk = analyzer.get_primary_key(table)

		if pk_field in current_pk:
			return None

		new_pk = current_pk + [pk_field]
		if "name" not in new_pk:
			new_pk = ["name"] + [pk_field]

		pk_columns = ", ".join([f"`{col}`" for col in new_pk])

		unique_indexes = frappe.db.sql(
			f"SHOW INDEXES FROM `{table}` WHERE Non_unique = 0", as_dict=True
		)

		index_columns = {}
		for idx in unique_indexes:
			idx_name = idx["Key_name"]
			if idx_name not in index_columns:
				index_columns[idx_name] = []
			index_columns[idx_name].append(idx["Column_name"])

		alter_parts = ["DROP PRIMARY KEY"]

		for idx_name, columns in index_columns.items():
			if idx_name == "PRIMARY":
				continue
			alter_parts.append(f"DROP INDEX `{idx_name}`")
			if pk_field not in columns:
				columns.append(pk_field)
			cols_str = ", ".join([f"`{col}`" for col in columns])
			alter_parts.append(f"ADD UNIQUE INDEX `{idx_name}` ({cols_str})")

		alter_parts.append(f"ADD PRIMARY KEY ({pk_columns})")
		alter_statement = ", ".join(alter_parts)

		dsn = db.get_dsn(table)

		user_arg = f"--user={db.user}" if db.user else "--user=YOUR_DB_USER"
		pass_arg = (
			f"--password={db.password}" if db.password else "--password=YOUR_DB_PASSWORD"
		)

		cmd_parts = [
			"pt-online-schema-change",
			shlex.quote(f"--alter={alter_statement}"),
			shlex.quote(dsn),
			shlex.quote(user_arg),
			shlex.quote(pass_arg),
			"--execute",
			"--chunk-size=10000",
			"--max-load=Threads_running=100",
			"--critical-load=Threads_running=200",
			"--recursion-method=none",
			"--progress=time,30",
			"--print",
			"--no-check-unique-key-change",
			"--no-check-alter",
			"--preserve-triggers",
		]

		return " ".join(cmd_parts)

	except Exception as e:
		print(f"    ERROR generating command for {table}: {e}")
		return None


def _generate_percona_script(
	commands: list, tables_info: list, db: DatabaseConnection
) -> str:
	"""Generate a shell script with all Percona commands"""

	# Check if credentials are placeholders
	has_real_creds = db.user and db.password and "YOUR_" not in str(db.user)

	script = f"""#!/bin/bash
#
# Percona PK Modification Script
# Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
# Database: {db.database}
# Host: {db.host}
#
# This script modifies primary keys to include date fields for partitioning.
# Run each command one at a time and wait for completion before the next.
#
"""

	if not has_real_creds:
		script += """# IMPORTANT: Database credentials were not provided.
# Please replace YOUR_DB_USER and YOUR_DB_PASSWORD in the commands below.
#
# Option: Find and replace in this file:
#   sed -i 's/YOUR_DB_USER/actual_user/g' percona_pk_commands.sh
#   sed -i 's/YOUR_DB_PASSWORD/actual_password/g' percona_pk_commands.sh
#
"""

	script += f"""# Usage: bash {os.path.basename("percona_pk_commands.sh")}
#
# After all commands complete successfully, run in bench console:
#   create_partition_phase4()
#

set -e  # Exit on error

echo "Starting Percona PK modifications..."
echo "Database: {db.database}"
echo "Host: {db.host}"
echo "Tables to modify: {len(commands)}"
echo ""

"""

	for i, (cmd, info) in enumerate(zip(commands, tables_info), 1):
		script += f"""
# ============================================================================
# {i}/{len(commands)}: {info['doctype']} ({info['type']})
# Table: {info['table']}
# ============================================================================
echo ""
echo "Processing {i}/{len(commands)}: {info['table']}..."
echo ""

{cmd}

echo ""
echo "✓ Completed: {info['table']}"
echo ""

"""

	script += """
echo ""
echo "============================================================================"
echo "All PK modifications completed successfully!"
echo "============================================================================"
echo ""
echo "Next step: Run in bench console:"
echo "  create_partition_phase4()"
echo ""
"""

	return script


def create_partition_phase4(
	doc=None,
	years_ahead=10,
	partition_doctypes=None,
):
	"""
	PHASE 4: Apply partitioning (after PK modification is complete)

	This phase applies RANGE partitioning to tables. It assumes that primary keys
	have already been modified to include the date field (via Phase 3 script).

	Note: Partitioning uses native ALTER TABLE, not Percona, as pt-online-schema-change
	does not support partitioning operations. This is generally fast as RANGE
	partitioning is primarily a metadata operation.

	Args:
	doc: Optional document to process only that doctype
	years_ahead: Number of years ahead to create partitions for (default: 10)
	partition_doctypes: Optional dict to bypass hooks

	Returns:
	bool: True if all tables were partitioned successfully
	"""
	from frappe.utils import get_table_name

	phase_start_time = time.time()

	if partition_doctypes is None:
		partition_doctypes = frappe.get_hooks("partition_doctypes")

	if not partition_doctypes:
		print("\nNo partition_doctypes found in hooks")
		return False

	if doc:
		if doc.doctype in partition_doctypes:
			partition_doctypes = {doc.doctype: partition_doctypes[doc.doctype]}
		else:
			print(f"ERROR: {doc.doctype} not in partition_doctypes hook")
			return False

	db = DatabaseConnection()
	engine = PartitionEngine(db, use_percona=False)
	processed_child_tables = set()

	print(f"\n{'='*80}")
	print("PHASE 4: Applying partitioning")
	print(f"{'='*80}\n")

	success_count = 0
	total_count = 0
	timing_details = []

	for doctype, settings in partition_doctypes.items():
		doctype_start = time.time()
		original_partition_field = _get_setting_value(settings, "field", "posting_date")
		partition_by = _get_setting_value(settings, "partition_by", "month")

		print(f"\n{'='*80}")
		print(f"Processing: {doctype}")
		print(f"{'='*80}")

		actual_date_field = DOCTYPE_DATE_FIELD_MAP.get(doctype, original_partition_field)

		# Partition main table
		main_table = get_table_name(doctype)
		total_count += 1

		main_start = time.time()
		main_success = engine.partition_table(
			main_table,
			partition_field=actual_date_field,
			strategy=partition_by,
			years_back=2,
			years_ahead=years_ahead,
			dry_run=False,
			pk_field=actual_date_field,
		)
		if main_success:
			success_count += 1
			print(f"✓ Partitioned {doctype}")
		else:
			print(f"✗ Failed to partition {doctype}")

		main_elapsed = time.time() - main_start
		timing_details.append(
			{
				"doctype": doctype,
				"table": main_table,
				"type": "parent",
				"elapsed": main_elapsed,
				"success": main_success,
			}
		)

		# Partition child tables
		meta = frappe.get_meta(doctype)
		for df in meta.get_table_fields():
			child_doctype = df.options
			child_table = get_table_name(df.options)

			if child_table in processed_child_tables:
				print(f"\nINFO: {child_doctype} already processed, skipping...")
				continue

			processed_child_tables.add(child_table)

			print(f"\n{'='*80}")
			print(f"Processing child table: {child_doctype}")
			print(f"{'='*80}")

			total_count += 1
			child_start = time.time()
			child_success = engine.partition_table(
				child_table,
				partition_field="posting_date",
				strategy=partition_by,
				years_back=2,
				years_ahead=years_ahead,
				dry_run=False,
				pk_field="posting_date",
			)
			if child_success:
				success_count += 1
				print(f"✓ Partitioned {child_doctype}")
			else:
				print(f"✗ Failed to partition {child_doctype}")

			child_elapsed = time.time() - child_start
			timing_details.append(
				{
					"doctype": child_doctype,
					"table": child_table,
					"type": "child",
					"elapsed": child_elapsed,
					"success": child_success,
				}
			)
			print(
				f"\n  {child_doctype} completed in {child_elapsed:.1f}s ({child_elapsed/60:.1f} min)"
			)

		doctype_elapsed = time.time() - doctype_start
		print(
			f"\n  {doctype} (with children) completed in {doctype_elapsed:.1f}s ({doctype_elapsed/60:.1f} min)"
		)

	phase_elapsed = time.time() - phase_start_time

	print(f"\n{'='*80}")
	print("PHASE 4 Summary")
	print("=" * 80)
	print(f"Tables partitioned: {success_count}/{total_count}")
	print(f"Total time: {phase_elapsed:.1f}s ({phase_elapsed/60:.1f} minutes)")
	print(f"{'='*80}\n")

	if timing_details:
		print("Timing Breakdown:")
		print(f"{'Table':<50} {'Type':<8} {'Status':<8} {'Time':>12}")
		print(f"{'-'*50} {'-'*8} {'-'*8} {'-'*12}")
		for t in sorted(timing_details, key=lambda x: -x["elapsed"]):
			status = "✓" if t["success"] else "✗"
			print(f"{t['doctype'][:45]:<50} {t['type']:<8} {status:<8} {t['elapsed']:>12.1f}s")
		print()

	return success_count == total_count


# Legacy function - kept for backward compatibility
def create_partition_phase3_legacy(
	doc=None,
	years_ahead=10,
	use_percona=False,
	root_user=None,
	root_password=None,
	partition_doctypes=None,
):
	"""
	LEGACY PHASE 3: Apply partitioning (modify PK and create partitions)

	WARNING: This function executes Percona directly which can cause connection
	issues when run from bench console. Use create_partition_phase3() instead
	to generate commands for manual execution.

	- Modifies primary keys to include posting_date/transaction_date
	- Applies RANGE partitioning to tables
	- Uses Percona if enabled for large tables
	"""
	from frappe.utils import get_table_name

	phase_start_time = time.time()

	if partition_doctypes is None:
		partition_doctypes = frappe.get_hooks("partition_doctypes")

	if not partition_doctypes:
		print("\nNo partition_doctypes found in hooks")
		return False

	if doc:
		if doc.doctype in partition_doctypes:
			partition_doctypes = {doc.doctype: partition_doctypes[doc.doctype]}
		else:
			print(f"ERROR: {doc.doctype} not in partition_doctypes hook")
			return False

	db = DatabaseConnection(root_user, root_password)
	engine = PartitionEngine(db, use_percona)
	processed_child_tables = set()

	print(f"\n{'='*80}")
	print("PHASE 3 (LEGACY): Applying partitioning")
	print(f"{'='*80}\n")

	success_count = 0
	total_count = 0
	timing_details = []

	for doctype, settings in partition_doctypes.items():
		doctype_start = time.time()
		original_partition_field = _get_setting_value(settings, "field", "posting_date")
		partition_by = _get_setting_value(settings, "partition_by", "month")

		print(f"\n{'='*80}")
		print(f"Processing: {doctype}")
		print(f"{'='*80}")

		actual_date_field = DOCTYPE_DATE_FIELD_MAP.get(doctype, original_partition_field)

		# Partition main table
		main_table = get_table_name(doctype)
		total_count += 1

		main_start = time.time()
		main_success = engine.partition_table(
			main_table,
			partition_field=actual_date_field,
			strategy=partition_by,
			years_back=2,
			years_ahead=years_ahead,
			dry_run=False,
			pk_field=actual_date_field,
		)
		if main_success:
			success_count += 1
			print(f"✓ Partitioned {doctype}")
		else:
			print(f"✗ Failed to partition {doctype}")

		main_elapsed = time.time() - main_start
		timing_details.append(
			{
				"doctype": doctype,
				"table": main_table,
				"type": "parent",
				"elapsed": main_elapsed,
				"success": main_success,
			}
		)

		# Partition child tables
		meta = frappe.get_meta(doctype)
		for df in meta.get_table_fields():
			child_doctype = df.options
			child_table = get_table_name(df.options)

			if child_table in processed_child_tables:
				print(f"\nINFO: {child_doctype} already processed, skipping...")
				continue

			processed_child_tables.add(child_table)

			print(f"\n{'='*80}")
			print(f"Processing child table: {child_doctype}")
			print(f"{'='*80}")

			total_count += 1
			child_start = time.time()
			child_success = engine.partition_table(
				child_table,
				partition_field="posting_date",
				strategy=partition_by,
				years_back=2,
				years_ahead=years_ahead,
				dry_run=False,
				pk_field="posting_date",
			)
			if child_success:
				success_count += 1
				print(f"✓ Partitioned {child_doctype}")
			else:
				print(f"✗ Failed to partition {child_doctype}")

			child_elapsed = time.time() - child_start
			timing_details.append(
				{
					"doctype": child_doctype,
					"table": child_table,
					"type": "child",
					"elapsed": child_elapsed,
					"success": child_success,
				}
			)
			print(
				f"\n  {child_doctype} completed in {child_elapsed:.1f}s ({child_elapsed/60:.1f} min)"
			)

		doctype_elapsed = time.time() - doctype_start
		print(
			f"\n  {doctype} (with children) completed in {doctype_elapsed:.1f}s ({doctype_elapsed/60:.1f} min)"
		)

	phase_elapsed = time.time() - phase_start_time

	print(f"\n{'='*80}")
	print("PHASE 3 (LEGACY) Summary")
	print("=" * 80)
	print(f"Tables partitioned: {success_count}/{total_count}")
	print(f"Total time: {phase_elapsed:.1f}s ({phase_elapsed/60:.1f} minutes)")
	print(f"{'='*80}\n")

	# Print timing breakdown sorted by longest first
	if timing_details:
		print("Timing Breakdown:")
		print(f"{'Table':<50} {'Type':<8} {'Status':<8} {'Time':>12}")
		print(f"{'-'*50} {'-'*8} {'-'*8} {'-'*12}")
		for t in sorted(timing_details, key=lambda x: -x["elapsed"]):
			status = "✓" if t["success"] else "✗"
			print(f"{t['doctype'][:45]:<50} {t['type']:<8} {status:<8} {t['elapsed']:>12.1f}s")
		print()

	return success_count == total_count


def scheduled_populate_partition_fields(
	max_hours: float = 2.0,
	chunk_size: int = 5000,
	partition_doctypes: dict = None,
):
	"""
	This function:
	- Finds doctypes that have Phase 1 complete but Phase 2 incomplete
	- Populates posting_date for child tables in chunks
	- Respects time limit and stops gracefully when reached
	- Can be resumed on next scheduler run (picks up where it left off)

	Args:
	max_hours: Maximum time to run in hours (default: 2.0)
	chunk_size: Number of rows to update per batch (default: 50000)
	partition_doctypes: Optional dict to bypass hooks, e.g.:

	Returns:
	dict: Summary of work done and status
	"""
	from frappe.utils import get_table_name

	start_time = time.time()
	max_seconds = max_hours * 3600
	end_time = start_time + max_seconds

	def time_remaining():
		return end_time - time.time()

	def should_continue():
		remaining = time_remaining()
		if remaining <= 0:
			print(f"\nTime limit reached ({max_hours} hours). Stopping gracefully...")
			return False
		return True

	if partition_doctypes is None:
		partition_doctypes = frappe.get_hooks("partition_doctypes")

	if not partition_doctypes:
		print("\nNo partition_doctypes found in hooks")
		return {"status": "no_config", "message": "No partition_doctypes in hooks"}

	processed_child_tables = set()

	print(f"\n{'='*80}")
	print("Scheduled Phase 2: Populating posting_date columns")
	print(f"Time limit: {max_hours} hours | Chunk size: {chunk_size:,}")
	print("=" * 80 + "\n")

	doctypes_to_process = []

	for doctype, settings in partition_doctypes.items():
		actual_date_field = DOCTYPE_DATE_FIELD_MAP.get(
			doctype, _get_setting_value(settings, "field", "posting_date")
		)

		meta = frappe.get_meta(doctype)
		children_needing_work = []

		for df in meta.get_table_fields():
			if not should_continue():
				break

			child_doctype = df.options
			child_table = get_table_name(df.options)

			# Check if Phase 2 work needed (has NULL values)
			try:
				result = frappe.db.sql(
					f"SELECT EXISTS(SELECT 1 FROM `{child_table}` WHERE `posting_date` IS NULL LIMIT 1)"
				)
				has_nulls = result[0][0] > 0 if result else False
			except Exception:
				has_nulls = True

			if has_nulls:
				children_needing_work.append(
					{
						"doctype": child_doctype,
						"table": child_table,
					}
				)

		if children_needing_work:
			doctypes_to_process.append(
				{
					"doctype": doctype,
					"settings": settings,
					"actual_date_field": actual_date_field,
					"children": children_needing_work,
				}
			)

	if not should_continue():
		elapsed = time.time() - start_time
		print("\nTime limit reached during discovery phase")
		print(f"Elapsed: {elapsed:.1f}s")
		return {
			"status": "time_limit",
			"message": "Time limit reached during discovery",
			"elapsed_seconds": elapsed,
		}

	if not doctypes_to_process:
		elapsed = time.time() - start_time
		print("\n All doctypes have Phase 2 complete!")
		print(f"Elapsed: {elapsed:.1f}s")
		return {
			"status": "complete",
			"message": "All Phase 2 work is done",
			"elapsed_seconds": elapsed,
		}

	print(f"\nFound {len(doctypes_to_process)} doctypes needing Phase 2 work:")
	for item in doctypes_to_process:
		print(f"  - {item['doctype']}: {len(item['children'])} child tables")

	summary = {
		"processed_doctypes": [],
		"processed_tables": [],
		"skipped_tables": [],
		"total_rows_updated": 0,
		"stopped_due_to_time": False,
	}

	for item in doctypes_to_process:
		if not should_continue():
			summary["stopped_due_to_time"] = True
			break

		doctype = item["doctype"]
		actual_date_field = item["actual_date_field"]

		print(f"\n{'='*80}")
		print(f"Processing: {doctype}")
		print(f"Time remaining: {time_remaining() / 60:.1f} minutes")
		print(f"{'='*80}")

		for child_info in item["children"]:
			if not should_continue():
				summary["stopped_due_to_time"] = True
				break

			child_doctype = child_info["doctype"]
			child_table = child_info["table"]

			if child_table in processed_child_tables:
				print(f"\nINFO: {child_doctype} already processed this run, skipping...")
				continue

			processed_child_tables.add(child_table)

			print(f"\n{'='*80}")
			print(f"Processing child table: {child_doctype}")
			print(f"Time remaining: {time_remaining() / 60:.1f} minutes")
			print(f"{'='*80}")

			rows_updated = _populate_with_time_limit(
				child_doctype=child_doctype,
				end_time=end_time,
				chunk_size=chunk_size,
				partition_doctypes=partition_doctypes,
			)

			summary["processed_tables"].append(
				{
					"doctype": child_doctype,
					"table": child_table,
					"rows_updated": rows_updated,
				}
			)
			summary["total_rows_updated"] += rows_updated

			frappe.db.commit()

			if not should_continue():
				summary["stopped_due_to_time"] = True
				break

		if not summary["stopped_due_to_time"]:
			summary["processed_doctypes"].append(doctype)

	elapsed = time.time() - start_time
	summary["elapsed_seconds"] = elapsed
	summary["status"] = "partial" if summary["stopped_due_to_time"] else "complete"

	print(f"\n{'='*80}")
	print("Scheduled Phase 2 Summary")
	print("=" * 80)
	print(f"Status: {summary['status']}")
	print(f"Elapsed: {elapsed / 60:.1f} minutes")
	print(f"Tables processed: {len(summary['processed_tables'])}")
	print(f"Total rows updated: {summary['total_rows_updated']:,}")
	if summary["stopped_due_to_time"]:
		print("Stopped due to time limit - will continue on next run")
	print("=" * 80 + "\n")

	return summary


def _populate_with_time_limit(
	child_doctype: str,
	end_time: float,
	chunk_size: int = 50000,
	partition_doctypes: dict = None,
) -> int:
	"""
	Populate posting_date for a child table using parent-based bulk updates.

	Strategy:
	1. Find distinct parents that have children with NULL posting_date
	2. Fetch parent documents in batches (cursor-based pagination)
	3. Bulk update all children of those parents in one query per batch

	Returns the number of rows updated.
	"""

	table = f"tab{child_doctype}"
	total_updated = 0

	def time_remaining():
		return end_time - time.time()

	def should_continue():
		return time.time() < end_time

	if not should_continue():
		print(f"\nINFO: Time limit reached, skipping '{child_doctype}'")
		return 0

	print(
		f"\nINFO: Populating 'posting_date' in '{child_doctype}' (parent-based bulk updates)..."
	)
	sys.stdout.flush()

	parent_types = []
	for doctype in partition_doctypes.keys():
		if not should_continue():
			print("\nTime limit reached during parent type discovery")
			return total_updated

		try:
			meta = frappe.get_meta(doctype)
			for df in meta.get_table_fields():
				if df.options == child_doctype:
					parent_types.append(doctype)
					break
		except Exception:
			pass

	if not parent_types:
		print(f"INFO: No parent types found in hooks for {child_doctype}")
		return 0

	print(f"INFO: Parent types from config: {parent_types}")
	sys.stdout.flush()

	for parent_type in parent_types:
		if not should_continue():
			print(f"\nTime limit reached before processing {parent_type}, stopping...")
			break

		print(f"\nINFO: Processing parent type: {parent_type}")
		sys.stdout.flush()

		try:
			null_check = frappe.db.sql(
				f"""
				SELECT EXISTS(
					SELECT 1 FROM `{table}`
					WHERE parenttype = %s
					AND posting_date IS NULL
					LIMIT 1
				)
			""",
				(parent_type,),
			)
			has_nulls = null_check[0][0] > 0 if null_check else False
		except Exception as e:
			print(f"  WARNING: Could not check for NULLs: {e}")
			has_nulls = True

		if not has_nulls:
			print(f"  No NULL posting_date rows for {parent_type}, skipping...")
			continue

		parent_table = f"tab{parent_type}"
		source_field = "posting_date"

		if not frappe.db.sql(
			f"""
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = DATABASE()
			AND table_name = '{parent_table}'
			AND column_name = 'posting_date'
		"""
		):
			if parent_type in DOCTYPE_DATE_FIELD_MAP:
				source_field = DOCTYPE_DATE_FIELD_MAP[parent_type]
			else:
				source_field = "creation"
			print(f"  Using field: {source_field}")

		batch_num = 0
		batch_start_time = time.time()

		while should_continue():
			batch_num += 1
			batch_start = time.time()

			try:
				parents_with_nulls = frappe.db.sql(
					f"""
					SELECT DISTINCT p.name, p.`{source_field}` as date_value
					FROM `{parent_table}` p
					INNER JOIN `{table}` c ON c.parent = p.name
					WHERE c.parenttype = %s
					AND c.posting_date IS NULL
					LIMIT %s
				""",
					(parent_type, chunk_size),
					as_dict=True,
				)

				if not parents_with_nulls:
					if batch_num == 1:
						print(f"  No parents with NULL children for {parent_type}")
					break

				date_groups = {}
				for p in parents_with_nulls:
					d = str(p["date_value"])
					if d not in date_groups:
						date_groups[d] = []
					date_groups[d].append(p["name"])

				rows_this_batch = 0
				for date_val, parent_list in date_groups.items():
					if not should_continue():
						print(
							f"\nTime limit reached during batch {batch_num}, committing and stopping..."
						)
						frappe.db.commit()
						return total_updated + rows_this_batch

					parent_placeholders = ", ".join(["%s"] * len(parent_list))

					frappe.db.sql(
						f"""
						UPDATE `{table}`
						SET posting_date = %s
						WHERE parent IN ({parent_placeholders})
						AND parenttype = %s
						AND posting_date IS NULL
						""",
						[date_val] + parent_list + [parent_type],
					)

					rows = frappe.db.sql("SELECT ROW_COUNT()")[0][0]
					rows_this_batch += rows

				total_updated += rows_this_batch
				frappe.db.commit()

				batch_time = time.time() - batch_start
				total_time = time.time() - batch_start_time
				rate = total_updated / total_time if total_time > 0 else 0
				remaining = time_remaining() / 60

				print(
					f"  Batch {batch_num}: {len(parents_with_nulls)} parents, "
					f"{rows_this_batch:,} rows in {batch_time:.1f}s "
					f"(Total: {total_updated:,}, Rate: {rate:,.0f}/s, {remaining:.1f}min left)"
				)
				sys.stdout.flush()

				if len(parents_with_nulls) < chunk_size:
					break

			except Exception as e:
				print(f"  ERROR in batch {batch_num}: {e}")
				frappe.db.rollback()
				break

		if total_updated > 0:
			total_time = time.time() - batch_start_time
			avg_rate = total_updated / total_time if total_time > 0 else 0
			print(
				f"SUCCESS: {total_updated:,} rows updated for {parent_type} "
				f"in {total_time:.1f}s ({avg_rate:,.0f} rows/s)"
			)
			sys.stdout.flush()

		if not should_continue():
			print(f"\nTime limit reached after {parent_type}, stopping...")
			break

	return total_updated


def get_phase_status(doc=None, partition_doctypes=None):
	"""
	Check the status of each phase for doctypes
	Returns a report showing which phases are complete for each doctype
	"""
	from frappe.utils import get_table_name

	if partition_doctypes is None:
		partition_doctypes = frappe.get_hooks("partition_doctypes")

	if not partition_doctypes:
		print("\nNo partition_doctypes found in hooks")
		return {}

	if doc:
		if doc.doctype in partition_doctypes:
			partition_doctypes = {doc.doctype: partition_doctypes[doc.doctype]}

	db = DatabaseConnection()
	analyzer = TableAnalyzer(db)

	print(f"\n{'='*80}")
	print("Partition Phase Status")
	print(f"{'='*80}\n")

	status = {}

	for doctype, settings in partition_doctypes.items():
		actual_date_field = DOCTYPE_DATE_FIELD_MAP.get(
			doctype, _get_setting_value(settings, "field", "posting_date")
		)
		main_table = get_table_name(doctype)

		# Check Phase 1: Virtual column exists
		phase1_complete = True
		if actual_date_field == "transaction_date":
			phase1_complete = db.column_exists(main_table, "posting_date")

		# Check child tables for Phase 1
		meta = frappe.get_meta(doctype)
		child_status = []

		for df in meta.get_table_fields():
			child_doctype = df.options
			child_table = get_table_name(df.options)

			has_column = db.column_exists(child_table, "posting_date")

			# Check Phase 2: Data populated
			null_count = 0
			phase2_complete = False
			if has_column:
				try:
					result = frappe.db.sql(
						f"SELECT COUNT(*) as cnt FROM `{child_table}` WHERE `posting_date` IS NULL"
					)
					null_count = result[0][0] if result else 0
					phase2_complete = null_count == 0
				except Exception:
					null_count = -1

			# Check Phase 3: Partitioned
			phase3_complete = analyzer.is_partitioned(child_table)

			child_status.append(
				{
					"doctype": child_doctype,
					"table": child_table,
					"phase1": has_column,
					"phase2": phase2_complete,
					"phase3": phase3_complete,
					"null_count": null_count,
				}
			)

			if not has_column:
				phase1_complete = False

		# Check main table Phase 3
		main_partitioned = analyzer.is_partitioned(main_table)

		status[doctype] = {
			"main_table": main_table,
			"date_field": actual_date_field,
			"phase1_complete": phase1_complete,
			"phase2_complete": all(c["phase2"] for c in child_status),
			"phase3_complete": main_partitioned and all(c["phase3"] for c in child_status),
			"main_partitioned": main_partitioned,
			"child_tables": child_status,
		}

		# Print status
		print(f"\n{doctype}:")
		print(
			f"  Phase 1 (Columns):    {'✓ Complete' if phase1_complete else '✗ Incomplete'}"
		)
		print(
			f"  Phase 2 (Populate):   {'✓ Complete' if status[doctype]['phase2_complete'] else '✗ Incomplete'}"
		)
		print(
			f"  Phase 3 (Partition):  {'✓ Complete' if status[doctype]['phase3_complete'] else '✗ Incomplete'}"
		)

		for child in child_status:
			p1 = "✓" if child["phase1"] else "✗"
			p2 = (
				"✓"
				if child["phase2"]
				else f"✗ ({child['null_count']:,} nulls)"
				if child["null_count"] >= 0
				else "?"
			)
			p3 = "✓" if child["phase3"] else "✗"
			print(f"    {child['doctype']}: P1:{p1} P2:{p2} P3:{p3}")

	print(f"\n{'='*80}\n")
	return status


def populate_partition_fields(doc, event=None):
	if doc.doctype not in DOCTYPE_DATE_FIELD_MAP:
		partition_doctypes = frappe.get_hooks("partition_doctypes") or {}
		if doc.doctype not in partition_doctypes:
			return

	source_date_field = DOCTYPE_DATE_FIELD_MAP.get(doc.doctype)

	if not source_date_field:
		partition_doctypes = frappe.get_hooks("partition_doctypes") or {}
		if doc.doctype in partition_doctypes:
			source_date_field = partition_doctypes[doc.doctype].get("field", ["posting_date"])[0]
		else:
			return

	date_value = doc.get(source_date_field) or doc.get("posting_date")

	if not date_value:
		return

	meta = frappe.get_meta(doc.doctype)

	for df in meta.get_table_fields():
		child_doctype = df.options
		child_fieldname = df.fieldname
		child_meta = frappe.get_meta(child_doctype)
		if not child_meta._fields.get("posting_date"):
			if not frappe.get_all(
				"Custom Field", filters={"dt": child_doctype, "fieldname": "posting_date"}
			):
				try:
					custom_field = frappe.get_doc(
						{
							"doctype": "Custom Field",
							"dt": child_doctype,
							"fieldname": "posting_date",
							"fieldtype": "Date",
							"label": "Posting Date",
							"read_only": 1,
							"hidden": 1,
							"default": "Today",
						}
					)
					custom_field.insert(ignore_permissions=True)
					frappe.db.commit()
				except Exception:
					pass

			child_meta = frappe.get_meta(child_doctype, cached=False)
			if not child_meta._fields.get("posting_date"):
				continue

		for row in doc.get(child_fieldname) or []:
			setattr(row, "posting_date", date_value)


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


def check_partition_status(doctype: str) -> dict:
	"""
	Check if a doctype and all its child doctypes are partitioned.
	"""
	from frappe.utils import get_table_name

	db = DatabaseConnection()
	analyzer = TableAnalyzer(db)

	main_table = get_table_name(doctype)
	meta = frappe.get_meta(doctype)

	result = {
		"doctype": doctype,
		"main_table": {"table": main_table, "partitioned": False, "partitions": 0},
		"child_tables": [],
		"all_partitioned": True,
	}

	# Check main table
	is_partitioned = analyzer.is_partitioned(main_table)
	partitions = analyzer.get_existing_partitions(main_table) if is_partitioned else []
	result["main_table"]["partitioned"] = is_partitioned
	result["main_table"]["partitions"] = len(partitions)

	if not is_partitioned:
		result["all_partitioned"] = False

	# Check child tables
	for df in meta.get_table_fields():
		child_doctype = df.options
		child_table = get_table_name(child_doctype)

		is_child_partitioned = analyzer.is_partitioned(child_table)
		child_partitions = (
			analyzer.get_existing_partitions(child_table) if is_child_partitioned else []
		)

		child_info = {
			"doctype": child_doctype,
			"table": child_table,
			"partitioned": is_child_partitioned,
			"partitions": len(child_partitions),
		}
		result["child_tables"].append(child_info)

		if not is_child_partitioned:
			result["all_partitioned"] = False

	print(f"\n{'='*80}")
	print(f"Partition Status: {doctype}")
	print(f"{'='*80}\n")

	status_icon = "✓" if result["main_table"]["partitioned"] else "✗"
	print(
		f"{status_icon} {main_table}: {'Partitioned' if result['main_table']['partitioned'] else 'NOT Partitioned'}",
		end="",
	)
	if result["main_table"]["partitioned"]:
		print(f" ({result['main_table']['partitions']} partitions)")
	else:
		print()

	for child in result["child_tables"]:
		status_icon = "✓" if child["partitioned"] else "✗"
		print(
			f"  {status_icon} {child['table']}: {'Partitioned' if child['partitioned'] else 'NOT Partitioned'}",
			end="",
		)
		if child["partitioned"]:
			print(f" ({child['partitions']} partitions)")
		else:
			print()

	print()
	if result["all_partitioned"]:
		print("✓ All tables are partitioned!")
	else:
		not_partitioned = []
		if not result["main_table"]["partitioned"]:
			not_partitioned.append(main_table)
		not_partitioned.extend(
			[c["table"] for c in result["child_tables"] if not c["partitioned"]]
		)
		print(f"Tables NOT partitioned: {', '.join(not_partitioned)}")

	print()
	return result


def get_partition_progress(doctype: str) -> dict:
	"""
	Check the progress of partitioning for a doctype.
	Useful for resuming interrupted operations.
	"""
	from frappe.utils import get_table_name

	db = DatabaseConnection()
	analyzer = TableAnalyzer(db)

	main_table = get_table_name(doctype)
	meta = frappe.get_meta(doctype)

	progress = {
		"doctype": doctype,
		"main_table": {
			"table": main_table,
			"partitioned": analyzer.is_partitioned(main_table),
			"has_posting_date": db.column_exists(main_table, "posting_date"),
			"pk_includes_posting_date": "posting_date" in analyzer.get_primary_key(main_table),
		},
		"child_tables": [],
		"ready_for_partition": True,
	}

	print(f"\n{'='*80}")
	print(f"Partition Progress: {doctype}")
	print(f"{'='*80}\n")

	# Main table status
	mt = progress["main_table"]
	print(f"Main Table: {main_table}")
	print(f"  - Has posting_date: {'✓' if mt['has_posting_date'] else '✗'}")
	print(
		f"  - PK includes posting_date: {'✓' if mt['pk_includes_posting_date'] else '✗'}"
	)
	print(f"  - Partitioned: {'✓' if mt['partitioned'] else '✗'}")

	if not mt["partitioned"]:
		progress["ready_for_partition"] = False

	print("\nChild Tables:")

	for df in meta.get_table_fields():
		child_doctype = df.options
		child_table = get_table_name(child_doctype)

		has_col = db.column_exists(child_table, "posting_date")
		null_count = 0
		if has_col:
			try:
				null_count = frappe.db.sql(
					f"SELECT COUNT(*) FROM `{child_table}` WHERE `posting_date` IS NULL"
				)[0][0]
			except Exception:
				null_count = -1  # Unknown

		is_partitioned = analyzer.is_partitioned(child_table)
		pk_ready = "posting_date" in analyzer.get_primary_key(child_table)

		child_info = {
			"doctype": child_doctype,
			"table": child_table,
			"has_posting_date": has_col,
			"null_count": null_count,
			"pk_includes_posting_date": pk_ready,
			"partitioned": is_partitioned,
		}
		progress["child_tables"].append(child_info)

		if not is_partitioned:
			progress["ready_for_partition"] = False

		status_icons = []
		status_icons.append("col:✓" if has_col else "col:✗")
		if has_col:
			status_icons.append(f"nulls:{null_count:,}" if null_count >= 0 else "nulls:?")
		status_icons.append("pk:✓" if pk_ready else "pk:✗")
		status_icons.append("part:✓" if is_partitioned else "part:✗")

		print(f"  - {child_doctype}: [{' | '.join(status_icons)}]")

	print(f"\n{'='*80}")
	if progress["ready_for_partition"]:
		print("All tables are partitioned!")
	else:
		not_done = []
		if not mt["partitioned"]:
			not_done.append(main_table)
		not_done.extend(
			[c["table"] for c in progress["child_tables"] if not c["partitioned"]]
		)
		print(f"○ Tables pending: {len(not_done)}")
	print(f"{'='*80}\n")

	return progress


def get_largest_tables(limit=50, include_child_tables=True):
	"""
	Get the tables with the most rows in the ERPNext instance.
	"""
	db_name = frappe.conf.db_name

	query = f"""
		SELECT
			TABLE_NAME,
			TABLE_ROWS,
			ROUND(DATA_LENGTH / 1024 / 1024, 2) as DATA_MB,
			ROUND(INDEX_LENGTH / 1024 / 1024, 2) as INDEX_MB,
			ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) as TOTAL_MB
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = '{db_name}'
		AND TABLE_TYPE = 'BASE TABLE'
		AND TABLE_NAME LIKE 'tab%'
		ORDER BY TABLE_ROWS DESC
		LIMIT {limit * 2}
	"""

	results = frappe.db.sql(query, as_dict=True)
	output = []
	for row in results:
		table_name = row["TABLE_NAME"]
		doctype = table_name[3:] if table_name.startswith("tab") else table_name

		is_child = False
		try:
			meta = frappe.get_meta(doctype)
			is_child = meta.istable
		except Exception:
			pass

		if not include_child_tables and is_child:
			continue

		output.append(
			{
				"doctype": doctype,
				"table": table_name,
				"rows": row["TABLE_ROWS"] or 0,
				"data_mb": row["DATA_MB"] or 0,
				"index_mb": row["INDEX_MB"] or 0,
				"total_mb": row["TOTAL_MB"] or 0,
				"is_child": is_child,
			}
		)

		if len(output) >= limit:
			break

	return output


def print_largest_tables(limit=50, include_child_tables=True):
	"""Print a formatted table of the largest tables"""

	tables = get_largest_tables(limit, include_child_tables)

	print(f"\n{'='*100}")
	print(f"Top {limit} Largest Tables in ERPNext")
	print(f"{'='*100}\n")
	print(
		f"{'#':<4} {'Doctype':<45} {'Rows':>12} {'Data MB':>10} {'Total MB':>10} {'Type':<8}"
	)
	print(f"{'-'*4} {'-'*45} {'-'*12} {'-'*10} {'-'*10} {'-'*8}")

	total_rows = 0
	total_size = 0

	for i, t in enumerate(tables, 1):
		table_type = "Child" if t["is_child"] else "Parent"
		print(
			f"{i:<4} {t['doctype']:<45} {t['rows']:>12,} {t['data_mb']:>10.2f} {t['total_mb']:>10.2f} {table_type:<8}"
		)
		total_rows += t["rows"]
		total_size += t["total_mb"]

	print(f"{'-'*4} {'-'*45} {'-'*12} {'-'*10} {'-'*10} {'-'*8}")
	print(f"{'':4} {'TOTAL':<45} {total_rows:>12,} {'':>10} {total_size:>10.2f}")
	print()


def get_partition_candidates(min_rows=100000):
	"""Get tables that are good candidates for partitioning"""

	tables = get_largest_tables(limit=100, include_child_tables=False)

	candidates = []

	for t in tables:
		if t["rows"] < min_rows:
			continue

		doctype = t["doctype"]
		try:
			meta = frappe.get_meta(doctype)
			has_date_field = False
			date_field = None

			for field in ["posting_date", "transaction_date", "creation"]:
				if meta.has_field(field) or field == "creation":
					has_date_field = True
					date_field = field
					break

			if has_date_field:
				child_tables = [df.options for df in meta.get_table_fields()]
				candidates.append(
					{
						**t,
						"date_field": date_field,
						"child_count": len(child_tables),
						"child_tables": child_tables,
					}
				)
		except Exception:
			pass

	return candidates


def print_partition_candidates(min_rows=100000, limit=20):
	"""Print tables that are good candidates for partitioning"""

	candidates = get_partition_candidates(min_rows)
	print(f"\n{'='*110}")
	print(f"Partition Candidates (tables with >= {min_rows:,} rows)")
	print(f"{'='*110}\n")

	if not candidates:
		print(f"No tables found with >= {min_rows:,} rows")
		return

	print(
		f"{'#':<4} {'Doctype':<40} {'Rows':>12} {'Size MB':>10} {'Date Field':<18} {'Children':<8}"
	)
	print(f"{'-'*4} {'-'*40} {'-'*12} {'-'*10} {'-'*18} {'-'*8}")

	for i, t in enumerate(candidates, 1):
		print(
			f"{i:<4} {t['doctype']:<40} {t['rows']:>12,} {t['total_mb']:>10.2f} {t['date_field']:<18} {t['child_count']:<8}"
		)

	print()

	print("Suggested partition_doctypes config for hooks.py:")
	print("-" * 50)
	print("partition_doctypes = {")
	for t in candidates[:limit]:
		field = t["date_field"]
		print(f'    "{t["doctype"]}": {{"field": ["{field}"], "partition_by": ["month"]}},')
	print("}")
	print()


def create_posting_date_defaults_phase(
	doc=None,
	output_file: str = None,
	root_user: str = None,
	root_password: str = None,
	partition_doctypes=None,
):
	"""
	Generate Percona commands to add DEFAULT (CURRENT_DATE) to posting_date columns
	in all child tables.
	"""
	from frappe.utils import get_table_name

	if partition_doctypes is None:
		partition_doctypes = frappe.get_hooks("partition_doctypes")

	if not partition_doctypes:
		print("\nNo partition_doctypes found in hooks")
		return False

	if doc:
		if doc.doctype in partition_doctypes:
			partition_doctypes = {doc.doctype: partition_doctypes[doc.doctype]}
		else:
			print(f"ERROR: {doc.doctype} not in partition_doctypes hook")
			return False

	db = DatabaseConnection(root_user, root_password)
	analyzer = TableAnalyzer(db)
	processed_child_tables = set()

	commands = []
	tables_info = []

	print(f"\n{'='*80}")
	print("Generating Percona commands for posting_date DEFAULT (CURRENT_DATE)")
	print(f"{'='*80}\n")

	for doctype, settings in partition_doctypes.items():
		print(f"\nProcessing: {doctype}")

		meta = frappe.get_meta(doctype)
		for df in meta.get_table_fields():
			child_doctype = df.options
			child_table = get_table_name(df.options)

			if child_table in processed_child_tables:
				print(f"  ⊘ {child_doctype} already processed, skipping...")
				continue

			processed_child_tables.add(child_table)

			if not db.column_exists(child_table, "posting_date"):
				print(f"  ⊘ {child_doctype}: posting_date column doesn't exist")
				continue

			column_info = frappe.db.sql(
				f"""
                SELECT
                    COLUMN_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                AND table_name = '{child_table}'
                AND column_name = 'posting_date'
            """,
				as_dict=True,
			)

			if not column_info:
				print(f"  ✗ {child_doctype}: Could not get column info")
				continue

			current_default = column_info[0].get("COLUMN_DEFAULT")

			# Check if already has expression default
			if current_default and "curdate" in str(current_default).lower():
				print(f"  ⊘ {child_doctype}: Already has DEFAULT (CURRENT_DATE)")
				continue

			# Check if table is partitioned
			is_partitioned = analyzer.is_partitioned(child_table)

			# Generate Percona command
			cmd = _generate_posting_date_default_command(
				table=child_table,
				db=db,
			)
			if cmd:
				commands.append(cmd)
				tables_info.append(
					{
						"table": child_table,
						"doctype": child_doctype,
						"type": "child",
						"partitioned": is_partitioned,
					}
				)
				print(f"  ✓ {child_doctype} - command generated")
			else:
				print(f"  ⊘ {child_doctype} - skipped")

	if not commands:
		print("\nNo DEFAULT modifications needed - all columns already have defaults")
		return True

	if output_file is None:
		output_file = "./percona_posting_date_defaults.sh"

	script_content = _generate_posting_date_defaults_script(commands, tables_info, db)

	with open(output_file, "w") as f:
		f.write(script_content)

	os.chmod(output_file, 0o755)

	print(f"\n{'='*80}")
	print("Summary")
	print(f"{'='*80}")
	print(f"Commands generated: {len(commands)}")
	print(f"Shell script written to: {output_file}")
	print("\nNext steps:")
	print(f"  1. Review and execute the script: bash {output_file}")
	print(f"{'='*80}\n")

	# Also print commands to console
	print("\n" + "=" * 80)
	print("GENERATED COMMANDS (also saved to script file):")
	print("=" * 80 + "\n")
	for i, cmd in enumerate(commands, 1):
		info = tables_info[i - 1]
		partitioned_note = " (PARTITIONED)" if info.get("partitioned") else ""
		print(f"# {i}. {info['doctype']}{partitioned_note}")
		print(cmd)
		print()

	return True


def _generate_posting_date_default_command(
	table: str,
	db: DatabaseConnection,
) -> str | None:
	"""
	Generate the Percona pt-online-schema-change command for adding DEFAULT to posting_date.

	Returns None if column doesn't exist or on error.
	"""
	import shlex

	try:
		# Build ALTER statement - MariaDB 10.6+ supports expression defaults
		alter_statement = "MODIFY COLUMN `posting_date` DATE NULL DEFAULT (CURRENT_DATE)"

		dsn = db.get_dsn(table)

		user_arg = f"--user={db.user}" if db.user else "--user=YOUR_DB_USER"
		pass_arg = (
			f"--password={db.password}" if db.password else "--password=YOUR_DB_PASSWORD"
		)

		cmd_parts = [
			"pt-online-schema-change",
			shlex.quote(f"--alter={alter_statement}"),
			shlex.quote(dsn),
			shlex.quote(user_arg),
			shlex.quote(pass_arg),
			"--execute",
			"--chunk-size=10000",
			"--max-load=Threads_running=100",
			"--critical-load=Threads_running=200",
			"--recursion-method=none",
			"--progress=time,30",
			"--print",
			"--no-check-alter",
		]

		return " ".join(cmd_parts)

	except Exception as e:
		print(f"    ERROR generating command for {table}: {e}")
		return None


def _generate_posting_date_defaults_script(
	commands: list,
	tables_info: list,
	db: DatabaseConnection,
) -> str:
	"""Generate a shell script with all Percona commands for posting_date defaults"""

	has_real_creds = db.user and db.password and "YOUR_" not in str(db.user)

	script = f"""#!/bin/bash
#
# Percona posting_date DEFAULT Script
# Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
# Database: {db.database}
# Host: {db.host}
#
# This script adds DEFAULT (CURRENT_DATE) to posting_date columns in child tables.
# This ensures NULL errors don't occur when hooks fail to populate the field.
#
"""

	if not has_real_creds:
		script += """# IMPORTANT: Database credentials were not provided.
# Please replace YOUR_DB_USER and YOUR_DB_PASSWORD in the commands below.
#
# Option: Find and replace in this file:
#   sed -i 's/YOUR_DB_USER/actual_user/g' percona_posting_date_defaults.sh
#   sed -i 's/YOUR_DB_PASSWORD/actual_password/g' percona_posting_date_defaults.sh
#
"""

	script += f"""# Usage: bash percona_posting_date_defaults.sh
#

set -e  # Exit on error

echo "Starting Percona posting_date DEFAULT modifications..."
echo "Database: {db.database}"
echo "Host: {db.host}"
echo "Tables to modify: {len(commands)}"
echo ""

"""

	for i, (cmd, info) in enumerate(zip(commands, tables_info), 1):
		partitioned_note = (
			" (PARTITIONED - may take longer)" if info.get("partitioned") else ""
		)
		script += f"""
# ============================================================================
# {i}/{len(commands)}: {info['doctype']}{partitioned_note}
# Table: {info['table']}
# ============================================================================
echo ""
echo "Processing {i}/{len(commands)}: {info['table']}..."
echo ""

{cmd}

echo ""
echo "✓ Completed: {info['table']}"
echo ""

"""

	script += """
echo ""
echo "============================================================================"
echo "All posting_date DEFAULT modifications completed successfully!"
echo "============================================================================"
echo ""
echo "Custom Field metadata has also been updated to set default='Today'"
echo ""
"""

	return script
