import frappe

MYCNF_SNIPPET = """Add to /etc/mysql/mariadb.conf.d/99-performance-schema.cnf:

[mysqld]
performance_schema=ON
event_scheduler=ON

Restart MariaDB. enable() configures instruments and consumers at runtime."""

CONSUMERS = (
	"global_instrumentation",
	"thread_instrumentation",
	"statements_digest",
	"events_statements_current",
	"events_statements_history",
	"events_waits_current",
)

EVENT_NAME = "ps_snapshot_event"

CHILD_TABLES = (
	"ps_snapshot_digest",
	"ps_snapshot_wait",
	"ps_snapshot_table_io",
)

PERFORMANCE_SCHEMA_READ_TABLES = (
	"events_statements_summary_by_digest",
	"events_waits_summary_global_by_event_name",
	"table_io_waits_summary_by_table",
	"setup_consumers",
)

SNAPSHOT_HEADER_SQL = """
INSERT INTO ps_snapshot (collected_at, uptime_seconds)
SELECT NOW(), CAST(VARIABLE_VALUE AS UNSIGNED)
FROM information_schema.GLOBAL_STATUS
WHERE VARIABLE_NAME = 'Uptime'
"""

SNAPSHOT_DETAIL_SQL = (
	"""
	INSERT INTO ps_snapshot_digest (
		snapshot_id,
		schema_name,
		digest,
		digest_text,
		count_star,
		sum_timer_wait,
		sum_lock_time,
		sum_rows_affected,
		sum_rows_sent,
		sum_rows_examined,
		sum_created_tmp_disk_tables,
		sum_created_tmp_tables,
		sum_select_full_join,
		sum_select_scan,
		sum_no_index_used
	)
	SELECT
		{snapshot_id},
		COALESCE(SCHEMA_NAME, ''),
		COALESCE(DIGEST, ''),
		DIGEST_TEXT,
		COUNT_STAR,
		SUM_TIMER_WAIT,
		SUM_LOCK_TIME,
		SUM_ROWS_AFFECTED,
		SUM_ROWS_SENT,
		SUM_ROWS_EXAMINED,
		SUM_CREATED_TMP_DISK_TABLES,
		SUM_CREATED_TMP_TABLES,
		SUM_SELECT_FULL_JOIN,
		SUM_SELECT_SCAN,
		SUM_NO_INDEX_USED
	FROM performance_schema.events_statements_summary_by_digest
	""",
	"""
	INSERT INTO ps_snapshot_wait (snapshot_id, event_name, count_star, sum_timer_wait)
	SELECT
		{snapshot_id},
		EVENT_NAME,
		COUNT_STAR,
		SUM_TIMER_WAIT
	FROM performance_schema.events_waits_summary_global_by_event_name
	WHERE COUNT_STAR > 0
	""",
	"""
	INSERT INTO ps_snapshot_table_io (
		snapshot_id,
		object_schema,
		object_name,
		count_star,
		sum_timer_wait
	)
	SELECT
		{snapshot_id},
		OBJECT_SCHEMA,
		OBJECT_NAME,
		COUNT_STAR,
		SUM_TIMER_WAIT
	FROM performance_schema.table_io_waits_summary_by_table
	WHERE COUNT_STAR > 0
	""",
)

SCHEDULER_HOOK = (
	"scheduler_events = {\n"
	'    "cron": {"*/15 * * * *": ["test_utils.utils.mariadb_analytics.snapshot"]}\n'
	"}"
)


def enable(
	schedule_minutes=15,
	retain_days=7,
	root_login=None,
	root_password=None,
):
	if not require_mariadb() or not require_performance_schema():
		return

	create_snapshot_tables()
	root_conn = get_superuser_connection(root_login, root_password)
	event_created = False
	try:
		configure_instrumentation(root_conn)
		if schedule_minutes:
			event_created = create_snapshot_event(
				root_conn,
				schedule_minutes=schedule_minutes,
				retain_days=retain_days,
			)
	finally:
		root_conn.close()

	print("MariaDB performance analytics enabled.")
	print(
		"  Snapshot tables: ps_snapshot, ps_snapshot_digest, ps_snapshot_wait, ps_snapshot_table_io"
	)
	if schedule_minutes and event_created:
		print(f"  Scheduled event: {EVENT_NAME} every {schedule_minutes} minute(s)")
	elif schedule_minutes:
		print("  MariaDB EVENT not created; call snapshot() manually or use hooks.py")
	else:
		print(
			"  No MariaDB EVENT (schedule_minutes=0). Call snapshot() manually or use hooks.py."
		)
	print(f"  Retention: {retain_days} day(s)")


def status():
	if not require_mariadb():
		return {}

	ps_on = is_performance_schema_enabled()
	print(f"Performance Schema: {'ON' if ps_on else 'OFF'}")
	if not ps_on:
		print_performance_schema_off()
		return {"performance_schema": False}

	consumer_rows = get_consumer_status()
	if consumer_rows:
		print("\nConsumers:")
		for row in consumer_rows:
			print(f"  {row['NAME']}: {row['ENABLED']}")

	digest_full = digest_table_is_full()
	if digest_full is not None:
		print(f"\nDigest table full (DIGEST IS NULL row): {'yes' if digest_full else 'no'}")

	last = get_last_snapshot()
	if last:
		print(
			f"\nLast snapshot: id={last['snapshot_id']} "
			f"at {last['collected_at']} (uptime {last['uptime_seconds']}s)"
		)
	else:
		print("\nLast snapshot: none")

	tables_exist = snapshot_tables_exist()
	print(f"\nSnapshot tables present: {'yes' if tables_exist else 'no'}")

	return {
		"performance_schema": True,
		"consumers": consumer_rows,
		"digest_table_full": digest_full,
		"last_snapshot": last,
		"snapshot_tables": tables_exist,
	}


def snapshot(retain_days=7):
	if not require_mariadb() or not require_performance_schema():
		return None

	if not snapshot_tables_exist():
		create_snapshot_tables()

	digest_full = digest_table_is_full()
	snapshot_id = insert_snapshot_rows()
	prune_snapshots(retain_days)

	if digest_full:
		truncate_digest_table()

	print(f"Snapshot {snapshot_id} collected.")
	return snapshot_id


def top_statements(limit=20):
	pair = load_snapshot_pair()
	if not pair:
		return []

	newer, older = pair
	rows = frappe.db.sql(
		"""
		SELECT
			newer.schema_name,
			newer.digest,
			newer.digest_text,
			newer.count_star AS newer_count,
			older.count_star AS older_count,
			newer.sum_timer_wait AS newer_timer,
			older.sum_timer_wait AS older_timer,
			newer.sum_rows_examined AS newer_examined,
			older.sum_rows_examined AS older_examined,
			newer.sum_select_scan AS newer_scan,
			older.sum_select_scan AS older_scan,
			newer.sum_no_index_used AS newer_no_index,
			older.sum_no_index_used AS older_no_index
		FROM ps_snapshot_digest newer
		LEFT JOIN ps_snapshot_digest older
			ON older.snapshot_id = %(older_id)s
			AND newer.schema_name <=> older.schema_name
			AND newer.digest <=> older.digest
		WHERE newer.snapshot_id = %(newer_id)s
			AND newer.digest != ''
		ORDER BY
			CASE
				WHEN older.sum_timer_wait IS NULL THEN newer.sum_timer_wait
				WHEN newer.sum_timer_wait >= older.sum_timer_wait
					THEN newer.sum_timer_wait - older.sum_timer_wait
				ELSE newer.sum_timer_wait
			END DESC
		LIMIT %(limit)s
		""",
		{"newer_id": newer["snapshot_id"], "older_id": older["snapshot_id"], "limit": limit},
		as_dict=True,
	)

	results = []
	reset_note = False
	for row in rows:
		count_delta, timer_delta, reset = row_deltas(row)
		reset_note = reset_note or reset
		results.append(
			{
				"schema_name": row["schema_name"],
				"digest": row["digest"],
				"digest_text": row["digest_text"],
				"count_delta": count_delta,
				"seconds_delta": seconds_from_timer(timer_delta),
				"rows_examined_delta": counter_delta(row["newer_examined"], row["older_examined"]),
				"select_scan_delta": counter_delta(row["newer_scan"], row["older_scan"]),
				"no_index_used_delta": counter_delta(row["newer_no_index"], row["older_no_index"]),
			}
		)

	print_top_table(
		f"Top statements ({older['snapshot_id']} -> {newer['snapshot_id']})",
		results,
		[
			("schema", "schema_name", 14),
			("count", "count_delta", 8),
			("seconds", "seconds_delta", 9),
			("examined", "rows_examined_delta", 10),
			("scan", "select_scan_delta", 6),
			("no_idx", "no_index_used_delta", 6),
			("digest_text", "digest_text", 48),
		],
	)
	print_reset_note(reset_note)
	return results


def top_waits(limit=20):
	return top_named_deltas(
		title="Top waits",
		table="ps_snapshot_wait",
		name_column="event_name",
		extra_join="",
		limit=limit,
		name_width=40,
	)


def top_table_io(limit=20):
	return top_named_deltas(
		title="Top table I/O",
		table="ps_snapshot_table_io",
		name_column="object_name",
		extra_join="AND newer.object_schema <=> older.object_schema",
		limit=limit,
		name_width=28,
		extra_select="newer.object_schema AS object_schema",
		extra_fields=("object_schema",),
		extra_columns=(("schema", "object_schema", 16),),
	)


def require_mariadb():
	if frappe.conf.db_type == "mariadb":
		return True
	print(f"MariaDB analytics requires db_type=mariadb (got {frappe.conf.db_type!r}).")
	return False


def require_performance_schema():
	if is_performance_schema_enabled():
		return True
	print_performance_schema_off()
	return False


def print_performance_schema_off():
	print("Performance Schema is OFF. Add the following to my.cnf and restart MariaDB:\n")
	print(MYCNF_SNIPPET)


def is_performance_schema_enabled():
	value = frappe.db.sql("SHOW VARIABLES LIKE 'performance_schema'", as_dict=True)
	if not value:
		return False
	return str(value[0].get("Value", "")).upper() in ("ON", "1", "YES")


def is_select_denied(exc):
	return getattr(exc, "args", (None,))[0] == 1142


def snapshot_tables_exist():
	db_name = frappe.conf.db_name
	row = frappe.db.sql(
		"""
		SELECT COUNT(*) AS cnt
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = %(db_name)s
			AND TABLE_NAME = 'ps_snapshot'
		""",
		{"db_name": db_name},
		as_dict=True,
	)
	return bool(row and row[0]["cnt"])


def create_snapshot_tables():
	frappe.db.sql(
		"""
		CREATE TABLE IF NOT EXISTS ps_snapshot (
			snapshot_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			collected_at DATETIME NOT NULL,
			uptime_seconds BIGINT NOT NULL
		)
		"""
	)
	frappe.db.sql(
		"""
		CREATE TABLE IF NOT EXISTS ps_snapshot_digest (
			snapshot_id BIGINT NOT NULL,
			schema_name VARCHAR(64),
			digest VARCHAR(64),
			digest_text LONGTEXT,
			count_star BIGINT,
			sum_timer_wait BIGINT,
			sum_lock_time BIGINT,
			sum_rows_affected BIGINT,
			sum_rows_sent BIGINT,
			sum_rows_examined BIGINT,
			sum_created_tmp_disk_tables BIGINT,
			sum_created_tmp_tables BIGINT,
			sum_select_full_join BIGINT,
			sum_select_scan BIGINT,
			sum_no_index_used BIGINT,
			PRIMARY KEY (snapshot_id, schema_name, digest)
		)
		"""
	)
	frappe.db.sql(
		"""
		CREATE TABLE IF NOT EXISTS ps_snapshot_wait (
			snapshot_id BIGINT NOT NULL,
			event_name VARCHAR(128) NOT NULL,
			count_star BIGINT,
			sum_timer_wait BIGINT,
			PRIMARY KEY (snapshot_id, event_name)
		)
		"""
	)
	frappe.db.sql(
		"""
		CREATE TABLE IF NOT EXISTS ps_snapshot_table_io (
			snapshot_id BIGINT NOT NULL,
			object_schema VARCHAR(64) NOT NULL,
			object_name VARCHAR(64) NOT NULL,
			count_star BIGINT,
			sum_timer_wait BIGINT,
			PRIMARY KEY (snapshot_id, object_schema, object_name)
		)
		"""
	)
	frappe.db.commit()


def configure_instrumentation(root_conn):
	consumer_names = ", ".join(f"'{name}'" for name in CONSUMERS)
	root_conn.sql(
		f"""
		UPDATE performance_schema.setup_consumers
		SET ENABLED = 'YES'
		WHERE NAME IN ({consumer_names})
		"""
	)
	root_conn.sql(
		"""
		UPDATE performance_schema.setup_instruments
		SET ENABLED = 'YES', TIMED = 'YES'
		WHERE NAME LIKE 'wait/%%' OR NAME LIKE 'statement/%%'
		"""
	)
	try:
		root_conn.sql("SET GLOBAL event_scheduler = ON")
	except Exception as exc:
		print(f"Could not SET GLOBAL event_scheduler=ON: {exc}")
		print("Add event_scheduler=ON to my.cnf if you want the MariaDB EVENT to run.")

	grant_performance_schema_read(root_conn)


def grant_performance_schema_read(root_conn):
	db_user = frappe.conf.get("db_user") or frappe.conf.db_name
	accounts = root_conn.sql(
		"SELECT Host FROM mysql.user WHERE User = %s",
		(db_user,),
		as_dict=True,
	)
	if not accounts:
		accounts = [{"Host": "localhost"}, {"Host": "%"}]

	for account in accounts:
		host = account["Host"]
		quoted_user = quote_ident(db_user)
		quoted_host = quote_ident(host)
		for table in PERFORMANCE_SCHEMA_READ_TABLES:
			root_conn.sql(
				f"GRANT SELECT ON performance_schema.`{table}` TO {quoted_user}@{quoted_host}"
			)

	root_conn.sql("FLUSH PRIVILEGES")
	print(f"Granted site user {db_user!r} SELECT on Performance Schema summary tables.")


def quote_ident(value):
	return "`" + str(value).replace("`", "``") + "`"


def create_snapshot_event(root_conn, schedule_minutes, retain_days):
	db_name = frappe.conf.db_name
	event_body = build_event_body_sql(retain_days)
	try:
		root_conn.sql_ddl(f"DROP EVENT IF EXISTS `{db_name}`.`{EVENT_NAME}`")
		root_conn.sql_ddl(
			f"""
			CREATE EVENT `{db_name}`.`{EVENT_NAME}`
			ON SCHEDULE EVERY {int(schedule_minutes)} MINUTE
			STARTS CURRENT_TIMESTAMP
			ON COMPLETION PRESERVE
			ENABLE
			DO
			{event_body}
			"""
		)
		print(f"Created event {EVENT_NAME} in {db_name}.")
		return True
	except Exception as exc:
		print(f"Could not create MariaDB EVENT: {exc}")
		print("Optional Frappe scheduler instead:")
		print(SCHEDULER_HOOK)
		return False


def snapshot_insert_statements(snapshot_id):
	return [sql.format(snapshot_id=snapshot_id) for sql in SNAPSHOT_DETAIL_SQL]


def insert_snapshot_rows():
	frappe.db.sql(SNAPSHOT_HEADER_SQL)
	snapshot_id = frappe.db.sql("SELECT LAST_INSERT_ID() AS id", as_dict=True)[0]["id"]
	for statement in snapshot_insert_statements(snapshot_id):
		frappe.db.sql(statement)
	frappe.db.commit()
	return snapshot_id


def prune_statements(interval_sql):
	statements = []
	for table in CHILD_TABLES:
		statements.append(
			f"""
			DELETE FROM {table}
			WHERE snapshot_id IN (
				SELECT snapshot_id FROM (
					SELECT snapshot_id
					FROM ps_snapshot
					WHERE collected_at < DATE_SUB(NOW(), INTERVAL {interval_sql} DAY)
				) old_snapshots
			)
			"""
		)
	statements.append(
		f"""
		DELETE FROM ps_snapshot
		WHERE collected_at < DATE_SUB(NOW(), INTERVAL {interval_sql} DAY)
		"""
	)
	return statements


def prune_snapshots(retain_days):
	for statement in prune_statements(int(retain_days)):
		frappe.db.sql(statement)
	frappe.db.commit()


def build_event_body_sql(retain_days):
	retain_days = int(retain_days)
	detail_sql = ";\n".join(snapshot_insert_statements("snap_id"))
	prune_sql = ";\n".join(prune_statements(retain_days))
	return f"""
BEGIN
	DECLARE snap_id BIGINT;
	DECLARE digest_full INT DEFAULT 0;

	SELECT COUNT(*) INTO digest_full
	FROM performance_schema.events_statements_summary_by_digest
	WHERE DIGEST IS NULL;

	{SNAPSHOT_HEADER_SQL};
	SET snap_id = LAST_INSERT_ID();
	{detail_sql};
	{prune_sql};

	IF digest_full > 0 THEN
		TRUNCATE TABLE performance_schema.events_statements_summary_by_digest;
	END IF;
END
"""


def get_consumer_status():
	consumer_names = ", ".join(f"'{name}'" for name in CONSUMERS)
	try:
		return frappe.db.sql(
			f"""
			SELECT NAME, ENABLED
			FROM performance_schema.setup_consumers
			WHERE NAME IN ({consumer_names})
			ORDER BY NAME
			""",
			as_dict=True,
		)
	except Exception as exc:
		if is_select_denied(exc):
			print(
				"\nConsumers: site user cannot read performance_schema.setup_consumers "
				"(run enable() to GRANT SELECT)."
			)
			return []
		raise


def digest_table_is_full():
	try:
		row = frappe.db.sql(
			"""
			SELECT COUNT(*) AS cnt
			FROM performance_schema.events_statements_summary_by_digest
			WHERE DIGEST IS NULL
			""",
			as_dict=True,
		)
	except Exception as exc:
		if is_select_denied(exc):
			print(
				"\nDigest table fullness: site user cannot read "
				"events_statements_summary_by_digest (run enable() to GRANT SELECT)."
			)
			return None
		raise
	return bool(row and row[0]["cnt"])


def truncate_digest_table():
	try:
		frappe.db.sql_ddl(
			"TRUNCATE TABLE performance_schema.events_statements_summary_by_digest"
		)
		print("Truncated performance_schema.events_statements_summary_by_digest.")
	except Exception as exc:
		print(f"Could not truncate digest table (needs elevated privileges): {exc}")


def get_last_snapshot():
	if not snapshot_tables_exist():
		return None
	rows = frappe.db.sql(
		"""
		SELECT snapshot_id, collected_at, uptime_seconds
		FROM ps_snapshot
		ORDER BY snapshot_id DESC
		LIMIT 1
		""",
		as_dict=True,
	)
	return rows[0] if rows else None


def get_last_two_snapshots():
	return frappe.db.sql(
		"""
		SELECT snapshot_id, collected_at, uptime_seconds
		FROM ps_snapshot
		ORDER BY snapshot_id DESC
		LIMIT 2
		""",
		as_dict=True,
	)


def load_snapshot_pair():
	if not snapshot_tables_exist():
		print("No snapshot tables. Run enable() first.")
		return None
	snapshots = get_last_two_snapshots()
	if len(snapshots) < 2:
		print("Need at least two snapshots for deltas. Run snapshot() again.")
		return None
	return snapshots[0], snapshots[1]


def top_named_deltas(
	title,
	table,
	name_column,
	extra_join,
	limit,
	name_width,
	extra_select="",
	extra_fields=(),
	extra_columns=(),
):
	pair = load_snapshot_pair()
	if not pair:
		return []

	newer, older = pair
	select_extra = f", {extra_select}" if extra_select else ""
	rows = frappe.db.sql(
		f"""
		SELECT
			newer.{name_column} AS {name_column}
			{select_extra},
			newer.count_star AS newer_count,
			older.count_star AS older_count,
			newer.sum_timer_wait AS newer_timer,
			older.sum_timer_wait AS older_timer
		FROM {table} newer
		LEFT JOIN {table} older
			ON older.snapshot_id = %(older_id)s
			AND newer.{name_column} <=> older.{name_column}
			{extra_join}
		WHERE newer.snapshot_id = %(newer_id)s
		ORDER BY
			CASE
				WHEN older.sum_timer_wait IS NULL THEN newer.sum_timer_wait
				WHEN newer.sum_timer_wait >= older.sum_timer_wait
					THEN newer.sum_timer_wait - older.sum_timer_wait
				ELSE newer.sum_timer_wait
			END DESC
		LIMIT %(limit)s
		""",
		{"newer_id": newer["snapshot_id"], "older_id": older["snapshot_id"], "limit": limit},
		as_dict=True,
	)

	results = []
	reset_note = False
	for row in rows:
		count_delta, timer_delta, reset = row_deltas(row)
		reset_note = reset_note or reset
		item = {
			name_column: row[name_column],
			"count_delta": count_delta,
			"seconds_delta": seconds_from_timer(timer_delta),
		}
		for field in extra_fields:
			item[field] = row.get(field)
		results.append(item)

	print_top_table(
		f"{title} ({older['snapshot_id']} -> {newer['snapshot_id']})",
		results,
		[
			*extra_columns,
			(name_column.replace("_", " "), name_column, name_width),
			("count", "count_delta", 10),
			("seconds", "seconds_delta", 12),
		],
	)
	print_reset_note(reset_note)
	return results


def get_superuser_connection(root_login=None, root_password=None):
	from inspect import signature

	from frappe.database.mariadb.setup_db import get_root_connection

	root_login = (
		root_login or frappe.conf.get("mariadb_root_login") or frappe.conf.get("root_login")
	)
	root_password = (
		root_password
		or frappe.conf.get("mariadb_root_password")
		or frappe.conf.get("root_password")
	)

	if len(signature(get_root_connection).parameters) == 0:
		if root_login:
			frappe.flags.root_login = root_login
		if root_password:
			frappe.flags.root_password = root_password
		return get_root_connection()

	return get_root_connection(root_login, root_password)


def counter_delta(newer, older):
	if newer is None:
		return 0
	if older is None:
		return newer
	if newer >= older:
		return newer - older
	return newer


def seconds_from_timer(timer_delta):
	return (timer_delta or 0) / 1e12


def row_deltas(row):
	count_delta = counter_delta(row["newer_count"], row["older_count"])
	timer_delta = counter_delta(row["newer_timer"], row["older_timer"])
	reset = (
		row["older_count"] is not None and row["newer_count"] < row["older_count"]
	) or (row["older_timer"] is not None and row["newer_timer"] < row["older_timer"])
	return count_delta, timer_delta, reset


def print_reset_note(reset_note):
	if reset_note:
		print(
			"(Counters reset during interval; newer absolute values used where deltas went backwards.)"
		)


def print_top_table(title, rows, columns):
	print(f"\n{title}")
	if not rows:
		print("  (no rows)")
		return

	header = "  ".join(name.ljust(width) for name, _, width in columns)
	print(header)
	print("  ".join("-" * width for _, _, width in columns))
	for row in rows:
		values = []
		for _, key, width in columns:
			value = row.get(key, "")
			if isinstance(value, float):
				text = f"{value:.3f}"
			else:
				text = str(value or "")
			if len(text) > width:
				text = text[: width - 3] + "..."
			values.append(text.ljust(width))
		print("  ".join(values))
