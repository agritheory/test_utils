import frappe

MYCnfSnippet = """[mysqld]
performance_schema=ON
performance-schema-instrument='wait/%=ON'
performance-schema-instrument='statement/%=ON'
performance-schema-consumer-events-waits-current=ON
performance-schema-consumer-events-statements-current=ON
performance-schema-consumer-events-statements-history=ON
performance-schema-consumer-statements-digest=ON"""

CONSUMERS = (
	"global_instrumentation",
	"thread_instrumentation",
	"statements_digest",
	"events_statements_current",
	"events_statements_history",
	"events_waits_current",
)

EVENT_NAME = "ps_snapshot_event"


def enable(
	schedule_minutes=15,
	retain_days=7,
	root_login=None,
	root_password=None,
):
	if frappe.conf.db_type != "mariadb":
		print(f"MariaDB analytics requires db_type=mariadb (got {frappe.conf.db_type!r}).")
		return

	if not is_performance_schema_enabled():
		print("Performance Schema is OFF. Add the following to my.cnf and restart MariaDB:\n")
		print(MYCnfSnippet)
		return

	create_snapshot_tables()
	configure_instrumentation(root_login=root_login, root_password=root_password)

	if schedule_minutes:
		create_snapshot_event(
			schedule_minutes=schedule_minutes,
			retain_days=retain_days,
			root_login=root_login,
			root_password=root_password,
		)

	print("MariaDB performance analytics enabled.")
	print(
		"  Snapshot tables: ps_snapshot, ps_snapshot_digest, ps_snapshot_wait, ps_snapshot_table_io"
	)
	if schedule_minutes:
		print(f"  Scheduled event: {EVENT_NAME} every {schedule_minutes} minute(s)")
	else:
		print(
			"  No MariaDB EVENT (schedule_minutes=0). Call snapshot() manually or use hooks.py."
		)
	print(f"  Retention: {retain_days} day(s)")


def status():
	if frappe.conf.db_type != "mariadb":
		print(f"MariaDB analytics requires db_type=mariadb (got {frappe.conf.db_type!r}).")
		return {}

	ps_on = is_performance_schema_enabled()
	print(f"Performance Schema: {'ON' if ps_on else 'OFF'}")
	if not ps_on:
		print("\nAdd the following to my.cnf and restart MariaDB:\n")
		print(MYCnfSnippet)
		return {"performance_schema": False}

	consumer_rows = frappe.db.sql(
		"""
		SELECT NAME, ENABLED
		FROM performance_schema.setup_consumers
		WHERE NAME IN %(consumers)s
		ORDER BY NAME
		""",
		{"consumers": CONSUMERS},
		as_dict=True,
	)
	print("\nConsumers:")
	for row in consumer_rows:
		print(f"  {row['NAME']}: {row['ENABLED']}")

	digest_full = digest_table_is_full()
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
	if frappe.conf.db_type != "mariadb":
		print(f"MariaDB analytics requires db_type=mariadb (got {frappe.conf.db_type!r}).")
		return None

	if not is_performance_schema_enabled():
		print("Performance Schema is OFF. Add the following to my.cnf and restart MariaDB:\n")
		print(MYCnfSnippet)
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
	if not snapshot_tables_exist():
		print("No snapshot tables. Run enable() first.")
		return []

	snapshots = get_last_two_snapshots()
	if len(snapshots) < 2:
		print("Need at least two snapshots for deltas. Run snapshot() again.")
		return []

	newer, older = snapshots
	reset_note = False
	rows = frappe.db.sql(
		"""
		SELECT
			COALESCE(newer.schema_name, older.schema_name) AS schema_name,
			COALESCE(newer.digest, older.digest) AS digest,
			COALESCE(newer.digest_text, older.digest_text) AS digest_text,
			newer.count_star AS newer_count,
			older.count_star AS older_count,
			newer.sum_timer_wait AS newer_timer,
			older.sum_timer_wait AS older_timer
		FROM ps_snapshot_digest newer
		INNER JOIN ps_snapshot_digest older
			ON newer.schema_name <=> older.schema_name
			AND newer.digest <=> older.digest
		WHERE newer.snapshot_id = %(newer_id)s
			AND older.snapshot_id = %(older_id)s
			AND (newer.digest IS NOT NULL AND newer.digest != '')
		ORDER BY (
			CASE
				WHEN newer.count_star >= older.count_star THEN newer.count_star - older.count_star
				ELSE newer.count_star
			END
		) DESC
		LIMIT %(limit)s
		""",
		{"newer_id": newer["snapshot_id"], "older_id": older["snapshot_id"], "limit": limit},
		as_dict=True,
	)

	results = []
	for row in rows:
		count_delta = counter_delta(row["newer_count"], row["older_count"])
		timer_delta = counter_delta(row["newer_timer"], row["older_timer"])
		if row["newer_count"] < row["older_count"] or row["newer_timer"] < row["older_timer"]:
			reset_note = True
		results.append(
			{
				"schema_name": row["schema_name"],
				"digest": row["digest"],
				"digest_text": row["digest_text"],
				"count_delta": count_delta,
				"seconds_delta": timer_delta / 1e12 if timer_delta else 0,
			}
		)

	print_top_table(
		f"Top statements ({older['snapshot_id']} -> {newer['snapshot_id']})",
		results,
		[
			("schema", "schema_name", 16),
			("count", "count_delta", 10),
			("seconds", "seconds_delta", 10),
			("digest_text", "digest_text", 60),
		],
	)
	if reset_note:
		print(
			"(Counters reset during interval; newer absolute values used where deltas went backwards.)"
		)

	return results


def top_waits(limit=20):
	if not snapshot_tables_exist():
		print("No snapshot tables. Run enable() first.")
		return []

	snapshots = get_last_two_snapshots()
	if len(snapshots) < 2:
		print("Need at least two snapshots for deltas. Run snapshot() again.")
		return []

	newer, older = snapshots
	reset_note = False
	rows = frappe.db.sql(
		"""
		SELECT
			newer.event_name,
			newer.count_star AS newer_count,
			older.count_star AS older_count,
			newer.sum_timer_wait AS newer_timer,
			older.sum_timer_wait AS older_timer
		FROM ps_snapshot_wait newer
		INNER JOIN ps_snapshot_wait older
			ON newer.event_name = older.event_name
		WHERE newer.snapshot_id = %(newer_id)s
			AND older.snapshot_id = %(older_id)s
		ORDER BY (
			CASE
				WHEN newer.count_star >= older.count_star THEN newer.count_star - older.count_star
				ELSE newer.count_star
			END
		) DESC
		LIMIT %(limit)s
		""",
		{"newer_id": newer["snapshot_id"], "older_id": older["snapshot_id"], "limit": limit},
		as_dict=True,
	)

	results = []
	for row in rows:
		count_delta = counter_delta(row["newer_count"], row["older_count"])
		timer_delta = counter_delta(row["newer_timer"], row["older_timer"])
		if row["newer_count"] < row["older_count"] or row["newer_timer"] < row["older_timer"]:
			reset_note = True
		results.append(
			{
				"event_name": row["event_name"],
				"count_delta": count_delta,
				"seconds_delta": timer_delta / 1e12 if timer_delta else 0,
			}
		)

	print_top_table(
		f"Top waits ({older['snapshot_id']} -> {newer['snapshot_id']})",
		results,
		[
			("event", "event_name", 40),
			("count", "count_delta", 10),
			("seconds", "seconds_delta", 12),
		],
	)
	if reset_note:
		print(
			"(Counters reset during interval; newer absolute values used where deltas went backwards.)"
		)

	return results


def is_performance_schema_enabled():
	value = frappe.db.sql("SHOW VARIABLES LIKE 'performance_schema'", as_dict=True)
	if not value:
		return False
	return str(value[0].get("Value", "")).upper() in ("ON", "1", "YES")


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


def configure_instrumentation(root_login=None, root_password=None):
	root_conn = get_superuser_connection(root_login, root_password)
	try:
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
			SET ENABLED = 'YES'
			WHERE NAME LIKE 'wait/%%' OR NAME LIKE 'statement/%%'
			"""
		)
		try:
			root_conn.sql("SET GLOBAL event_scheduler = ON")
		except Exception as exc:
			print(f"Could not SET GLOBAL event_scheduler=ON: {exc}")
			print("Add event_scheduler=ON to my.cnf if you want the MariaDB EVENT to run.")
	finally:
		root_conn.close()


def create_snapshot_event(
	schedule_minutes,
	retain_days,
	root_login=None,
	root_password=None,
):
	db_name = frappe.conf.db_name
	event_body = build_event_body_sql(retain_days)
	root_conn = get_superuser_connection(root_login, root_password)
	try:
		root_conn.sql(f"DROP EVENT IF EXISTS `{db_name}`.`{EVENT_NAME}`")
		root_conn.sql(
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
	except Exception as exc:
		print(f"Could not create MariaDB EVENT: {exc}")
		print("Optional Frappe scheduler instead:")
		print(
			"scheduler_events = {\n"
			'    "cron": {"*/15 * * * *": ["test_utils.utils.mariadb_analytics.snapshot"]}\n'
			"}"
		)
	finally:
		root_conn.close()


def insert_snapshot_rows():
	uptime = frappe.db.sql("SHOW GLOBAL STATUS LIKE 'Uptime'", as_dict=True)
	uptime_seconds = int(uptime[0]["Value"]) if uptime else 0

	frappe.db.sql(
		"""
		INSERT INTO ps_snapshot (collected_at, uptime_seconds)
		VALUES (NOW(), %(uptime)s)
		""",
		{"uptime": uptime_seconds},
	)
	snapshot_id = frappe.db.sql("SELECT LAST_INSERT_ID() AS id", as_dict=True)[0]["id"]

	for statement in snapshot_insert_statements(snapshot_id):
		frappe.db.sql(statement)

	frappe.db.commit()
	return snapshot_id


def snapshot_insert_statements(snapshot_id):
	snapshot_id = int(snapshot_id)
	return [
		f"""
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
		f"""
		INSERT INTO ps_snapshot_wait (snapshot_id, event_name, count_star, sum_timer_wait)
		SELECT
			{snapshot_id},
			EVENT_NAME,
			COUNT_STAR,
			SUM_TIMER_WAIT
		FROM performance_schema.events_waits_summary_global_by_event_name
		WHERE COUNT_STAR > 0
		""",
		f"""
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
	]


def build_event_body_sql(retain_days):
	retain_days = int(retain_days)
	return f"""
BEGIN
	DECLARE snap_id BIGINT;

	INSERT INTO ps_snapshot (collected_at, uptime_seconds)
	SELECT NOW(), CAST(VARIABLE_VALUE AS UNSIGNED)
	FROM information_schema.GLOBAL_STATUS
	WHERE VARIABLE_NAME = 'Uptime';

	SET snap_id = LAST_INSERT_ID();

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
		snap_id,
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
	FROM performance_schema.events_statements_summary_by_digest;

	INSERT INTO ps_snapshot_wait (snapshot_id, event_name, count_star, sum_timer_wait)
	SELECT snap_id, EVENT_NAME, COUNT_STAR, SUM_TIMER_WAIT
	FROM performance_schema.events_waits_summary_global_by_event_name
	WHERE COUNT_STAR > 0;

	INSERT INTO ps_snapshot_table_io (
		snapshot_id,
		object_schema,
		object_name,
		count_star,
		sum_timer_wait
	)
	SELECT snap_id, OBJECT_SCHEMA, OBJECT_NAME, COUNT_STAR, SUM_TIMER_WAIT
	FROM performance_schema.table_io_waits_summary_by_table
	WHERE COUNT_STAR > 0;

	DELETE FROM ps_snapshot_digest
	WHERE snapshot_id IN (
		SELECT snapshot_id FROM ps_snapshot
		WHERE collected_at < DATE_SUB(NOW(), INTERVAL {retain_days} DAY)
	);

	DELETE FROM ps_snapshot_wait
	WHERE snapshot_id IN (
		SELECT snapshot_id FROM ps_snapshot
		WHERE collected_at < DATE_SUB(NOW(), INTERVAL {retain_days} DAY)
	);

	DELETE FROM ps_snapshot_table_io
	WHERE snapshot_id IN (
		SELECT snapshot_id FROM ps_snapshot
		WHERE collected_at < DATE_SUB(NOW(), INTERVAL {retain_days} DAY)
	);

	DELETE FROM ps_snapshot
	WHERE collected_at < DATE_SUB(NOW(), INTERVAL {retain_days} DAY);
END
"""


def prune_snapshots(retain_days):
	retain_days = int(retain_days)
	frappe.db.sql(
		"""
		DELETE FROM ps_snapshot_digest
		WHERE snapshot_id IN (
			SELECT snapshot_id FROM (
				SELECT snapshot_id
				FROM ps_snapshot
				WHERE collected_at < DATE_SUB(NOW(), INTERVAL %(days)s DAY)
			) old_snapshots
		)
		""",
		{"days": retain_days},
	)
	frappe.db.sql(
		"""
		DELETE FROM ps_snapshot_wait
		WHERE snapshot_id IN (
			SELECT snapshot_id FROM (
				SELECT snapshot_id
				FROM ps_snapshot
				WHERE collected_at < DATE_SUB(NOW(), INTERVAL %(days)s DAY)
			) old_snapshots
		)
		""",
		{"days": retain_days},
	)
	frappe.db.sql(
		"""
		DELETE FROM ps_snapshot_table_io
		WHERE snapshot_id IN (
			SELECT snapshot_id FROM (
				SELECT snapshot_id
				FROM ps_snapshot
				WHERE collected_at < DATE_SUB(NOW(), INTERVAL %(days)s DAY)
			) old_snapshots
		)
		""",
		{"days": retain_days},
	)
	frappe.db.sql(
		"""
		DELETE FROM ps_snapshot
		WHERE collected_at < DATE_SUB(NOW(), INTERVAL %(days)s DAY)
		""",
		{"days": retain_days},
	)
	frappe.db.commit()


def digest_table_is_full():
	row = frappe.db.sql(
		"""
		SELECT COUNT(*) AS cnt
		FROM performance_schema.events_statements_summary_by_digest
		WHERE DIGEST IS NULL
		""",
		as_dict=True,
	)
	return bool(row and row[0]["cnt"])


def truncate_digest_table():
	try:
		frappe.db.sql("TRUNCATE TABLE performance_schema.events_statements_summary_by_digest")
		frappe.db.commit()
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


def get_superuser_connection(root_login=None, root_password=None):
	if root_login:
		frappe.flags.root_login = root_login
	if root_password:
		frappe.flags.root_password = root_password

	from frappe.database.mariadb.setup_db import get_root_connection

	return get_root_connection()


def counter_delta(newer, older):
	if newer is None:
		return 0
	if older is None:
		return newer
	if newer >= older:
		return newer - older
	return newer


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
