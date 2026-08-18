# MariaDB performance analytics

Portable MariaDB Performance Schema helpers for Frappe benches. Snapshots statement digests, wait events, and table I/O into tables in the site database, then reports deltas between snapshots.

Runs under `bench --site … console` or `bench execute`. Not a Frappe app hook or patch — call `enable()` once per site after installing `test_utils`.

## Prerequisites

MariaDB with Performance Schema enabled. If it is off, `enable()` prints guidance and exits without failing.

On Debian/Ubuntu, put **server-only** settings in `mariadb.conf.d` under `[mysqld]` — not in shared sections of `mariadb.cnf` (the client will reject them):

```bash
sudo nano /etc/mysql/mariadb.conf.d/99-performance-schema.cnf
```

```ini
[mysqld]
performance_schema=ON
event_scheduler=ON
```

Restart MariaDB after editing `my.cnf`.

Instruments and consumers are turned on at runtime by `enable()` (superuser). You do not need the MySQL-style `performance-schema-instrument` / `performance-schema-consumer-*` lines in `my.cnf` on MariaDB.

For the optional MariaDB `EVENT` scheduler, `event_scheduler=ON` in `[mysqld]` is required if `SET GLOBAL event_scheduler=ON` is blocked. `enable()` tries the global SET using the superuser connection.

## Credentials

Two MariaDB roles, same pattern as `bench new-site`:

| Role | Connection | Used for |
|------|------------|----------|
| Site user | `frappe.db` (`db_name` / `db_password` in site config) | Snapshot tables, summary reads, `snapshot()`, `top_*`, most of `status()` |
| Superuser | Frappe `get_root_connection()` | `enable()` only — instrumentation, `CREATE EVENT`, and `GRANT SELECT` on Performance Schema summary tables for the site user |

During `enable()`, the superuser grants the site database user `SELECT` on the Performance Schema tables used by `snapshot()` and `status()`. Without that grant, Frappe site users cannot read `events_statements_summary_by_digest` and similar tables.

`enable()` uses Frappe’s `get_root_connection()`:

1. `root_login` / `root_password` kwargs, or `mariadb_root_*` / `root_*` from `common_site_config.json`
2. Username defaults to `root` if unset
3. Password is prompted (`MySQL root password:`) if unset — same as `bench new-site`

If your bench already has root credentials for bubble backup, no prompt appears:

```bash
bench set-config -g root_login root
bench set-config -g root_password your_password
```

Pass credentials explicitly for non-interactive `bench execute`:

```bash
bench --site your_site execute test_utils.utils.mariadb_analytics.enable \
  --kwargs "{'root_login': 'root', 'root_password': 'your_password'}"
```

Prompted passwords are not written back to site config.

## One-time setup

```python
# bench --site your_site console
from test_utils.utils.mariadb_analytics import (
    enable, status, snapshot, top_statements, top_waits, top_table_io
)

enable()                    # default: EVENT every 15 min, retain 7 days
enable(schedule_minutes=0)  # skip MariaDB EVENT; use manual snapshot or hooks
status()
```

Or:

```bash
bench --site your_site execute test_utils.utils.mariadb_analytics.enable
```

## Ongoing use

```python
snapshot()           # collect now (also runs on EVENT schedule if enabled)
top_statements(20)   # delta between last two snapshots (includes first-seen SQL)
top_waits(20)
top_table_io(20)
status()
```

`top_*` uses a left join from the newer snapshot, so statements/waits that first appear in the interval are included. Timer columns are picoseconds; reports are seconds (`/ 1e12`). `top_statements` also shows `rows_examined`, `select_scan`, and `no_index_used` deltas. Ranked by wait time. If counters reset, the newer absolute value is used for that interval.

## Snapshot tables

Created in the site schema without the `tab` prefix (Frappe does not sync them):

- `ps_snapshot` — snapshot id, time, server uptime
- `ps_snapshot_digest` — statement digest counters
- `ps_snapshot_wait` — wait event counters
- `ps_snapshot_table_io` — per-table I/O waits

Older snapshots are pruned after `retain_days` (default 7, baked into the MariaDB `EVENT` body).

## Optional Frappe scheduler

If MariaDB `EVENT` creation fails or you prefer Frappe's scheduler:

```python
# hooks.py — in a consuming app, not part of test_utils
scheduler_events = {
    "cron": {"*/15 * * * *": ["test_utils.utils.mariadb_analytics.snapshot"]}
}
```

## Instrumentation scope

Consumers enabled (not everything):

- `global_instrumentation`, `thread_instrumentation`, `statements_digest`
- `events_statements_current`, `events_statements_history`, `events_waits_current`

Instruments: `wait/%` and `statement/%` only (`ENABLED` and `TIMED`).

## Digest table full

When `events_statements_summary_by_digest` fills, Performance Schema adds a row with `DIGEST IS NULL`. `status()` reports this (or says SELECT was denied). `snapshot()` and the MariaDB `EVENT` capture the overflow row first, then `TRUNCATE` the digest table. The Python path is best-effort as the site user; the EVENT runs as the superuser DEFINER.

## Out of scope

AWS, CloudWatch, Performance Insights, parameter groups, Desk reports, and Greensight-specific patches. After installing `test_utils` on a bench, run `enable()` once on each site that needs analytics.
