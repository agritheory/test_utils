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

`enable()` resolves the superuser the same way `bench new-site` does:

1. `root_login` / `root_password` (or `mariadb_root_*`) from `common_site_config.json` if set
2. Otherwise interactive prompts: `Enter mysql super user [root]:` and `MySQL root password:`

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
from test_utils.utils.mariadb_analytics import enable, status, snapshot, top_statements, top_waits

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
top_statements(20)   # delta between last two snapshots
top_waits(20)
status()
```

Timer columns from Performance Schema are picoseconds; `top_*` reports seconds (`/ 1e12`). If counters reset between snapshots, the newer absolute value is used for that interval.

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

Instruments: `wait/%` and `statement/%` only.

## Digest table full

When `events_statements_summary_by_digest` fills, Performance Schema adds a row with `DIGEST IS NULL`. `status()` reports this. `snapshot()` captures the overflow row, then attempts `TRUNCATE` on the digest table as the site user (best-effort; may need superuser or a restart if denied).

## Out of scope

AWS, CloudWatch, Performance Insights, parameter groups, Desk reports, and Greensight-specific patches. After installing `test_utils` on a bench, run `enable()` once on each site that needs analytics.
