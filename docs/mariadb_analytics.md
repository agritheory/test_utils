# MariaDB performance analytics

Portable MariaDB Performance Schema helpers for Frappe benches. Snapshots statement digests, wait events, and table I/O into tables in the site database, then reports deltas between snapshots.

Runs under `bench --site … console` or `bench execute`. Not a Frappe app hook or patch — call `enable()` once per site after installing `test_utils`.

## Prerequisites

MariaDB with Performance Schema enabled. If it is off, `enable()` prints a `my.cnf` snippet and exits without failing:

```ini
[mysqld]
performance_schema=ON
performance-schema-instrument='wait/%=ON'
performance-schema-instrument='statement/%=ON'
performance-schema-consumer-events-waits-current=ON
performance-schema-consumer-events-statements-current=ON
performance-schema-consumer-events-statements-history=ON
performance-schema-consumer-statements-digest=ON
```

Restart MariaDB after editing `my.cnf`.

For the optional MariaDB `EVENT` scheduler, `event_scheduler=ON` is also required (in `my.cnf` or set globally). `enable()` tries `SET GLOBAL event_scheduler=ON` using the superuser connection.

## Credentials

Two MariaDB roles, same pattern as `bench new-site`:

| Role | Connection | Used for |
|------|------------|----------|
| Site user | `frappe.db` (`db_name` / `db_password` in site config) | Snapshot tables, reads from Performance Schema, `status()`, `snapshot()`, `top_*` |
| Superuser | Frappe `get_root_connection()` | `enable()` only — `UPDATE performance_schema.setup_*`, `CREATE EVENT` |

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
