## Using create_partition and restore_partitions

### Hooks available

#### Partition DocTypes - `partition_doctypes`

Defines the Tables to be partitioned and the criteria.

```python
partition_doctypes = {
    "Sales Order": {
        "field": "transaction_date",
        "partition_by": "fiscal_year",  # Options: "fiscal_year", "quarter", "month", "field"
    },
    "Sales Invoice": {
        "field": "posting_date",
        "partition_by": "fiscal_year",  # Options: "fiscal_year", "quarter", "month", "field"
    },
    "Item": {
        "field": "disabled",
        "partition_by": "field",  # Options: "fiscal_year", "quarter", "month", "field"
    },
}
```

#### Exclude Tables - `exclude_tables`

Defines the tables to be ignored in the full database backup.

```python
exclude_tables = ["__global_search", "tabAccess Log", "tabActivity Log", "tabData Import"]
```


## Create Partition

Uses the `partition_doctypes` hook

- Adds custom fields for child tables.
- Modifies primary keys.
- Creates the partitions.

### Usage

```bash
bench --site XXX console
```

```python
from test_utils.utils.create_partition import create_partition
create_partition()
```


## Restore Partition

- Dumps only the full schema of the source database.
- Does a full backup of the source database excluding partitioned tables.
- Merges the schema dump and full dump in one file.
- Restores the previous file into target database.
- Backup and restore the partitioned data into target database.

### Usage
```bash
bench --site XXX console
```

```python
from test_utils.utils.restore_partitions import restore

restore(
	from_site="ultrapro.agritheory.com",
	to_site="upro-m.agritheory.com",
	mariadb_user="root",
	mariadb_password="123",
	mariadb_host="localhost",
	backup_dir="/tmp",
	partitioned_doctypes_to_restore=None,  # None tp restore all partitioned doctypes or list of DocTypes to restore ["Sales Order", "Sales Invoice"]
	last_n_partitions=3,  # Number of partitions to restore
	compress=False,
	delete_files=False,
)
```
