## Using create_partition and restore_partitions

### Hooks available

#### Partition DocTypes - `partition_doctypes`

The `partition_doctypes` hook defines the tables (DocTypes) that will be partitioned and specifies the criteria for partitioning. Below is an example configuration:

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

The `exclude_tables` hook specifies the tables that should be ignored during a full database backup. For example:

```python
exclude_tables = ["__global_search", "tabAccess Log", "tabActivity Log", "tabData Import"]
```

### Before Migrate

#### hooks.py
```python
before_migrate = "your_app.utils.before_migrate"
```

#### utils.py
```python
import frappe
from test_utils.utils.create_partition import create_partition

def before_migrate():
	create_partition(years_ahead=10)
```

### Doc Events


#### hooks.py
```python
doc_events = {
    "*": {
        "before_insert": "your_app.utils.populate_partition_fields",
        "before_save": "your_app.utils.populate_partition_fields",
    }
}
```

#### utils.py

```python
import frappe
from test_utils.utils.create_partition import populate_partition_fields as _populate_partition_fields

def populate_partition_fields(doc, event):
	_populate_partition_fields(doc, event)
```


## Create Partition

The **Create Partition** utility uses the `partition_doctypes` hook to perform several actions:

- **Add Custom Fields:** Introduces custom fields for child tables.
- **Modify Primary Keys:** Adjusts primary keys to support partitioning.
- **Populates Custom Fields:** Populates custom fields from child tables with existing data.
- **Create Partitions:** Sets up the defined partitions based on the configuration.

### Usage

1. Open the bench console for your target site:

```bash
bench --site your_site_name console
```

2. Run the partition creation function:

```python
from test_utils.utils.create_partition import create_partition
create_partition(years_ahead=10)
```

> It is recommended to create partitions 10 years ahead rather than just 1 year. There is no significant overhead in MariaDB/MySQL for having many empty future partitions, and this approach prevents issues with missing partitions as new data is added over time.


## Restore Partition

The **Restore Partition** utility facilitates restoring a database backup from one Frappe site to another. It handles both non-partitioned and partitioned tables by performing the following steps:

- **Schema Dump:** Dumps the full schema of the source database.
- **Full Backup:** Creates a full backup of the source database excluding partitioned tables.
- **Merge Files:** Combines the schema dump and full backup into a single file.
- **Restore Target:** Restores the merged file into the target database.
- **Partitioned Data:** Backs up and restores the partitioned data into the target database.


### Usage

1. Open the bench console for the source site:

```bash
bench --site your_source_site console
```

2. Execute the restore function:

```python
from test_utils.utils.restore_partitions import restore

restore(
    mariadb_user="root",                  # MariaDB root username
    mariadb_password="123",               # MariaDB root password
    to_site="demo2.agritheory.com",       # Target site URL
    to_database=None,                     # Optional: specify a target database name (instead a to_site)
    backup_dir="/tmp",                    # Directory where the backup file will be stored
    partitioned_doctypes_to_restore=None, # Restore all partitioned doctypes if None, or specify a list (e.g., ["Sales Order", "Sales Invoice"])
    last_n_partitions=3,                  # Number of partitions to restore per DocType
    compress=False,                       # Set to True to compress the temporary and backup file
    delete_files=True,                    # Delete temporary files after the operation
)
```


## Bubble Backup

The Bubble Backup utility creates a comprehensive backup of a Frappe site's database. It includes:

- **Non-Partitioned Tables:** Full backup of all tables that are not partitioned.
- **Partitioned Tables:** A specified number of recent partitions for each partitioned table.

The backup is saved in the site's **/private/backups/** directory.

### Configuration 

Before using `bubble_backup`, ensure that your site's `common_site_config.json` contains the necessary database credentials. Specifically, you must define:

- `root_login`: The database root username.
- `root_password`: The database root password.

You can set these configuration values using the following commands:

```bash
bench set-config -g root_login root
bench set-config -g root_password 123
```

_Replace "root" and "123" with your actual database username and password._

### Usage

1. Open the bench console for your site:

```bash
bench --site your_site_name console
```

2. Run the bubble backup function:

```python
from test_utils.utils.restore_partitions import bubble_backup

bubble_backup(
    backup_dir="/tmp",                      # Directory where the backup file will be stored
    partitioned_doctypes_to_restore=None,   # Process all partitioned doctypes if None, or specify a list
    last_n_partitions=1,                    # Number of most recent partitions to include
    delete_files=True,                      # Delete temporary files after the backup process
    keep_temp_db=False,                     # Do not retain the temporary database after the backup
)
```
