#!/bin/bash
DB_USER=$1
DB_PASS=$2
DB_HOST=$3
DB_NAME=$4
OUTPUT_FILE=$5
shift 5
IGNORE_TABLES=("$@")

CMD="mysqldump -u $DB_USER -p$DB_PASS -h $DB_HOST $DB_NAME --no-create-db --routines --triggers --events --quick --single-transaction"

for TABLE in "${IGNORE_TABLES[@]}"; do
    CMD="$CMD --ignore-table=\"$DB_NAME.$TABLE\""
done

CMD="$CMD > $OUTPUT_FILE"

eval $CMD
