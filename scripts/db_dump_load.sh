#!/bin/bash
# db_dump_load.sh
# Usage:
#   ./db_dump_load.sh dump   # Dumps schema and data to db_dump.sql
#   ./db_dump_load.sh load   # Loads db_dump.sql into the database
#
# Edit these variables as needed for your environment:
DB_CONTAINER="db"
DB_USER="dicom_processor_user"
DB_NAME="dicom_processor_db"
DUMP_FILE="db_dump.sql"

set -e

if [ "$1" == "dump" ]; then
    echo "[INFO] Dumping database $DB_NAME to $DUMP_FILE..."
    docker compose exec $DB_CONTAINER pg_dump --data-only -U $DB_USER $DB_NAME > $DUMP_FILE
    echo "[INFO] Dump complete: $DUMP_FILE"
elif [ "$1" == "load" ]; then
    echo "[INFO] Loading $DUMP_FILE into database $DB_NAME..."
    cat $DUMP_FILE | docker compose exec -T $DB_CONTAINER psql -U $DB_USER $DB_NAME
    echo "[INFO] Load complete."
else
    echo "Usage: $0 [dump|load]"
    exit 1
fi
