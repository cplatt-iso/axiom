#!/bin/bash
# db_dump_load.sh
# Usage:
#   ./db_dump_load.sh dump   # Dumps schema and data to db_dump.sql
#   ./db_dump_load.sh load   # Loads db_dump.sql into the database
#
# Auto-detects container names during transition from dicom_processor to axiom

set -e

# Auto-detect container and database names
if docker ps | grep -q "axiom-db"; then
    DB_CONTAINER="axiom-db"
    DB_USER="axiom_user" 
    DB_NAME="axiom_db"
    echo "[INFO] Using new Axiom Flow names"
elif docker ps | grep -q "dicom_processor_db"; then
    DB_CONTAINER="dicom_processor_db"
    DB_USER="dicom_processor_user"
    DB_NAME="dicom_processor_db" 
    echo "[INFO] Using legacy dicom_processor names"
else
    echo "[ERROR] No database container found (checked axiom-db and dicom_processor_db)"
    exit 1
fi

DUMP_FILE="db_dump.sql"

if [ "$1" == "dump" ]; then
    echo "[INFO] Dumping database $DB_NAME to $DUMP_FILE..."
    docker exec $DB_CONTAINER pg_dump --data-only -U $DB_USER $DB_NAME > $DUMP_FILE
    echo "[INFO] Dump complete: $DUMP_FILE"
elif [ "$1" == "load" ]; then
    echo "[INFO] Loading $DUMP_FILE into database $DB_NAME..."
    cat $DUMP_FILE | docker exec -i $DB_CONTAINER psql -U $DB_USER $DB_NAME
    echo "[INFO] Load complete."
else
    echo "Usage: $0 [dump|load]"
    exit 1
fi
