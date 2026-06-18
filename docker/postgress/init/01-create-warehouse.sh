#  Runs ONCE
set -e

echo "[init] Creating Data Warehouse database '${DWH_DATABASE}' if absent..."

# Create the warehouse DB only if it does not already exist (\gexec trick)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE ${DWH_DATABASE}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DWH_DATABASE}')\gexec
EOSQL

echo "[init] Creating staging schema '${DWH_STAGING_SCHEMA}' inside '${DWH_DATABASE}'..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DWH_DATABASE" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS ${DWH_STAGING_SCHEMA};
    CREATE SCHEMA IF NOT EXISTS marts;
EOSQL

echo "[init] Data Warehouse ready: db='${DWH_DATABASE}', schemas='${DWH_STAGING_SCHEMA}', 'marts'."
