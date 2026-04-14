#!/usr/bin/env bash
set -euo pipefail

export PGPASSWORD="${POSTGRES_PASSWORD:-${PGPASSWORD:-}}"

psql_args=(
  -v ON_ERROR_STOP=1
  -v role_de_password="${ROLE_DE_PASSWORD}"
  -v ds_user1_password="${DS_USER1_PASSWORD}"
  -v ds_user2_password="${DS_USER2_PASSWORD}"
  -v da_user_password="${DA_USER_PASSWORD}"
  -v audit_user_password="${AUDIT_USER_PASSWORD}"
  -v metabase_user_password="${METABASE_USER_PASSWORD}"
  --username "${POSTGRES_USER}"
  --dbname "${POSTGRES_DB}"
)

if [[ -n "${POSTGRES_HOST:-}" ]]; then
  psql_args+=(--host "${POSTGRES_HOST}")
fi

if [[ -n "${POSTGRES_PORT:-}" ]]; then
  psql_args+=(--port "${POSTGRES_PORT}")
fi

psql "${psql_args[@]}" <<SQL
SELECT format('CREATE ROLE role_de LOGIN PASSWORD %L', :'role_de_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_de');
\gexec

SELECT format('CREATE ROLE ds_user1 LOGIN PASSWORD %L', :'ds_user1_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ds_user1');
\gexec

SELECT format('CREATE ROLE ds_user2 LOGIN PASSWORD %L', :'ds_user2_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ds_user2');
\gexec

SELECT format('CREATE ROLE da_user LOGIN PASSWORD %L', :'da_user_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'da_user');
\gexec

SELECT format('CREATE ROLE audit_user LOGIN PASSWORD %L', :'audit_user_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'audit_user');
\gexec

SELECT format('CREATE ROLE metabase_user LOGIN PASSWORD %L', :'metabase_user_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metabase_user');
\gexec

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

GRANT ALL PRIVILEGES ON SCHEMA bronze, silver, gold TO role_de;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze, silver, gold TO role_de;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze, silver, gold TO role_de;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO role_de;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO role_de;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO role_de;

GRANT USAGE ON SCHEMA silver, gold TO ds_user1, ds_user2;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO ds_user1, ds_user2;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO ds_user1, ds_user2;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO ds_user1, ds_user2;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO ds_user1, ds_user2;

GRANT USAGE ON SCHEMA gold TO da_user, metabase_user;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO da_user, metabase_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO da_user, metabase_user;

GRANT USAGE ON SCHEMA bronze TO audit_user;
GRANT USAGE ON SCHEMA gold TO audit_user;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO audit_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO audit_user;

REVOKE ALL ON SCHEMA bronze FROM ds_user1, ds_user2, da_user, metabase_user;

DO \$\$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'bronze' AND table_name = 'ingestion_log'
  ) THEN
    GRANT SELECT ON TABLE bronze.ingestion_log TO audit_user;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'bronze' AND table_name = 'dbt_run_log'
  ) THEN
    GRANT SELECT ON TABLE bronze.dbt_run_log TO audit_user;
  END IF;
END
\$\$;
SQL
