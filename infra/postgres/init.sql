-- Initialisation de la base : schémas, extensions, tables d'audit
-- Exécuté une seule fois au premier démarrage PostgreSQL


-- Extensions 

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- Recherche textuelle fuzzy
CREATE EXTENSION IF NOT EXISTS btree_gin; -- Index GIN scalaires

-- Schemas

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Ingestion audit table
CREATE TABLE IF NOT EXISTS bronze.ingestion_log (
    id                  SERIAL PRIMARY KEY,
    source_id           VARCHAR(10)  NOT NULL,
    source_name         VARCHAR(200) NOT NULL,
    source_url          TEXT,
    millesime           VARCHAR(20),
    file_name           VARCHAR(255) NOT NULL,
    minio_path          TEXT         NOT NULL,
    file_size_bytes     BIGINT,
    row_count           INTEGER,
    checksum_md5        CHAR(32),
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ingested_by         VARCHAR(100)          DEFAULT current_user,
    pipeline_run_id     VARCHAR(200),
    dag_id              VARCHAR(200),
    task_id             VARCHAR(200),
    status              VARCHAR(20)  NOT NULL DEFAULT 'SUCCESS'
                        CHECK (status IN ('SUCCESS','FAILED','PARTIAL','RUNNING')),
    error_message       TEXT,
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_source_date
    ON bronze.ingestion_log (source_id, ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_log_status
    ON bronze.ingestion_log (status, ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_log_run
    ON bronze.ingestion_log (pipeline_run_id);

-- Table de suivi run DBT

CREATE TABLE IF NOT EXISTS bronze.dbt_run_log (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(200) NOT NULL,
    model_name      VARCHAR(200) NOT NULL,
    layer           VARCHAR(20)  NOT NULL CHECK (layer IN ('silver','gold')),
    status          VARCHAR(20)  NOT NULL CHECK (status IN ('SUCCESS','ERROR','SKIPPED')),
    rows_affected   INTEGER,
    execution_time_s NUMERIC(10,3),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_dbt_run_log_model
    ON bronze.dbt_run_log (model_name, started_at DESC);

-- Vue AUDIT
CREATE OR REPLACE VIEW gold.v_audit_ingestion_summary AS
SELECT
    source_id,
    source_name,
    millesime,
    MAX(ingested_at)    AS derniere_ingestion,
    COUNT(*)            AS nb_runs_total,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS nb_succes,
    SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS nb_echecs,
    MAX(row_count)      AS dernier_row_count,
    MAX(file_size_bytes) AS dernier_file_size_bytes
FROM bronze.ingestion_log
GROUP BY source_id, source_name, millesime
ORDER BY source_id;

COMMENT ON TABLE bronze.ingestion_log IS
"Journal de toutes les ingestions de données. Clé de l'auditabilité . Ne jamais supprimer de lignes.";
COMMENT ON VIEW gold.v_audit_ingestion_summary IS
'Vue synthétique des ingestions pour audit. Accessible via audit_user.';
