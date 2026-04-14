# Runbook Pipeline ESG Territorial

## Démarrage Standard

```bash
docker compose up
make run-bronze
```

Le déclenchement du DAG Bronze lance ensuite automatiquement :

1. `silver_transform_esg_territorial`
2. `gold_esg_territorial`
3. `quality_check_esg_territorial`

## Vérifications Opérationnelles

- Airflow UI : `http://localhost:8080`
- MinIO Console : `http://localhost:9001`
- Marquez : `http://localhost:3000`
- Grafana : `http://localhost:3002`
- Initialisation des rôles PostgreSQL : service `esg_postgres_init_users`

## Points D'Audit

- historique d'ingestion : `bronze.ingestion_log`
- historique des runs dbt : `bronze.dbt_run_log`
- synthèse d'audit : `gold.v_audit_ingestion_summary`
- contrôle qualité final : DAG `quality_check_esg_territorial`

## Requêtes De Contrôle

```sql
SELECT source_id, status, row_count, ingested_at
FROM bronze.ingestion_log
ORDER BY ingested_at DESC;
```

```sql
SELECT COUNT(*) AS nb_iris, ROUND(AVG(score_esg_0_100), 1) AS score_esg_moy
FROM gold.gold_esg_iris_final;
```

## Dashboard Metabase

Apres le premier demarrage et une execution complete du pipeline :

```bash
make setup-metabase
```

Cree automatiquement un compte admin, connecte la base Gold et provisionne
un dashboard avec 8 visualisations. Accessible sur `http://localhost:3001`.

## Exports Livrable

```bash
make export-livrable
```

## Chaîne De Scripts

- orchestration Airflow : `airflow/dags/`
- ingestion Python : `ingestion/`
- transformations SQL : `dbt/models/`, `dbt/tests/`, `dbt/macros/`
