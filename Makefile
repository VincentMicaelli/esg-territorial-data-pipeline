# Raccourcis pour les opérations courantes. Lancer avec: make <cible>

.PHONY: up down restart logs init-db run-bronze run-silver run-gold run-all test-dbt docs-dbt status status-gold setup-metabase export-livrable clean

## Démarrer tous les services
up:
	docker compose up -d
	@echo "Services démarrés. Attendre la fin du bootstrap Airflow avant de lancer Bronze."
	sleep 30
	@echo "Airflow:   http://localhost:8080"
	@echo "Metabase:  http://localhost:3001"
	@echo "Jupyter:   http://localhost:8888"
	@echo "Marquez:   http://localhost:3000"
	@echo "Grafana:   http://localhost:3002"
	@echo "MinIO:     http://localhost:9001"

## Arrêter tous les services
down:
	docker compose down

## Redémarrer
restart:
	docker compose restart

## Logs d'un service (ex: make logs SERVICE=airflow-scheduler)
logs:
	docker compose logs -f $(SERVICE)

## Initialiser la DB (exécuté automatiquement au premier up)
init-db:
	docker exec esg_postgres psql -U esg_admin -d esg_territorial -f /docker-entrypoint-initdb.d/01_init.sql
	docker exec esg_postgres bash -lc "export PGPASSWORD=\$$POSTGRES_PASSWORD && bash /docker-entrypoint-initdb.d/02_users.sh"

## Lancer le DAG Bronze manuellement
run-bronze:
	docker exec esg_airflow_scheduler airflow dags trigger bronze_ingestion_esg_territorial
	@echo "Le reste de la chaîne se déclenche automatiquement : Silver -> Gold -> Quality"

## Lancer les transformations Silver (dbt)
run-silver:
	docker exec esg_airflow_scheduler bash -c "cd /opt/dbt && dbt run --select silver --profiles-dir ."

## Lancer les transformations Gold (dbt)
run-gold:
	docker exec esg_airflow_scheduler bash -c "cd /opt/dbt && dbt run --select gold --profiles-dir ."

## Pipeline complet Silver + Gold
run-all:
	docker exec esg_airflow_scheduler bash -c "cd /opt/dbt && dbt run --profiles-dir ."

## Tests dbt
test-dbt:
	docker exec esg_airflow_scheduler bash -c "cd /opt/dbt && dbt test --profiles-dir ."

## Générer et servir la documentation dbt
docs-dbt:
	docker exec esg_airflow_scheduler bash -c "cd /opt/dbt && dbt docs generate --profiles-dir . && dbt docs serve --port 8090"

## Statut des dernières ingestions
status:
	docker exec esg_postgres psql -U esg_admin -d esg_territorial -c \
	"SELECT source_id, millesime, row_count, status, ingested_at::date FROM bronze.ingestion_log ORDER BY ingested_at DESC LIMIT 20;"

## Statut de la table Gold finale
status-gold:
	docker exec esg_postgres psql -U esg_admin -d esg_territorial -c \
	"SELECT COUNT(*) nb_iris, ROUND(AVG(pct_completude),1) completude_moy, ROUND(AVG(score_esg_0_100),1) score_esg_moy FROM gold.gold_esg_iris_final;"

## Provisionner Metabase (dashboard + questions)
setup-metabase:
	bash scripts/setup_metabase.sh

## Export des jeux finaux pour le livrable
export-livrable:
	bash scripts/export_livrable.sh

## Supprimer volumes (DANGER: supprime toutes les données)
clean:
	docker compose down -v
	@echo "ATTENTION: tous les volumes supprimés"
