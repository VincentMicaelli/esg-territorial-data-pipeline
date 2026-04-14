# ESG Territorial Data Platform

Socle de données territoriales ESG à la maille IRIS et bassin de vie pour un cas d'usage bancaire.

## Démarrage Rapide

Le chemin nominal du projet est :

1. copier `.env.example` vers `.env` et ajuster les secrets si nécessaire
2. copier `cartographie_bassin_vie.csv` vers `./data/` (Creer le dossier si non present)
3. `docker compose up`
4. attendre la fin de `esg_postgres_init_users` puis du bootstrap Airflow
5. déclencher `bronze_ingestion_esg_territorial` dans Airflow, ou `make run-bronze`
6. laisser la chaîne s'exécuter automatiquement :
   `Bronze -> Silver -> Gold -> Quality`

Raccourcis utiles :

```bash
make up
make run-bronze
make status
make status-gold
make export-livrable
```

## Services

- Airflow : `http://localhost:8080`
- Marquez : `http://localhost:3000`
- Metabase : `http://localhost:3001`
- Grafana : `http://localhost:3002`
- MinIO : `http://localhost:9001`
- JupyterLab : `http://localhost:8888`

## Livrable

Le dossier de remise se trouve dans [livrable/](livrable/README.md).

Il contient :

- le mapping explicite aux attendus de l'étude de cas,
- la localisation des scripts SQL/Python de traitement,
- le dossier DMO,
- les exports finaux Gold,
- les liens vers le catalogue, le runbook et le notebook d'analyse.
