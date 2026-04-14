# Livrable Etude De Cas

Ce dossier regroupe les éléments attendus pour la remise du cas d'usage :

- [Dossier DMO](dmo_dossier.md)
- [Note methodologique — Choix, limites et perspectives](note_methodologique.md)
- [Exports finaux](exports/)
- [Scripts Python d'ingestion](../ingestion/)
- [DAGs Airflow](../airflow/dags/)
- [Transformations et tests dbt](../dbt/)
- [Catalogue des données](../docs/data_catalog.md)
- [Runbook opérationnel](../docs/runbook_pipeline.md)
- [Notebook d'analyse](../notebooks/01_exploration_esg.ipynb)

Correspondance avec la consigne :

- `jeu de données final exploitable` : `exports/`
- `scripts SQL et/ou Python documentant les traitements` : `../ingestion/`, `../airflow/dags/`, `../dbt/`
- `éléments DMO de gouvernance auditable` : `dmo_dossier.md`, `../docs/data_catalog.md`, `../docs/runbook_pipeline.md`

Les exports de `exports/` se régénèrent depuis la stack locale avec :

```bash
make export-livrable
```
