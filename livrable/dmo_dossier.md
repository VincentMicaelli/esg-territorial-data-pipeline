# Dossier DMO - Socle ESG Territorial

## Objectif

Construire un socle de données territorial homogène, auditable et exploitable pour analyser des indicateurs ESG à la maille IRIS et bassin de vie, à partir d'un référentiel interne et de sources open data publiques.

## Contexte Métier

Le livrable principal est un dataset final par IRIS, complété d'un agrégat bassin de vie et de la documentation nécessaire à un contrôle Data Management Office.

## Valeur Démontrée

Le socle produit répond au besoin métier de lecture fine des territoires sur deux axes complémentaires :

- pilotage local à la maille IRIS pour repérer les zones fragiles ou dynamiques ;
- consolidation bassin de vie pour objectiver des comparaisons inter-territoires compatibles avec un usage réseau.

La valeur du dispositif vient de la combinaison d'un référentiel interne banque, de signaux open data multi-domaines et d'une chaîne de traitement industrialisée, relançable et auditable.

## Sources Mobilisées

Les sources retenues couvrent les trois piliers ESG ainsi que le référentiel métier :

| ID | Source | Usage |
|----|--------|-------|
| S01 | FILOSOFI IRIS 2021 | Revenu médian, pauvreté, inégalités |
| S02 | RP Population 2022 | Population totale, structure démographique |
| S03 | RP Activité 2022 | Activité, emploi, chômage |
| S04 | RP Logement 2022 | Ancienneté du parc, vacance |
| S05 | BPE IRIS 2024 | Services et équipements de proximité |
| S09 | Passoires énergétiques IRIS 2022 | Fragilité énergétique du parc résidentiel |
| S11 | ZRR 2021 | Marqueur de gouvernance territoriale |
| S12 | Cartographie interne BV/IRIS | Périmètre bancaire et rattachement bassin de vie |

## Chaîne De Traitement

### Bronze

- téléchargement des fichiers distants et lecture du fichier interne,
- détection du format réel de chaque source,
- normalisation systématique en CSV UTF-8,
- chargement dans PostgreSQL `bronze.*`,
- journalisation d'audit dans `bronze.ingestion_log`.

### Silver

- normalisation des clés IRIS et communes,
- contrôles de cohérence métier,
- exposition de tables analytiques par domaine (`silver_*`).

### Gold

- calcul des indicateurs E, S et G,
- calcul du score ESG final par IRIS,
- agrégation pondérée par bassin de vie.

## Structuration Analytique

- une table Gold finale IRIS pour les analyses fines et les croisements territoriaux ;
- une table Gold bassin de vie pour les usages de pilotage consolidé ;
- une séparation Bronze / Silver / Gold qui distingue clairement stockage brut, normalisation métier et restitution analytique.

## Règles Métier Principales

- un IRIS du périmètre banque correspond à une seule ligne finale ;
- les secrets statistiques INSEE sont conservés comme `NULL` et tracés via `flag_secret_statistique` ;
- le score environnemental repose désormais sur les passoires énergétiques, l'ancienneté du parc et la vacance ;
- le score social combine revenu, pauvreté, chômage et structure de population ;
- le score gouvernance reflète l'accès aux équipements, à la santé et le zonage ZRR.

## Qualité Et Auditabilité

- audit des ingestions dans `bronze.ingestion_log`,
- audit des transformations dbt dans `bronze.dbt_run_log`,
- tests dbt sur les modèles Silver et Gold,
- contrôle Great Expectations sur `gold.gold_esg_iris_final`,
- documentation du dictionnaire métier dans `docs/data_catalog.md`.

## Gouvernance Mise À Disposition Du DMO

- traçabilité des sources, millésimes et URLs dans `ingestion/config.py` ;
- règles de calcul, de sélection et de normalisation décrites dans ce dossier et dans `docs/data_catalog.md` ;
- scripts de traitement versionnés dans `ingestion/`, `airflow/dags/` et `dbt/` ;
- runbook d'exploitation et de vérification dans `docs/runbook_pipeline.md` ;
- exports finaux régénérables depuis la base Gold avec `make export-livrable`.

## Limites Et Hypothèses

- certaines colonnes FILOSOFI sont absentes ou secrétisées selon les IRIS ;
- la couverture des passoires énergétiques et des équipements ne recouvre pas nécessairement 100% des IRIS ;
- les millésimes ne sont pas homogènes entre toutes les sources, mais ils sont explicitement tracés ;
- les fichiers présents dans `data/` servent uniquement à l'inspection de structure et jamais au hardcoding.

## Usage Et Rafraîchissement

- démarrage de la stack avec `docker compose up -d`,
- déclenchement unique de `bronze_ingestion_esg_territorial` dans Airflow,
- enchaînement automatique Bronze → Silver → Gold → Quality,
- régénération des exports finaux avec `make export-livrable`.
