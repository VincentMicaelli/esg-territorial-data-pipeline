"""
Configuration centralisée de toutes les sources de données.
Modifier SOURCE_CATALOG pour ajouter/mettre à jour une source.
"""
import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

@dataclass
class DataSource:
    source_id: str
    name: str
    url: str
    millesime: str
    file_format: str   # Hint: 'csv', 'zip_csv', 'xls', 'xlsx', 'gpkg', 'zip_shp'
    encoding: str = "utf-8"
    separator: str = ";"
    minio_bucket: str = "bronze"
    description: str = ""
    update_frequency: str = "annual"
    esg_pillar: str = ""   # 'E', 'S', 'G', 'referentiel'
    license: str = "Etalab 2.0"
    expected_min_rows: int = 1_000
    expected_max_rows: int = 500_000
    key_columns: list = field(default_factory=list)   # Colonnes attendues (contrôle schéma)
    sheet_name: Optional[str | int] = None
    layer_name: Optional[str] = None
    archive_member_pattern: Optional[str] = None
    geometry_handling: str = "drop"
    is_internal: bool = False   # True = source interne
    
SOURCE_CATALOG: dict[str, DataSource] = {

    "S01": DataSource(
        source_id="S01",
        name="FILOSOFI – Revenus et pauvreté IRIS 2021",
        url="https://www.insee.fr/fr/statistiques/fichier/8229323/BASE_TD_FILO_IRIS_2021_DISP_CSV.zip",
        millesime="2021",
        file_format="csv",
        encoding="utf-8",
        separator=";",
        description=(
            "Revenus disponibles, taux de pauvreté seuil 60%, déciles de niveau de vie "
            "à la maille IRIS. Millésime 2021 = dernier publié (2022 annulé par INSEE "
            "pour qualité insuffisante des sources fiscales post-suppression taxe habitation)."
        ),
        esg_pillar="S",
        expected_min_rows=15_000,
        expected_max_rows=60_000,
        key_columns=["IRIS", "DISP_MED21", "DISP_TP6021"],
    ),

    "S02": DataSource(
        source_id="S02",
        name="RP – Population par IRIS 2022",
        url="https://www.insee.fr/fr/statistiques/fichier/8647014/base-ic-evol-struct-pop-2022_csv.zip",
        millesime="2022",
        file_format="zip_csv",
        encoding="utf-8",
        separator=";",
        description="Population totale, structure par âge et CSP à la maille IRIS.",
        esg_pillar="S",
        expected_min_rows=45_000,
        expected_max_rows=70_000,
        key_columns=["IRIS", "P22_POP"],
    ),

    "S03": DataSource(
        source_id="S03",
        name="RP – Activité des résidents IRIS 2022",
        url="https://www.insee.fr/fr/statistiques/fichier/8647006/base-ic-activite-residents-2022_csv.zip",
        millesime="2022",
        file_format="zip_csv",
        encoding="utf-8",
        separator=";",
        description="Emploi, chômage, activité économique à la maille IRIS.",
        esg_pillar="S",
        expected_min_rows=45_000,
        expected_max_rows=70_000,
        key_columns=["IRIS", "P22_ACT1564", "P22_CHOM1564"],
    ),

    "S04": DataSource(
        source_id="S04",
        name="RP – Logement par IRIS 2022",
        url="https://www.insee.fr/fr/statistiques/fichier/8647012/base-ic-logement-2022_csv.zip",
        millesime="2022",
        file_format="zip_csv",
        encoding="utf-8",
        separator=";",
        description="Statut occupation logement, types, ancienneté à la maille IRIS.",
        esg_pillar="E",
        expected_min_rows=45_000,
        expected_max_rows=70_000,
        key_columns=["IRIS", "P22_RP", "P22_RP_PROP"],
    ),

    "S05": DataSource(
        source_id="S05",
        name="BPE – Équipements et services par IRIS 2024",
        url="https://www.insee.fr/fr/statistiques/fichier/8217527/ds_bpe_iris_2024_geo_2024.zip",
        millesime="2024",
        file_format="zip_csv",
        encoding="utf-8",
        separator=";",
        description="Plus de 2.7M équipements géolocalisés à la maille IRIS (229 types).",
        esg_pillar="G",
        expected_min_rows=1_000_000,
        expected_max_rows=4_000_000,
        key_columns=["GEO", "FACILITY_TYPE", "OBS_VALUE"],
    ),

    "S09": DataSource(
        source_id="S09",
        name="Nombre et taux de passoires énergétiques par IRIS",
        url="https://www.data.gouv.fr/api/1/datasets/r/1cb99923-a62d-4f93-b054-c4bd83c83c96",
        millesime="2022",
        file_format="gpkg",
        encoding="utf-8",
        separator=",",
        description="estimation des nombres et taux de passoires énergétiques (classes énergétiques F et G) parmi les résidences principales pour chaque IRIS de France.",
        esg_pillar="E",
        expected_min_rows=30_000,
        expected_max_rows=100_000,
        key_columns=["district", "nombre", "taux"],
        layer_name="districts_with_result_final",
        geometry_handling="drop",
    ),

    "S11": DataSource(
        source_id="S11",
        name="Zones de Revitalisation Rurale (ZRR) - 2021",
        url="https://www.data.gouv.fr/api/1/datasets/r/4160134c-017c-42c7-b838-6048ad56e5f2",
        millesime="2021",
        file_format="xls",
        encoding="utf-8",
        separator=",",
        description="Communes classées en zone de revitalisation rurale (ZRR) pour indicateurs de gouvernance locale.",
        esg_pillar="G",
        expected_min_rows=10_000,
        expected_max_rows=50_000,
        key_columns=["CODGEO"],
    ),

    "S12": DataSource(
        source_id="S12",
        name="Cartographie interne BV/IRIS – Banque",
        url="",  # Fichier interne, path défini dans le pipeline
        millesime="v1",
        file_format="csv",
        encoding="utf-8",
        separator=";",
        description=(
            "Référentiel interne : association bassins de vie → codes IRIS "
            "du périmètre commercial de la banque."
        ),
        esg_pillar="referentiel",
        license="Propriétaire – Usage interne uniquement",
        is_internal=True,
        expected_min_rows=100,
        expected_max_rows=60_000,
        key_columns=["code_iris", "code_bv"],
    ),
}

# Parametre de connexion
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY  = os.getenv("MINIO_ROOT_PASSWORD", "")
MINIO_BUCKET_BRONZE = "bronze"
MINIO_BUCKET_SILVER = "silver"

POSTGRES_USER     = os.getenv("POSTGRES_USER", "esg_admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "esg_territorial")

PG_CONN_STRING = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Chemin du fichier interne (monté dans le container Airflow)
INTERNAL_BV_FILE_PATH = os.getenv(
    "INTERNAL_BV_FILE_PATH",
    "/opt/airflow/data/cartographie_bassin_vie.csv"
)
