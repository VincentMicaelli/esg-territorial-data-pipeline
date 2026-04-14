# Catalogue de Données ESG Territorial
---

## Table : `gold.gold_esg_iris_final`

**Description** : Table finale ESG par IRIS. UN IRIS = UNE LIGNE. Périmètre banque uniquement.
**Clé primaire** : `iris_code`
**Fréquence de mise à jour** : Hebdomadaire (lundi matin)
**Accès** : `ds_user1`, `ds_user2`, `da_user` (lecture)

### Dictionnaire des colonnes

| Colonne | Type | Nullable | Description | Source | Règle calcul |
|---------|------|----------|-------------|--------|--------------|
| `iris_code` | VARCHAR(9) | NON | Code IRIS INSEE 9 chiffres (PK) | S12 | LPAD 9 chars |
| `iris_label` | TEXT | OUI | Libellé IRIS | S12 | Direct |
| `bv_code` | VARCHAR(20) | NON | Code bassin de vie | S12 | Direct |
| `bv_label` | TEXT | OUI | Libellé bassin de vie | S12 | Direct |
| `dep_code` | VARCHAR(2) | NON | Code département | S12 | LEFT(iris_code,2) |
| `com_code` | VARCHAR(5) | NON | Code commune | S12 | LEFT(iris_code,5) |
| `population_totale` | INTEGER | OUI | Population totale IRIS | S02/RP2022 | Direct |
| `revenu_median_uc` | NUMERIC(10,2) | OUI | Revenu médian disponible par UC (€) | S01/FILOSOFI2021 | Direct (NULL si secret) |
| `taux_pauvrete_60` | NUMERIC(5,2) | OUI | Taux de pauvreté seuil 60% (%) | S01/FILOSOFI2021 | Direct (NULL si secret) |
| `ratio_interdecile_d9d1` | NUMERIC(5,2) | OUI | D9/D1 (mesure inégalités) | S01 | D9÷D1 |
| `taux_chomage_pct` | NUMERIC(5,2) | OUI | Taux de chômage BIT 15-64 ans (%) | S03/RP2022 | chom1564÷act1564×100 |
| `taux_emploi_pct` | NUMERIC(5,2) | OUI | Taux d'emploi 15-64 ans (%) | S03/RP2022 | actocc1564÷pop1564×100 |
| `part_logements_avant_1971_pct` | NUMERIC(5,2) | OUI | % logements construits avant 1971 | S04/RP2022 | (1919+1945+1970)÷rp×100 |
| `taux_passoires_energetiques_pct` | NUMERIC(7,4) | OUI | Taux estimé de passoires énergétiques (%) | S09/Passoires2022 | Direct |
| `nb_passoires_energetiques_estime` | NUMERIC(14,4) | OUI | Nombre estimé de passoires énergétiques | S09/Passoires2022 | Direct |
| `nb_equipements_total` | INTEGER | OUI | Nb total équipements BPE | S05/BPE2024 | SUM(`OBS_VALUE`) par IRIS |
| `nb_medecins_generalistes` | INTEGER | OUI | Nb médecins généralistes | S05/BPE2024 | SUM(`OBS_VALUE`) sur `FACILITY_TYPE` IN ('A206','A207','A208') |
| `indice_presence_services` | NUMERIC(5,2) | OUI | Rang percentile équipements (0-100) | S05/BPE2024 | PERCENT_RANK() |
| `est_zrr` | BOOLEAN | NON | Commune en Zone de Revitalisation Rurale | S11 | Direct |
| `flag_secret_statistique` | BOOLEAN | NON | TRUE si FILOSOFI secrétisé pour cet IRIS | S01 | med=s AND tp=s |
| `score_env_0_100` | NUMERIC(5,2) | NON | Score Environnemental synthétique | Calculé | Pondération passoires+ancienneté+vacance |
| `score_social_0_100` | NUMERIC(5,2) | NON | Score Social synthétique | Calculé | Pondération chômage+revenu+pauvreté |
| `score_gouvernance_0_100` | NUMERIC(5,2) | NON | Score Gouvernance synthétique | Calculé | Pondération services+santé+enseignement |
| `score_esg_0_100` | NUMERIC(5,2) | NON | Score ESG global (S×50%+E×25%+G×25%) | Calculé | Pondéré population |
| `pct_completude` | NUMERIC(5,0) | NON | % indicateurs clés non-NULL | Calculé | 10 indicateurs testés |
| `dbt_updated_at` | TIMESTAMPTZ | NON | Date de dernière transformation dbt | Système | NOW() |
| `version_modele` | VARCHAR(10) | NON | Version du modèle de données | Système | Variable dbt |

### Valeurs manquantes (NULL)

Les NULL dans cette table ont deux origines distinctes :
1. **Secret statistique INSEE** : FILOSOFI ne publie pas les indicateurs si l'IRIS contient < 11 ménages fiscaux. `flag_secret_statistique = TRUE` identifie ces cas.
2. **Hors champ géographique** : certaines sources (passoires énergétiques, BPE) ne couvrent pas tous les IRIS.

**Ne jamais interpréter un NULL comme un "zéro".**

---

## Inventaire des Sources

| ID | Nom | URL | Millésime | Licence | Prochaine MAJ attendue |
|----|-----|-----|-----------|---------|------------------------|
| S01 | FILOSOFI IRIS | https://www.insee.fr/fr/statistiques/8229323 | 2021 | Etalab 2.0 | 2025 (incertain) |
| S02 | RP Population | https://www.insee.fr/fr/statistiques/8268806 | 2022 | Etalab 2.0 | 2025 (RP2023) |
| S03 | RP Activité | https://www.insee.fr/fr/statistiques/8268843 | 2022 | Etalab 2.0 | 2025 (RP2023) |
| S04 | RP Logement | https://www.insee.fr/fr/statistiques/8268838 | 2022 | Etalab 2.0 | 2025 (RP2023) |
| S05 | BPE 2024 | https://www.insee.fr/fr/statistiques/8217537 | 2024 | Etalab 2.0 | 2025 (BPE2025) |
| S09 | Passoires énergétiques par IRIS | https://www.data.gouv.fr/api/1/datasets/r/1cb99923-a62d-4f93-b054-c4bd83c83c96 | 2022 | Etalab 2.0 | Ponctuelle |
| S11 | ZRR | https://www.data.gouv.fr/api/1/datasets/r/4160134c-017c-42c7-b838-6048ad56e5f2 | 2021 | Etalab 2.0 | Ponctuelle |
| S12 | Cartographie BV/IRIS interne | (interne) | v1 | Propriétaire | Sur demande |
