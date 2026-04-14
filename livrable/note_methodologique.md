# Note Methodologique — Choix, Limites et Perspectives

## Objectif du document

Ce document justifie les choix de conception retenus pour le socle ESG territorial, en expliquant les arbitrages effectues entre profondeur analytique et fiabilite du pipeline. Il identifie egalement les axes d'enrichissement prioritaires pour une version ulterieure.

---

## 1. Philosophie generale

L'objectif de cette etude de cas etait de livrer un **pipeline fonctionnel, auditable et reproductible** plutot qu'un jeu de donnees exhaustif. Le parti pris a ete de privilegier :

- la **traceabilite** de bout en bout (ingestion log, dbt run log, Great Expectations),
- la **reproductibilite** (un seul `docker compose up` + `make run-bronze` relance tout),
- la **qualite structurelle** (architecture Bronze/Silver/Gold, tests automatises, roles d'acces),

plutot qu'une granularite maximale des indicateurs, qui aurait demande davantage de temps de normalisation et de validation metier sans garantir la coherence de l'ensemble.

---

## 2. Donnees sources disponibles mais non exploitees

Les sources INSEE mobilisees (FILOSOFI, RP Population, RP Activite, RP Logement) contiennent au total plus de **320 colonnes**. Le pipeline n'en exploite qu'environ 20 a 25 %. Ce choix est delibere.

### 2.1 Colonnes non retenues et justification

#### S01 — FILOSOFI IRIS 2021 (30 colonnes, ~10 utilisees)

| Colonne source | Description | Pilier | Raison de l'exclusion |
|----------------|-------------|--------|----------------------|
| `DISP_GI21` | Indice de Gini | S | Indicateur d'inegalite pertinent, mais necessite une mise en contexte metier (seuils, interpretation) qui depasse le scope du PoC |
| `DISP_S80S2021` | Ratio S80/S20 | S | Redondant avec le ratio interdecile deja calcule |
| `DISP_PACT21`, `DISP_PTSA21` | Part revenus d'activite/salaires | S | Decomposition fine des revenus — pertinente en phase 2 |
| `DISP_PCHO21` | Part revenus du chomage | S | Signal de fragilite, a integrer en enrichissement |
| `DISP_PPEN21` | Part revenus des retraites | S | Indicateur de dependance au vieillissement |
| `DISP_PPSOC21` | Part prestations sociales | S | Signal social fort, mais necessite un croisement avec d'autres indicateurs pour eviter les biais d'interpretation |
| `DISP_PPMINI21` | Part minima sociaux (RSA, AAH...) | S | Indicateur de precarite profonde — prioritaire en phase 2 |
| `DISP_PPLOGT21` | Part aides au logement | S | Complementaire aux prestations sociales |

#### S03 — RP Activite 2022 (119 colonnes, ~7 utilisees)

| Colonne source | Description | Pilier | Raison de l'exclusion |
|----------------|-------------|--------|----------------------|
| `P22_SAL15P_CDI` | Salaries en CDI | S | La precarite de l'emploi (CDI vs CDD vs interim) est un axe d'analyse important, mais necessite un calcul de ratio et une validation metier des seuils |
| `P22_SAL15P_CDD` | Salaries en CDD | S | Idem |
| `P22_SAL15P_INTERIM` | Interimaires | S | Idem |
| `P22_CHOM_DIPLMIN` | Chomeurs sans diplome | S | Croisement emploi/diplome — enrichissement possible |
| `P22_ETUD1564` | Population etudiante | S | Utile pour les territoires universitaires |
| `C22_ACTOCC15P_VOIT` | Actifs se deplacant en voiture | E | Dependance automobile — proxy d'empreinte carbone. Necessite un denominateur fiable pour calculer un taux |
| `C22_ACTOCC15P_TCOM` | Actifs en transport en commun | E | Mobilite verte — complementaire |
| `C22_ACTOCC15P_VELO` | Actifs a velo | E | Idem |

#### S04 — RP Logement 2022 (101 colonnes, ~15 utilisees)

| Colonne source | Description | Pilier | Raison de l'exclusion |
|----------------|-------------|--------|----------------------|
| `P22_RP_CFIOUL` | Chauffage au fioul | E | **Signal fort de transition energetique.** Le mix energetique du chauffage est un indicateur environnemental majeur, mais son exploitation pertinente necessite de croiser les quatre modes (fioul, gaz, electrique, autre) en parts relatives |
| `P22_RP_CGAZV`, `P22_RP_CELEC`, `P22_RP_CGAZB` | Autres modes de chauffage | E | Idem — les quatre colonnes doivent etre traitees ensemble |
| `C22_RP_SUROCC_MOD`, `C22_RP_SUROCC_ACC` | Surpeuplement modere/accentue | S | Indicateur de vulnerabilite du logement |
| `P22_RP_VOIT1P`, `P22_RP_VOIT2P` | Menages avec 1+ / 2+ voitures | E | Dependance automobile |
| `P22_MAISON`, `P22_APPART` | Maisons vs appartements | E | Tissu urbain — contextuel |
| `P22_MEN` | Nombre de menages | — | Denominateur utile pour plusieurs ratios |

#### S02 — RP Population 2022 (76 colonnes, ~20 utilisees)

La couverture de cette source est deja bonne. Les colonnes restantes (repartition H/F detaillee, population etrangere/immigree) relevent d'axes d'analyse specifiques qui depassent le cadre de ce PoC.

### 2.2 Priorites d'enrichissement en phase 2

Si le socle devait etre enrichi, les ajouts prioritaires seraient, par ordre d'impact :

1. **Mix energetique du chauffage** (S04 : fioul, gaz, electrique) — renforcement majeur du pilier E
2. **Indice de Gini** (S01 : `DISP_GI21`) — indicateur standard attendu dans toute analyse d'inegalites
3. **Precarite de l'emploi** (S03 : CDI/CDD/interim) — renforcement du pilier S
4. **Modes de deplacement domicile-travail** (S03 : voiture/TC/velo) — signal environnemental complementaire
5. **Part des minima sociaux** (S01 : `DISP_PPMINI21`) — precarite profonde

---

## 3. Valeurs manquantes (NULL) dans le jeu de donnees final

Le dataset final (`gold_esg_iris_final`) presente un taux de completude moyen de **69,2 %**. Cette situation est connue et documentee.

### 3.1 Origine des NULL

Les valeurs manquantes ont trois origines distinctes :

#### Secret statistique INSEE (83,9 % des IRIS pour FILOSOFI)

Les indicateurs FILOSOFI (revenu median, taux de pauvrete, ratio interdecile, etc.) sont masques par l'INSEE lorsque l'IRIS contient **moins de 11 menages fiscaux**. Le territoire couvert (Charente-Maritime et Deux-Sevres) est majoritairement rural, avec de nombreux IRIS communaux a faible population. C'est un comportement attendu et non une anomalie du pipeline.

Le champ `flag_secret_statistique` identifie ces cas.

#### Couverture geographique partielle (BPE, passoires energetiques)

Certaines sources ne couvrent pas tous les IRIS du referentiel interne. Par exemple, un IRIS sans equipement BPE aura des NULL sur les colonnes `nb_equip_*` — ce qui signifie "pas de donnee", pas "zero equipement". La distinction est preservee volontairement.

#### Colonnes non alimentees (taux_pauvrete_40, nb_menages_fiscaux)

Deux colonnes sont a 100 % NULL. Elles ont ete ajoutees au schema dans l'anticipation d'un enrichissement, mais les sources IRIS de l'INSEE ne fournissent pas ces variables a cette maille :

- `taux_pauvrete_40` : l'INSEE ne publie le taux de pauvrete qu'au seuil de 60 % a la maille IRIS
- `nb_menages_fiscaux` : cette variable existe au niveau communal mais pas dans le fichier IRIS FILOSOFI « Distribution »

Ces colonnes auraient pu etre retirees du schema. Elles ont ete conservees comme documentation des limites connues, mais leur suppression serait justifiee dans une version de production.

### 3.2 Pourquoi ne pas avoir comble ces NULL ?

Il est techniquement possible d'ameliorer la completude, par exemple :

- en **remontant a la maille communale** pour les IRIS sous secret statistique (les donnees FILOSOFI communales sont moins secretisees),
- en **imputant** des valeurs a partir des communes ou cantons voisins.

Ces approches n'ont pas ete retenues car elles introduisent des hypotheses de modelisation (imputation, agregation) qui necessitent une validation metier explicite. Dans un contexte de PoC ou l'auditabilite est prioritaire, il a ete juge preferable de **conserver les NULL et de les documenter** plutot que de produire des valeurs interpolees dont la fiabilite ne serait pas garantie.

---

## 4. Perimetre geographique

Le dataset couvre **857 IRIS** repartis sur **2 departements** (17 — Charente-Maritime, 79 — Deux-Sevres) et **14 bassins de vie**. Ce perimetre correspond strictement au referentiel interne fourni (`cartographie_bassin_vie.csv`).

Le pipeline est concu pour fonctionner sur n'importe quel perimetre : il suffit de modifier le fichier de cartographie interne pour etendre la couverture a d'autres departements ou regions. Aucune logique metier n'est codee en dur sur les departements 17 ou 79.

---

## 5. Choix d'architecture et de surequipement apparent

Le socle deploie 12 services Docker (Airflow, Marquez, Metabase, Grafana, Prometheus, MinIO, JupyterLab, etc.), ce qui peut paraitre disproportionne pour un PoC. Ce choix est volontaire :

- **Airflow** : orchestre le pipeline de bout en bout et garantit l'enchainement Bronze → Silver → Gold → Quality sans intervention manuelle.
- **MinIO** : fournit un stockage objet intermediaire auditable (fichiers bruts horodates, checksums).
- **Marquez / OpenLineage** : trace la lineage des donnees automatiquement, ce qui est un attendu standard du DMO.
- **Prometheus / Grafana** : supervision du pipeline (nombre d'IRIS charges, completude moyenne, score ESG moyen).
- **Metabase** : permet une consultation ad hoc du jeu de donnees final sans ecrire de SQL.

L'objectif n'etait pas de livrer une plateforme de production, mais de **demontrer qu'un socle de donnees territorial peut etre industrialise et gouverne** des sa premiere iteration. Les briques sont la, fonctionnelles et documentees ; elles peuvent etre simplifiees ou remplacees selon les contraintes d'infrastructure de la Caisse.

---

## 6. Synthese des arbitrages

| Arbitrage | Choix retenu | Alternative possible | Justification |
|-----------|-------------|---------------------|---------------|
| Nombre d'indicateurs | ~25 colonnes metier | 80+ colonnes possibles | Privilegier la fiabilite et l'auditabilite du PoC |
| NULL FILOSOFI | Conserves tels quels | Imputation communale | Eviter les hypotheses non validees metier |
| Colonnes vides | Conservees dans le schema | Suppression | Documentation des limites connues |
| Infrastructure | 12 services Docker | Script Python unique | Demontrer la capacite d'industrialisation |
| Score ESG | Ponderation fixe (S 50%, E 25%, G 25%) | Ponderation parametre | Simplicite pour le PoC, variable dbt modifiable |
| Millesimes | Heterogenes (2021-2024) | Attente d'un millesime unique | Les sources INSEE n'ont pas le meme calendrier de publication |
