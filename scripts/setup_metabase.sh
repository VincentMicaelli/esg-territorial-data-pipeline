#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# setup_metabase.sh — Provision Metabase: admin account, DB connection,
#                     saved questions and dashboard for ESG Territorial.
#
# Idempotent: safe to re-run on a fresh or existing Metabase.
# Requires Metabase to be healthy on port 3001 and Gold tables populated.
#
# Usage:  bash scripts/setup_metabase.sh
# ----------------------------------------------------------------------------
set -euo pipefail

MB_URL="${MB_URL:-http://localhost:3001}"
MB_EMAIL="${MB_EMAIL:-admin@esg-territorial.local}"
MB_PASSWORD="${MB_PASSWORD:-Admin123!}"
MB_FIRST="${MB_FIRST:-Admin}"
MB_LAST="${MB_LAST:-ESG}"

PG_HOST="${POSTGRES_HOST:-postgres}"
PG_PORT="${POSTGRES_PORT:-5432}"
PG_DB="${POSTGRES_DB:-esg_territorial}"
PG_USER="${POSTGRES_USER:-esg_admin}"
PG_PASS="${POSTGRES_PASSWORD:-admin}"

info()  { echo "[metabase-setup] $*"; }
fail()  { echo "[metabase-setup] ERROR: $*" >&2; exit 1; }

# ── Wait for Metabase ───────────────────────────────────────────────────────

info "Waiting for Metabase at ${MB_URL} ..."
for i in $(seq 1 60); do
  curl -sf "${MB_URL}/api/health" >/dev/null 2>&1 && break
  sleep 2
done
curl -sf "${MB_URL}/api/health" >/dev/null 2>&1 || fail "Metabase not reachable after 120s"
info "Metabase is healthy."

# ── Step 1: Initial setup (first-time only) ─────────────────────────────────

HAS_SETUP=$(curl -sf "${MB_URL}/api/session/properties" \
  | python -c "import json,sys; print(json.load(sys.stdin).get('has-user-setup', False))" 2>/dev/null)

if [ "$HAS_SETUP" != "True" ]; then
  SETUP_TOKEN=$(curl -sf "${MB_URL}/api/session/properties" \
    | python -c "import json,sys; print(json.load(sys.stdin).get('setup-token',''))" 2>/dev/null)
  [ -z "$SETUP_TOKEN" ] && fail "No setup token found."

  info "Running first-time Metabase setup ..."
  curl -sf -X POST "${MB_URL}/api/setup" \
    -H "Content-Type: application/json" \
    -d "{
      \"token\": \"${SETUP_TOKEN}\",
      \"user\": {
        \"email\": \"${MB_EMAIL}\",
        \"first_name\": \"${MB_FIRST}\",
        \"last_name\": \"${MB_LAST}\",
        \"password\": \"${MB_PASSWORD}\"
      },
      \"prefs\": {
        \"site_name\": \"ESG Territorial\",
        \"site_locale\": \"fr\"
      }
    }" >/dev/null
  info "Admin account created (${MB_EMAIL})."
else
  info "Metabase already set up — skipping initial setup."
fi

# ── Step 2: Authenticate ────────────────────────────────────────────────────

info "Logging in as ${MB_EMAIL} ..."
SESSION_ID=$(curl -sf -X POST "${MB_URL}/api/session" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${MB_EMAIL}\",\"password\":\"${MB_PASSWORD}\"}" \
  | python -c "import json,sys; print(json.load(sys.stdin)['id'])" 2>/dev/null) \
  || fail "Login failed. If Metabase was set up with different credentials, pass MB_EMAIL and MB_PASSWORD env vars."

info "Authenticated."

mb() {
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -sf -X "$method" "${MB_URL}${path}" \
      -H "Content-Type: application/json" \
      -H "X-Metabase-Session: ${SESSION_ID}" \
      -d "$body" 2>/dev/null
  else
    curl -sf -X "$method" "${MB_URL}${path}" \
      -H "Content-Type: application/json" \
      -H "X-Metabase-Session: ${SESSION_ID}" 2>/dev/null
  fi
}

# ── Step 3: Add database (if needed) ────────────────────────────────────────

DB_ID=$(mb GET "/api/database" | python -c "
import json, sys
for db in json.load(sys.stdin).get('data', []):
    if db.get('engine') == 'postgres' and db['id'] != 1:
        print(db['id']); break
" 2>/dev/null)

if [ -n "$DB_ID" ]; then
  info "PostgreSQL database already connected (id=${DB_ID})."
else
  info "Adding PostgreSQL database ..."
  DB_ID=$(mb POST "/api/database" "{
    \"engine\": \"postgres\",
    \"name\": \"ESG Territorial\",
    \"details\": {
      \"host\": \"${PG_HOST}\",
      \"port\": ${PG_PORT},
      \"dbname\": \"${PG_DB}\",
      \"user\": \"${PG_USER}\",
      \"password\": \"${PG_PASS}\",
      \"schema-filters-type\": \"inclusion\",
      \"schema-filters-patterns\": \"gold,silver,bronze\"
    }
  }" | python -c "import json,sys; print(json.load(sys.stdin)['id'])" 2>/dev/null) \
    || fail "Failed to add database."
  info "Database added (id=${DB_ID}). Waiting for sync ..."
  sleep 10
fi

# ── Step 4: Create saved questions ──────────────────────────────────────────

EXISTING_CARDS=$(mb GET "/api/card" 2>/dev/null || echo "[]")

card_exists() {
  echo "$EXISTING_CARDS" | python -c "
import json, sys
name = sys.argv[1]
for c in json.load(sys.stdin):
    if c.get('name') == name:
        print(c['id']); break
" "$1" 2>/dev/null
}

create_card() {
  local name="$1" desc="$2" sql="$3" display="$4"
  local existing
  existing=$(card_exists "$name")
  if [ -n "$existing" ]; then
    info "  [skip] '${name}' (id=${existing})"
    echo "$existing"
    return
  fi
  local sql_json
  sql_json=$(python -c "import json; print(json.dumps('''$sql'''.strip()))" 2>/dev/null)
  local cid
  cid=$(mb POST "/api/card" "{
    \"name\": $(python -c "import json; print(json.dumps('$name'))" 2>/dev/null),
    \"description\": $(python -c "import json; print(json.dumps('$desc'))" 2>/dev/null),
    \"dataset_query\": {
      \"type\": \"native\",
      \"native\": {\"query\": ${sql_json}},
      \"database\": ${DB_ID}
    },
    \"display\": \"${display}\",
    \"visualization_settings\": {}
  }" | python -c "import json,sys; print(json.load(sys.stdin)['id'])" 2>/dev/null)
  if [ -n "$cid" ]; then
    info "  [new]  '${name}' (id=${cid})"
    echo "$cid"
  else
    info "  [FAIL] '${name}'"
  fi
}

info "Creating saved questions ..."

SQL1="SELECT bv_code || ' - ' || bv_label AS bassin_de_vie, ROUND(score_esg_pondere, 1) AS score_esg, ROUND(score_social_pondere, 1) AS score_social, ROUND(score_env_pondere, 1) AS score_env, ROUND(score_gouvernance_pondere, 1) AS score_gouvernance, nb_iris, population_totale_bv::INTEGER AS population FROM gold.gold_esg_bassin_vie_final ORDER BY score_esg_pondere ASC"

SQL2="SELECT CASE WHEN score_esg_0_100 < 30 THEN '0-30 Fragile' WHEN score_esg_0_100 < 45 THEN '30-45 Faible' WHEN score_esg_0_100 < 55 THEN '45-55 Moyen' WHEN score_esg_0_100 < 65 THEN '55-65 Correct' ELSE '65+ Favorable' END AS tranche_score, COUNT(*) AS nb_iris FROM gold.gold_esg_iris_final GROUP BY 1 ORDER BY MIN(score_esg_0_100)"

SQL3="SELECT iris_code, iris_label, bv_label AS bassin_de_vie, ROUND(score_esg_0_100, 1) AS score_esg, ROUND(score_social_0_100, 1) AS score_social, ROUND(score_env_0_100, 1) AS score_env, ROUND(score_gouvernance_0_100, 1) AS score_gouv, population_totale::INTEGER AS population FROM gold.gold_esg_iris_final ORDER BY score_esg_0_100 ASC LIMIT 10"

SQL4="SELECT bv_code || ' - ' || bv_label AS bassin_de_vie, ROUND(completude_moy_pct, 1) AS completude_pct, nb_iris, nb_iris_incomplets FROM gold.gold_esg_bassin_vie_final ORDER BY completude_moy_pct ASC"

SQL5="SELECT bv_label AS bassin_de_vie, ROUND(score_social_pondere, 1) AS social, ROUND(score_env_pondere, 1) AS environnement, ROUND(score_gouvernance_pondere, 1) AS gouvernance FROM gold.gold_esg_bassin_vie_final ORDER BY bv_label"

SQL6="SELECT COUNT(*) AS nb_iris, COUNT(DISTINCT bv_code) AS nb_bassins_vie, ROUND(AVG(score_esg_0_100), 1) AS score_esg_moyen, ROUND(AVG(pct_completude), 1) AS completude_moyenne_pct, ROUND(AVG(taux_chomage_pct), 1) AS taux_chomage_moyen_pct, SUM(population_totale)::INTEGER AS population_totale FROM gold.gold_esg_iris_final"

SQL7="SELECT iris_label, taux_chomage_pct, score_social_0_100, bv_label AS bassin_de_vie FROM gold.gold_esg_iris_final WHERE taux_chomage_pct IS NOT NULL ORDER BY taux_chomage_pct DESC"

SQL8="SELECT CASE WHEN est_zrr THEN 'En ZRR' ELSE 'Hors ZRR' END AS zone, COUNT(*) AS nb_iris, ROUND(AVG(score_esg_0_100), 1) AS score_esg_moyen FROM gold.gold_esg_iris_final GROUP BY est_zrr"

CARD1=$(create_card "Score ESG moyen par bassin de vie"           "Score ESG pondere par bassin de vie"                             "$SQL1" "bar")
CARD2=$(create_card "Distribution des scores ESG par IRIS"        "Repartition des IRIS selon leur score ESG (0-100)"               "$SQL2" "bar")
CARD3=$(create_card "Top 10 IRIS les plus fragiles"               "Les 10 IRIS avec le score ESG le plus faible"                    "$SQL3" "table")
CARD4=$(create_card "Completude moyenne par bassin de vie"        "Pourcentage moyen des indicateurs renseignes par bassin"          "$SQL4" "bar")
CARD5=$(create_card "Comparaison des piliers E, S, G par bassin"  "Scores Environnement, Social et Gouvernance par bassin de vie"   "$SQL5" "bar")
CARD6=$(create_card "Indicateurs cles du territoire"              "Metriques principales du socle ESG"                              "$SQL6" "table")
CARD7=$(create_card "Chomage vs Score Social par IRIS"            "Relation entre taux de chomage et score social"                   "$SQL7" "scatter")
CARD8=$(create_card "Repartition ZRR / hors ZRR"                  "Proportion des IRIS en ZRR et score ESG moyen"                   "$SQL8" "pie")

# ── Step 5: Create dashboard ───────────────────────────────────────────────

info "Creating dashboard ..."

DASH_ID=$(mb GET "/api/dashboard" | python -c "
import json, sys
for d in json.load(sys.stdin):
    if d.get('name') == 'Socle ESG Territorial':
        print(d['id']); break
" 2>/dev/null)

if [ -n "$DASH_ID" ]; then
  info "Dashboard already exists (id=${DASH_ID})."
else
  DASH_ID=$(mb POST "/api/dashboard" '{
    "name": "Socle ESG Territorial",
    "description": "Tableau de bord ESG a la maille IRIS et bassin de vie — Credit Agricole"
  }' | python -c "import json,sys; print(json.load(sys.stdin)['id'])" 2>/dev/null) \
    || fail "Failed to create dashboard."
  info "Dashboard created (id=${DASH_ID})."
fi

# Build dashcards payload — only include cards that were created successfully
CARDS_JSON="["
SEP=""
add() {
  local cid="$1" r="$2" c="$3" w="$4" h="$5" neg="$6"
  [ -z "$cid" ] && return
  CARDS_JSON+="${SEP}{\"id\":${neg},\"card_id\":${cid},\"row\":${r},\"col\":${c},\"size_x\":${w},\"size_y\":${h}}"
  SEP=","
}
add "${CARD6:-}" 0  0  18 3 -1
add "${CARD1:-}" 3  0  9  7 -2
add "${CARD2:-}" 3  9  9  7 -3
add "${CARD5:-}" 10 0  12 7 -4
add "${CARD8:-}" 10 12 6  7 -5
add "${CARD3:-}" 17 0  18 7 -6
add "${CARD4:-}" 24 0  9  7 -7
add "${CARD7:-}" 24 9  9  7 -8
CARDS_JSON+="]"

mb PUT "/api/dashboard/${DASH_ID}" "{\"dashcards\": ${CARDS_JSON}}" >/dev/null 2>&1

info ""
info "============================================="
info "  Metabase provisioning complete!"
info "============================================="
info ""
info "  Dashboard:  ${MB_URL}/dashboard/${DASH_ID}"
info "  Login:      ${MB_EMAIL}"
info "  Password:   ${MB_PASSWORD}"
info ""
info "  8 saved questions + 1 dashboard created."
info ""
