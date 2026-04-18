#!/bin/bash
set -e
set -u
set -o pipefail

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME:-PDM}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@local}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

superset init

# Best effort: create/update warehouse connection so datasets are ready to use.
superset set_database_uri \
  -d "Gold Warehouse" \
  -u "postgresql+psycopg2://pdm:pdm@dashboard-db:5432/pdm_dashboard" || true

echo "Superset initialization completed."
