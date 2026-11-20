#!/usr/bin/env bash
# Carga las salidas de Spark en BigQuery (requiere: gcloud auth login + proyecto configurado).
# Alternativa Snowflake: crear stage y usar COPY INTO con los mismos CSV.
set -e
bq mk --dataset --location=US fraude 2>/dev/null || true
bq load --replace --autodetect --source_format=CSV --skip_leading_rows=1 \
   fraude.transacciones   salida_spark/export/transacciones_enriquecidas.csv
bq load --replace --autodetect --source_format=CSV --skip_leading_rows=1 \
   fraude.cuentas_riesgo  salida_spark/export/cuentas.csv
echo "Listo. Ahora ejecuta las consultas de bigquery/consultas_negocio.sql en la consola de BigQuery."
