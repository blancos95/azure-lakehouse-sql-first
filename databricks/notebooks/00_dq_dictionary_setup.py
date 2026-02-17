# Databricks notebook source
# ============================================================
# 00_dq_dictionary_setup
#
# Scopo:
# - Creare una tabella "dizionario" per regole di Data Quality / casting
# - Pattern enterprise: regole guidate da metadata (env, tabella, colonna, codice regola)
# - Questo notebook non applica le regole: prepara solo il dizionario
# ============================================================

from pyspark.sql import functions as F

# COMMAND ----------
# 1) Creo lo schema "meta" (dove metto tabelle di configurazione/metadata)
spark.sql("CREATE SCHEMA IF NOT EXISTS meta")

# COMMAND ----------
# 2) Creo la tabella dizionario (Delta)
# Campi:
# - env: DEV/TEST/PROD
# - source_system: nome sorgente (es. ERP/CRM) o generico
# - table_name: tabella target (es. bronze.nometabella_bronze)
# - column_name: colonna su cui applicare regola
# - rule_code: codice regola (FTL, ITS, TRIM, NOT_NULL, RANGE, ...)
# - rule_params: eventuali parametri (JSON string o testo)
# - enabled: abilita/disabilita regola
spark.sql("""
CREATE TABLE IF NOT EXISTS meta.dq_dictionary (
  env STRING,
  source_system STRING,
  table_name STRING,
  column_name STRING,
  rule_code STRING,
  rule_params STRING,
  enabled BOOLEAN,
  created_ts TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------
# 3) Inserisco regole DEMO (portfolio)
# Nota: qui uso INSERT semplice, così è facile da capire e modificare.

spark.sql("""
INSERT INTO meta.dq_dictionary
SELECT
  'DEV'                       AS env,
  'demo_source'               AS source_system,
  'bronze.nometabella_bronze' AS table_name,
  'campo1'                    AS column_name,
  'FTL'                       AS rule_code,        -- float to long (esempio cast)
  NULL                        AS rule_params,
  true                        AS enabled,
  current_timestamp()         AS created_ts
""")

spark.sql("""
INSERT INTO meta.dq_dictionary
SELECT
  'DEV',
  'demo_source',
  'bronze.nometabella_bronze',
  'campo2',
  'TRIM',                      -- trim spazi
  NULL,
  true,
  current_timestamp()
""")

spark.sql("""
INSERT INTO meta.dq_dictionary
SELECT
  'DEV',
  'demo_source',
  'bronze.nometabella_bronze',
  'campo2',
  'ITS',                       -- int to string (esempio cast)
  NULL,
  true,
  current_timestamp()
""")

spark.sql("""
INSERT INTO meta.dq_dictionary
SELECT
  'PROD',
  'demo_source',
  'bronze.nometabella_bronze',
  'campo1',
  'NOT_NULL',                  -- controllo qualità base
  NULL,
  true,
  current_timestamp()
""")

spark.sql("""
INSERT INTO meta.dq_dictionary
SELECT
  'PROD',
  'demo_source',
  'bronze.nometabella_bronze',
  'campo1',
  'RANGE',                     -- esempio range con parametri (min/max)
  '{"min": 0, "max": 999999}'  AS rule_params,
  true,
  current_timestamp()
""")

# COMMAND ----------
# 4) Mostro cosa ho inserito (verifica)
display(
  spark.table("meta.dq_dictionary")
  .orderBy(F.col("env"), F.col("table_name"), F.col("column_name"), F.col("rule_code"))
)

# COMMAND ----------
# 5) Query tipiche (come le userebbe una pipeline)
# - prendo regole abilitate per una tabella e un env

ENV = "DEV"
TABLE = "bronze.nometabella_bronze"

rules_df = (
  spark.table("meta.dq_dictionary")
  .filter((F.col("env") == ENV) & (F.col("table_name") == TABLE) & (F.col("enabled") == True))
  .select("column_name", "rule_code", "rule_params")
  .orderBy("column_name", "rule_code")
)

print(f"Rules for env={ENV}, table={TABLE}")
display(rules_df)

# ============================================================
# ESEMPIO (stile aziendale): aggiungo una regola nel dizionario DQ
# NB: sostituisci dataplatform/metastore/bronze con i tuoi catalog/schema reali
#
# INSERT INTO dataplatform.metastore.bronze.dizionario_dq
# VALUES (
#   "sorgente_tabella",   -- es: ERP / CRM / nome linked service
#   "nome_tabella",       -- es: bronze.nometabella_bronze (oppure solo nometabella)
#   "campo_cambiato",     -- es: campo1
#   "codice_data_quality" -- es: FTL / ITS / TRIM / NOT_NULL / RANGE
# );
# ============================================================