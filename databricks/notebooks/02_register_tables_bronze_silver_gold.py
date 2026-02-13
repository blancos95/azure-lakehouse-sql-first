# Databricks notebook source
# ============================================================
# 02_register_tables_bronze_silver_gold
#
# COSA FA (in parole semplici):
# - NON carica CSV
# - NON trasforma dati
# - NON scrive record
#
# Fa solo "CREATE METADATA":
# - crea (se manca) gli schemi bronze/silver/gold
# - registra le tabelle nel metastore (Unity Catalog) puntando a una LOCATION su ADLS/Blob
#
# Quando serve:
# - quando ADF / un job Databricks ha già scritto i dati in formato Delta nello storage
# - e tu vuoi poter fare: SELECT * FROM bronze.xxx / silver.xxx / gold.xxx
#
# NOTA IMPORTANTE:
# - Le LOCATION DEVONO contenere Delta (cioè la cartella deve avere _delta_log/)
# - Se la cartella NON è Delta o è vuota, il notebook non deve esplodere: stampa "NOT READY"
# ============================================================

# COMMAND ----------
# =========================
# 0) PARAMETRI DA CAMBIARE (COPIA/INCOLLA)
# =========================

# Nomi tabelle come le queryerai in Databricks SQL / notebook
# Esempio: SELECT * FROM gold.nometabella_actual;
BRONZE_TABLE = "bronze.nometabella"
SILVER_TABLE = "silver.nometabella"
GOLD_ACTUAL  = "gold.nometabella_actual"
#GOLD_SCD2    = "gold.nometabella_scd2"   # opzionale: storico SCD2

# LOCATION nello storage (ADLS/Blob) dove si trovano i file Delta
# Questi path li devi impostare tu in base al tuo storage/account/container.
# DEVONO puntare a cartelle "Delta", quindi con dentro _delta_log/
BRONZE_LOCATION = "abfss://<container>@<account>.dfs.core.windows.net/bronze/nometabella/delta/nometabella/"
SILVER_LOCATION = "abfss://<container>@<account>.dfs.core.windows.net/silver/nometabella/delta/nometabella/"

# In gold spesso ci sono due output:
# - actual = fotografia corrente
# - scd2   = storico (valid_from/valid_to/is_current, ecc.)
# selezionarne 1 in base a come si vuole creare la tabella in gold
GOLD_ACTUAL_LOCATION = "abfss://<container>@<account>.dfs.core.windows.net/gold/nometabella/delta/actual/"
GOLD_SCD2_LOCATION   = "abfss://<container>@<account>.dfs.core.windows.net/gold/nometabella/delta/scd2/"

# COMMAND ----------
# =========================
# 1) CREO GLI SCHEMI (layer) SE NON ESISTONO
# =========================
# Questo crea "contenitori logici" nel metastore.
# Non crea file nello storage, non scrive dati.

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------
# =========================
# 2) FUNZIONE: REGISTRA UNA TABELLA DELTA SU UNA LOCATION
# =========================
# - Se la location è Delta valida: crea la tabella nel metastore e fine
# - Se la location non è pronta/Delta: stampa "NOT READY" e continua (non blocca il notebook)

def try_register_delta_table(table_name: str, location: str):
    """
    Registra una tabella Delta esterna:
    CREATE TABLE <table_name> USING DELTA LOCATION '<location>'
    """
    try:
        # Questo comando NON scrive dati.
        # Dice solo: "la tabella si trova fisicamente in questa cartella dello storage".
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{location}'
        """)
        print(f"OK - Registered: {table_name}")
        print(f"   -> {location}")
    except Exception as e:
        # Caso tipico: la cartella non contiene ancora _delta_log/ oppure mancano permessi
        print(f"SKIP - Not ready: {table_name}")
        print(f"  -> {location}")
        print(f"Reason: {str(e)[:200]}...")