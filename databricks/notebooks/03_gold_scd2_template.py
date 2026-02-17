# Databricks notebook source
# ============================================================
# 03_gold_scd2_template (PORTFOLIO)
#
# Scopo:
# - Simulare la gestione SCD2 in Gold partendo da:
#   - T1: tabella ACTUAL (snapshot corrente, overwrite)
#   - T0: tabella SCD2 (storico con valid_from / valid_to)
#
# Note importanti:
# - Questo è un TEMPLATE semplificato per portfolio.
# - Pattern "safe": scrivo prima una tabella/target di backup e poi overwrite su SCD2.
# - Se ACTUAL è vuota, SKIP (evita di chiudere tutti i record in SCD2).
# ============================================================

from pyspark.sql import functions as F

# COMMAND ----------
# =========================
# PARAMETRI (DA PERSONALIZZARE)
# =========================

# Tabelle Gold (esempi)
GOLD_ACTUAL_TABLE = "gold.nometabella_actual"
GOLD_SCD2_TABLE   = "gold.nometabella_scd2"
GOLD_BKP_TABLE    = "gold.nometabella_bkp"   # backup safety

# Chiavi di business (identificano un record)
KEY_COLS = ["id"]  # <-- sostituisci con le tue chiavi (es. ["CODICE_FISCALE"])

# Colonne "business" da confrontare per capire se il record è cambiato
# (di solito tutte le colonne escluse key e tecniche)
COMPARE_COLS = ["campo1", "campo2"]  # <-- esempio

# Colonne SCD2
VALID_FROM_COL = "record_validity_from_date"
VALID_TO_COL   = "record_validity_to_date"
OPEN_END_DATE  = "9999-01-01"

# Campo tecnico che rappresenta "data del cambiamento" (se presente in actual)
# Se non c'è, useremo current_timestamp()
CHANGE_TS_COL = "insert_date_ts"  # placeholder (se non esiste nel tuo actual, va in fallback)

# COMMAND ----------
# =========================
# 1) Leggo ACTUAL (T1)
# =========================

df_t1 = spark.table(GOLD_ACTUAL_TABLE)

# Se ACTUAL è vuota, SKIP (pattern aziendale)
if df_t1.isEmpty():
    print(f"SKIP - {GOLD_ACTUAL_TABLE} is empty. Avoid closing all records in SCD2.")
else:
    # COMMAND ----------
    # =========================
    # 2) Leggo SCD2 corrente (T0) se esiste, altrimenti inizializzo
    # =========================
    try:
        df_t0 = spark.table(GOLD_SCD2_TABLE)
        scd2_exists = True
    except Exception:
        df_t0 = None
        scd2_exists = False

    # Determino la "data cambio" (usata per chiudere i record vecchi)
    # - se in actual c'è insert_date_ts uso la max
    # - altrimenti current_timestamp()
    try:
        change_ts = df_t1.agg(F.max(F.col(CHANGE_TS_COL))).collect()[0][0]
        if change_ts is None:
            change_ts = F.current_timestamp()
        else:
            change_ts = F.lit(change_ts)
    except Exception:
        change_ts = F.current_timestamp()

    # COMMAND ----------
    # =========================
    # 3) Caso 1: Primo caricamento (SCD2 non esiste o è vuota)
    # =========================
    if (not scd2_exists) or df_t0.isEmpty():
        print("INIT - First load SCD2 from ACTUAL")

        df_init = (
            df_t1
            .withColumn(VALID_FROM_COL, F.current_date())
            .withColumn(VALID_TO_COL, F.lit(OPEN_END_DATE).cast("date"))
        )

        # Safe write: backup -> scd2
        df_init.write.mode("overwrite").format("delta").saveAsTable(GOLD_BKP_TABLE)
        spark.table(GOLD_BKP_TABLE).write.mode("overwrite").insertInto(GOLD_SCD2_TABLE, overwrite=True)

        print(f"OK - Initialized SCD2: {GOLD_SCD2_TABLE}")

    # COMMAND ----------
    # =========================
    # 4) Caso 2: Update SCD2 (confronto T0 vs T1)
    # =========================
    else:
        print("UPDATE - SCD2 update from ACTUAL")

        # Tengo lo storico chiuso e separo gli "attivi"
        df_closed_hist = df_t0.filter(F.col(VALID_TO_COL) != F.lit(OPEN_END_DATE).cast("date"))
        df_open_t0     = df_t0.filter(F.col(VALID_TO_COL) == F.lit(OPEN_END_DATE).cast("date"))

        # Creo un hash per confrontare velocemente i record (chiave + colonne business)
        def with_hash(df):
            cols_for_hash = KEY_COLS + COMPARE_COLS
            return df.withColumn(
                "_hash",
                F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols_for_hash]), 256)
            )

        t0h = with_hash(df_open_t0)
        t1h = with_hash(df_t1)

        # Record invariati (stesso hash)
        unchanged = t0h.join(t1h.select("_hash"), on="_hash", how="inner").drop("_hash")

        # Record da chiudere (presenti in T0 open ma non più uguali in T1)
        to_close = t0h.join(t1h.select("_hash"), on="_hash", how="left_anti").drop("_hash") \
                     .withColumn(VALID_TO_COL, F.to_date(change_ts))

        # Nuovi record da inserire (presenti in T1 ma non già invariati in T0)
        to_insert = t1h.join(t0h.select("_hash"), on="_hash", how="left_anti").drop("_hash") \
                      .withColumn(VALID_FROM_COL, F.to_date(change_ts)) \
                      .withColumn(VALID_TO_COL, F.lit(OPEN_END_DATE).cast("date"))

        # Unisco:
        # - storico già chiuso
        # - invariati
        # - chiusi oggi
        # - nuove versioni aperte
        df_final = df_closed_hist.unionByName(unchanged).unionByName(to_close).unionByName(to_insert)

        # Safe write: backup -> scd2 (pattern aziendale)
        df_final.write.mode("overwrite").format("delta").saveAsTable(GOLD_BKP_TABLE)
        spark.table(GOLD_BKP_TABLE).write.mode("overwrite").insertInto(GOLD_SCD2_TABLE, overwrite=True)

        print(f"OK - Updated SCD2: {GOLD_SCD2_TABLE}")

# COMMAND ----------
# NOTE:
# - Per leggere lo "stato attuale" dalla SCD2:
#   SELECT * FROM gold.nometabella_scd2 WHERE record_validity_to_date = '9999-01-01';
