# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

# Parametri
BRONZE_RAW_PATH = "abfss://<container>@<account>.dfs.core.windows.net/bronze/nometabella/"
TABLE_FULL_NAME = "bronze.nometabella_bronze"
BATCH_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S") #etichetta che identifica  1esecuzione della pipeline/notebook
IS_FIRST_LOAD = True

# 1) Lettura CSV raw
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(BRONZE_RAW_PATH)
)

df.printSchema()
display(df.limit(20))

# 2) Colonne tecniche
df_enriched = (
    df
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("data_colonns_dp", F.current_date())
)

df_enriched.printSchema()

# 3) Create table (solo prima ingestion)
if IS_FIRST_LOAD:
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_FULL_NAME} (
      campo1 INT,
      campo2 STRING,
      ingest_ts TIMESTAMP,
      source_file STRING,
      batch_id STRING,
      data_colonns_dp DATE
    )
    USING DELTA
    PARTITIONED BY (data_colonns_dp)
    LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/bronze/nometabella/delta/nometabella_bronze/'
    """)

# 4) Scrittura Delta (append) - stesso path della LOCATION
(
    df_enriched.write.format("delta")
    .mode("append")
    .partitionBy("data_colonns_dp")
    .save("abfss://<container>@<account>.dfs.core.windows.net/bronze/nometabella/delta/nometabella_bronze/")
)

print(f"OK - Bronze ingestion done. batch={BATCH_ID}, table={TABLE_FULL_NAME}")