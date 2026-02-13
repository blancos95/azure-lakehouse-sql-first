from pyspark.sql import functions as F
from datetime import datetime

BRONZE_RAW_PATH = "abfss://<container>@<account>.dfs.core.windows.net/bronze/nometabella/"
TABLE_FULL_NAME = "bronze.nometabella_bronze"
DELTA_LOCATION = "abfss://<container>@<account>.dfs.core.windows.net/bronze/nometabella/delta/nometabella_bronze/"

FIRST_FILE_PATH  = BRONZE_RAW_PATH + "esempio.csv"
UPDATE_FILE_PATH = BRONZE_RAW_PATH + "esempio_v2.csv"

# =========================
# SEZIONE 1: FIRST INGESTION
# =========================
IS_FIRST_LOAD = True
BATCH_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(FIRST_FILE_PATH)
)

df.printSchema()
display(df.limit(20))

df_enriched = (
    df
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("data_colonns_dp", F.current_date())
)

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
    LOCATION '{DELTA_LOCATION}'
    """)

(
    df_enriched.write.format("delta")
    .mode("append")
    .partitionBy("data_colonns_dp")
    .save(DELTA_LOCATION)
)

print(f"OK - FIRST ingestion done. batch={BATCH_ID}, table={TABLE_FULL_NAME}")
spark.sql(f"SHOW PARTITIONS {TABLE_FULL_NAME}").show(truncate=False)

# =========================
# SEZIONE 2: UPDATE (nuovo file)
# =========================
IS_FIRST_LOAD = False
BATCH_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

df_upd = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(UPDATE_FILE_PATH)
)

df_upd.printSchema()
display(df_upd.limit(20))

df_upd_enriched = (
    df_upd
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("data_colonns_dp", F.current_date())
)

(
    df_upd_enriched.write.format("delta")
    .mode("append")
    .partitionBy("data_colonns_dp")
    .save(DELTA_LOCATION)
)

print(f"OK - UPDATE done. batch={BATCH_ID}, table={TABLE_FULL_NAME}")
spark.sql(f"SHOW PARTITIONS {TABLE_FULL_NAME}").show(truncate=False)