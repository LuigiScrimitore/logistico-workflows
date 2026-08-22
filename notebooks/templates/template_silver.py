# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ### Silver Transformation Template
# MAGIC
# MAGIC **Scopo:** Trasformazione Bronze → Silver con deduplication e MERGE INTO.
# MAGIC
# MAGIC **Pattern:**
# MAGIC 1. Lettura Bronze Delta
# MAGIC 2. Deduplication via ROW_NUMBER() su chiave + _ingestion_timestamp
# MAGIC 3. Cast decimali
# MAGIC 4. MERGE INTO Silver (upsert)
# MAGIC 5. Data Quality check post-scrittura
# MAGIC
# MAGIC **Parametri widget:**
# MAGIC - `source_catalog` — Catalogo Bronze sorgente (es. "bronze_dev")
# MAGIC - `source_schema` — Schema sorgente (es. "logistica")
# MAGIC - `source_table` — Tabella Bronze (es. "carichi_testate")
# MAGIC - `target_catalog` — Catalogo Silver (es. "silver_dev")
# MAGIC - `target_schema` — Schema Silver (es. "logistica")
# MAGIC - `target_table` — Tabella Silver target (es. "carichi_testate")
# MAGIC - `key_cols` — Chiave business comma-separated (es. "CARICO_ID" o "CARICO_ID,RIGA_ID")
# MAGIC - `decimal_cols` — Colonne da castare a DECIMAL comma-separated (opzionale)
# MAGIC - `run_date` — Data di run YYYY-MM-DD

# COMMAND ----------

import time
import sys

sys.path.insert(0, "/Workspace/Repos/logistico/lib")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from logistica_utils import (
    Logger,
    DeltaHelper,
    DQHelper,
    get_run_date,
    cast_decimal,
)

# COMMAND ----------
# MAGIC %md #### 1. Parametri widget

# COMMAND ----------

dbutils.widgets.text("source_catalog", "bronze_dev", "Catalogo Bronze sorgente")
dbutils.widgets.text("source_schema", "logistica", "Schema sorgente")
dbutils.widgets.text("source_table", "carichi_testate", "Tabella Bronze sorgente")
dbutils.widgets.text("target_catalog", "silver_dev", "Catalogo Silver target")
dbutils.widgets.text("target_schema", "logistica", "Schema Silver target")
dbutils.widgets.text("target_table", "carichi_testate", "Tabella Silver target")
dbutils.widgets.text("key_cols", "CARICO_ID", "Chiave business (comma-separated)")
dbutils.widgets.text("decimal_cols", "", "Colonne DECIMAL (comma-separated, opzionale)")
dbutils.widgets.text("run_date", "", "Data di run (YYYY-MM-DD, vuoto=oggi)")

SOURCE_CATALOG = dbutils.widgets.get("source_catalog")
SOURCE_SCHEMA = dbutils.widgets.get("source_schema")
SOURCE_TABLE = dbutils.widgets.get("source_table")
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")
TARGET_TABLE = dbutils.widgets.get("target_table")
RUN_DATE = dbutils.widgets.get("run_date") or get_run_date(spark)

KEY_COLS = [c.strip() for c in dbutils.widgets.get("key_cols").split(",") if c.strip()]
_decimal_raw = dbutils.widgets.get("decimal_cols")
DECIMAL_COLS = [c.strip() for c in _decimal_raw.split(",") if c.strip()] if _decimal_raw else []

# COMMAND ----------
# MAGIC %md #### 2. Init Logger

# COMMAND ----------

log = Logger(
    notebook_name=f"silver_{TARGET_TABLE}",
    area=TARGET_SCHEMA,
    layer="silver",
)
log.log_run_start(
    source_table=f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE}",
    target_table=f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}",
    run_date=RUN_DATE,
)
start_time = time.time()

# COMMAND ----------
# MAGIC %md #### 3. Lettura Bronze

# COMMAND ----------

bronze_fqn = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"
bronze_df = spark.read.format("delta").table(bronze_fqn)
rows_read = bronze_df.count()
log.info("Bronze letto", table=bronze_fqn, rows=rows_read)

# COMMAND ----------
# MAGIC %md #### 4. Deduplication: ROW_NUMBER() per chiave + _ingestion_timestamp DESC

# COMMAND ----------

dedup_window = Window.partitionBy(*KEY_COLS).orderBy(
    F.col("_ingestion_timestamp").desc()
)

deduped_df = (
    bronze_df
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

rows_after_dedup = deduped_df.count()
log.info(
    "Deduplication completata",
    rows_before=rows_read,
    rows_after=rows_after_dedup,
    duplicates_removed=rows_read - rows_after_dedup,
    key_cols=KEY_COLS,
)

# COMMAND ----------
# MAGIC %md #### 5. Cast colonne decimali

# COMMAND ----------

if DECIMAL_COLS:
    deduped_df = cast_decimal(deduped_df, DECIMAL_COLS, precision=18, scale=4)
    log.info("Cast decimali applicato", decimal_cols=DECIMAL_COLS)

# COMMAND ----------
# MAGIC %md #### 6. MERGE INTO Silver

# COMMAND ----------

dh = DeltaHelper(spark, catalog=TARGET_CATALOG, schema=TARGET_SCHEMA)
dh.merge_into(
    target_table=TARGET_TABLE,
    source_df=deduped_df,
    merge_keys=KEY_COLS,
)
log.info(
    "MERGE INTO Silver completato",
    target=f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}",
    merge_keys=KEY_COLS,
)

# COMMAND ----------
# MAGIC %md #### 7. Data Quality checks post-merge

# COMMAND ----------

silver_df = spark.read.format("delta").table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")

dq = DQHelper(
    spark=spark,
    df=silver_df,
    table_name=f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}",
    logger=log,
)

dq_report = dq.run_all([
    ("check_no_duplicates", {"key_cols": KEY_COLS}),
    ("check_row_count", {"expected_min": 1}),
])

if not dq_report["all_passed"]:
    log.error(
        "DQ checks falliti dopo Silver merge",
        failed_count=dq_report["failed_count"],
    )
    # Non blocca il job ma solleva warning visibile
    # Per blocco hard: raise RuntimeError("DQ checks falliti")

# COMMAND ----------
# MAGIC %md #### 8. Log finale

# COMMAND ----------

duration = time.time() - start_time
log.log_run_end(
    rows_read=rows_read,
    rows_written=rows_after_dedup,
    duration_seconds=duration,
)

dbutils.notebook.exit("SUCCESS")
