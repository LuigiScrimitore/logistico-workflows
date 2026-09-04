# Databricks notebook source
# Area: Giacenze / Stock (migrazione TO-BE)
# Layer: Bronze
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Ingestion 1:1 della sorgente RAW logistix CATENA_ESTERNI verso Bronze Delta.
#              Sorgente: CATENA_ESTERNI @LOG (multi-sito via db-link) -> landing logistix-landing/{sito}/catena_esterni.
#              MODE = SNAPSHOT (catasta giacenze esterne, fotografia giornaliera).
#              Schema-on-read (StringType), SELECT colonne sorgente + metadati _bronze_*.
#              Bronze 1:1 PURO: NIENTE derivate (no mag_sito_cod, no ART_RADICE/VAR), NIENTE
#              rimappatura CAES_*->CATE_* (anch'essa cleansing -> Silver). Multi-sito (union siti).
#              Riferimento: Revisione AS-IS to-be - Migrazione CDT_ESTR.md §11 (riga WL1_CATENA_ESTERNI).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog, detect_format, read_landing

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from datetime import date
from functools import reduce

# COMMAND ----------
# MAGIC %md #### 1. Widget standard

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("landing_base_path", "abfss://logistica@<storage>.dfs.core.windows.net", "Landing base path")
dbutils.widgets.text("file_format", "auto", "csv | parquet | auto")
dbutils.widgets.text("siti", "lgax,lgcx,lcax,lccx,lexx,locx,lonx,lscx,lslx", "Siti Logistix")

env               = dbutils.widgets.get("env")
run_date          = dbutils.widgets.get("run_date")
landing_base_path = dbutils.widgets.get("landing_base_path").rstrip("/")
file_format       = dbutils.widgets.get("file_format").strip().lower()
siti              = [s.strip() for s in dbutils.widgets.get("siti").split(",") if s.strip()]

# COMMAND ----------
# MAGIC %md #### 2. Parametri specifici del notebook

# COMMAND ----------

NOTEBOOK_NAME  = "bronze_catena_esterni"
SOURCE_SYSTEM  = "logistix"
TABLE_NAME     = "catena_esterni"
MODE           = "SNAPSHOT"
IS_MULTISITE   = True

SOURCE_COLS = []  # estrae tutto (schema-on-read)

TARGET_CATALOG = get_catalog("bronze", env)
TARGET_SCHEMA  = "logistica"
FULL_TARGET    = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

logger = get_logger(NOTEBOOK_NAME)
year, month, day = run_date.split("-")

# COMMAND ----------
# MAGIC %md #### 3. Costruzione path landing (logistix-landing/{sito}/catena_esterni)

# COMMAND ----------

def landing_paths():
    return [f"{landing_base_path}/logistix-landing/{s}/{TABLE_NAME}/{year}/{month}/{day}/" for s in siti]

def read_one(path):
    fmt = detect_format(path, file_format, dbutils)
    if fmt == "parquet":
        df = spark.read.format("parquet").load(path)
    else:
        df = (spark.read.option("header", "true").option("inferSchema", "false")
              .option("sep", ";").option("encoding", "UTF-8").csv(f"{path}*.csv"))
    return df.withColumn("_source_file", F.col("_metadata.file_path"))

# COMMAND ----------
# MAGIC %md #### 4. Lettura (unitaria, dato grezzo) — union dei siti

# COMMAND ----------

logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | MODE={MODE}")

frames = []
for p in landing_paths():
    try:
        df = read_one(p)
        df.columns  # forza la risoluzione del path (se manca -> eccezione -> skip)
        frames.append(df)
        logger.info(f"Letto: {p}")
    except Exception as _e:
        logger.warning(f"Path non trovato/illeggibile: {p} — skip ({type(_e).__name__})")

if not frames:
    logger.info("Nessun file in landing per la run_date. Notebook terminato.")
    dbutils.notebook.exit("NO_DATA")

raw_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
if SOURCE_COLS:
    raw_df = raw_df.select([c for c in SOURCE_COLS if c in raw_df.columns] + [c for c in ["_source_file"] if c in raw_df.columns])

# COMMAND ----------
# MAGIC %md #### 5. Metadati Bronze (incl. _sito_estrazione dal db-link)

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_bronze_load_date", F.lit(run_date).cast("date"))
    .withColumn("_bronze_insert_ts", F.current_timestamp())
    .withColumn("_sito_estrazione", F.regexp_extract(F.col("_source_file"), r"/logistix-landing/([^/]+)/", 1))
)

rows_read = bronze_df.count()
logger.info(f"Righe lette: {rows_read}")
if rows_read == 0:
    logger.warning("Nessuna riga in landing per la run_date. Notebook terminato.")
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------
# MAGIC %md #### 6. Scrittura SNAPSHOT (append partizionato per data)

# COMMAND ----------

# SNAPSHOT idempotente: dynamic partition overwrite (no raddoppio su re-run stesso giorno).
(bronze_df.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic")
 .partitionBy("_bronze_load_date").option("mergeSchema", "true")
 .saveAsTable(FULL_TARGET))
logger.info(f"SNAPSHOT (dyn overwrite) {FULL_TARGET} ({rows_read} righe per run_date={run_date})")

logger.info(f"END {NOTEBOOK_NAME} | righe_processate={rows_read}")
