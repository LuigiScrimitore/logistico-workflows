# Databricks notebook source
# Area: Preparazione Spedizioni
# Layer: Bronze
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Ingestion 1:1 della sorgente RAW ARTDGENE (anagrafica articoli) verso Bronze Delta.
#              Ridisegno SCELTA B: si legge la VERA sorgente raw (cdt-estr-raw-landing/artdgene),
#              NON piu' lo staging wl1_artdgene. Nessuna derivata (ART_RADICE/VAR -> Silver).
#              MODE = FULL_OVERWRITE (anagrafica). Schema-on-read (StringType). Bronze 1:1 puro.
#              Riferimento: config landing simulator (systems.cdt_estr_raw.artdgene); §11 catalogo RAW.

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

# COMMAND ----------
# MAGIC %md #### 1. Widget standard

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("landing_base_path", "abfss://logistica@<storage>.dfs.core.windows.net", "Landing base path")
dbutils.widgets.text("file_format", "auto", "csv | parquet | auto")

env               = dbutils.widgets.get("env")
run_date          = dbutils.widgets.get("run_date")
landing_base_path = dbutils.widgets.get("landing_base_path").rstrip("/")
file_format       = dbutils.widgets.get("file_format").strip().lower()

# COMMAND ----------
# MAGIC %md #### 2. Parametri specifici del notebook

# COMMAND ----------

NOTEBOOK_NAME  = "bronze_artdgene"
SOURCE_SYSTEM  = "cdt_estr_raw"
TABLE_NAME     = "artdgene"
MODE           = "FULL_OVERWRITE"

SOURCE_COLS = []  # estrae tutto

TARGET_CATALOG = get_catalog("bronze", env)
TARGET_SCHEMA  = "logistica"
FULL_TARGET    = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

logger = get_logger(NOTEBOOK_NAME)
year, month, day = run_date.split("-")

# COMMAND ----------
# MAGIC %md #### 3. Costruzione path landing (cdt-estr-raw-landing/artdgene)

# COMMAND ----------

def landing_path():
    return f"{landing_base_path}/cdt-estr-raw-landing/{TABLE_NAME}/{year}/{month}/{day}/"

def read_one(path):
    fmt = detect_format(path, file_format, dbutils)
    if fmt == "parquet":
        df = spark.read.format("parquet").load(path)
    else:
        df = (spark.read.option("header", "true").option("inferSchema", "false")
              .option("sep", ";").option("encoding", "UTF-8").csv(f"{path}*.csv"))
    return df.withColumn("_source_file", F.col("_metadata.file_path"))

# COMMAND ----------
# MAGIC %md #### 4. Lettura (unitaria, dato grezzo)

# COMMAND ----------

logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | MODE={MODE}")

path = landing_path()
try:
    raw_df = read_one(path)
    raw_df.columns  # LL-021: risoluzione EAGER del path (lettura lazy). artdgene e' DISMESSA:
                    # spesso assente in landing -> skip pulito invece di far fallire il job.
    logger.info(f"Letto: {path}")
except Exception as _e:
    logger.warning(f"File non trovato/illeggibile in landing ({path}) — skip ({type(_e).__name__}).")
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------
# MAGIC %md #### 5. Metadati Bronze

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_bronze_load_date", F.lit(run_date).cast("date"))
    .withColumn("_bronze_insert_ts", F.current_timestamp())
)

rows_read = bronze_df.count()
logger.info(f"Righe lette: {rows_read}")
if rows_read == 0:
    logger.warning("Nessuna riga in landing per la run_date. Notebook terminato.")
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------
# MAGIC %md #### 6. Scrittura FULL_OVERWRITE (stato corrente)

# COMMAND ----------

(bronze_df.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(FULL_TARGET))
logger.info(f"FULL OVERWRITE {FULL_TARGET} ({rows_read} righe)")

logger.info(f"END {NOTEBOOK_NAME} | righe_processate={rows_read}")
