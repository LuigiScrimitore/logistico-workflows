# Databricks notebook source
# Area: Trasporti
# Layer: Bronze
# Versione: 2.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-14
# Descrizione: Ingestion incrementale WL1.SWAP da landing zone (CSV).
#              Sorgente: logistix-landing/swap/{yyyy}/{mm}/{dd}/
#              L'estrattore (logistix_wl1) filtra per DATA_SWAP sulla finestra delta.
#              Bronze: DELTA_MERGE su chiave (ORDINE_ID_ORIG, ORDINE_ID_SOST, DATA_SWAP)
#              con row_hash pruning.

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, add_row_hash, detect_format, read_landing

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from datetime import date

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

NOTEBOOK_NAME  = "bronze_swap"
SOURCE_SYSTEM  = "logistix"
TABLE_NAME     = "swap"
MODE           = "DELTA_MERGE"
MERGE_KEYS     = ["ORDINE_ID_ORIG", "ORDINE_ID_SOST", "DATA_SWAP"]

SOURCE_COLS = [
    "ORDINE_ID_ORIG", "ORDINE_ID_SOST",
    "DATA_SWAP", "MOTIVO", "OPERATORE_ID"
]

TARGET_CATALOG = get_catalog("bronze", env)
TARGET_SCHEMA  = "logistica"
FULL_TARGET    = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

logger = get_logger(NOTEBOOK_NAME)
year, month, day = run_date.split("-")

# COMMAND ----------

def landing_path():
    return f"{landing_base_path}/logistix-landing/{TABLE_NAME}/{year}/{month}/{day}/"

def read_one(path):
    fmt = detect_format(path, file_format, dbutils)
    if fmt == "parquet":
        df = spark.read.format("parquet").load(path)
    else:
        df = (spark.read.option("header", "true").option("inferSchema", "false")
              .option("sep", ";").option("encoding", "UTF-8").csv(f"{path}*.csv"))
    return df.withColumn("_source_file", F.col("_metadata.file_path"))

# COMMAND ----------

logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | MODE={MODE}")

path = landing_path()
try:
    raw_df = read_one(path)
except AnalysisException:
    logger.warning(f"File non trovato in landing: {path} — terminato.")
    dbutils.notebook.exit("NO_DATA")

if SOURCE_COLS:
    raw_df = raw_df.select([c for c in SOURCE_COLS if c in raw_df.columns] + [c for c in ["_source_file"] if c in raw_df.columns])

bronze_df = (
    raw_df
    .withColumn("_bronze_load_date", F.lit(run_date).cast("date"))
    .withColumn("_bronze_insert_ts", F.current_timestamp())
)

rows_read = bronze_df.count()
logger.info(f"Righe lette da landing: {rows_read}")

if rows_read == 0:
    logger.warning("Nessuna riga in landing. Notebook terminato.")
    dbutils.notebook.exit("NO_DATA")

check_not_null(bronze_df, ["ORDINE_ID_ORIG", "ORDINE_ID_SOST", "DATA_SWAP"], NOTEBOOK_NAME)
check_row_count(bronze_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

# COMMAND ----------

bronze_df = add_row_hash(bronze_df)

if not spark.catalog.tableExists(FULL_TARGET):
    bronze_df.write.format("delta").option("mergeSchema", "true").saveAsTable(FULL_TARGET)
    logger.info(f"Creazione iniziale {FULL_TARGET} ({rows_read} righe)")
else:
    cond = " AND ".join(f"tgt.{k} <=> src.{k}" for k in MERGE_KEYS)
    update_set = {c: f"src.{c}" for c in bronze_df.columns
                  if c not in MERGE_KEYS and c != "_bronze_insert_ts"}
    (DeltaTable.forName(spark, FULL_TARGET).alias("tgt")
     .merge(bronze_df.alias("src"), cond)
     .whenMatchedUpdate(condition="tgt._row_hash <> src._row_hash", set=update_set)
     .whenNotMatchedInsertAll()
     .execute())
    logger.info(f"MERGE INTO {FULL_TARGET} completato ({rows_read} righe sorgente)")

logger.info(f"END {NOTEBOOK_NAME} | righe_processate={rows_read}")
