# Databricks notebook source
# Area: Economia Operatori / STAT
# Layer: Bronze
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Ingestion giornaliera BUONI_ECO da ADLS Gen2 landing zone (CSV/Parquet).
#              MODE = DELTA_MERGE: MERGE su chiave naturale BUONO_COD.
#              NOTA: tabella fuori scope core — da rivalutare in fase di scoping.
#              Tutti i campi sorgente trattati come StringType (schema-on-read).
#              Riferimento: DOCS/Landing & Bronze - Revision Spec.md
#              Path convention pending OP-07 (struttura Foconi da confermare con Reply).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog, add_row_hash, detect_format, read_landing

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
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

NOTEBOOK_NAME  = "bronze_buoni_eco"
SOURCE_SYSTEM  = "stat"
TABLE_NAME     = "buoni_eco"
MODE           = "DELTA_MERGE"

MERGE_KEYS     = ["BUOE_COD_SITO", "BUOE_COD_BUONO"]

# Schema sorgente esplicito (colonne reali verificate da header CSV STAT — prefisso BUOE_).
SOURCE_COLS = [
    "BUOE_COD_SITO", "BUOE_COD_BUONO", "BUOE_TIPO_BUONO", "BUOE_DATA_ORA_INI",
    "BUOE_DATA_ORA_FINE", "BUOE_ORE_MINUTI", "BUOE_TIPO_ORARIO", "BUOE_FASCIA_ORARIA",
    "BUOE_COD_OPE", "BUOE_DES_OPE", "BUOE_COD_ATT", "BUOE_TIP_ATT",
    "BUOE_REPARTO_RIC", "BUOE_COSTO", "BUOE_NOTE", "BUOE_DATA_ESTRAZIONE_DWH",
    "BUOE_FLAG_APPR", "BUOE_UTE_APPR", "BUOE_FLAG_ANNULLATO", "BUOE_STATO",
    "BUOE_NOTE_STATO", "BUOE_FLAG_ESTRAZIONE_DWH", "BUOE_ID_MAGGR", "BUOE_COD_CENTRO"
]

TARGET_CATALOG = get_catalog("bronze", env)
TARGET_SCHEMA  = "logistica"
FULL_TARGET    = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

logger = get_logger(NOTEBOOK_NAME)
year, month, day = run_date.split("-")

# COMMAND ----------
# MAGIC %md #### 3. Costruzione path landing (<source>-landing)

# COMMAND ----------

def landing_path():
    return f"{landing_base_path}/{SOURCE_SYSTEM}-landing/{TABLE_NAME}/{year}/{month}/{day}/"

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
    logger.info(f"Letto: {path}")
except AnalysisException:
    logger.warning(f"File non trovato in landing per la run_date: {path} — terminato.")
    dbutils.notebook.exit("NO_DATA")

if SOURCE_COLS:
    raw_df = raw_df.select([c for c in SOURCE_COLS if c in raw_df.columns] + [c for c in ["_source_file"] if c in raw_df.columns])

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
# MAGIC %md #### 6. Scrittura DELTA_MERGE (chiave naturale, no partizione)

# COMMAND ----------

# Dedup sulla chiave naturale: garantisce sorgente univoca per il MERGE (evita
# "multiple source rows matched"). La finestra di estrazione puo' contenere duplicati.
bronze_df = bronze_df.dropDuplicates(MERGE_KEYS)

# PRUNING update (OP-30): firma contenuto riga -> propaga solo il delta reale a valle.
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
