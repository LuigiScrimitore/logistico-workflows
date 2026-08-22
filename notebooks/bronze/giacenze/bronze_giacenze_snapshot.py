# Databricks notebook source
# Area: Giacenze / CND
# Layer: Bronze
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Ingestion giornaliera T_STOCK da ADLS Gen2 landing zone (CSV/Parquet).
#              MODE = SNAPSHOT: snapshot giornaliero storicizzato (replaceWhere su
#              _bronze_load_date, partizionato per data). Nessun MERGE.
#              Tutti i campi sorgente trattati come StringType (schema-on-read).
#              Riferimento: DOCS/Landing & Bronze - Revision Spec.md
#              Path convention pending OP-07 (struttura Foconi da confermare con Reply).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

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

NOTEBOOK_NAME  = "bronze_giacenze_snapshot"
SOURCE_SYSTEM  = "cnd"
TABLE_NAME     = "t_stock"
MODE           = "SNAPSHOT"

# SNAPSHOT non usa MERGE_KEYS (replaceWhere sulla data)

# Schema sorgente esplicito (colonne reali verificate — NON modificare/inventare)
SOURCE_COLS = [
    "STKNMAG", "STKCINT", "ART_RADICE_COD", "ART_VAR_LOGIS_COD", "STKEAN",
    "STKQTAPZ", "STKQTAUF", "STKPMP", "STKULTSTOCK", "STKDATAMINSCAD",
    "STKQTAINSCAD", "STKDCRE", "STKDMAJ", "STKUTIL", "STKULTPRZCOM",
    "STKULTPRZFAT", "STKQTAPZPREPCLIE", "STKQTAPZORDCLIE", "STKULTPRZNET"
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
        return spark.read.format("parquet").load(path)
    return (spark.read.option("header", "true").option("inferSchema", "false")
            .option("sep", ";").option("encoding", "UTF-8").csv(f"{path}*.csv"))

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
    raw_df = raw_df.select([c for c in SOURCE_COLS if c in raw_df.columns])

# COMMAND ----------
# MAGIC %md #### 5. Metadati Bronze

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_bronze_load_date", F.lit(run_date).cast("date"))
    .withColumn("_bronze_insert_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

rows_read = bronze_df.count()
logger.info(f"Righe lette: {rows_read}")
if rows_read == 0:
    logger.warning("Nessuna riga in landing per la run_date. Notebook terminato.")
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------
# MAGIC %md #### 6. Scrittura SNAPSHOT (replaceWhere su _bronze_load_date)

# COMMAND ----------

(bronze_df.write.format("delta").mode("overwrite")
 .option("replaceWhere", f"_bronze_load_date = '{run_date}'")
 .partitionBy("_bronze_load_date").option("overwriteSchema", "true")
 .saveAsTable(FULL_TARGET))
logger.info(f"SNAPSHOT replaceWhere _bronze_load_date={run_date} ({rows_read} righe)")

logger.info(f"END {NOTEBOOK_NAME} | righe_processate={rows_read}")
