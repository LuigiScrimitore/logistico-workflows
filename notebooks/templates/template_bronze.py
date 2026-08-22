# Databricks notebook source
# Area: <AREA>
# Layer: Bronze
# Versione: 3.0.0
# Data: 2026-06-08
# Descrizione: TEMPLATE canonico Bronze — Landing Zone Push Pattern.
#              Riferimento: DOCS/Landing & Bronze - Revision Spec.md
#              Path convention pending OP-07 (struttura Foconi da confermare con Reply).
#
# Tre modalita' di scrittura (impostare MODE):
#   - DELTA_MERGE     : transazionali/movimenti. MERGE su chiave naturale (no data in condizione, no partizione).
#   - FULL_OVERWRITE  : anagrafiche. Overwrite completo (stato corrente).
#   - SNAPSHOT        : giacenze. replaceWhere su _bronze_load_date, partizionato per data.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from datetime import date
from functools import reduce

# COMMAND ----------
# MAGIC %md #### 1. Widget standard

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("landing_base_path", "abfss://logistica@<storage>.dfs.core.windows.net", "Landing base path")
dbutils.widgets.text("file_format", "auto", "csv | parquet | auto")
dbutils.widgets.text("siti", "lgax,lgcx,lcax,lccx,lexx,locx,lonx,lscx,lslx", "Siti Logistix (solo multi-sito)")

env               = dbutils.widgets.get("env")
run_date          = dbutils.widgets.get("run_date")
landing_base_path = dbutils.widgets.get("landing_base_path").rstrip("/")
file_format       = dbutils.widgets.get("file_format").strip().lower()
siti              = [s.strip() for s in dbutils.widgets.get("siti").split(",") if s.strip()]

# COMMAND ----------
# MAGIC %md #### 2. Parametri specifici del notebook (da personalizzare per tabella)

# COMMAND ----------

NOTEBOOK_NAME = "template_bronze"
SOURCE_SYSTEM = "logistix"          # logistix | cnd | stat
TABLE_NAME    = "<table>"           # nome cartella landing e tabella Delta target
MODE          = "DELTA_MERGE"       # DELTA_MERGE | FULL_OVERWRITE | SNAPSHOT
IS_MULTISITE  = SOURCE_SYSTEM == "logistix"

MERGE_KEYS    = []                  # solo per DELTA_MERGE (chiave naturale)
# SNAPSHOT non usa MERGE_KEYS (replaceWhere su data)

# Schema sorgente esplicito (colonne reali verificate — NON modificare/inventare)
SOURCE_COLS = [
    # "COL1", "COL2", ...
]

TARGET_CATALOG = get_catalog("bronze", env)
TARGET_SCHEMA  = "logistica"
FULL_TARGET    = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

logger = get_logger(NOTEBOOK_NAME)
year, month, day = run_date.split("-")

# COMMAND ----------
# MAGIC %md #### 3. Costruzione path landing (<source>-landing)

# COMMAND ----------

def landing_paths():
    """Path per la run. Logistix: un path per sito. CND/STAT: path unico."""
    if IS_MULTISITE:
        return [f"{landing_base_path}/logistix-landing/{s}/{TABLE_NAME}/{year}/{month}/{day}/" for s in siti]
    return [f"{landing_base_path}/{SOURCE_SYSTEM}-landing/{TABLE_NAME}/{year}/{month}/{day}/"]

def detect_format(path):
    if file_format != "auto":
        return file_format
    try:
        for f in dbutils.fs.ls(path):
            if f.name.endswith(".parquet"):
                return "parquet"
            if f.name.endswith(".csv"):
                return "csv"
    except Exception:
        pass
    return "csv"

def read_one(path):
    fmt = detect_format(path)
    if fmt == "parquet":
        return spark.read.format("parquet").load(path)
    return (spark.read.option("header", "true").option("inferSchema", "false")
            .option("sep", ";").option("encoding", "UTF-8").csv(f"{path}*.csv"))

# COMMAND ----------
# MAGIC %md #### 4. Lettura (unitaria, dato grezzo)

# COMMAND ----------

logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | MODE={MODE}")

frames = []
for p in landing_paths():
    try:
        frames.append(read_one(p))
        logger.info(f"Letto: {p}")
    except AnalysisException:
        logger.warning(f"File non trovato: {p} — skip")

if not frames:
    logger.info("Nessun file in landing per la run_date. Notebook terminato.")
    dbutils.notebook.exit("NO_DATA")

raw_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
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
if IS_MULTISITE:
    bronze_df = bronze_df.withColumn(
        "_sito_cod", F.regexp_extract(F.input_file_name(), r"/logistix-landing/([^/]+)/", 1)
    )

rows_read = bronze_df.count()
logger.info(f"Righe lette: {rows_read}")

# COMMAND ----------
# MAGIC %md #### 6. Scrittura secondo MODE

# COMMAND ----------

if MODE == "DELTA_MERGE":
    if not spark.catalog.tableExists(FULL_TARGET):
        bronze_df.write.format("delta").option("mergeSchema", "true").saveAsTable(FULL_TARGET)
        logger.info(f"Creazione iniziale {FULL_TARGET} ({rows_read} righe)")
    else:
        cond = " AND ".join(f"tgt.{k} = src.{k}" for k in MERGE_KEYS)
        update_set = {c: f"src.{c}" for c in bronze_df.columns
                      if c not in MERGE_KEYS and c != "_bronze_insert_ts"}
        (DeltaTable.forName(spark, FULL_TARGET).alias("tgt")
         .merge(bronze_df.alias("src"), cond)
         .whenMatchedUpdate(set=update_set)
         .whenNotMatchedInsertAll()
         .execute())
        logger.info(f"MERGE INTO {FULL_TARGET} completato")

elif MODE == "FULL_OVERWRITE":
    (bronze_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(FULL_TARGET))
    logger.info(f"FULL OVERWRITE {FULL_TARGET} ({rows_read} righe)")

elif MODE == "SNAPSHOT":
    (bronze_df.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"_bronze_load_date = '{run_date}'")
     .partitionBy("_bronze_load_date").option("overwriteSchema", "true")
     .saveAsTable(FULL_TARGET))
    logger.info(f"SNAPSHOT replaceWhere _bronze_load_date={run_date} ({rows_read} righe)")

else:
    raise ValueError(f"MODE non valido: {MODE}")

logger.info(f"END {NOTEBOOK_NAME} | righe_processate={rows_read}")
