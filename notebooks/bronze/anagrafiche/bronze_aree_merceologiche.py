# Databricks notebook source
# Area: Anagrafiche
# Layer: Bronze
# Versione: 3.0.0
# Data: 2026-06-08
# Descrizione: Ingestion file landing zone -> Delta Bronze per AREE_MERCEOLOGICHE (logistix multi-sito).
#              MODE=FULL_OVERWRITE: l'anagrafica e' FULL (AS-IS truncate+insert). Overwrite completo della tabella
#              ogni giorno (stato corrente). NIENTE MERGE, NIENTE partizione _bronze_load_date (correzione OP-08).
#              L'overwrite scrive l'unione dei full di tutti i siti del giorno.
#              OP-10: filtro ARM_TIPO_AREA=1 applicato lato sorgente (estrazione), da confermare con Foconi/Reply.
#                     Il Bronze legge il dato grezzo cosi' come consegnato in landing, nessun filtro applicato qui.
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
# MAGIC %md #### 2. Parametri notebook

# COMMAND ----------

NOTEBOOK_NAME  = "bronze_aree_merceologiche"
SOURCE_SYSTEM  = "logistix"
TABLE_NAME     = "aree_merceologiche"
MODE           = "FULL_OVERWRITE"
IS_MULTISITE   = True

SOURCE_COLS = [
    "MAG_SITO_COD", "ARM_COD_AREA_MERCEOLOGICA",
    "ARM_DES_AREA_MERCEOLOGICA", "ARM_TIPO_PREPARAZIONE",
]

TARGET_CATALOG = get_catalog("bronze", env)
TARGET_SCHEMA  = "logistica"
FULL_TARGET    = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

logger = get_logger(NOTEBOOK_NAME)
year, month, day = run_date.split("-")

# COMMAND ----------
# MAGIC %md #### 3. Costruzione path landing (logistix-landing/{sito})

# COMMAND ----------

def landing_paths():
    return [f"{landing_base_path}/logistix-landing/{s}/{TABLE_NAME}/{year}/{month}/{day}/" for s in siti]

def read_one(path):
    fmt = detect_format(path, file_format, dbutils)
    if fmt == "parquet":
        return spark.read.format("parquet").load(path)
    return (spark.read.option("header", "true").option("inferSchema", "false")
            .option("sep", ";").option("encoding", "UTF-8").csv(f"{path}*.csv"))

# COMMAND ----------
# MAGIC %md #### 4. Lettura (unitaria, dato grezzo) — union dei siti

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
raw_df = raw_df.select([c for c in SOURCE_COLS if c in raw_df.columns])

# COMMAND ----------
# MAGIC %md #### 5. Metadati Bronze

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_bronze_load_date", F.lit(run_date).cast("date"))
    .withColumn("_bronze_insert_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_sito_cod", F.regexp_extract(F.input_file_name(), r"/logistix-landing/([^/]+)/", 1))
)

rows_read = bronze_df.count()
logger.info(f"Righe lette: {rows_read}")

# COMMAND ----------
# MAGIC %md #### 6. Scrittura MODE=FULL_OVERWRITE (overwrite completo, no MERGE, no partizione)

# COMMAND ----------

(bronze_df.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(FULL_TARGET))
logger.info(f"FULL OVERWRITE {FULL_TARGET} ({rows_read} righe)")

logger.info(f"END {NOTEBOOK_NAME} | righe_processate={rows_read}")
