# Databricks notebook source
# Area: Trasporti (migrazione TO-BE)
# Layer: Silver (cleansing)
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Cleansing 1:1 di bronze.logistica.automezzi (sorgente RAW AUTOMEZZI).
#              SOLO pulizia: trim/cast/null. Anagrafica -> FULL_OVERWRITE.
#              NESSUNA business logic. Serve come lookup al join in silver_trasp_mtv_build.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_row_count
from utils import get_catalog

from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_automezzi_clean"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.automezzi"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.automezzi_clean"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    src = spark.table(SOURCE_TABLE)
    rows_read = src.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_read}")
    if rows_read == 0:
        logger.warning("Sorgente vuota. Notebook terminato.")
        dbutils.notebook.exit("NO_DATA")

    # Cleansing generico: trim su tutte le colonne string del Bronze (no _bronze_*).
    silver_df = src
    for f in src.schema.fields:
        if f.name.startswith("_"):
            continue
        if str(f.dataType) == "StringType()":
            silver_df = silver_df.withColumn(f.name, F.trim(F.col(f.name)))

    silver_df = (silver_df
                 .withColumn("_silver_ts", F.current_timestamp())
                 .withColumn("_silver_load_date", F.lit(run_date).cast("date")))

    rows_clean = silver_df.count()
    logger.info(f"Righe silver: {rows_clean}")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    (silver_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))
    logger.info(f"FULL OVERWRITE {TARGET_TABLE} ({rows_clean} righe)")

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
