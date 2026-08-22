# Databricks notebook source
# Area: Trasporti (migrazione TO-BE)
# Layer: Silver (cleansing)
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Cleansing 1:1 dell'anagrafica vettori. Decisione D (§11): esistono DUE bronze
#              distinti — vettori_track (@TRACK, autoritativo per V_VETTORI legacy) e
#              vettori_locale (schema CDT_ESTR). Questo notebook produce DUE tabelle clean
#              separate (no merge tra le due fonti = sarebbe business): la scelta della fonte
#              autoritativa avviene in silver_t_vettori.
#              SOLO pulizia: trim/cast/null. Anagrafiche -> FULL_OVERWRITE.

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

NOTEBOOK_NAME  = "silver_vettori_clean"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"

# (bronze_table, silver_table) per ciascuna fonte
# NB (2026-06-11): vettori_locale DISMESSO — CDT_ESTR.VETTORI era doppione del vettori@TRACK
# (stessa anagrafica, 96 righe) e vettori_locale_clean non aveva consumer a valle. Fonte
# autoritativa unica = vettori_track (-> silver_t_vettori, silver_dim_corriere).
SOURCES = [
    (f"{BRONZE_CATALOG}.{SCHEMA}.vettori_track",  f"{SILVER_CATALOG}.{SCHEMA}.vettori_track_clean"),
]

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

def cleanse_source(source_table, target_table):
    if not spark.catalog.tableExists(source_table):
        logger.warning(f"Sorgente {source_table} non esiste — skip.")
        return
    src = spark.table(source_table)
    rows_read = src.count()
    logger.info(f"Righe lette da {source_table}: {rows_read}")
    if rows_read == 0:
        logger.warning(f"{source_table} vuota — skip.")
        return

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
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)
    (silver_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(target_table))
    logger.info(f"FULL OVERWRITE {target_table} ({rows_clean} righe)")

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")
    for s, t in SOURCES:
        cleanse_source(s, t)
    logger.info(f"END {NOTEBOOK_NAME}")
except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
