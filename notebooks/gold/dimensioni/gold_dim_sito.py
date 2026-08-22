# Databricks notebook source
# Area: Dimensioni — Gold
# Versione: 3.0.0  Data: 2026-06-08
# Descrizione: LU_SITO (lookup sito magazzino). Sorgente: silver.logistica.dim_sito (colonne reali: SITO_COD).
# Pattern: overwrite completo (dimensione full dal Silver).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "gold_lu_sito"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica.dim_sito"
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.LU_SITO"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    src = spark.table(SOURCE_TABLE)
    # Colonne reali del Silver dim_sito: SITO_COD (+ _silver_ts).
    # SITO_DESC non esiste nel sorgente: portato null come placeholder (OP-aperto).
    gold = (
        src
        .select(F.col("SITO_COD").cast("string").alias("SITO_COD"))
        .filter(F.col("SITO_COD").isNotNull())
        .dropDuplicates(["SITO_COD"])
        .withColumn("SITO_DESC", F.lit(None).cast("string"))
        .withColumn("DWH_UPDATED_AT", F.current_timestamp())
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (gold.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe={gold.count()} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
