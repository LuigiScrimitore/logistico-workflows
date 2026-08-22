# Databricks notebook source
# Area: Dimensioni — Gold
# Versione: 3.0.0  Data: 2026-06-08
# Descrizione: LU_OPERATORE (lookup operatori). Sorgente: silver.logistica.dim_operatore.
# Colonne reali Silver: OPERATORE_COD, SITO_COD, TIPO_OPERATORE, DESCRIZIONE, FLG_ATTIVO (+ _silver_ts).
# Chiave composita: OPERATORE_COD + SITO_COD + TIPO_OPERATORE. Pattern: overwrite completo.

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

NOTEBOOK_NAME  = "gold_lu_operatore"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica.dim_operatore"
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.LU_OPERATORE"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    gold = (
        spark.table(SOURCE_TABLE)
        .select(
            F.col("OPERATORE_COD").cast("string").alias("OPERATORE_COD"),
            F.col("SITO_COD").cast("string").alias("SITO_COD"),
            F.col("TIPO_OPERATORE").cast("string").alias("TIPO_OPERATORE"),
            F.col("DESCRIZIONE").cast("string").alias("DESCRIZIONE"),
            F.col("FLG_ATTIVO").cast("string").alias("FLG_ATTIVO"),
        )
        .filter(F.col("OPERATORE_COD").isNotNull() & F.col("SITO_COD").isNotNull()
                & F.col("TIPO_OPERATORE").isNotNull())
        .withColumn("DWH_UPDATED_AT", F.current_timestamp())
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (gold.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe={gold.count()} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
