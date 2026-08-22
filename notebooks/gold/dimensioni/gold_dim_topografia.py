# Databricks notebook source
# Area: Dimensioni — Gold
# Versione: 3.0.0  Data: 2026-06-08
# Descrizione: LU_TOPOGRAFIA (lookup celle/ubicazioni). Sorgente: silver.logistica.dim_topografia.
# Colonne reali Silver: CELLA_COD, SITO_COD, MAG_COD, CORSIA, COLONNA, PIANO,
#                       COD_CLAS_POSPA, COD_ZONA_MAG, COD_SETTOR_MAG, STATO_POSPA.
# Chiave: CELLA_COD. Pattern: overwrite completo.

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

NOTEBOOK_NAME  = "gold_lu_topografia"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica.dim_topografia"
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.LU_TOPOGRAFIA"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    gold = (
        spark.table(SOURCE_TABLE)
        .select(
            F.col("CELLA_COD").cast("string").alias("CELLA_COD"),
            F.col("SITO_COD").cast("string").alias("SITO_COD"),
            F.col("MAG_COD").cast("string").alias("MAG_COD"),
            F.col("CORSIA").cast("string").alias("CORSIA"),
            F.col("COLONNA").cast("string").alias("COLONNA"),
            F.col("PIANO").cast("string").alias("PIANO"),
            F.col("COD_CLAS_POSPA").cast("string").alias("COD_CLAS_POSPA"),
            F.col("COD_ZONA_MAG").cast("string").alias("COD_ZONA_MAG"),
            F.col("COD_SETTOR_MAG").cast("string").alias("COD_SETTOR_MAG"),
            F.col("STATO_POSPA").cast("string").alias("STATO_POSPA"),
        )
        .filter(F.col("CELLA_COD").isNotNull())
        .dropDuplicates(["CELLA_COD"])
        .withColumn("DWH_UPDATED_AT", F.current_timestamp())
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (gold.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe={gold.count()} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
