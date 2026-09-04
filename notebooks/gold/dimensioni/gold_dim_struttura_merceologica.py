# Databricks notebook source
# Area: Dimensioni — Gold
# Versione: 3.0.0  Data: 2026-06-08
# Descrizione: LU_AREA_MERCL_LOGIS (merceologica logistica). Sorgente: bronze.logistica.aree_merceologiche
# (la Silver e' confluita in dim_articolo deprecato OP-02).
# Colonne reali Bronze: MAG_SITO_COD, ARM_COD_AREA_MERCEOLOGICA, ARM_DES_AREA_MERCEOLOGICA, ARM_TIPO_PREPARAZIONE.
# Chiave: COD_AREA_MERC. Pattern: overwrite completo.
# Nota: LU_MACRO_AGG_MERCL (livello superiore) rinviata (OP-03/04).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "gold_lu_area_mercl_logis"
BRONZE_CATALOG = get_catalog("bronze", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{BRONZE_CATALOG}.logistica.aree_merceologiche"
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.LU_AREA_MERCL_LOGIS"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    src = spark.table(SOURCE_TABLE)
    # Dedup multi-sito (stessa area replicata su piu' siti) — tieni ultima per _bronze_insert_ts
    w = Window.partitionBy("ARM_COD_AREA_MERCEOLOGICA").orderBy(F.col("_bronze_insert_ts").desc())
    gold = (
        src
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("ARM_COD_AREA_MERCEOLOGICA").cast("string").alias("COD_AREA_MERC"),
            F.col("ARM_DES_AREA_MERCEOLOGICA").cast("string").alias("DES_AREA_MERC"),
            F.col("ARM_TIPO_PREPARAZIONE").cast("string").alias("TIPO_PREPARAZIONE"),
        )
        .filter(F.col("COD_AREA_MERC").isNotNull())
        .withColumn("DWH_UPDATED_AT", F.current_timestamp())
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (gold.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe={gold.count()} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
