# Databricks notebook source
# Area: Dimensioni — Gold
# Versione: 3.0.0  Data: 2026-06-08
# Descrizione: LU_SITO (lookup sito magazzino). Sorgente: silver.logistica.dim_sito
#              (SITO_COD numerico + SITO_DESC reale da S_LOGISTIX, ACT_9026).
# Pattern: overwrite completo (dimensione full dal Silver).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

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
    # Silver dim_sito (ACT_9026): SITO_COD numerico (chiave) + attributi alfabetici + descrizione.
    # Il gold espone ENTRAMBI gli alfabetici come attributi di display, chiave di join numerica:
    #   SITO_COD      = 20      (numerico, chiave — join integrity)
    #   SITO_COD_ALFA = LGAX    (dblink 4 char, tecnico Logistix/catena)
    #   SITO_COD_MAG  = 0020A   (5 char, codice utente finale CDT_DW/S_LOGISTIX)
    #   SITO_DESC     = MONTOPOLI - GENERI VARI
    # Fallback null se una colonna non e' presente nel Silver (compat versioni precedenti).
    def _col(name):
        return (F.col(name) if name in src.columns else F.lit(None)).cast("string").alias(name)
    gold = (
        src
        .select(
            F.col("SITO_COD").cast("string").alias("SITO_COD"),
            _col("SITO_COD_ALFA"),
            _col("SITO_COD_MAG"),
            _col("SITO_DESC"),
        )
        .filter(F.col("SITO_COD").isNotNull())
        .dropDuplicates(["SITO_COD"])
        .withColumn("DWH_UPDATED_AT", F.current_timestamp())
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (gold.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe={gold.count()} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
