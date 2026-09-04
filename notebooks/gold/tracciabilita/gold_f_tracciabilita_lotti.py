# Databricks notebook source
# Area: Tracciabilita — Gold (Fact)
# Versione: 3.0.0  Data: 2026-06-08
# Tabella: F_TRACCIABILITA_LOTTI
# Grain: (SITO_COD, CARICO_NRO, MSI_COD, DATA_CARICO).
#
# Sorgente Silver (colonne reali OP-27): silver.logistica.tracciabilita_lotto
#   SITO_COD, CARICO_NRO, MSI_COD, DATA_CARICO, NUM_ETICHETTE, NUM_SSCC, NUM_ANNULLATE,
#   NUM_TRASFERITE_STAT.
#
# NIENTE QTA_LOTTO (non esiste nel sorgente).
# TASSO_ANNULLAMENTO = NUM_ANNULLATE / NUM_ETICHETTE (null se 0).
# Pattern: replaceWhere su ANNO_MESE.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, DateType, IntegerType
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")
ANNO_MESE = run_date[:4] + run_date[5:7]

# COMMAND ----------

NOTEBOOK_NAME  = "gold_f_tracciabilita_lotti"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica.tracciabilita_lotto"
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.F_TRACCIABILITA_LOTTI"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | ANNO_MESE={ANNO_MESE}")

    src = (spark.read.table(SOURCE_TABLE)
           .filter(F.date_format(F.col("DATA_CARICO"), "yyyyMM") == F.lit(ANNO_MESE)))

    fact = (src
        .withColumn("TASSO_ANNULLAMENTO",
            F.when(F.col("NUM_ETICHETTE") > 0,
                   F.col("NUM_ANNULLATE").cast(DoubleType()) / F.col("NUM_ETICHETTE").cast(DoubleType()))
             .otherwise(F.lit(None).cast(DoubleType())))
        .withColumn("ANNO_MESE", F.date_format(F.col("DATA_CARICO"), "yyyyMM"))
        .select(
            F.col("SITO_COD").cast(StringType()).alias("SITO_COD"),
            F.col("CARICO_NRO").cast(StringType()).alias("CARICO_NRO"),
            F.col("ART_COD_MSI").cast(StringType()).alias("MSI_COD"),
            F.col("DATA_CARICO").cast(DateType()).alias("DATA_CARICO"),
            F.col("NUM_ETICHETTE").cast(IntegerType()).alias("NUM_ETICHETTE"),
            F.col("NUM_SSCC").cast(IntegerType()).alias("NUM_SSCC"),
            F.col("NUM_ANNULLATE").cast(IntegerType()).alias("NUM_ANNULLATE"),
            F.col("NUM_TRASFERITE_STAT").cast(IntegerType()).alias("NUM_TRASFERITE_STAT"),
            F.col("TASSO_ANNULLAMENTO").cast(DoubleType()).alias("TASSO_ANNULLAMENTO"),
            F.col("ANNO_MESE").cast(StringType()).alias("ANNO_MESE"),
            F.current_timestamp().alias("DWH_UPDATED_AT"),
        )
    )

    rows = fact.count()
    logger.info(f"F_TRACCIABILITA_LOTTI righe per ANNO_MESE={ANNO_MESE}: {rows}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (fact.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"ANNO_MESE = '{ANNO_MESE}'")
        .option("mergeSchema", "true")
        .partitionBy("ANNO_MESE")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_scritte={rows} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
