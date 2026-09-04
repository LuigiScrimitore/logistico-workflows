# Databricks notebook source
# Area: Giacenze — Gold (Fact)
# Versione: 3.0.0  Data: 2026-06-08
# Tabella: F_GIACENZE_DAILY
# Grain: (DATA_FOTO, ART_COD_INTERNO, MAG_COD).
#
# Sorgente Silver (colonne reali OP-27): silver.logistica.giacenza_daily
#   DATA_FOTO (da _bronze_load_date), ART_COD_INTERNO, ART_RADICE, ART_VAR, MAG_COD,
#   QTA_PEZZI, QTA_UF, QTA_IN_SCADENZA, QTA_PZ_ORD_CLIENTE, QTA_PZ_PREP_CLIENTE,
#   PREZZO_MEDIO_PONDERATO, DATA_MIN_SCADENZA, ULT_PREZZO_*, EAN, DATA_ULT_STOCK.
#
# Nota: le giacenze sono per MAG_COD (non per SITO). NIENTE QTA_GIACENZA/DISPONIBILE inventate.
# NIENTE join dim_topografia/dim_articolo (master non risolvibile, OP-02).
# Pattern: replaceWhere su DATA_FOTO, partizione DATA_FOTO.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, DateType
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "gold_f_giacenze_daily"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
# RICABLATO (standard 2-notebook §1-bis): legge dal prep, non piu' da silver.logistica.giacenza_daily.
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica_curated.giacenze"
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.F_GIACENZE_DAILY"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    src = (spark.read.table(SOURCE_TABLE)
           .filter(F.col("DATA_FOTO") == F.lit(run_date).cast(DateType())))

    fact = src.select(
        F.col("DATA_FOTO").cast(DateType()).alias("DATA_FOTO"),
        F.col("ART_COD_INTERNO").cast(StringType()).alias("ART_COD_INTERNO"),
        F.col("ART_RADICE").cast(StringType()).alias("ART_RADICE"),
        F.col("ART_VAR").cast(StringType()).alias("ART_VAR"),
        F.col("MAG_COD").cast(StringType()).alias("MAG_COD"),
        # EAN non esposto da V_STOCK (sorgente migrata CDT_ESTR) -> null placeholder
        F.lit(None).cast(StringType()).alias("EAN"),
        F.col("QTA_PEZZI").cast(DoubleType()).alias("QTA_PEZZI"),
        F.col("QTA_UF").cast(DoubleType()).alias("QTA_UF"),
        # QTA_IN_SCADENZA / QTA_PZ_*_CLIENTE non disponibili in V_STOCK -> null
        F.lit(None).cast(DoubleType()).alias("QTA_IN_SCADENZA"),
        F.lit(None).cast(DoubleType()).alias("QTA_PZ_ORD_CLIENTE"),
        F.lit(None).cast(DoubleType()).alias("QTA_PZ_PREP_CLIENTE"),
        F.col("PREZZO_MEDIO_PONDERATO").cast(DoubleType()).alias("PREZZO_MEDIO_PONDERATO"),
        F.col("DATA_MIN_SCADENZA").cast(DateType()).alias("DATA_MIN_SCADENZA"),
        # DATA_ULT_STOCK non in V_STOCK -> null
        F.lit(None).cast(DateType()).alias("DATA_ULT_STOCK"),
        F.current_timestamp().alias("DWH_UPDATED_AT"),
    )

    rows = fact.count()
    logger.info(f"F_GIACENZE_DAILY righe per DATA_FOTO={run_date}: {rows}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (fact.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"DATA_FOTO = '{run_date}'")
        .option("mergeSchema", "true")
        .partitionBy("DATA_FOTO")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_scritte={rows} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
