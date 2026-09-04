# Databricks notebook source
# Area: Aggregati DataMart — Gold
# Versione: 3.0.0  Data: 2026-06-08
# Tabella: A_STOCK_MENSILE (schema gold_prod.logistica_dm)
# Sorgente: gold_prod.logistica_dm.A_GIACENZE_MONTHLY — passthrough con DWH_UPDATED_AT aggiornato
#           (a_stock = vista business orientata; eventuali misure aggiuntive future).

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
ANNO_MESE = run_date[:4] + run_date[5:7]

# COMMAND ----------

NOTEBOOK_NAME = "gold_a_stock_mensile"
GOLD_CATALOG  = get_catalog("gold", env)
SOURCE_TABLE  = f"{GOLD_CATALOG}.logistica_dm.A_GIACENZE_MONTHLY"
TARGET_TABLE  = f"{GOLD_CATALOG}.logistica_dm.A_STOCK_MENSILE"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | ANNO_MESE={ANNO_MESE}")

    df = spark.read.table(SOURCE_TABLE).filter(F.col("ANNO_MESE") == F.lit(ANNO_MESE))

    out = (df
        .drop("DWH_UPDATED_AT")
        .withColumn("DWH_UPDATED_AT", F.current_timestamp())
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica_dm")
    (out.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"ANNO_MESE = '{ANNO_MESE}'")
        .option("mergeSchema", "true")
        .partitionBy("ANNO_MESE")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe={out.count()} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
