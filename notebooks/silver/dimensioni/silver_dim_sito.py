# Databricks notebook source
# Area: Dimensioni
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Dimensione Sito logistico — codici sito distinti da bronze.logistica.struttura_mag
#              (MAG_SITO_COD). Nel sorgente non esiste un nome/descrizione sito ne' un flag attivo:
#              SITO_DESC e' lasciato null (placeholder, eventuale derivazione futura da tabgen
#              TGEN_NRO_TAB=7). Anagrafica logistica FULL -> overwrite completo (stato corrente).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, normalize_sito, get_sito_alias_map

from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------
# MAGIC %md #### 1. Widget standard

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------
# MAGIC %md #### 2. Parametri notebook

# COMMAND ----------

NOTEBOOK_NAME  = "silver_dim_sito"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.struttura_mag"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.dim_sito"
MERGE_KEY      = "SITO_COD"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------
# MAGIC %md #### 3. Lettura, cleansing e dedup (colonne reali Bronze)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    # Anagrafica logistica FULL: leggi tutto (nessun filtro su _bronze_load_date)
    raw_df = spark.table(SOURCE_TABLE)
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_read}")

    check_not_null(raw_df, ["MAG_SITO_COD"], NOTEBOOK_NAME)

    # Normalizzazione sito al canonico numerico 2 cifre (LGAX->20, ecc.)
    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")

    silver_df = (
        raw_df
        .select(normalize_sito(F.col("MAG_SITO_COD"), _amap).alias("SITO_COD"))
        .filter(F.col("SITO_COD").isNotNull())
        .distinct()
        .withColumn("SITO_DESC", F.lit(None).cast("string"))
        .withColumn("_silver_ts", F.current_timestamp())
        .select("SITO_COD", "SITO_DESC", "_silver_ts")
    )

    rows_clean = silver_df.count()
    logger.info(f"Righe dopo dedup: {rows_clean}")
    check_row_count(silver_df, min_rows=1, notebook_name=NOTEBOOK_NAME)

    # COMMAND ----------
    # MAGIC %md #### 4. Scrittura — overwrite completo (anagrafica FULL, stato corrente)

    # COMMAND ----------

    (silver_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
