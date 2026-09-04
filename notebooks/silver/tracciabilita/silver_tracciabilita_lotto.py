# Databricks notebook source
# Area: Tracciabilità
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Tracciabilità Lotto Silver — lettura da bronze.logistica.tracciace178 (CE178_).
#              Allineato alle COLONNE REALI CE178_ del Bronze (bronze_traccia_ce178.py).
#              Aggregazione per ART (=CE178_COD_MSI) + CARICO + DATA + SITO:
#              NUM_ETICHETTE, NUM_ANNULLATE, NUM_TRASFERITE_STAT.
#              Pattern write: replaceWhere su DATA_CARICO = run_date.
#
# NOTA REVISIONE 3.0.0: nel Bronze reale NON esistono CE178_COD_ARTICOLO, CE178_QTA,
#   CE178_FLAG_STORNATA. L'identificativo articolo/missione è CE178_COD_MSI (mappato su
#   ART_COD_MSI). Non c'è una quantità: rimossa QTA_LOTTO (non aggregabile). Il conteggio
#   storni è approssimato da CE178_ANNULLATO ('S'). Aggiunto NUM_TRASFERITE_STAT da
#   CE178_TRASFERITO_STAT. Chiave di grana: SITO + CARICO + DATA + COD_MSI.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, julian_to_date, normalize_sito, get_sito_alias_map

from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_tracciabilita_lotto"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.tracciace178"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.tracciabilita_lotto"

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    raw_df = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_bronze_load_date") == run_date)
    )
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} per run_date={run_date}: {rows_read}")

    check_not_null(raw_df, ["MAG_SITO_COD", "CE178_COD_MSI", "CE178_NRO_CARICO", "CE178_DATA_CARICO"], NOTEBOOK_NAME)

    # Cast e mapping prima dell'aggregazione (solo colonne reali CE178_)
    mapped_df = (
        raw_df
        .withColumn("SITO_COD",      normalize_sito(F.col("MAG_SITO_COD"), _amap))
        .withColumn("CARICO_NRO",    F.col("CE178_NRO_CARICO").cast("string"))
        # Identificativo articolo/missione reale: CE178_COD_MSI (non esiste CE178_COD_ARTICOLO)
        .withColumn("ART_COD_MSI",   F.col("CE178_COD_MSI").cast("string"))
        .withColumn("DATA_CARICO",   julian_to_date(F.col("CE178_DATA_CARICO")))
        # Storno approssimato da CE178_ANNULLATO ('S' = annullata)
        .withColumn(
            "FLG_ANNULLATA_INT",
            F.when(F.col("CE178_ANNULLATO") == F.lit("S"), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "FLG_TRASFERITA_INT",
            F.when(F.col("CE178_TRASFERITO_STAT") == F.lit("S"), F.lit(1)).otherwise(F.lit(0))
        )
    )

    # Aggregazione per lotto (SITO_COD + ART_COD_MSI + CARICO_NRO + DATA_CARICO)
    silver_df = (
        mapped_df
        .groupBy("SITO_COD", "ART_COD_MSI", "CARICO_NRO", "DATA_CARICO")
        .agg(
            F.count("*").cast("int").alias("NUM_ETICHETTE"),
            F.countDistinct("CE178_SSCC").cast("int").alias("NUM_SSCC"),
            F.sum("FLG_ANNULLATA_INT").cast("int").alias("NUM_ANNULLATE"),
            F.sum("FLG_TRASFERITA_INT").cast("int").alias("NUM_TRASFERITE_STAT"),
        )
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_clean = silver_df.count()
    logger.info(f"Lotti aggregati: {rows_clean}")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    # I dati hanno DATA_CARICO nella finestra lookback (7gg), non solo run_date.
    # Prima esecuzione: CTAS. Successive: MERGE su chiave lotto.
    MERGE_KEYS = ["SITO_COD", "ART_COD_MSI", "CARICO_NRO", "DATA_CARICO"]
    if not spark.catalog.tableExists(TARGET_TABLE):
        (silver_df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))
        logger.info(f"CTAS {TARGET_TABLE} ({rows_clean} righe)")
    else:
        from delta.tables import DeltaTable
        cond = " AND ".join([f"tgt.{k} = src.{k}" for k in MERGE_KEYS])
        DeltaTable.forName(spark, TARGET_TABLE).alias("tgt").merge(
            silver_df.alias("src"), cond
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info(f"MERGE INTO {TARGET_TABLE} completato")

    logger.info(f"END {NOTEBOOK_NAME} | run_date={run_date} | righe_lette={rows_read} | lotti={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
