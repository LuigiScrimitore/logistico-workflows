# Databricks notebook source
# Area: Giacenze
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Giacenze Aggregate Silver — legge silver.logistica.giacenza_daily per la data corrente
#              (NON dal Bronze), aggrega per (MAG_COD, DATA_FOTO) sulle colonne reali presenti nella
#              giacenza_daily, replaceWhere idempotente su DATA_FOTO in silver.logistica.giacenza_aggregata.
#              NOTA: la giacenza_daily espone QTA_PEZZI/QTA_UF (colonne reali da t_stock); non esiste
#              QTA_GIACENZA/QTA_DISPONIBILE → aggrego su QTA_PEZZI e QTA_UF.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog

from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_giacenze_aggregata"
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
# RICABLATO (standard 2-notebook): legge dal prep giacenze, non piu' da giacenza_daily.
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica_curated.giacenze"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.giacenza_aggregata"
PARTITION_COL  = "DATA_FOTO"

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    # ── Leggi la partizione del giorno dalla Silver giacenza_daily ────────────
    daily_df = (
        spark.table(SOURCE_TABLE)
        .filter(F.col(PARTITION_COL) == F.lit(run_date).cast("date"))
    )
    rows_read = daily_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} per DATA_FOTO={run_date}: {rows_read}")

    check_not_null(daily_df, ["DATA_FOTO", "MAG_COD", "ART_COD_INTERNO"], NOTEBOOK_NAME)
    check_row_count(daily_df, min_rows=1, notebook_name=NOTEBOOK_NAME)

    # ── Aggregazione per (MAG_COD, DATA_FOTO) su colonne reali ───────────────
    agg_df = (
        daily_df
        .groupBy("MAG_COD", "DATA_FOTO")
        .agg(
            F.sum("QTA_PEZZI").cast("decimal(16,3)").alias("QTA_PEZZI_TOT"),
            F.sum("QTA_UF").cast("decimal(16,3)").alias("QTA_UF_TOT"),
            # QTA_IN_SCADENZA non disponibile nella V_STOCK migrata (era solo in CND.T_STOCK).
            # Placeholder 0 fino a quando il dato sara' arricchito da lookup cndstostock.
            F.lit(0).cast("decimal(16,3)").alias("QTA_IN_SCADENZA_TOT"),
            F.countDistinct("ART_COD_INTERNO").alias("NUM_ARTICOLI")
        )
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_agg = agg_df.count()
    logger.info(f"Righe aggregate: {rows_agg}")

    # Scrittura idempotente: dynamic partition overwrite per DATA_FOTO (no raddoppio su re-run).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (agg_df.write.format("delta").mode("overwrite")
     .option("mergeSchema", "true").partitionBy(PARTITION_COL)
     .saveAsTable(TARGET_TABLE))
    logger.info(f"SNAPSHOT (dyn overwrite) {TARGET_TABLE} ({rows_agg} righe per DATA_FOTO={run_date})")
    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_aggregate={rows_agg}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
