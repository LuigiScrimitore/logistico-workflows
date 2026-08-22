# Databricks notebook source
# Area: Trasporti / Ordini — Fact ordini (testate carico)
# Layer: Silver PREP (Fase 1 modellazione + Fase 2 calcolo)  →  silver.logistica_curated.ordini
# Versione: 1.0.0
# Data: 2026-06-10
# Descrizione: STRATO PREP (standard 2-notebook, Linee guida §1-bis).
#              SORGENTE (silver.clean): silver.logistica.ordine (cleansing di sto_tes_carichi,
#              prodotto da silver_ordini.py: julian->date, normalize_sito, dedup, proxy stato).
#              FASE 1: nessun join (la testata ordine e' single-source).
#              FASE 2: derivazione chiavi-tempo ANNO_MESE + GIORNO_CARICO_ID (daykey).
#              Le surrogate key dimensionali NON si fanno qui: vivono nel Gold (Fase 3).
#              MODE: FULL_OVERWRITE (testate piccole; la finestra la gestisce il Gold).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, clean_dat_d

from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_prep_ordini"
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.ordine"             # silver.clean
TARGET_TABLE   = f"{SILVER_CATALOG}.logistica_curated.ordini"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    src = spark.read.table(SOURCE_TABLE)
    rows_read = src.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_read}")
    if rows_read == 0:
        dbutils.notebook.exit("NO_DATA")

    # ── FASE 2: chiavi-tempo derivate ──────────────────────────────────────────
    prep_df = (
        src.select(
            "SITO_COD", "ORDINE_NRO", "CARICO_NRO", "MAG_COD",
            "FORNITORE_COD", "CORRIERE_COD",
            "DATA_CARICO", "DATA_EMISS_ORDINE", "DATA_CONFERMA_ORDINE",
            "TIPO_ORDINE", "TIPO_CONSEGNA", "FLAG_TRASFERITO",
        )
        .withColumn("ANNO_MESE", F.date_format(F.col("DATA_CARICO"), "yyyyMM"))
        .withColumn("GIORNO_CARICO_ID", clean_dat_d(F.col("DATA_CARICO")))
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_silver_load_date", F.lit(run_date).cast("date"))
    )

    check_not_null(prep_df, ["SITO_COD", "CARICO_NRO", "MAG_COD"], NOTEBOOK_NAME)
    rows_out = prep_df.count()
    logger.info(f"Righe prep ordini: {rows_out}")
    check_row_count(prep_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.logistica_curated")
    (prep_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))
    logger.info(f"FULL OVERWRITE {TARGET_TABLE} ({rows_out} righe)")

    logger.info(f"END {NOTEBOOK_NAME} | righe={rows_out}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
