# Databricks notebook source
# Area: Dimensioni
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Dimensione Corriere/Vettore — da bronze.logistica.t_vettori (sistema CND).
#              Colonne reali verificate (NON VET_RAGIONE_SOC / VET_FLG_ATTIVO, inesistenti):
#                VET_CODICE->CORRIERE_COD, VET_DESCRIZIONE->RAGIONE_SOCIALE, VET_INDIRIZZO->INDIRIZZO,
#                VET_CITTA->CITTA, VET_PROVINCIA->PROVINCIA, VET_CAP->CAP, VET_STATO->FLG_ATTIVO.
#              Anagrafica FULL -> overwrite completo (stato corrente).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog

from pyspark.sql import functions as F
from pyspark.sql.window import Window
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

NOTEBOOK_NAME  = "silver_dim_corriere"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
# FIX (scelta B): la staging WL1_VETTORI_TRASPO e' dismessa. Sorgente autoritativa =
# vettori@TRACK (bronze.logistica.vettori_track), stesse colonne VET_* (WL1 ne era copia).
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.vettori_track"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.dim_corriere"
MERGE_KEY      = "CORRIERE_COD"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------
# MAGIC %md #### 3. Lettura, cleansing e dedup (colonne reali Bronze VET_*)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    # Anagrafica FULL: leggi tutto (nessun filtro su _bronze_load_date)
    raw_df = spark.table(SOURCE_TABLE)
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_read}")

    check_not_null(raw_df, ["VET_CODICE"], NOTEBOOK_NAME)

    # Dedup per VET_CODICE: ultima versione per _bronze_insert_ts
    w = Window.partitionBy("VET_CODICE").orderBy(F.col("_bronze_insert_ts").desc())

    silver_df = (
        raw_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        # Mapping colonne reali VET_* -> business (cast espliciti)
        .select(
            F.col("VET_CODICE").cast("string").alias("CORRIERE_COD"),
            F.col("VET_DESCRIZIONE").cast("string").alias("RAGIONE_SOCIALE"),
            F.col("VET_INDIRIZZO").cast("string").alias("INDIRIZZO"),
            F.col("VET_CITTA").cast("string").alias("CITTA"),
            F.col("VET_PROVINCIA").cast("string").alias("PROVINCIA"),
            F.col("VET_CAP").cast("string").alias("CAP"),
            F.col("VET_STATO").cast("string").alias("FLG_ATTIVO"),
        )
        .withColumn("_silver_ts", F.current_timestamp())
        .select(
            "CORRIERE_COD", "RAGIONE_SOCIALE", "INDIRIZZO", "CITTA",
            "PROVINCIA", "CAP", "FLG_ATTIVO", "_silver_ts",
        )
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
