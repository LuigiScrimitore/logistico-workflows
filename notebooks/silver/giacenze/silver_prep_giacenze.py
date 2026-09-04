# Databricks notebook source
# Area: Giacenze
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Giacenze Daily Silver — sorgente bronze.logistica.t_stock (snapshot giornaliero, prefisso STK*).
#              Filtra il Bronze per _bronze_load_date = run_date (snapshot del giorno), rinomina le
#              colonne reali (verificate da SOURCE_COLS del Bronze), cast espliciti, deduplica su
#              chiave naturale (Window su _bronze_insert_ts DESC).
#              Pattern snapshot: NON usa MERGE — replaceWhere su DATA_FOTO / _bronze_load_date.
#              NOTA: il Bronze t_stock NON contiene STKDATAG/STKQGIAC/STKQPREN/STKQBLOC/STKQDISP:
#              colonne inventate rimosse. DATA_FOTO deriva da _bronze_load_date (data dello snapshot).
#              Le quantita' reali sono STKQTAPZ (pezzi) e STKQTAUF (unita' fisiche). L'articolo e' gia'
#              normalizzato in radice/variante alla sorgente (ART_RADICE_COD / ART_VAR_LOGIS_COD).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
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

NOTEBOOK_NAME  = "silver_prep_giacenze"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
# STRATO PREP (standard 2-notebook, Linee guida §1-bis): produce il dataset giacenze daily
# consumato dal Gold. Sorgente = silver.logistica.t_stock (elaborazione intermedia: catena
# V_STOCK_PICKING + V_STOCK_SCORTE via catena_unificata + struttura_mag + cndstostock).
# Le colonne STK* non esistono piu'; usa nomi V_STOCK (ART_COD, PZ_STOCK, etc.).
SOURCE_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.t_stock"
TARGET_TABLE   = f"{SILVER_CATALOG}.logistica_curated.giacenze"
PARTITION_COL  = "DATA_FOTO"

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    # ── Leggi lo snapshot del giorno dalla Silver t_stock ────────────────────
    raw_df = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_silver_load_date") == F.lit(run_date))
    )
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} per _silver_load_date={run_date}: {rows_read}")

    check_not_null(raw_df, ["MAG_SITO_COD", "ART_COD"], NOTEBOOK_NAME)
    check_row_count(raw_df, min_rows=1, notebook_name=NOTEBOOK_NAME)

    # ── Rinomina colonne V_STOCK → nomi business ────────────────────────────
    #     DATA_FOTO = data dello snapshot (_silver_load_date)
    renamed_df = (
        raw_df
        .withColumn("DATA_FOTO", F.col("_silver_load_date"))
        .withColumnRenamed("MAG_SITO_COD",      "MAG_COD")
        .withColumnRenamed("ART_COD",           "ART_COD_INTERNO")
        .withColumnRenamed("ART_RADICE_COD",    "ART_RADICE")
        .withColumnRenamed("ART_VAR_LOGIS_COD", "ART_VAR")
        # Colonne V_STOCK → nomi business (dalla catena CDT_ESTR migrata)
        .withColumnRenamed("PZ_STOCK",           "QTA_PEZZI")
        .withColumnRenamed("QTA_UF_STOCK",       "QTA_UF")
        .withColumnRenamed("VAL_STOCK_MED_POND", "PREZZO_MEDIO_PONDERATO")
        .withColumnRenamed("DATA_SCAD_STOCK",    "DATA_MIN_SCADENZA")
        .withColumnRenamed("NUM_ETICH",          "NUM_ETICHETTA")
        .withColumnRenamed("FORN_COD",           "FORNITORE_COD")
        .withColumnRenamed("MAPPA_CORSIA",       "CORSIA")
        .withColumnRenamed("MAPPA_COL",          "COLONNA")
        .withColumnRenamed("MAPPA_PIANO",        "PIANO")
        .withColumnRenamed("MAPPA_LIV",          "LIVELLO")
        .withColumnRenamed("MAPPA_SERV_FLAG",    "FLAG_SERVIZIO")
        .withColumnRenamed("NUM_PICK",           "NUM_PICKING")
        .withColumnRenamed("VAL_STOCK_NET_ACQ",  "VAL_NETTO_ACQUISTO")
        .withColumnRenamed("VAL_STOCK_ULT_ACQ",  "VAL_ULTIMO_ACQUISTO")
        .withColumnRenamed("NUMERO_CAR",         "NUM_CARICO")
        .withColumnRenamed("NUMERO_ORDINE",      "NUM_ORDINE")
    )

    # ── Deduplica: ultima versione per chiave naturale (MAG_COD, ART_COD_INTERNO, DATA_FOTO) ──
    ts_col = "_silver_ts" if "_silver_ts" in renamed_df.columns else "_bronze_insert_ts"
    w = Window.partitionBy("MAG_COD", "ART_COD_INTERNO", "DATA_FOTO").orderBy(F.col(ts_col).desc())
    deduped_df = (
        renamed_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Cast tipi (Silver upstream e' gia' tipizzato ma cast espliciti per sicurezza) ──
    silver_df = (
        deduped_df
        .withColumn("DATA_FOTO",              F.col("DATA_FOTO").cast("date"))
        .withColumn("DATA_MIN_SCADENZA",      F.col("DATA_MIN_SCADENZA").cast("date"))
        .withColumn("QTA_PEZZI",              F.col("QTA_PEZZI").cast("decimal(14,3)"))
        .withColumn("QTA_UF",                 F.col("QTA_UF").cast("decimal(14,3)"))
        .withColumn("PREZZO_MEDIO_PONDERATO", F.col("PREZZO_MEDIO_PONDERATO").cast("decimal(16,4)"))
        .withColumn("VAL_NETTO_ACQUISTO",    F.col("VAL_NETTO_ACQUISTO").cast("decimal(16,4)"))
        .withColumn("VAL_ULTIMO_ACQUISTO",   F.col("VAL_ULTIMO_ACQUISTO").cast("decimal(16,4)"))
        .withColumn("_silver_ts",             F.current_timestamp())
    )

    rows_clean = silver_df.count()
    logger.info(f"Righe dopo deduplica: {rows_clean}")

    # Scrittura idempotente: dynamic partition overwrite per DATA_FOTO (snapshot giornaliero).
    # Ri-eseguire lo stesso giorno sovrascrive SOLO quella DATA_FOTO (no raddoppio), storico intatto.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.logistica_curated")
    (silver_df.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic")
     .option("mergeSchema", "true").partitionBy(PARTITION_COL)
     .saveAsTable(TARGET_TABLE))
    logger.info(f"SNAPSHOT (dyn overwrite) {TARGET_TABLE} ({rows_clean} righe per DATA_FOTO={run_date})")
    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
