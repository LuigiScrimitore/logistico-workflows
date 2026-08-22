# Databricks notebook source
# Area: Carichi
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Pesate Silver — sorgente bronze.logistica.pesate (prefisso PSP_).
#              Filtra il Bronze per _bronze_load_date = run_date (delta del giorno), rinomina le
#              colonne Oracle reali (verificate da SOURCE_COLS del Bronze), cast espliciti,
#              converte PSP_TRASFERITO ('S'=true), deduplica su chiave naturale
#              (Window su _bronze_insert_ts DESC), MERGE INTO silver.logistica.pesata (CTAS la prima volta).
#              NOTA: il Bronze PESATE NON contiene PESO_NETTO/DIFFERENZA/COD_OPERATORE/ORA_PESATA/
#              NUMEROBOLLA/CODART/DESCART/FLAG_STORNATA/CODBILANCIA/NOTE: colonne inventate rimosse.
#              Il peso disponibile e' PSP_PESOLORDO (peso lordo) e PSP_PESOMEDIO; non esiste un calcolo
#              di scarto kg sul Bronze reale → FLAG_SCARTO rimosso.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, normalize_sito, get_sito_alias_map

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_pesate"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.pesate"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.pesata"

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    # ── Leggi solo il delta del giorno dalla Bronze ───────────────────────────
    raw_df = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_bronze_load_date") == F.lit(run_date))
    )
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} per _bronze_load_date={run_date}: {rows_read}")

    check_not_null(raw_df, ["MAG_SITO_COD", "PSP_NUMETIC", "PSP_DATABOLLA"], NOTEBOOK_NAME)
    check_row_count(raw_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    # ── Rinomina colonne Oracle reali → nomi business (solo colonne presenti in SOURCE_COLS) ──
    renamed_df = (
        raw_df
        .withColumn("MAG_SITO_COD", normalize_sito(F.col("MAG_SITO_COD"), _amap))
        .withColumnRenamed("MAG_SITO_COD",      "SITO_COD")
        .withColumnRenamed("PSP_NUMETIC",       "ETICHET_NRO")
        .withColumnRenamed("PSP_DATABOLLA",     "DATA_BOLLA")
        .withColumnRenamed("PSP_NUMBOL",        "BOLLA_NRO")
        .withColumnRenamed("PSP_NRCARLOG",      "CARICO_LOG_NRO")
        .withColumnRenamed("PSP_SITO",          "SITO_SORGENTE")
        .withColumnRenamed("PSP_NCOM",          "COMMESSA_NRO")
        .withColumnRenamed("PSP_ARTEAN13",      "ART_EAN13")
        .withColumnRenamed("PSP_PESOLORDO",     "PESO_LORDO")
        .withColumnRenamed("PSP_PESOMEDIO",     "PESO_MEDIO")
        .withColumnRenamed("PSP_NRCOLLI",       "NRO_COLLI")
        .withColumnRenamed("PSP_PZXCART",       "PZ_PER_CARTONE")
        .withColumnRenamed("PSP_QTA_UF_RIL",    "QTA_UF_RILEVATA")
        .withColumnRenamed("PSP_CODCAUZPRINC",  "CAUZ_PRINC_COD")
        .withColumnRenamed("PSP_TARACAUZPRINC", "CAUZ_PRINC_TARA")
        .withColumnRenamed("PSP_CODCAUZSECOND", "CAUZ_SECOND_COD")
        .withColumnRenamed("PSP_TARACAUZSECOND","CAUZ_SECOND_TARA")
        .withColumnRenamed("PSP_CODCAUZFORN",   "CAUZ_FORN_COD")
        .withColumnRenamed("PSP_CODVUOADD",     "VUOTO_ADD_COD")
        .withColumnRenamed("PSP_LOCAZIONE",     "LOCAZIONE")
        .withColumnRenamed("PSP_DATA_SCADENZA", "DATA_SCADENZA")
        .withColumnRenamed("PSP_DATAINS",       "DATA_INSERIMENTO")
        .withColumnRenamed("PSP_TRASFERITO",    "FLG_TRASFERITO_RAW")
    )

    # ── Deduplica: ultima versione per chiave naturale ────────────────────────
    w = Window.partitionBy("SITO_COD", "ETICHET_NRO", "DATA_BOLLA").orderBy(F.col("_bronze_insert_ts").desc())
    deduped_df = (
        renamed_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Cast tipi (Bronze è tutto StringType) ─────────────────────────────────
    silver_df = (
        deduped_df
        .withColumn("DATA_BOLLA",       F.col("DATA_BOLLA").cast("date"))
        .withColumn("DATA_SCADENZA",    F.col("DATA_SCADENZA").cast("date"))
        .withColumn("DATA_INSERIMENTO", F.col("DATA_INSERIMENTO").cast("timestamp"))
        .withColumn("PESO_LORDO",       F.col("PESO_LORDO").cast("decimal(12,3)"))
        .withColumn("PESO_MEDIO",       F.col("PESO_MEDIO").cast("decimal(12,3)"))
        .withColumn("NRO_COLLI",        F.col("NRO_COLLI").cast("int"))
        .withColumn("PZ_PER_CARTONE",   F.col("PZ_PER_CARTONE").cast("int"))
        .withColumn("QTA_UF_RILEVATA",  F.col("QTA_UF_RILEVATA").cast("decimal(12,3)"))
        .withColumn("CAUZ_PRINC_TARA",  F.col("CAUZ_PRINC_TARA").cast("decimal(12,3)"))
        .withColumn("CAUZ_SECOND_TARA", F.col("CAUZ_SECOND_TARA").cast("decimal(12,3)"))
        # ── Conversione boolean PSP_TRASFERITO: 'S' → true ───────────────────
        .withColumn(
            "FLG_TRASFERITO",
            F.upper(F.trim(F.col("FLG_TRASFERITO_RAW"))) == F.lit("S")
        )
        .drop("FLG_TRASFERITO_RAW")
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_clean = silver_df.count()
    logger.info(f"Righe dopo deduplica: {rows_clean}")

    # ── MERGE INTO Silver (CTAS la prima volta) ───────────────────────────────
    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info(f"Creazione iniziale tabella {TARGET_TABLE}")
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(TARGET_TABLE)
        )
    else:
        delta_target = DeltaTable.forName(spark, TARGET_TABLE)
        (
            delta_target.alias("tgt")
            .merge(
                silver_df.alias("src"),
                (
                    "tgt.SITO_COD = src.SITO_COD AND "
                    "tgt.ETICHET_NRO = src.ETICHET_NRO AND "
                    "tgt.DATA_BOLLA = src.DATA_BOLLA"
                )
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
