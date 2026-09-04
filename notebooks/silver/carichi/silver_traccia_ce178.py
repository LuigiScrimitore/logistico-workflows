# Databricks notebook source
# Area: Carichi - Tracciabilità CE178
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Traccia CE178 Silver — sorgente bronze.logistica.tracciace178 (prefisso CE178_).
#              Filtra il Bronze per _bronze_load_date = run_date (delta del giorno), rinomina le
#              colonne Oracle reali (verificate da SOURCE_COLS del Bronze), cast espliciti,
#              converte CE178_ANNULLATO ('S'=true), deduplica su chiave naturale
#              (Window su _bronze_insert_ts DESC), MERGE INTO silver.logistica.traccia_ce178 (CTAS la prima volta).
#              NOTA: il Bronze TRACCIACE178 NON contiene QTA/COD_ARTICOLO/COD_FORNITORE/COD_OPERATORE/
#              COD_MAGAZZINO/COD_CELLA_DEST/ORA_SCAN/FLAG_STORNATA/NOTE: colonne inventate rimosse.
#              Articolo disponibile come CE178_COD_MSI; identificativo collo come CE178_SSCC.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, julian_to_date, normalize_sito, get_sito_alias_map

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

NOTEBOOK_NAME  = "silver_traccia_ce178"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.tracciace178"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.traccia_ce178"

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

    check_not_null(raw_df, ["MAG_SITO_COD", "CE178_NRO_ETICHETTA", "CE178_NRO_CARICO"], NOTEBOOK_NAME)
    check_row_count(raw_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    # ── Rinomina colonne Oracle reali → nomi business (solo colonne presenti in SOURCE_COLS) ──
    renamed_df = (
        raw_df
        .withColumn("MAG_SITO_COD", normalize_sito(F.col("MAG_SITO_COD"), _amap))
        .withColumnRenamed("MAG_SITO_COD",         "SITO_COD")
        .withColumnRenamed("CE178_NRO_ETICHETTA",  "ETICHET_NRO")
        .withColumnRenamed("CE178_NRO_CARICO",     "CARICO_NRO")
        .withColumnRenamed("CE178_SSCC",           "SSCC")
        .withColumnRenamed("CE178_COD_MSI",        "MSI_COD")
        .withColumnRenamed("CE178_NRO_ORDINE",     "ORDINE_NRO")
        .withColumnRenamed("CE178_NOME_UTENTE",    "UTENTE_NOME")
        .withColumnRenamed("CE178_DATA_CARICO",    "DATA_CARICO")
        .withColumnRenamed("CE178_DATA_INSERIMENTO","DATA_INSERIMENTO")
        .withColumnRenamed("CE178_DATA_MODIFICA",  "DATA_MODIFICA")
        .withColumnRenamed("CE178_ANNULLATO",      "FLG_ANNULLATO_RAW")
        .withColumnRenamed("CE178_TRASFERITO_STAT","FLG_TRASFERITO_STAT_RAW")
    )

    # ── Deduplica: ultima versione per chiave naturale ────────────────────────
    w = Window.partitionBy("SITO_COD", "ETICHET_NRO", "CARICO_NRO").orderBy(F.col("_bronze_insert_ts").desc())
    deduped_df = (
        renamed_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Cast tipi (Bronze è tutto StringType) ─────────────────────────────────
    silver_df = (
        deduped_df
        .withColumn("DATA_CARICO",      julian_to_date(F.col("DATA_CARICO")))
        .withColumn("DATA_INSERIMENTO", F.col("DATA_INSERIMENTO").cast("timestamp"))
        .withColumn("DATA_MODIFICA",    F.col("DATA_MODIFICA").cast("timestamp"))
        # ── Conversioni boolean ('S' → true) su colonne reali ────────────────
        .withColumn(
            "FLG_ANNULLATO",
            F.upper(F.trim(F.col("FLG_ANNULLATO_RAW"))) == F.lit("S")
        )
        .withColumn(
            "FLG_TRASFERITO_STAT",
            F.upper(F.trim(F.col("FLG_TRASFERITO_STAT_RAW"))) == F.lit("S")
        )
        .drop("FLG_ANNULLATO_RAW", "FLG_TRASFERITO_STAT_RAW")
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
                    "tgt.CARICO_NRO = src.CARICO_NRO"
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
