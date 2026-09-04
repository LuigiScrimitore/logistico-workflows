# Databricks notebook source
# Area: Preparazione Spedizioni
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Riepiloghi Preparazione Silver — lettura da bronze.logistica.storico_riepiloghi.
#              Sorgente STAT (OP-16): storico_riepiloghi NON proviene da Logistix ma dal sistema
#              STAT (path unico, NON multi-sito -> nessun _sito_cod nel Bronze).
#              Mapping prefisso RPLPR_* -> nomi business SOLO su colonne realmente presenti nel
#              Bronze (47 col, vedi SOURCE_SCHEMA di bronze_prep_riepiloghi.py). Cast espliciti,
#              deduplica Window su _bronze_insert_ts DESC, _silver_ts, MERGE INTO su chiave
#              naturale (CTAS prima esecuzione).
#              Chiave: RPLPR_SITO + RPLPR_NRO_RIEPILOGO + RPLPR_DATA_PREPARAZ.

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

NOTEBOOK_NAME  = "silver_prep_riepiloghi"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.storico_riepiloghi"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.prep_riepilogo"

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    # Bronze STAT (OP-16): MODE DELTA_MERGE -> filtra il delta del giorno corrente
    raw_df = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_bronze_load_date") == run_date)
    )
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} per run_date={run_date}: {rows_read}")

    check_not_null(raw_df, ["RPLPR_SITO", "RPLPR_NRO_RIEPILOGO", "RPLPR_DATA_PREPARAZ"], NOTEBOOK_NAME)

    # Deduplica su chiave naturale — record piu' recente per _bronze_insert_ts
    w = Window.partitionBy("RPLPR_SITO", "RPLPR_NRO_RIEPILOGO", "RPLPR_DATA_PREPARAZ") \
              .orderBy(F.col("_bronze_insert_ts").desc())

    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")

    # STAT bronze e' tutto StringType e puo' contenere junk non-numerico (es. 'S') nei campi
    # numerici: try_cast -> NULL sul malformato invece di far fallire il job (CAST_INVALID_INPUT).
    def _int(name):  return F.expr(f"try_cast(`{name}` as int)")
    def _dec(name):  return F.expr(f"try_cast(`{name}` as decimal(14,3))")

    silver_df = (
        raw_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        # Mapping RPLPR_* -> business SOLO su colonne reali (Bronze STAT, tutto StringType)
        .select(
            normalize_sito(F.col("RPLPR_SITO"), _amap).alias("SITO_COD"),
            F.col("RPLPR_COD_MAGAZZINO").cast("string").alias("MAGAZZINO_COD"),
            F.col("RPLPR_NRO_RIEPILOGO").cast("string").alias("RIEPILOGO_NRO"),
            julian_to_date(F.col("RPLPR_DATA_PREPARAZ")).alias("DATA_PREPARAZ"),
            F.col("RPLPR_COD_NEGOZIO").cast("string").alias("NEGOZIO_COD"),
            F.col("RPLPR_COD_REPAR_PREP").cast("string").alias("REPARTO_PREP_COD"),
            F.col("RPLPR_AREA_NEGOZIO").cast("string").alias("AREA_NEGOZIO"),
            F.col("RPLPR_COD_SETTOR_MAG").cast("string").alias("SETTORE_MAG_COD"),
            F.col("RPLPR_TIPO_RIEPILOGO").cast("string").alias("TIPO_RIEPILOGO"),
            F.col("RPLPR_PTY_SCHEDULAZI").cast("string").alias("PRIORITA_SCHEDULAZIONE"),
            F.col("RPLPR_COD_PREPARATOR").cast("string").alias("PREPARATORE_COD"),
            F.col("RPLPR_FLAG_ESEGUITO").cast("string").alias("FLAG_ESEGUITO"),
            julian_to_date(F.col("RPLPR_DATA_INIZ_PREP")).alias("DATA_INIZIO_PREP"),
            F.col("RPLPR_ORA_INIZ_PREP").cast("string").alias("ORA_INIZIO_PREP"),
            julian_to_date(F.col("RPLPR_DATA_FINE_PREP")).alias("DATA_FINE_PREP"),
            F.col("RPLPR_ORA_FINE_PREP").cast("string").alias("ORA_FINE_PREP"),
            _int("RPLPR_NRO_REFERENZE").alias("NUM_REFERENZE"),
            _dec("RPLPR_TOT_CARTONI").alias("TOT_CARTONI"),
            _dec("RPLPR_TOT_QUINTALI").alias("TOT_QUINTALI"),
            _int("RPLPR_NRO_PREPARATI").alias("NUM_PREPARATI"),
            _dec("RPLPR_TOT_CART_PREP").alias("TOT_CARTONI_PREP"),
            _dec("RPLPR_TOT_QUIN_PREP").alias("TOT_QUINTALI_PREP"),
            _int("RPLPR_GABBIE_PREPARA").alias("GABBIE_PREPARATE"),
            _dec("RPLPR_QTA_DA_EV_UDM").alias("QTA_DA_EVADERE_UDM"),
            F.col("RPLPR_FLAG_BOLLE").cast("string").alias("FLAG_BOLLE"),
            _int("RPLPR_NRO_INEVASI").alias("NUM_INEVASI"),
            _dec("RPLPR_TOT_CART_INEV").alias("TOT_CARTONI_INEVASI"),
            _dec("RPLPR_TOT_QUIN_INEV").alias("TOT_QUINTALI_INEVASI"),
            _int("RPLPR_GABBIE_TRATT").alias("GABBIE_TRATTATE"),
            F.col("RPLPR_NRO_GABBIA_MIN").cast("string").alias("NUM_GABBIA_MIN"),
            F.col("RPLPR_FLAG_INIZIATO").cast("string").alias("FLAG_INIZIATO"),
            F.col("RPLPR_FLAG_FINITO").cast("string").alias("FLAG_FINITO"),
            F.col("RPLPR_NRO_COMMISSIONE").cast("string").alias("COMMISSIONE_NRO"),
            F.col("RPLPR_NOME_UTENTE").cast("string").alias("NOME_UTENTE"),
            julian_to_date(F.col("RPLPR_DATA_MODIFICA")).alias("DATA_MODIFICA"),
            _int("RPLPR_PL_INT_GENERATI").alias("PALLET_INT_GENERATI"),
            F.col("RPLPR_PORTA_USCITA").cast("string").alias("PORTA_USCITA"),
            F.col("RPLPR_COD_ZONA_MAG").cast("string").alias("ZONA_MAG_COD"),
            F.col("RPLPR_GITA").cast("string").alias("GITA"),
            F.col("RPLPR_COD_AREA_MERCEOLOGICA").cast("string").alias("AREA_MERCEOLOGICA_COD"),
            _dec("RPLPR_VOLUME").alias("VOLUME"),
            F.col("RPLPR_NOME_TERMINALE").cast("string").alias("NOME_TERMINALE"),
            F.col("RPLPR_CARRELLO").cast("string").alias("CARRELLO"),
            julian_to_date(F.col("RPLPR_DATA_INSERIMENTO")).alias("DATA_INSERIMENTO"),
            F.col("RPLPR_TIPO_IMBALLO").cast("string").alias("TIPO_IMBALLO"),
            F.col("_bronze_insert_ts"),
            F.col("_bronze_load_date"),
        )
        # Calcoli Silver basati su colonne reali (cartoni/quintali preparati vs totali)
        .withColumn(
            "DELTA_CARTONI_INEVASI",
            (F.col("TOT_CARTONI") - F.col("TOT_CARTONI_PREP")).cast("decimal(14,3)")
        )
        .withColumn(
            "FLAG_COMPLETO",
            (F.col("NUM_INEVASI").isNull()) | (F.col("NUM_INEVASI") == F.lit(0))
        )
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_clean = silver_df.count()
    logger.info(f"Righe silver dopo deduplica: {rows_clean}")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info(f"Prima esecuzione — CTAS {TARGET_TABLE}")
        silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
    else:
        DeltaTable.forName(spark, TARGET_TABLE).alias("tgt").merge(
            silver_df.alias("src"),
            "tgt.SITO_COD = src.SITO_COD "
            "AND tgt.RIEPILOGO_NRO = src.RIEPILOGO_NRO "
            "AND tgt.DATA_PREPARAZ = src.DATA_PREPARAZ"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
