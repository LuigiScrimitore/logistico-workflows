# Databricks notebook source
# Area: Trasporti / Carichi
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Ordini Silver — gli "ordini" derivano dalle testate carico.
#              SORGENTE EFFETTIVA: bronze.logistica.sto_tes_carichi (STCAR_*), sistema Logistix
#              (vedi bronze_carichi_testate.py). Allineato alle COLONNE REALI STCAR_.
#              Chiave naturale: MAG_SITO_COD + STCAR_NRO_CARICO + STCAR_COD_MAGAZZINO.
#              MERGE INTO silver.logistica.ordine (CTAS prima volta).
#
# NOTA REVISIONE 3.0.0:
#  - Nel Bronze reale NON esiste STCAR_COD_STATO: non c'è una colonna di stato carico.
#    Lo stato "non chiuso" si approssima con STCAR_FLAG_TRASFERITO != 'S' (carico non
#    ancora trasferito a valle = pendente). Filtro segnalato come PROXY, da confermare.
#  - Non esistono STCAR_QTA_SPEDITA / STCAR_QTA_RICEVUTA: rimosse (erano inventate).
#  - Chiave naturale allineata a quella del Bronze (NRO_CARICO+COD_MAGAZZINO), NON include
#    STCAR_NRO_ORDINE che può essere null/multiplo per carico.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, julian_to_date, normalize_sito, get_sito_alias_map, read_watermark, update_watermark

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Full refresh")
dbutils.widgets.text("process_from", "", "Process from (override watermark)")

env                  = dbutils.widgets.get("env")
run_date             = dbutils.widgets.get("run_date")
full_refresh         = dbutils.widgets.get("full_refresh") == "true"
process_from_widget  = dbutils.widgets.get("process_from").strip()

# COMMAND ----------

NOTEBOOK_NAME  = "silver_ordini"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.sto_tes_carichi"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.ordine"

WM_STAGE, WM_SISTEMA, WM_TABELLA = "bronze_to_clean", "logistix", "ordini"

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    raw_df = spark.table(SOURCE_TABLE)
    incremental = (not full_refresh) and spark.catalog.tableExists(TARGET_TABLE)
    process_from = None
    if incremental:
        process_from = process_from_widget or read_watermark(spark, env, WM_STAGE, WM_SISTEMA, WM_TABELLA)
        if process_from:
            raw_df = raw_df.filter(F.col("_bronze_load_date") > F.lit(str(process_from)).cast("date"))
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} ({'INCREMENTALE >'+str(process_from) if incremental and process_from else 'FULL'}): {rows_read}")

    # PROXY stato: non esiste STCAR_COD_STATO nel Bronze reale.
    # Approssimazione "ordine/carico non chiuso" = non ancora trasferito a valle.
    raw_df = raw_df.filter(F.coalesce(F.col("STCAR_FLAG_TRASFERITO"), F.lit("")) != "S")
    rows_pendenti = raw_df.count()
    logger.info(f"Ordini pendenti (proxy FLAG_TRASFERITO != S): {rows_pendenti}")

    check_not_null(raw_df, ["MAG_SITO_COD", "STCAR_NRO_CARICO", "STCAR_COD_MAGAZZINO"], NOTEBOOK_NAME)

    # Deduplica su chiave naturale (allineata al Bronze testate carico)
    w = Window.partitionBy("MAG_SITO_COD", "STCAR_NRO_CARICO", "STCAR_COD_MAGAZZINO") \
              .orderBy(F.col("_bronze_insert_ts").desc())

    silver_df = (
        raw_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            normalize_sito(F.col("MAG_SITO_COD"), _amap).alias("SITO_COD"),
            F.col("STCAR_NRO_CARICO").cast("string").alias("CARICO_NRO"),
            F.col("STCAR_COD_MAGAZZINO").cast("string").alias("MAG_COD"),
            F.col("STCAR_NRO_ORDINE").cast("string").alias("ORDINE_NRO"),
            julian_to_date(F.col("STCAR_DATA_CARICO")).alias("DATA_CARICO"),
            F.col("STCAR_COD_FORNITORE").cast("string").alias("FORNITORE_COD"),
            F.col("STCAR_COD_CORRIERE").cast("string").alias("CORRIERE_COD"),
            F.col("STCAR_TIPO_ORDINE").cast("string").alias("TIPO_ORDINE"),
            F.col("STCAR_TIPO_CONSEGNA").cast("string").alias("TIPO_CONSEGNA"),
            julian_to_date(F.col("STCAR_DATA_EMISS_ORD")).alias("DATA_EMISS_ORDINE"),
            julian_to_date(F.col("STCAR_DATA_CONFE_ORD")).alias("DATA_CONFERMA_ORDINE"),
            F.col("STCAR_FLAG_TRASFERITO").cast("string").alias("FLAG_TRASFERITO"),
            F.col("_bronze_insert_ts"),
            F.col("_bronze_load_date"),
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
            "AND tgt.CARICO_NRO = src.CARICO_NRO "
            "AND tgt.MAG_COD = src.MAG_COD"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | pendenti={rows_pendenti} | righe_silver={rows_clean}")

    new_wm = silver_df.agg(F.max("_bronze_load_date")).collect()[0][0]
    if new_wm is not None:
        update_watermark(spark, env, WM_STAGE, WM_SISTEMA, WM_TABELLA,
                         last_processed_date=new_wm, rows_processed=rows_clean, esito="OK")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    try:
        update_watermark(spark, env, WM_STAGE, WM_SISTEMA, WM_TABELLA,
                         esito="FAIL", message=str(e)[:500])
    except Exception:
        pass
    raise
