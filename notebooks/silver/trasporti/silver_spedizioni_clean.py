# Databricks notebook source
# Area: Trasporti (migrazione TO-BE)
# Layer: Silver (cleansing)
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Cleansing 1:1 di bronze.logistica.spedizioni (sorgente RAW SPEDIZIONI).
#              SOLO pulizia: date Julian/standard -> date, normalize_sito, trim, cast.
#              NESSUNA business logic (azioni/fill-down/cons-transito sono in silver_trasp_mtv_build).
#              Dedup tecnica: ultima versione per SP_ID (Window su _bronze_insert_ts DESC).
#              MODE = DELTA_MERGE (chiave SP_ID). Template stile: silver_carichi_dettagli.

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

NOTEBOOK_NAME  = "silver_spedizioni_clean"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.spedizioni"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.spedizioni_clean"

WM_STAGE, WM_SISTEMA, WM_TABELLA = "bronze_to_clean", "track", "spedizioni"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    raw_df = spark.table(SOURCE_TABLE)
    incremental = (not full_refresh) and spark.catalog.tableExists(TARGET_TABLE)
    process_from = None
    if incremental:
        process_from = process_from_widget or read_watermark(spark, env, WM_STAGE, WM_SISTEMA, WM_TABELLA)
        if process_from:
            raw_df = raw_df.filter(F.col("_bronze_load_date") > F.lit(str(process_from)).cast("date"))
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} ({'INCREMENTALE >'+str(process_from) if incremental and process_from else 'FULL'}): {rows_read}")
    if rows_read == 0:
        logger.warning("Nessuna riga da elaborare. Notebook terminato.")
        dbutils.notebook.exit("NO_DATA")

    check_not_null(raw_df, ["SP_ID"], NOTEBOOK_NAME)

    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")

    # ── Dedup tecnica: ultima versione per SP_ID ──────────────────────────────
    w = Window.partitionBy("SP_ID").orderBy(F.col("_bronze_insert_ts").desc())
    deduped = (raw_df.withColumn("_rn", F.row_number().over(w))
               .filter(F.col("_rn") == 1).drop("_rn"))

    silver_df = deduped

    # ── Normalizzazione siti (canonico 2 cifre) sui veri campi SP_* ───────────
    for sito_col in ["SP_MAGAZZINO", "SP_MAGTRANSITO"]:
        if sito_col in silver_df.columns:
            silver_df = silver_df.withColumn(sito_col, normalize_sito(F.col(sito_col), _amap))

    # ── Date -> DateType. SPEDIZIONI@TRACK le espone come date standard (YYYY-MM-DD),
    #    estratte come stringa: cast diretto. Nessuna Julian su questa sorgente.
    SP_DATE_COLS = [
        "SP_DATABOLLA", "SP_DATASPED", "SP_DATACONSPREV", "SP_DATASCARICO",
        "SP_DATAPRESACARICO", "SP_DATAVALIDAZIONE", "SP_DATAINVIOGOLD",
        "SP_DATAFATTURA", "SP_DATAFIRMAAUTISTA", "SP_DATAFIRMANEGOZIO",
        "SP_DATA_FLAG_ELABORATO_DWH",
    ]
    for dcol in SP_DATE_COLS:
        if dcol in silver_df.columns:
            silver_df = silver_df.withColumn(dcol, F.col(dcol).cast("date"))

    # ── Trim su tutte le stringhe di business (escludi metadati _*) ───────────
    for f in silver_df.schema.fields:
        if f.dataType.simpleString() == "string" and not f.name.startswith("_"):
            silver_df = silver_df.withColumn(f.name, F.trim(F.col(f.name)))

    silver_df = (silver_df
                 .withColumn("_silver_ts", F.current_timestamp())
                 .withColumn("_silver_load_date", F.lit(run_date).cast("date")))

    rows_clean = silver_df.count()
    logger.info(f"Righe silver (dedup per SP_ID): {rows_clean}")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info(f"Prima esecuzione — CTAS {TARGET_TABLE}")
        (silver_df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))
    else:
        (DeltaTable.forName(spark, TARGET_TABLE).alias("tgt")
         .merge(silver_df.alias("src"), "tgt.SP_ID = src.SP_ID")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        logger.info(f"MERGE INTO {TARGET_TABLE} completato")

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

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
