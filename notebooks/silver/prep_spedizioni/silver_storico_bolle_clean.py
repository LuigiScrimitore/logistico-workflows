# Databricks notebook source
# Area: Preparazione Spedizioni
# Layer: Silver (cleansing)
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Cleansing 1:1 di bronze.logistica.storico_bolle (prefisso BOL_).
#              SOLO pulizia (nessuna business logic, nessun join, nessuna aggregazione):
#                - date Julian -> DateType (julian_to_date) su BOL_DATA_*
#                - normalize_sito su BOL_SITO
#                - trim sugli identificativi/flag
#              1 riga bronze -> 1 riga silver (idempotente). L'elaborazione UNICHE
#              (group by 8 chiavi, MAX su date/prezzi) e' a valle in silver_storico_bolle_uniche.
#              MODE: FULL_OVERWRITE del clean.
#              Riferimento: Linee guida §3; Revisione §3 (BOL_DATA_BOLLA_DATE era gia' cleansed in WL1).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, julian_to_date, normalize_sito, get_sito_alias_map, read_watermark, update_watermark

from pyspark.sql import functions as F
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

NOTEBOOK_NAME  = "silver_storico_bolle_clean"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.storico_bolle"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.storico_bolle_clean"

WM_STAGE, WM_SISTEMA, WM_TABELLA = "bronze_to_clean", "stat", "storico_bolle"

# Chiave naturale per il MERGE upsert incrementale (allineata ai merge_keys del bronze).
MERGE_KEYS = ["BOL_SITO", "BOL_NRO_BOLLA", "BOL_DATA_BOLLA", "BOL_NRO_RIGA"]

# Colonne data in JDN da convertire a DateType.
JULIAN_DATE_COLS = [
    "BOL_DATA_ORDIN_NEG",
    "BOL_DATA_BOLLA",
    "BOL_DATA_PARTENZA",
]

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
        dbutils.notebook.exit("NO_DATA")

    check_not_null(raw_df, ["BOL_SITO", "BOL_COD_MSI"], NOTEBOOK_NAME)

    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")

    df = raw_df

    # ── Normalizzazione sito ──────────────────────────────────────────────────
    df = df.withColumn("BOL_SITO", normalize_sito(F.col("BOL_SITO"), _amap))

    # ── Date Julian -> DateType ───────────────────────────────────────────────
    for c in JULIAN_DATE_COLS:
        if c in df.columns:
            df = df.withColumn(c, julian_to_date(F.col(c)))

    # ── Trim su tutte le colonne stringa di business ──────────────────────────
    business_str_cols = [
        f.name for f in df.schema.fields
        if f.dataType.simpleString() == "string" and not f.name.startswith("_")
    ]
    for c in business_str_cols:
        df = df.withColumn(c, F.trim(F.col(c)))

    silver_df = (
        df
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_silver_load_date", F.lit(run_date).cast("date"))
        # Dedup difensivo per chiave naturale -> sorgente univoca al MERGE (no "multiple
        # source rows matched"). Su bolle la chiave e' gia' univoca (no-op), ma robusto.
        .dropDuplicates(MERGE_KEYS)
    )

    rows_clean = silver_df.count()
    logger.info(f"Righe silver clean: {rows_clean} (dedup per chiave; lette={rows_read})")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    if incremental:
        # null-safe (<=>): NULL matcha NULL (robustezza su chiavi con null sporadici)
        cond = " AND ".join(f"tgt.{k} <=> src.{k}" for k in MERGE_KEYS)
        (DeltaTable.forName(spark, TARGET_TABLE).alias("tgt")
         .merge(silver_df.alias("src"), cond)
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        logger.info(f"MERGE upsert {TARGET_TABLE} (batch {run_date}, {rows_clean} righe)")
    else:
        (silver_df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))
        logger.info(f"FULL OVERWRITE {TARGET_TABLE} ({rows_clean} righe)")

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
