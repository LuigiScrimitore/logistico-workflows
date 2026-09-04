# Databricks notebook source
# Area: Preparazione Spedizioni
# Layer: Silver (cleansing)
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Cleansing 1:1 di bronze.logistica.storico_liste (prefisso LSPRL_).
#              SOLO pulizia (nessuna business logic, nessun join, nessuna aggregazione):
#                - date Julian -> DateType (julian_to_date) sulle date di prelievo/ordine
#                - normalize_sito su LSPRL_SITO (alias TABGEN nro_tab=7)
#                - trim sugli identificativi/flag
#              1 riga bronze -> 1 riga silver (idempotente). L'elaborazione UNICHE
#              (group by 8 chiavi) e' a valle in silver_storico_liste_uniche.
#              MODE: FULL_OVERWRITE del clean (Bronze gia' delta-merged a monte).
#              Riferimento: Linee guida §3; Revisione §6.2.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import (get_catalog, julian_to_date, normalize_sito, get_sito_alias_map,
                   read_watermark, update_watermark)

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
# full_refresh=true: rilegge TUTTA la bronze (backfill/prima volta). Default: incrementale
# via WATERMARK (OP-35): processa il range _bronze_load_date > last_processed_date.
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Full refresh")
# process_from (YYYY-MM-DD): override esplicito del watermark (catch-up manuale). Vuoto = usa watermark.
dbutils.widgets.text("process_from", "", "Process from (override watermark)")

env                 = dbutils.widgets.get("env")
run_date            = dbutils.widgets.get("run_date")
full_refresh        = dbutils.widgets.get("full_refresh") == "true"
process_from_widget = dbutils.widgets.get("process_from").strip()

# COMMAND ----------

NOTEBOOK_NAME  = "silver_storico_liste_clean"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.storico_liste"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.storico_liste_clean"

# Chiave naturale (8 chiavi di prelievo) per il MERGE upsert incrementale.
MERGE_KEYS = ["LSPRL_SITO", "LSPRL_NRO_GABBIA", "LSPRL_NRO_ORDINE_NEG", "LSPRL_COD_NEGOZIO",
              "LSPRL_COD_MSI", "LSPRL_DATA_ORDIN_NEG", "LSPRL_SEQUE_PRELIEVO", "LSPRL_FLAG_SCARTATO"]

# Watermark incrementale (OP-35): stage bronze->clean. STAT non e' multi-sito -> sito _ALL_.
WM_STAGE, WM_SISTEMA, WM_TABELLA = "bronze_to_clean", "stat", "storico_liste"

# Colonne data in JDN (Julian Day Number) da convertire a DateType.
JULIAN_DATE_COLS = [
    "LSPRL_DATA_ORDIN_NEG",
    "LSPRL_DATA_INIZIO_PRELIEVO",
    "LSPRL_DATA_FINE_PRELIEVO",
]

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    raw_df = spark.table(SOURCE_TABLE)
    # Incrementale via WATERMARK (OP-35): processa il range _bronze_load_date > process_from
    # (catch-up multi-giorno in un solo run). full_refresh o target assente -> FULL.
    incremental = (not full_refresh) and spark.catalog.tableExists(TARGET_TABLE)
    process_from = None
    if incremental:
        # override esplicito (interim) ha precedenza sul watermark persistente
        process_from = process_from_widget or read_watermark(spark, env, WM_STAGE, WM_SISTEMA, WM_TABELLA)
        if process_from:
            raw_df = raw_df.filter(F.col("_bronze_load_date") > F.lit(str(process_from)).cast("date"))
    rows_read = raw_df.count()
    _mode = (f"INCREMENTALE > {process_from}" if (incremental and process_from)
             else "INCREMENTALE (no watermark: tutto)" if incremental else "FULL")
    logger.info(f"Righe lette da {SOURCE_TABLE} ({_mode}): {rows_read}")
    if rows_read == 0:
        logger.info("Nessuna riga nuova nel range: watermark invariato.")
        dbutils.notebook.exit("NO_DATA")

    check_not_null(raw_df, ["LSPRL_SITO", "LSPRL_COD_MSI"], NOTEBOOK_NAME)

    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")

    df = raw_df

    # ── Normalizzazione sito (canonico 2 cifre via TABGEN) ────────────────────
    df = df.withColumn("LSPRL_SITO", normalize_sito(F.col("LSPRL_SITO"), _amap))

    # ── Date Julian -> DateType ───────────────────────────────────────────────
    for c in JULIAN_DATE_COLS:
        if c in df.columns:
            df = df.withColumn(c, julian_to_date(F.col(c)))

    # ── Trim su tutte le colonne stringa di business (escludi metadati) ───────
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
        # Dedup per chiave naturale: la sorgente contiene duplicati di riga IDENTICI sulle
        # righe con LSPRL_SEQUE_PRELIEVO nullo (verificato: byte-identici). Necessario per
        # garantire sorgente univoca al MERGE (altrimenti "multiple source rows matched").
        .dropDuplicates(MERGE_KEYS)
    )

    rows_clean = silver_df.count()
    logger.info(f"Righe silver clean: {rows_clean} (dedup per chiave; lette={rows_read})")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    if incremental:
        # MERGE upsert sulla chiave naturale: aggiorna i variati, inserisce i nuovi.
        # null-safe (<=>): LSPRL_SEQUE_PRELIEVO e' null reale sul ~17% -> NULL matcha NULL
        cond = " AND ".join(f"tgt.{k} <=> src.{k}" for k in MERGE_KEYS)
        (DeltaTable.forName(spark, TARGET_TABLE).alias("tgt")
         .merge(silver_df.alias("src"), cond)
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        logger.info(f"MERGE upsert {TARGET_TABLE} ({rows_clean} righe, range > {process_from})")
    else:
        (silver_df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))
        logger.info(f"FULL OVERWRITE {TARGET_TABLE} ({rows_clean} righe)")

    # Watermark (OP-35): avanza alla MAX data bronze processata, SOLO dopo write riuscito.
    new_wm = silver_df.agg(F.max("_bronze_load_date")).collect()[0][0]
    if new_wm is not None:
        update_watermark(spark, env, WM_STAGE, WM_SISTEMA, WM_TABELLA,
                         last_processed_date=new_wm, rows_processed=rows_clean, esito="OK")
        logger.info(f"Watermark {WM_STAGE}/{WM_SISTEMA}/{WM_TABELLA} -> {new_wm}")

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    # NotebookExit (NO_DATA/NO_SOURCE) non e' un errore: ri-solleva senza marcare FAIL.
    if type(e).__name__ == "NotebookExit":
        raise
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    try:
        update_watermark(spark, env, WM_STAGE, WM_SISTEMA, WM_TABELLA,
                         esito="FAIL", message=str(e)[:500])
    except Exception:
        pass
    raise
