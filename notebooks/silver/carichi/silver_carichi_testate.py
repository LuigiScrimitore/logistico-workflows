# Databricks notebook source
# Area: Carichi
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Testate Carichi Silver — sorgente bronze.logistica.sto_tes_carichi (prefisso STCAR_).
#              Filtra il Bronze per _bronze_load_date = run_date (delta del giorno), rinomina le
#              colonne Oracle reali (verificate da SOURCE_COLS del Bronze), cast espliciti,
#              deduplica su chiave naturale (Window su _bronze_insert_ts DESC),
#              MERGE INTO silver.logistica.carico_testata (CTAS la prima volta), partizionato per DATA_CARICO.
#              NOTA: il Bronze STO_TES_CARICHI NON contiene quantita'/colli/stati a livello di testata
#              (no STCAR_QTA_SPEDITA/RICEVUTA/NUM_RIGHE/COLLI/STATO/DATA_CHIUSURA): tali colonne erano
#              inventate e sono state rimosse. SCARTO_QTA/FLAG_SCARTO si calcolano sul dettaglio (righe).

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

NOTEBOOK_NAME  = "silver_carichi_testate"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.sto_tes_carichi"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.carico_testata"
PARTITION_COL  = "DATA_CARICO"

WM_STAGE, WM_SISTEMA, WM_TABELLA = "bronze_to_clean", "logistix", "sto_tes_carichi"

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
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

    check_not_null(raw_df, ["MAG_SITO_COD", "STCAR_NRO_CARICO", "STCAR_COD_MAGAZZINO"], NOTEBOOK_NAME)
    check_row_count(raw_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")

    # ── Rinomina colonne Oracle reali → nomi business (solo colonne presenti in SOURCE_COLS) ──
    renamed_df = (
        raw_df
        .withColumn("MAG_SITO_COD", normalize_sito(F.col("MAG_SITO_COD"), _amap))
        .withColumnRenamed("MAG_SITO_COD",         "SITO_COD")
        .withColumnRenamed("STCAR_NRO_CARICO",     "CARICO_NRO")
        .withColumnRenamed("STCAR_COD_MAGAZZINO",  "MAG_COD")
        .withColumnRenamed("STCAR_COD_SEDE",       "SEDE_COD")
        .withColumnRenamed("STCAR_NRO_ORDINE",     "ORDINE_NRO")
        .withColumnRenamed("STCAR_COD_FORNITORE",  "FORNITORE_COD")
        .withColumnRenamed("STCAR_DIV_FORNITORE",  "FORNITORE_DIV")
        .withColumnRenamed("STCAR_DATA_CARICO",    "DATA_CARICO")
        .withColumnRenamed("STCAR_DATA_EMISS_ORD", "DATA_EMISSIONE_ORD")
        .withColumnRenamed("STCAR_DATA_CONFE_ORD", "DATA_CONFERMA_ORD")
        .withColumnRenamed("STCAR_TIPO_ORDINE",    "TIPO_ORDINE")
        .withColumnRenamed("STCAR_COD_GESTIONE",   "GESTIONE_COD")
        .withColumnRenamed("STCAR_TIPO_CONSEGNA",  "TIPO_CONSEGNA")
        .withColumnRenamed("STCAR_COD_CORRIERE",   "CORRIERE_COD")
        .withColumnRenamed("STCAR_TIPO_PAGAMENTO", "TIPO_PAGAMENTO")
        .withColumnRenamed("STCAR_TIPO_TRASPORTO", "TIPO_TRASPORTO")
        .withColumnRenamed("STCAR_NRO_SOLLECITI",  "NRO_SOLLECITI")
        .withColumnRenamed("STCAR_DATA_MODIFICA",  "DATA_MODIFICA")
        .withColumnRenamed("STCAR_FLAG_TRASFERITO","FLG_TRASFERITO_RAW")
        .withColumnRenamed("STCAR_COD_OPERATORE",  "OPERATORE_COD")
        .withColumnRenamed("STCAR_NOTE",           "NOTE")
        .withColumnRenamed("STCAR_NOTE_COMMERCIA", "NOTE_COMMERCIALE")
    )

    # ── Deduplica: ultima versione per chiave naturale ────────────────────────
    w = Window.partitionBy("SITO_COD", "CARICO_NRO", "MAG_COD").orderBy(F.col("_bronze_insert_ts").desc())
    deduped_df = (
        renamed_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Cast tipi (Bronze è tutto StringType) ─────────────────────────────────
    silver_df = (
        deduped_df
        # Date Julian Day legacy (NUMBER) -> date calendario (vedi julian_to_date)
        .withColumn("DATA_CARICO",        julian_to_date(F.col("DATA_CARICO")))
        .withColumn("DATA_EMISSIONE_ORD", julian_to_date(F.col("DATA_EMISSIONE_ORD")))
        .withColumn("DATA_CONFERMA_ORD",  julian_to_date(F.col("DATA_CONFERMA_ORD")))
        .withColumn("DATA_MODIFICA",      julian_to_date(F.col("DATA_MODIFICA")))
        .withColumn("NRO_SOLLECITI",      F.col("NRO_SOLLECITI").cast("int"))
        # ── Conversione boolean STCAR_FLAG_TRASFERITO: 'S' → true ────────────
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
            .partitionBy(PARTITION_COL)
            .saveAsTable(TARGET_TABLE)
        )
    else:
        delta_target = DeltaTable.forName(spark, TARGET_TABLE)
        (
            delta_target.alias("tgt")
            .merge(
                silver_df.alias("src"),
                "tgt.SITO_COD = src.SITO_COD AND tgt.CARICO_NRO = src.CARICO_NRO AND tgt.MAG_COD = src.MAG_COD"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

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
